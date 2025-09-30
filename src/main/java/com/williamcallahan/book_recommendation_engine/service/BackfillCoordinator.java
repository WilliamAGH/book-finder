package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.mapper.GoogleBooksMapper;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Coordinates asynchronous backfill operations for book data from external APIs.
 * <p>
 * Responsibilities:
 * - Enqueue backfill tasks (idempotent via dedupe_key)
 * - Process queue with @Scheduled worker
 * - Call external APIs with rate limiting (Resilience4j)
 * - Map responses via provider-specific mappers
 * - Persist via BookUpsertService
 * - Update task status with retry logic
 * <p>
 * Queue management:
 * - Tasks are deduplicated by (source|sourceId)
 * - Priority: 1=highest (user-facing search), 10=lowest (background)
 * - Status: QUEUED → PROCESSING → COMPLETED/FAILED
 * - Max retries: 3 (configurable per task)
 * <p>
 * Example:
 * <pre>
 * // Enqueue high-priority task (user just searched)
 * backfillCoordinator.enqueue("GOOGLE_BOOKS", "abc123", 1);
 *
 * // Background processor picks it up and:
 * // 1. Fetches from Google Books API
 * // 2. Maps via GoogleBooksMapper
 * // 3. Upserts via BookUpsertService
 * // 4. Updates task status
 * </pre>
 * <p>
 * Rate limiting is handled via Resilience4j annotations (applied at method level).
 * See application.yml for rate limiter configuration.
 */
@Service
@Slf4j
@org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
    name = "app.features.async-backfill.enabled", 
    havingValue = "true", 
    matchIfMissing = false
)
public class BackfillCoordinator {
    
    private final JdbcTemplate jdbcTemplate;
    private final GoogleApiFetcher googleApiFetcher;
    private final GoogleBooksMapper googleBooksMapper;
    private final BookUpsertService bookUpsertService;
    
    // Default batch size for processing
    private static final int BATCH_SIZE = 10;
    
    // Default processing interval (5 seconds)
    private static final long PROCESS_INTERVAL_MS = 5000;
    
    public BackfillCoordinator(
        JdbcTemplate jdbcTemplate,
        GoogleApiFetcher googleApiFetcher,
        GoogleBooksMapper googleBooksMapper,
        BookUpsertService bookUpsertService
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.googleApiFetcher = googleApiFetcher;
        this.googleBooksMapper = googleBooksMapper;
        this.bookUpsertService = bookUpsertService;
        log.info("BackfillCoordinator initialized with async backfill enabled");
    }
    
    /**
     * Enqueue a backfill task with default priority (5).
     * <p>
     * Idempotent: duplicate (source, sourceId) pairs are ignored.
     *
     * @param source External provider name (e.g., 'GOOGLE_BOOKS')
     * @param sourceId Provider's book identifier
     */
    public void enqueue(String source, String sourceId) {
        enqueue(source, sourceId, 5);
    }
    
    /**
     * Enqueue a backfill task with specific priority.
     * <p>
     * Priority levels:
     * - 1-3: High priority (user-facing operations like search)
     * - 4-6: Medium priority (recommendations, related books)
     * - 7-10: Low priority (background enrichment)
     * <p>
     * Idempotent: duplicate (source, sourceId) pairs are ignored via unique constraint on dedupe_key.
     *
     * @param source External provider name
     * @param sourceId Provider's book identifier
     * @param priority Priority level (1=highest, 10=lowest)
     */
    public void enqueue(String source, String sourceId, int priority) {
        if (source == null || source.isBlank() || sourceId == null || sourceId.isBlank()) {
            log.warn("Cannot enqueue backfill task with null/blank source or sourceId");
            return;
        }
        
        String dedupeKey = source + "|" + sourceId;
        
        try {
            jdbcTemplate.update(
                """
                INSERT INTO backfill_tasks (source, source_id, dedupe_key, priority, created_at, updated_at)
                VALUES (?, ?, ?, ?, NOW(), NOW())
                ON CONFLICT (dedupe_key) DO NOTHING
                """,
                source,
                sourceId,
                dedupeKey,
                priority
            );
            
            log.debug("Enqueued backfill task: {} {} (priority={})", source, sourceId, priority);
        } catch (Exception e) {
            log.error("Error enqueuing backfill task: {} {}", source, sourceId, e);
        }
    }
    
    /**
     * Process queued backfill tasks.
     * <p>
     * Runs every 5 seconds via @Scheduled.
     * Processes up to 10 tasks per batch.
     * <p>
     * For each task:
     * 1. Mark as PROCESSING
     * 2. Fetch from external API
     * 3. Map to BookAggregate
     * 4. Upsert via BookUpsertService
     * 5. Mark as COMPLETED or FAILED
     * <p>
     * Rate limiting and circuit breaker logic is handled by:
     * - GoogleApiFetcher (has circuit breaker)
     * - Resilience4j (if configured)
     */
    @Scheduled(fixedDelay = PROCESS_INTERVAL_MS)
    @Async
    public void processQueue() {
        try {
            List<BackfillTask> tasks = fetchQueuedTasks(BATCH_SIZE);
            
            if (tasks.isEmpty()) {
                return;
            }
            
            log.info("Processing {} backfill tasks", tasks.size());
            
            for (BackfillTask task : tasks) {
                processTask(task);
            }
        } catch (Exception e) {
            log.error("Error in backfill queue processor", e);
        }
    }
    
    /**
     * Fetch queued tasks ordered by priority and age.
     */
    private List<BackfillTask> fetchQueuedTasks(int limit) {
        try {
            return jdbcTemplate.query(
                """
                SELECT id, source, source_id, attempts, max_attempts
                FROM backfill_tasks
                WHERE status = 'QUEUED' AND attempts < max_attempts
                ORDER BY priority ASC, created_at ASC
                LIMIT ?
                """,
                (rs, rowNum) -> new BackfillTask(
                    rs.getLong("id"),
                    rs.getString("source"),
                    rs.getString("source_id"),
                    rs.getInt("attempts"),
                    rs.getInt("max_attempts")
                ),
                limit
            );
        } catch (Exception e) {
            log.error("Error fetching queued tasks", e);
            return List.of();
        }
    }
    
    /**
     * Process a single backfill task.
     * <p>
     * This method orchestrates the entire backfill flow:
     * 1. Fetch from external API
     * 2. Map to BookAggregate
     * 3. Upsert to database
     * 4. Update task status
     * <p>
     * Rate limiting and bulkheading via Resilience4j ensures API stability.
     * Fallback logic re-queues tasks when rate limits are exceeded.
     */
    @RateLimiter(name = "googleBooksServiceRateLimiter", fallbackMethod = "processTaskFallback")
    @Bulkhead(name = "googleBooksServiceBulkhead", fallbackMethod = "processTaskFallback")
    private void processTask(BackfillTask task) {
        try {
            markProcessing(task.getId());
            
            log.info("Processing backfill task: {} {} (attempt {}/{})",
                task.getSource(),
                task.getSourceId(),
                task.getAttempts() + 1,
                task.getMaxAttempts()
            );
            
            // Step 1: Fetch from external API
            JsonNode json = fetchExternalData(task.getSource(), task.getSourceId());
            
            if (json == null) {
                markFailed(task.getId(), "API returned null response");
                return;
            }
            
            // Step 2: Map to BookAggregate
            BookAggregate aggregate = mapToAggregate(task.getSource(), json);
            
            if (aggregate == null) {
                markFailed(task.getId(), "Mapper returned null (invalid data)");
                return;
            }
            
            // Step 3: Upsert to database
            try {
                BookUpsertService.UpsertResult result = bookUpsertService.upsert(aggregate);
                
                log.info("Backfill completed: {} {} → book_id={}, slug='{}', isNew={}",
                    task.getSource(),
                    task.getSourceId(),
                    result.getBookId(),
                    result.getSlug(),
                    result.isNew()
                );
                
                // Step 4: Mark as completed
                markCompleted(task.getId());
            } catch (Exception e) {
                log.error("Error upserting book: {} {}", task.getSource(), task.getSourceId(), e);
                markFailed(task.getId(), "Upsert failed: " + e.getMessage());
            }
            
        } catch (Exception e) {
            log.error("Error processing backfill task: {} {}", task.getSource(), task.getSourceId(), e);
            markFailed(task.getId(), "Processing error: " + e.getMessage());
        }
    }
    
    /**
     * Fetch data from external API.
     * <p>
     * Currently only supports GOOGLE_BOOKS.
     * Future: Add support for OPEN_LIBRARY, AMAZON, etc.
     */
    private JsonNode fetchExternalData(String source, String sourceId) {
        return switch (source) {
            case "GOOGLE_BOOKS" -> fetchFromGoogleBooks(sourceId);
            default -> {
                log.warn("Unsupported source: {}", source);
                yield null;
            }
        };
    }
    
    /**
     * Fetch from Google Books API.
     * Uses reactive WebClient - block() to convert to synchronous.
     */
    private JsonNode fetchFromGoogleBooks(String volumeId) {
        try {
            Mono<JsonNode> result = googleApiFetcher.fetchVolumeByIdAuthenticated(volumeId);
            
            // Block with timeout (circuit breaker is in GoogleApiFetcher)
            JsonNode json = result.block();
            
            if (json == null) {
                log.debug("Google Books API returned empty for volume {}", volumeId);
            }
            
            return json;
        } catch (Exception e) {
            log.error("Error fetching from Google Books: {}", volumeId, e);
            return null;
        }
    }
    
    /**
     * Fallback method when rate limiter or bulkhead rejects task processing.
     * Leaves task in PROCESSING state temporarily - will be retried automatically.
     */
    @SuppressWarnings("unused")
    private void processTaskFallback(BackfillTask task, Throwable t) {
        log.warn("Backfill task {} {} rejected by rate limiter or bulkhead: {}. Task will retry in next cycle.",
            task.getSource(), task.getSourceId(), t.getMessage());
        // Mark as queued to retry later
        try {
            jdbcTemplate.update(
                "UPDATE backfill_tasks SET status = 'QUEUED', updated_at = NOW() WHERE id = ?",
                task.getId()
            );
        } catch (Exception e) {
            log.error("Error resetting task to QUEUED in fallback", e);
        }
    }
    
    /**
     * Map external JSON to BookAggregate using appropriate mapper.
     */
    private BookAggregate mapToAggregate(String source, JsonNode json) {
        return switch (source) {
            case "GOOGLE_BOOKS" -> googleBooksMapper.map(json);
            default -> {
                log.warn("No mapper for source: {}", source);
                yield null;
            }
        };
    }
    
    /**
     * Update task status to PROCESSING.
     */
    private void markProcessing(Long taskId) {
        try {
            jdbcTemplate.update(
                "UPDATE backfill_tasks SET status = 'PROCESSING', updated_at = NOW() WHERE id = ?",
                taskId
            );
        } catch (Exception e) {
            log.error("Error marking task {} as PROCESSING", taskId, e);
        }
    }
    
    /**
     * Update task status to COMPLETED.
     */
    private void markCompleted(Long taskId) {
        try {
            jdbcTemplate.update(
                """
                UPDATE backfill_tasks 
                SET status = 'COMPLETED', completed_at = NOW(), updated_at = NOW() 
                WHERE id = ?
                """,
                taskId
            );
        } catch (Exception e) {
            log.error("Error marking task {} as COMPLETED", taskId, e);
        }
    }
    
    /**
     * Update task status to FAILED and increment attempts.
     * If attempts < max_attempts, task will be retried later.
     */
    private void markFailed(Long taskId, String errorMessage) {
        try {
            jdbcTemplate.update(
                """
                UPDATE backfill_tasks 
                SET status = CASE 
                    WHEN attempts + 1 >= max_attempts THEN 'FAILED'
                    ELSE 'QUEUED'
                END,
                attempts = attempts + 1,
                error_message = ?,
                updated_at = NOW()
                WHERE id = ?
                """,
                errorMessage,
                taskId
            );
            
            log.debug("Marked task {} as failed: {}", taskId, errorMessage);
        } catch (Exception e) {
            log.error("Error marking task {} as FAILED", taskId, e);
        }
    }
    
    /**
     * Get queue statistics.
     * Useful for monitoring and admin dashboards.
     */
    public QueueStats getQueueStats() {
        try {
            return jdbcTemplate.queryForObject(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'QUEUED') as queued,
                    COUNT(*) FILTER (WHERE status = 'PROCESSING') as processing,
                    COUNT(*) FILTER (WHERE status = 'COMPLETED') as completed,
                    COUNT(*) FILTER (WHERE status = 'FAILED') as failed
                FROM backfill_tasks
                WHERE created_at > NOW() - INTERVAL '1 hour'
                """,
                (rs, rowNum) -> new QueueStats(
                    rs.getInt("queued"),
                    rs.getInt("processing"),
                    rs.getInt("completed"),
                    rs.getInt("failed")
                )
            );
        } catch (Exception e) {
            log.error("Error fetching queue stats", e);
            return new QueueStats(0, 0, 0, 0);
        }
    }
    
    /**
     * Retry all failed tasks.
     * Useful for manual intervention (e.g., after API outage).
     */
    public int retryFailedTasks() {
        try {
            return jdbcTemplate.update(
                """
                UPDATE backfill_tasks
                SET status = 'QUEUED', attempts = 0, error_message = NULL, updated_at = NOW()
                WHERE status = 'FAILED'
                """
            );
        } catch (Exception e) {
            log.error("Error retrying failed tasks", e);
            return 0;
        }
    }
    
    /**
     * Backfill task data.
     */
    @Value
    private static class BackfillTask {
        Long id;
        String source;
        String sourceId;
        int attempts;
        int maxAttempts;
    }
    
    /**
     * Queue statistics for monitoring.
     */
    public record QueueStats(
        int queued,
        int processing,
        int completed,
        int failed
    ) {}
}
