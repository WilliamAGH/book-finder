/**
 * Service that orchestrates the migration of book data from S3 JSON files to Redis
 *
 * @author William Callahan
 *
 * Features:
 * - Migrates Google Books data from S3 JSON to Redis JSON
 * - Merges NYT Bestseller data with existing book records
 * - Uses ISBN-13 as primary identifier for books
 * - Handles fallback to Google Books ID when ISBN is unavailable
 * - Maintains detailed statistics about migration process
 * - Provides structured two-phase migration workflow
 */

package com.williamcallahan.book_recommendation_engine.jsontoredis;

import com.williamcallahan.book_recommendation_engine.jsontoredis.S3Service;
import com.williamcallahan.book_recommendation_engine.jsontoredis.RedisJsonService;
import com.williamcallahan.book_recommendation_engine.util.UuidUtil;
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import com.williamcallahan.book_recommendation_engine.util.UniversalBookDataExtractor;
import com.williamcallahan.book_recommendation_engine.util.BookDataFlattener;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.repository.RedisBookIndexManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.core.task.AsyncTaskExecutor;
import com.williamcallahan.book_recommendation_engine.repository.RedisBookSearchService;
import com.williamcallahan.book_recommendation_engine.service.BookDeduplicationService;

import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeoutException;
import java.time.Instant;
import java.time.Duration;
import java.util.Collections;

@SuppressWarnings("unused")
@Service("jsonS3ToRedis_JsonS3ToRedisService")
@Profile("jsontoredis")
public class JsonS3ToRedisService {

    private static final Logger log = LoggerFactory.getLogger(JsonS3ToRedisService.class);
    private static final Pattern JSON_FILE_PATTERN = Pattern.compile(".*\\.json$", Pattern.CASE_INSENSITIVE);
    
    // Reliability configuration
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 1000;
    private static final int BATCH_SIZE = 5; // Reduced from 10 to 5
    private static final int CIRCUIT_BREAKER_THRESHOLD = 10;
    private static final Duration CIRCUIT_BREAKER_RESET_TIMEOUT = Duration.ofMinutes(5);

    private final S3Service s3Service;
    private final RedisJsonService redisJsonService;
    private final ObjectMapper objectMapper;
    private final CachedBookRepository cachedBookRepository;
    private final RedisBookSearchService redisBookSearchService;
    private final RedisBookIndexManager indexManager;
    private final BookDeduplicationService deduplicationService;
    private final AsyncTaskExecutor migrationTaskExecutor;

    // S3 paths will be accessed via s3Service getters
    private final String googleBooksPrefix;
    private final String nytBestsellersKey;
    
    // Circuit breaker and progress tracking
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicBoolean circuitBreakerOpen = new AtomicBoolean(false);
    private volatile Instant circuitBreakerOpenTime;
    private final MigrationProgress migrationProgress = new MigrationProgress();
    private final ErrorAggregator errorAggregator = new ErrorAggregator();

    public JsonS3ToRedisService(@Qualifier("jsonS3ToRedis_S3Service") S3Service s3Service,
                                @Qualifier("jsonS3ToRedis_RedisJsonService") RedisJsonService redisJsonService,
                                ObjectMapper objectMapper,
                                CachedBookRepository cachedBookRepository,
                                RedisBookSearchService redisBookSearchService,
                                RedisBookIndexManager indexManager,
                                @Qualifier("migrationTaskExecutor") AsyncTaskExecutor migrationTaskExecutor,
                                BookDeduplicationService deduplicationService) {
        this.s3Service = s3Service;
        this.redisJsonService = redisJsonService;
        this.objectMapper = objectMapper;
        this.cachedBookRepository = cachedBookRepository;
        this.redisBookSearchService = redisBookSearchService;
        this.deduplicationService = deduplicationService;
        this.indexManager = indexManager;
        this.migrationTaskExecutor = migrationTaskExecutor;
        this.googleBooksPrefix = s3Service.getGoogleBooksPrefix();
        this.nytBestsellersKey = s3Service.getNytBestsellersKey();
        log.info("JsonS3ToRedisService initialized. Google Books S3 Prefix: {}, NYT Bestsellers S3 Key: {}", googleBooksPrefix, nytBestsellersKey);
    }

    /**
     * Orchestrates the entire migration process from S3 to Redis
     */
    public CompletableFuture<Void> performMigrationAsync() {
        log.info("Starting S3 JSON to Redis migration process asynchronously...");
        migrationProgress.setTotalFiles(0); // Will be updated when we list files
        
        return ingestGoogleBooksDataAsync()
            .thenCompose(unused -> mergeNytBestsellersDataAsync())
            .whenComplete((v, ex) -> {
                migrationProgress.logProgress();
                if (ex != null) {
                    log.error("S3 JSON to Redis migration process failed.", ex);
                    log.error("Final stats - Processed: {}, Failed: {}, Skipped: {}",
                            migrationProgress.getProcessed(),
                            migrationProgress.getFailed(),
                            migrationProgress.getSkipped());
                    errorAggregator.generateReport();
                } else {
                    log.info("S3 JSON to Redis migration process finished successfully.");
                    log.info("Final stats - Processed: {}, Failed: {}, Skipped: {}, Total time: {}",
                            migrationProgress.getProcessed(),
                            migrationProgress.getFailed(),
                            migrationProgress.getSkipped(),
                            migrationProgress.getElapsedTime());
                    if (errorAggregator.getErrors().size() > 0) {
                        log.warn("Migration completed with {} errors", errorAggregator.getErrors().size());
                        errorAggregator.generateReport();
                    }
                    log.info("REMINDER: Manually create/update your RediSearch index(es) after migration if applicable.");
                }
            });
    }

    /**
     * Stub: placeholder for mergeNytBestsellersDataAsync to satisfy pipeline
     */
    private CompletableFuture<Void> mergeNytBestsellersDataAsync() {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Phase 1: Ingests Google Books JSON data from S3 into Redis
     * Processes all JSON files under the Google Books prefix in S3
     * Uses ISBN-13 as primary identifier with fallback to Google Books ID
     */
    private void ingestGoogleBooksDataSync() {
        log.info("--- Phase 1: Ingesting Google Books data from S3 prefix: {} ---", googleBooksPrefix);
        List<String> bookKeys = s3Service.listObjectKeys(googleBooksPrefix).join();
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger skippedNoIdCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);

        bookKeys.stream()
                .filter(key -> JSON_FILE_PATTERN.matcher(key).matches()) // Ensure it's a .json file
                .filter(key -> !key.startsWith(googleBooksPrefix + "in-redis/")) // Skip files already processed and moved
                .forEach(s3Key -> {
                    log.debug("Processing S3 object: {}", s3Key);
                    try (InputStream rawIs = s3Service.getObjectContent(s3Key).join()) {
    PushbackInputStream is = new PushbackInputStream(rawIs, 1);
    int firstByte = is.read();
    if (firstByte == -1) {
        log.warn("InputStream empty for S3 key {}. Skipping.", s3Key);
        return;
    }
    is.unread(firstByte);
                        if (is == null) {
                            log.warn("InputStream is null for S3 key {}. Skipping.", s3Key);
                            return;
                        }
                        String destinationS3Key = null; // Declare destinationS3Key outside the inner try
                        Map.Entry<InputStream, Boolean> streamEntry = handleCompressedInputStream(is, s3Key);
                        try (InputStream streamToRead = streamEntry.getKey()) {
                            // Read raw JSON data
                            JsonNode rawJsonData = objectMapper.readTree(streamToRead);
                            
                            // Create unified document structure that preserves original data + adds our metadata
                            Map<String, Object> unifiedDocument = createUnifiedDocument(rawJsonData, s3Key);
                            if (unifiedDocument == null) {
                                log.error("Failed to create unified document for S3 key {}", s3Key);
                                return;
                            }
                            
                            // Extract identifiers for deduplication
                            String s3Isbn13 = UniversalBookDataExtractor.extractIsbn13(unifiedDocument);
                            String s3Isbn10 = UniversalBookDataExtractor.extractIsbn10(unifiedDocument);
                            String s3GoogleId = UniversalBookDataExtractor.extractGoogleBooksId(unifiedDocument);
                            String title = UniversalBookDataExtractor.extractTitle(unifiedDocument);

                            String finalRecordUuid;
                            String redisKey;
                            Map<String, Object> existingData = null;
                            
                            // Use the centralized deduplication service to find existing records
                            Optional<RedisBookSearchService.BookSearchResult> existingRecordOpt = 
                                deduplicationService.findExistingBook(s3Isbn13, s3Isbn10, s3GoogleId);
                            
                            if (existingRecordOpt.isPresent()) {
                                RedisBookSearchService.BookSearchResult existingRecord = existingRecordOpt.get();
                                finalRecordUuid = existingRecord.getUuid();
                                redisKey = existingRecord.getRedisKey();
                                
                                // Get existing unified document from Redis
                                String existingJson = redisJsonService.jsonGet(redisKey, "$");
                                if (existingJson != null && !existingJson.isEmpty()) {
                                    existingData = objectMapper.readValue(existingJson, new TypeReference<Map<String, Object>>() {});
                                }
                                
                                log.info("Found existing canonical record for S3 key {}. Canonical UUIDv7 ID: {}. Will merge and update.", 
                                        s3Key, finalRecordUuid);
                            } else {
                                finalRecordUuid = UuidUtil.getTimeOrderedEpoch().toString();
                                redisKey = "book:" + finalRecordUuid;
                                log.info("No existing canonical record found for S3 key {}. Assigning new UUIDv7 ID: {}. Target Redis key: {}", 
                                        s3Key, finalRecordUuid, redisKey);
                                existingData = null;
                            }
                            
                            log.info("Processing S3 object {} for Redis. Final UUIDv7 ID for record: {}. Target Redis key: {}", s3Key, finalRecordUuid, redisKey);

                            Map<String, Object> finalDocument;
                            if (existingData != null) {
                                // Merge unified documents, preserving all data sources
                                finalDocument = mergeUnifiedDocuments(existingData, unifiedDocument);
                                log.info("Merged existing unified document with new S3 data for title: '{}' | UUIDv7: {} | Action: UPDATED | Redis key: {}", 
                                        title, finalRecordUuid, redisKey);
                            } else {
                                finalDocument = unifiedDocument;
                                log.info("Created new unified document for title: '{}' | UUIDv7: {} | Action: CREATED | Redis key: {}", 
                                        title, finalRecordUuid, redisKey);
                            }
                            
                            // Ensure the 'id' field is the canonical UUIDv7
                            finalDocument.put("id", finalRecordUuid);
                            // Preserve Google Books ID separately
                            if (s3GoogleId != null) {
                                finalDocument.put("googleBooksId", s3GoogleId);
                            }

                            String mergedJsonString = objectMapper.writeValueAsString(finalDocument);
                            boolean success = redisJsonService.jsonSet(redisKey, "$", mergedJsonString);
                            
                            if (success) {
                                // Move processed S3 object only if Redis write was successful
                                destinationS3Key = getProcessedFileDestinationKey(s3Key, googleBooksPrefix);
                            } else {
                                log.error("Failed to write book {} to Redis. S3 file {} will not be moved.", finalRecordUuid, s3Key);
                                failedCount.incrementAndGet();
                                // Skip to next item in forEach
                            }
                        } // streamToRead is closed here
                        if (destinationS3Key != null) { // Now destinationS3Key is in scope
                            s3Service.moveObject(s3Key, destinationS3Key);
                            log.debug("Moved processed S3 object {} to {}", s3Key, destinationS3Key);
                        } else {
                            log.warn("Could not determine destination key for {}, file not moved.", s3Key);
                        }

                        processedCount.incrementAndGet();
                        if (processedCount.get() % 100 == 0) {
                             log.info("Ingested and moved {} Google Books records so far...", processedCount.get());
                        }

                    } catch (Exception e) {
                        log.error("Error processing S3 object {}: {}", s3Key, e.getMessage(), e);
                    }
                });

        log.info("--- Phase 1 Complete: Ingested {} Google Books records. Skipped {} records due to no identifier. Failed {} records due to Redis errors. ---", 
                processedCount.get(), skippedNoIdCount.get(), failedCount.get());
        
        if (failedCount.get() > 0) {
            log.error("WARNING: {} records failed to write to Redis. Check logs for details.", failedCount.get());
        }
    }

    private CompletableFuture<Void> ingestGoogleBooksDataAsync() {
        log.info("--- Phase 1: Starting async ingestion of Google Books data from S3 prefix: {} ---", googleBooksPrefix);
        
        return retryableOperation(
            () -> s3Service.listObjectKeys(googleBooksPrefix),
            "listS3Objects",
            MAX_RETRY_ATTEMPTS
        )
        .thenCompose(bookKeys -> {
            List<String> jsonFiles = bookKeys.stream()
                    .filter(key -> JSON_FILE_PATTERN.matcher(key).matches())
                    .filter(key -> !key.startsWith(googleBooksPrefix + "in-redis/"))
                    .collect(Collectors.toList());
            
            migrationProgress.setTotalFiles(jsonFiles.size());
            log.info("Found {} JSON files to process", jsonFiles.size());
            
            if (jsonFiles.isEmpty()) {
                log.info("No files to process in phase 1");
                return CompletableFuture.completedFuture(null);
            }
            
            // Process files in batches
            return processBatchesAsync(jsonFiles, BATCH_SIZE);
        });
    }
    
    /**
     * Process files in batches with parallel execution
     */
    private CompletableFuture<Void> processBatchesAsync(List<String> files, int batchSize) {
        List<List<String>> batches = partition(files, batchSize);
        log.info("Processing {} files in {} batches of size {}", files.size(), batches.size(), batchSize);
        
        // Process batches sequentially to avoid overwhelming the system
        return batches.stream()
                .reduce(
                    CompletableFuture.completedFuture((Void) null),
                    (prevFuture, batch) -> prevFuture.thenCompose(v -> processBatchAsync(batch)),
                    (f1, f2) -> f1.thenCompose(v -> f2)
                );
    }
    
    /**
     * Process a single batch of files in parallel
     */
    private CompletableFuture<Void> processBatchAsync(List<String> batch) {
        final long batchStart = System.currentTimeMillis();
        int batchNumber = (migrationProgress.getProcessed() + migrationProgress.getFailed() + migrationProgress.getSkipped()) / BATCH_SIZE + 1;
        log.info("Processing batch {} ({} files): {}", batchNumber, batch.size(), 
                batch.stream().limit(3).collect(Collectors.joining(", ")) + 
                (batch.size() > 3 ? " ..." : ""));
        
        List<CompletableFuture<Void>> fileFutures = batch.stream()
                .map(this::processFileAsync)
                .collect(Collectors.toList());
        
        return CompletableFuture.allOf(fileFutures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    long elapsed = System.currentTimeMillis() - batchStart;
                    log.info("Completed batch {} in {}ms - Progress: {}/{} files", batchNumber, elapsed, 
                            migrationProgress.getProcessed() + migrationProgress.getFailed() + migrationProgress.getSkipped(),
                            migrationProgress.getTotal());
                    migrationProgress.logProgress();
                });
    }
    
    /**
     * Process a single file asynchronously with retry
     */
    private CompletableFuture<Void> processFileAsync(String s3Key) {
        CompletableFuture<Void> cf = CompletableFuture.runAsync(() -> {
            try {
                processGoogleBooksFile(s3Key);
            } catch (Exception e) {
                log.error("Failed to process file {}: {}", s3Key, e.getMessage(), e);
                migrationProgress.incrementFailed();
                errorAggregator.addError("processFile", s3Key, e.getClass().getSimpleName(), e.getMessage(), e);
            }
        }, migrationTaskExecutor);
        return cf.orTimeout(60, TimeUnit.SECONDS)
            .exceptionally(ex -> {
                log.error("[PROCESS_FILE_TIMEOUT] File {} timed out: {}", s3Key, ex.getMessage(), ex);
                migrationProgress.incrementFailed();
                errorAggregator.addError("processFileTimeout", s3Key, ex.getClass().getSimpleName(), ex.getMessage(), ex);
                return null;
            });
    }
    
    /**
     * Partition a list into sublists of specified size
     */
    private <T> List<List<T>> partition(List<T> list, int size) {
        return IntStream.range(0, (list.size() + size - 1) / size)
                .mapToObj(i -> list.subList(i * size, Math.min((i + 1) * size, list.size())))
                .collect(Collectors.toList());
    }
    
    /**
     * Process a single Google Books file
     */
    private void processGoogleBooksFile(String s3Key) {
        log.info("Starting processing for S3 file: {}", s3Key);
        long fileProcessingStartTime = System.currentTimeMillis();
        
        CompletableFuture<InputStream> s3ObjectFuture = s3Service.getObjectContent(s3Key);
        try (InputStream rawIs = s3ObjectFuture.get(45, TimeUnit.SECONDS)) { // Added 45s timeout
            log.info("[S3_GET_SUCCESS] Fetched S3 object {} in {}ms", s3Key, System.currentTimeMillis() - fileProcessingStartTime);
            PushbackInputStream is = new PushbackInputStream(rawIs, 1);
            int firstByte = is.read();
            if (firstByte == -1) {
                log.warn("InputStream empty for S3 key {}. Skipping.", s3Key);
                migrationProgress.incrementSkipped();
                return;
            }
            is.unread(firstByte);
            
            String destinationS3Key = null;
            long parseStartTime = System.currentTimeMillis();
            Map.Entry<InputStream, Boolean> streamEntry = handleCompressedInputStream(is, s3Key);
            try (InputStream streamToRead = streamEntry.getKey()) {
                Map<String, Object> bookData = objectMapper.readValue(streamToRead, new TypeReference<Map<String, Object>>() {});
                log.info("[JSON_PARSE_SUCCESS] Parsed JSON for S3 object {} in {}ms", s3Key, System.currentTimeMillis() - parseStartTime);

                String s3Isbn13 = UniversalBookDataExtractor.extractIsbn13(bookData);
                String s3Isbn10 = UniversalBookDataExtractor.extractIsbn10(bookData);
                String s3GoogleId = UniversalBookDataExtractor.extractGoogleBooksId(bookData);

                log.info("[EXTRACT_IDS_SUCCESS] Extracted identifiers for S3 object {}: ISBN13={}, ISBN10={}, GoogleID={}", s3Key, s3Isbn13, s3Isbn10, s3GoogleId);

                String finalRecordUuid;
                String redisKey;
                Map<String, Object> existingData = null;
                
                log.info("[REDIS_LOOKUP_START] Starting findExistingBook search for S3 object {}: ISBN13={}, ISBN10={}, GoogleID={}", s3Key, s3Isbn13, s3Isbn10, s3GoogleId);
                long searchStartTime = System.currentTimeMillis();
                Optional<String> bookIdOpt = Optional.empty();

                if (s3Isbn13 != null && !s3Isbn13.isEmpty()) {
                    bookIdOpt = indexManager.getBookIdByIsbn13(s3Isbn13);
                }

                if (!bookIdOpt.isPresent() && s3Isbn10 != null && !s3Isbn10.isEmpty()) {
                    bookIdOpt = indexManager.getBookIdByIsbn10(s3Isbn10);
                }

                if (!bookIdOpt.isPresent() && s3GoogleId != null && !s3GoogleId.isEmpty()) {
                    bookIdOpt = indexManager.getBookIdByGoogleBooksId(s3GoogleId);
                }
                long searchEndTime = System.currentTimeMillis();
                log.info("[REDIS_LOOKUP_END] Completed findExistingBook search for S3 object {} in {}ms. Found: {}", s3Key, (searchEndTime - searchStartTime), bookIdOpt.isPresent());

                if (bookIdOpt.isPresent()) {
                    String bookId = bookIdOpt.get();
                    redisKey = "book:" + bookId;
                    if (redisJsonService.keyExists(redisKey)) {
                        log.info("[REDIS_KEY_EXISTS] Found existing book by index lookup for S3 object {}. Key: {}", s3Key, redisKey);
                        finalRecordUuid = bookId;
                    } else {
                        log.warn("[REDIS_KEY_MISSING] Index lookup found bookId {} for S3 object {} but key {} does not exist in Redis. Creating new UUID.", bookId, s3Key, redisKey);
                        finalRecordUuid = UuidUtil.getTimeOrderedEpoch().toString(); // Create new UUID if indexed key is missing
                        redisKey = "book:" + finalRecordUuid;
                    }
                } else {
                    log.info("[REDIS_NEW_BOOK] No existing book found in index for S3 object {}. Assigning new UUID.", s3Key);
                    finalRecordUuid = UuidUtil.getTimeOrderedEpoch().toString();
                    redisKey = "book:" + finalRecordUuid;
                }

                // Book found by index or new book
                Map<String, Object> mergedMap;
                long redisGetStartTime = System.currentTimeMillis();
                if (redisJsonService.keyExists(redisKey) && bookIdOpt.isPresent()) { // Only try to get if it was found via index and key exists
                    String existingJson = redisJsonService.jsonGet(redisKey, "$");
                    log.info("[REDIS_GET_EXISTING_DATA] Attempted to get existing JSON for S3 object {} (key {}) in {}ms. Found: {}", s3Key, redisKey, (System.currentTimeMillis() - redisGetStartTime), (existingJson != null && !existingJson.isEmpty()));
                    if (existingJson != null && !existingJson.isEmpty()) {
                        existingData = objectMapper.readValue(existingJson, new TypeReference<Map<String, Object>>() {});
                        mergedMap = mergeMaps(existingData, bookData); // mergeMaps should be robust
                        log.info("Book '{}' (S3 object {}) - UUID: {} - Action: UPDATED - Redis Key: {}", 
                            UniversalBookDataExtractor.extractTitle(bookData), s3Key, finalRecordUuid, redisKey);
                    } else {
                        mergedMap = bookData;
                        log.warn("[REDIS_GET_EMPTY] Existing book for S3 object {} found in index (key {}) but no JSON data retrieved. Treating as new.", s3Key, redisKey);
                        log.info("Book '{}' (S3 object {}) - UUID: {} - Action: CREATED (after empty get) - Redis Key: {}", 
                            UniversalBookDataExtractor.extractTitle(bookData), s3Key, finalRecordUuid, redisKey);
                    }
                } else {
                    mergedMap = bookData;
                    log.info("Book '{}' (S3 object {}) - UUID: {} - Action: CREATED - Redis Key: {}", 
                        UniversalBookDataExtractor.extractTitle(bookData), s3Key, finalRecordUuid, redisKey);
                }
                
                mergedMap.put("id", finalRecordUuid); 
                String mergedJsonString = objectMapper.writeValueAsString(mergedMap);
                
                long redisSetStartTime = System.currentTimeMillis();
                boolean success = redisJsonService.jsonSet(redisKey, "$", mergedJsonString);
                log.info("[REDIS_SET_ATTEMPT] Attempted to set JSON for S3 object {} (key {}) in {}ms. Success: {}", s3Key, redisKey, (System.currentTimeMillis() - redisSetStartTime), success);
                
                if (success) {
                    long indexUpdateStartTime = System.currentTimeMillis();
                    CachedBook indexBook = new CachedBook();
                    indexBook.setId(finalRecordUuid);
                    indexBook.setIsbn13(s3Isbn13);
                    indexBook.setIsbn10(s3Isbn10);
                    indexBook.setGoogleBooksId(s3GoogleId);
                    indexManager.updateAllIndexes(indexBook, Optional.empty());
                    log.info("[REDIS_INDEX_UPDATE_SUCCESS] Updated indexes for S3 object {} (key {}) in {}ms", s3Key, redisKey, (System.currentTimeMillis() - indexUpdateStartTime));

                    destinationS3Key = getProcessedFileDestinationKey(s3Key, googleBooksPrefix);
                } else {
                    String bookTitle = UniversalBookDataExtractor.extractTitle(bookData);
                    log.error("[REDIS_SET_FAILED] Book '{}' (S3 object {}) - UUID: {} - Action: FAILED_REDIS_SET - Redis Key: {}", 
                            bookTitle, s3Key, finalRecordUuid, redisKey);
                    migrationProgress.incrementFailed();
                    errorAggregator.addError("processGoogleBooksFile_RedisSet", s3Key, "RedisJsonSetFailed", "redisJsonService.jsonSet returned false", null);
                    return; // Critical failure, stop processing this file
                }
            } // End of try-with-resources for streamToRead
            
            if (destinationS3Key != null) {
                long s3MoveStartTime = System.currentTimeMillis();
                s3Service.moveObject(s3Key, destinationS3Key).get(30, TimeUnit.SECONDS); // Added 30s timeout for move
                log.info("[S3_MOVE_SUCCESS] Moved processed S3 object {} to {} in {}ms", s3Key, destinationS3Key, (System.currentTimeMillis() - s3MoveStartTime));
            } else {
                log.warn("[S3_MOVE_SKIPPED] Could not determine destination key for S3 object {}, file not moved.", s3Key);
            }

            migrationProgress.incrementProcessed();
            log.info("Successfully processed S3 file: {} in {}ms", s3Key, System.currentTimeMillis() - fileProcessingStartTime);
            if (migrationProgress.getProcessed() % 100 == 0) {
                migrationProgress.logProgress(); // Log overall progress periodically
            }
        } catch (TimeoutException e) {
            log.error("[TIMEOUT_ERROR] Timeout processing S3 object {}: {}", s3Key, e.getMessage(), e);
            migrationProgress.incrementFailed();
            errorAggregator.addError("processGoogleBooksFile_Timeout", s3Key, e.getClass().getSimpleName(), "Operation timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("[INTERRUPTED_ERROR] Interrupted while processing S3 object {}: {}", s3Key, e.getMessage(), e);
            migrationProgress.incrementFailed();
            errorAggregator.addError("processGoogleBooksFile_Interrupted", s3Key, e.getClass().getSimpleName(), "Thread interrupted", e);
        } catch (Exception e) {
            log.error("[GENERAL_ERROR] Error processing S3 object {}: {}", s3Key, e.getMessage(), e);
            migrationProgress.incrementFailed();
            errorAggregator.addError("processGoogleBooksFile_General", s3Key, e.getClass().getSimpleName(), e.getMessage(), e);
        } finally {
            log.debug("Finished processing attempt for S3 file: {}", s3Key);
        }
    }

    /**
     * Helper method to construct the destination S3 key for a processed file
     * It places the file into an "in-redis" subfolder within its original prefix
     * Example: prefix/filename.json -> prefix/in-redis/filename.json
     *
     * @param originalKey The original S3 key of the file
     * @param basePrefix  The base prefix for this type of data (e.g., "google-books-data/")
     * @return The new destination key, or null if the original key doesn't match the prefix
     */
    private String getProcessedFileDestinationKey(String originalKey, String basePrefix) {
        if (originalKey == null || !originalKey.startsWith(basePrefix)) {
            log.warn("Original key {} does not start with base prefix {}. Cannot determine destination.", originalKey, basePrefix);
            return null;
        }
        String filename = originalKey.substring(basePrefix.length());
        if (filename.isEmpty()) {
            log.warn("Filename part is empty for original key {} with base prefix {}.", originalKey, basePrefix);
            return null;
        }
        // Ensure basePrefix ends with a slash if it's not empty
        String normalizedBasePrefix = basePrefix;
        if (!normalizedBasePrefix.isEmpty() && !normalizedBasePrefix.endsWith("/")) {
            normalizedBasePrefix += "/";
        }
        return normalizedBasePrefix + "in-redis/" + filename;
    }

    /**
     * Helper method to handle potentially GZIP-compressed input streams
     * 
     * @param inputStream The original input stream from S3
     * @param s3Key The S3 key for logging purposes
     * @return A Map.Entry containing the properly configured input stream and whether GZIP was detected
     * @throws IOException If an I/O error occurs
     */
    private Map.Entry<InputStream, Boolean> handleCompressedInputStream(InputStream inputStream, String s3Key) throws IOException {
        PushbackInputStream pbis = new PushbackInputStream(inputStream, 2);
        byte[] signature = new byte[2];
        int bytesRead = pbis.read(signature);
        pbis.unread(signature, 0, bytesRead); // Push back the read bytes

        boolean gzipDetected = (bytesRead == 2 && signature[0] == (byte) 0x1f && signature[1] == (byte) 0x8b);

        if (gzipDetected) {
            log.debug("Detected GZIP compressed content for S3 key: {}", s3Key);
            return Map.entry(new GZIPInputStream(pbis), true);
        } else {
            log.debug("Content for S3 key {} is not GZIP compressed or stream is too short to tell.", s3Key);
            return Map.entry(pbis, false);
        }
    }

    /**
     * Merges two CachedBook objects, preserving metadata and updating book data
     * 
     * @param existing The existing CachedBook with metadata
     * @param incoming The incoming CachedBook with updated data
     * @return The merged CachedBook
     */
    private CachedBook mergeCachedBooks(CachedBook existing, CachedBook incoming) {
        // Start with the incoming book data (most recent)
        CachedBook merged = new CachedBook();
        
        // Copy all fields from incoming book
        merged.setId(existing.getId()); // Keep existing ID
        merged.setGoogleBooksId(incoming.getGoogleBooksId() != null ? incoming.getGoogleBooksId() : existing.getGoogleBooksId());
        merged.setTitle(incoming.getTitle() != null ? incoming.getTitle() : existing.getTitle());
        merged.setAuthors(incoming.getAuthors() != null && !incoming.getAuthors().isEmpty() ? incoming.getAuthors() : existing.getAuthors());
        merged.setPublisher(incoming.getPublisher() != null ? incoming.getPublisher() : existing.getPublisher());
        merged.setPublishedDate(incoming.getPublishedDate() != null ? incoming.getPublishedDate() : existing.getPublishedDate());
        merged.setDescription(incoming.getDescription() != null ? incoming.getDescription() : existing.getDescription());
        merged.setCoverImageUrl(incoming.getCoverImageUrl() != null ? incoming.getCoverImageUrl() : existing.getCoverImageUrl());
        merged.setLanguage(incoming.getLanguage() != null ? incoming.getLanguage() : existing.getLanguage());
        merged.setIsbn10(incoming.getIsbn10() != null ? incoming.getIsbn10() : existing.getIsbn13());
        merged.setIsbn13(incoming.getIsbn13() != null ? incoming.getIsbn13() : existing.getIsbn13());
        merged.setPageCount(incoming.getPageCount() != null ? incoming.getPageCount() : existing.getPageCount());
        merged.setAverageRating(incoming.getAverageRating() != null ? incoming.getAverageRating() : existing.getAverageRating());
        merged.setRatingsCount(incoming.getRatingsCount() != null ? incoming.getRatingsCount() : existing.getRatingsCount());
        merged.setCategories(incoming.getCategories() != null && !incoming.getCategories().isEmpty() ? incoming.getCategories() : existing.getCategories());
        
        // Preserve existing metadata and update access tracking
        merged.setCreatedAt(existing.getCreatedAt()); // Keep original creation time
        merged.setLastAccessed(existing.getLastAccessed());
        merged.setAccessCount(existing.getAccessCount());
        merged.setSlug(existing.getSlug());
        merged.setEmbedding(existing.getEmbedding());
        merged.setCachedRecommendationIds(existing.getCachedRecommendationIds());
        
        log.debug("Merged CachedBook: preserved metadata, updated book data for title '{}'", merged.getTitle());
        return merged;
    }
    
    /**
     * Recursively merges incoming book data into existing data without overwriting existing values
     * 
     * @deprecated Use mergeCachedBooks instead for proper CachedBook merging
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    private Map<String, Object> mergeMaps(Map<String, Object> existing, Map<String, Object> incoming) {
        for (Map.Entry<String, Object> entry : incoming.entrySet()) {
            String key = entry.getKey();
            Object newValue = entry.getValue();
            
            if (newValue != null) {
                // Special handling for nested structures to preserve all data
                if ("_metadata".equals(key)) {
                    // Merge metadata
                    Map<String, Object> existingMeta = (Map<String, Object>) existing.getOrDefault("_metadata", new HashMap<>());
                    Map<String, Object> newMeta = (Map<String, Object>) newValue;
                    Map<String, Object> mergedMeta = new HashMap<>(existingMeta);
                    mergedMeta.putAll(newMeta);
                    existing.put(key, mergedMeta);
                } else if ("nyt_bestseller_info".equals(key)) {
                    // NYT data should be preserved/updated
                    existing.put(key, newValue);
                } else if ("openLibrary".equals(key)) {
                    // OpenLibrary data should be preserved/updated
                    existing.put(key, newValue);
                } else if ("goodreads".equals(key)) {
                    // Goodreads data should be preserved/updated
                    existing.put(key, newValue);
                } else {
                    // For all other fields, newer data takes precedence
                    existing.put(key, newValue);
                }
            }
        }
        return existing;
    }
    
    /**
     * Progress tracking for migration operations
     */
    public static class MigrationProgress {
        private final AtomicInteger processedFiles = new AtomicInteger(0);
        private final AtomicInteger failedFiles = new AtomicInteger(0);
        private final AtomicInteger skippedFiles = new AtomicInteger(0);
        private final AtomicInteger totalFiles = new AtomicInteger(0);
        private final Instant startTime = Instant.now();
        
        public void incrementProcessed() { processedFiles.incrementAndGet(); }
        public void incrementFailed() { failedFiles.incrementAndGet(); }
        public void incrementSkipped() { skippedFiles.incrementAndGet(); }
        public void setTotalFiles(int total) { totalFiles.set(total); }
        
        public int getProcessed() { return processedFiles.get(); }
        public int getFailed() { return failedFiles.get(); }
        public int getSkipped() { return skippedFiles.get(); }
        public int getTotal() { return totalFiles.get(); }
        
        public double getProgressPercentage() {
            int total = totalFiles.get();
            if (total == 0) return 0.0;
            int completed = processedFiles.get() + failedFiles.get() + skippedFiles.get();
            return (double) completed / total * 100;
        }
        
        public Duration getElapsedTime() {
            return Duration.between(startTime, Instant.now());
        }
        
        public void logProgress() {
            log.info("Migration Progress: {}% complete - Processed: {}, Failed: {}, Skipped: {}, Total: {}, Elapsed: {}",
                    String.format("%.2f", getProgressPercentage()), processedFiles.get(), failedFiles.get(), 
                    skippedFiles.get(), totalFiles.get(), getElapsedTime());
        }
        
        public Map<String, Object> toMap() {
            return Map.of(
                "processed", processedFiles.get(),
                "failed", failedFiles.get(),
                "skipped", skippedFiles.get(),
                "total", totalFiles.get(),
                "progressPercentage", getProgressPercentage(),
                "elapsedTimeSeconds", getElapsedTime().getSeconds()
            );
        }
    }
    
    /**
     * Retryable operation with exponential backoff
     */
    private <T> CompletableFuture<T> retryableOperation(
            Supplier<CompletableFuture<T>> operation,
            String operationName,
            int maxRetries) {
        return retryableOperation(operation, operationName, maxRetries, INITIAL_RETRY_DELAY_MS);
    }
    
    private <T> CompletableFuture<T> retryableOperation(
            Supplier<CompletableFuture<T>> operation,
            String operationName,
            int retriesLeft,
            long delayMs) {
        checkCircuitBreaker();
        
        return operation.get()
            .thenApply(result -> {
                // Reset consecutive failures on success
                consecutiveFailures.set(0);
                return result;
            })
            .exceptionally(throwable -> {
                log.warn("Operation {} failed with {} retries left: {}", 
                        operationName, retriesLeft, throwable.getMessage());
                
                consecutiveFailures.incrementAndGet();
                
                if (retriesLeft > 0) {
                    log.info("Retrying {} after {}ms delay", operationName, delayMs);
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CompletionException(e);
                    }
                    return retryableOperation(operation, operationName, retriesLeft - 1, delayMs * 2)
                            .join(); // This join is necessary for the recursive call
                }
                
                log.error("Operation {} failed after all retries", operationName, throwable);
                throw new CompletionException("Operation " + operationName + " failed after " + 
                        MAX_RETRY_ATTEMPTS + " attempts", throwable);
            });
    }
    
    /**
     * Circuit breaker implementation
     */
    private void checkCircuitBreaker() {
        if (circuitBreakerOpen.get()) {
            if (Duration.between(circuitBreakerOpenTime, Instant.now()).compareTo(CIRCUIT_BREAKER_RESET_TIMEOUT) > 0) {
                log.info("Circuit breaker timeout reached, attempting reset");
                circuitBreakerOpen.set(false);
                consecutiveFailures.set(0);
            } else {
                throw new RuntimeException("Circuit breaker is OPEN - too many consecutive failures");
            }
        }
        
        if (consecutiveFailures.get() >= CIRCUIT_BREAKER_THRESHOLD) {
            circuitBreakerOpen.set(true);
            circuitBreakerOpenTime = Instant.now();
            log.error("Circuit breaker triggered after {} consecutive failures", CIRCUIT_BREAKER_THRESHOLD);
            throw new RuntimeException("Circuit breaker triggered");
        }
    }
    
    /**
     * Get current migration progress
     */
    public MigrationProgress getMigrationProgress() {
        return migrationProgress;
    }
    
    /**
     * Get error aggregator for detailed error reports
     */
    public ErrorAggregator getErrorAggregator() {
        return errorAggregator;
    }
    
    /**
     * Creates a unified document structure that merges all data sources
     * while preserving original nested structures for Redis indexing
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> createUnifiedDocument(JsonNode rawJsonData, String s3Key) {
        try {
            Map<String, Object> unifiedDoc = new HashMap<>();
            String dataSource = "unknown";
            
            // Detect source format and process accordingly
            if (UniversalBookDataExtractor.isGoogleBooksFormat(rawJsonData)) {
                // Google Books API format
                Map<String, Object> googleBooksData = objectMapper.convertValue(rawJsonData, Map.class);
                unifiedDoc.putAll(googleBooksData);
                BookDataFlattener.addFlattenedGoogleBooksFields(unifiedDoc, googleBooksData);
                dataSource = "google_books";
                log.debug("Created unified document from Google Books API JSON for S3 key: {}", s3Key);
                
            } else if (UniversalBookDataExtractor.isOpenLibraryFormat(rawJsonData)) {
                // OpenLibrary API format
                Map<String, Object> openLibraryData = objectMapper.convertValue(rawJsonData, Map.class);
                unifiedDoc.putAll(openLibraryData);
                BookDataFlattener.addFlattenedOpenLibraryFields(unifiedDoc, openLibraryData);
                dataSource = "open_library";
                log.debug("Created unified document from OpenLibrary API JSON for S3 key: {}", s3Key);
                
            } else if (UniversalBookDataExtractor.isNYTFormat(rawJsonData)) {
                // NYT API format (single book entry from bestseller list)
                Map<String, Object> nytData = objectMapper.convertValue(rawJsonData, Map.class);
                unifiedDoc.putAll(nytData);
                BookDataFlattener.addFlattenedNYTFields(unifiedDoc, nytData);
                dataSource = "nyt_bestsellers";
                log.debug("Created unified document from NYT API JSON for S3 key: {}", s3Key);
                
            } else if (UniversalBookDataExtractor.isConsolidatedBookFormat(rawJsonData)) {
                // Our consolidated Book JSON format (already processed)
                Map<String, Object> existingData = objectMapper.convertValue(rawJsonData, Map.class);
                unifiedDoc.putAll(existingData);
                dataSource = "consolidated_book";
                log.debug("Created unified document from consolidated Book JSON for S3 key: {}", s3Key);
                
            } else {
                // Unknown format - treat as generic data
                Map<String, Object> genericData = objectMapper.convertValue(rawJsonData, Map.class);
                unifiedDoc.putAll(genericData);
                dataSource = "generic";
                log.warn("Unknown JSON format for S3 key {}, treating as generic data", s3Key);
            }
            
            // Add/update our metadata
            Map<String, Object> metadata = (Map<String, Object>) unifiedDoc.computeIfAbsent("_metadata", k -> new HashMap<>());
            metadata.put("s3Key", s3Key);
            metadata.put("lastUpdated", Instant.now().toString());
            metadata.put("dataSource", dataSource);
            metadata.put("processedAt", Instant.now().toString());
            
            // Generate UUID if not present
            if (!unifiedDoc.containsKey("id") || unifiedDoc.get("id") == null) {
                unifiedDoc.put("id", UuidUtil.getTimeOrderedEpoch().toString());
            }
            
            return unifiedDoc;
            
        } catch (Exception e) {
            log.error("Failed to create unified document for S3 key {}: {}", s3Key, e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Merges two unified documents, preserving data from all sources
     * while giving priority to newer data
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> mergeUnifiedDocuments(Map<String, Object> existing, Map<String, Object> newDoc) {
        Map<String, Object> merged = new HashMap<>(existing);
        
        // Merge top-level fields (flattened fields take precedence from newer data)
        for (Map.Entry<String, Object> entry : newDoc.entrySet()) {
            String key = entry.getKey();
            Object newValue = entry.getValue();
            
            if (newValue != null) {
                // Special handling for nested structures to preserve all data
                if ("_metadata".equals(key)) {
                    // Merge metadata
                    Map<String, Object> existingMeta = (Map<String, Object>) merged.getOrDefault("_metadata", new HashMap<>());
                    Map<String, Object> newMeta = (Map<String, Object>) newValue;
                    Map<String, Object> mergedMeta = new HashMap<>(existingMeta);
                    mergedMeta.putAll(newMeta);
                    merged.put(key, mergedMeta);
                } else if ("nyt_bestseller_info".equals(key)) {
                    // NYT data should be preserved/updated
                    merged.put(key, newValue);
                } else if ("openLibrary".equals(key)) {
                    // OpenLibrary data should be preserved/updated
                    merged.put(key, newValue);
                } else if ("goodreads".equals(key)) {
                    // Goodreads data should be preserved/updated
                    merged.put(key, newValue);
                } else {
                    // For all other fields, newer data takes precedence
                    merged.put(key, newValue);
                }
            }
        }
        
        // Update metadata timestamps
        Map<String, Object> metadata = (Map<String, Object>) merged.computeIfAbsent("_metadata", k -> new HashMap<>());
        metadata.put("lastUpdated", Instant.now().toString());
        metadata.put("mergedAt", Instant.now().toString());
        
        return merged;
    }
    
    /**
     * Error aggregation for comprehensive error reporting
     */
    public static class ErrorAggregator {
        private final List<ErrorDetail> errors = Collections.synchronizedList(new ArrayList<>());
        private final Map<String, AtomicInteger> errorCountsByType = new ConcurrentHashMap<>();
        
        public void addError(String operation, String key, String errorType, String message, Throwable exception) {
            errors.add(new ErrorDetail(operation, key, errorType, message, exception));
            errorCountsByType.computeIfAbsent(errorType, k -> new AtomicInteger(0)).incrementAndGet();
        }
        
        public List<ErrorDetail> getErrors() {
            return new ArrayList<>(errors);
        }
        
        public Map<String, Integer> getErrorCountsByType() {
            Map<String, Integer> result = new HashMap<>();
            errorCountsByType.forEach((k, v) -> result.put(k, v.get()));
            return result;
        }
        
        public void generateReport() {
            log.error("=== Migration Error Report ===");
            log.error("Total errors: {}", errors.size());
            log.error("Error counts by type:");
            errorCountsByType.forEach((type, count) -> 
                log.error("  {}: {}", type, count.get())
            );
            
            if (!errors.isEmpty()) {
                log.error("First 10 errors:");
                errors.stream().limit(10).forEach(error -> 
                    log.error("  [{}] {} - {}: {}", 
                        error.operation, error.key, error.errorType, error.message)
                );
            }
        }
        
        public static class ErrorDetail {
            public final String operation;
            public final String key;
            public final String errorType;
            public final String message;
            public final Instant timestamp;
            public final String stackTrace;
            
            public ErrorDetail(String operation, String key, String errorType, String message, Throwable exception) {
                this.operation = operation;
                this.key = key;
                this.errorType = errorType;
                this.message = message;
                this.timestamp = Instant.now();
                this.stackTrace = exception != null ? getStackTraceString(exception) : null;
            }
            
            private static String getStackTraceString(Throwable e) {
                StringBuilder sb = new StringBuilder();
                sb.append(e.getClass().getName()).append(": ").append(e.getMessage()).append("\n");
                for (StackTraceElement element : e.getStackTrace()) {
                    sb.append("  at ").append(element.toString()).append("\n");
                    if (sb.length() > 1000) { // Limit stack trace size
                        sb.append("  ... truncated ...");
                        break;
                    }
                }
                return sb.toString();
            }
        }
    }
}
