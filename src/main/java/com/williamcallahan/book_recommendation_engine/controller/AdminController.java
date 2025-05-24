/**
 * REST Controller for administrative operations
 * 
 * @author William Callahan
 * 
 * Features:
 * - Provides endpoints for S3 cover image cleanup operations
 * - Supports dry run mode for evaluating cleanup impact
 * - Handles moving flagged images to quarantine
 * - Configurable batch processing limits
 * - Detailed logging and error handling
 * - Returns operation summaries in text and JSON formats
 */

package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.scheduler.NewYorkTimesBestsellerScheduler;
import com.williamcallahan.book_recommendation_engine.scheduler.BookCacheWarmingScheduler;
import com.williamcallahan.book_recommendation_engine.service.S3CoverCleanupService;
import com.williamcallahan.book_recommendation_engine.service.ApiCircuitBreakerService;
import com.williamcallahan.book_recommendation_engine.service.BookDataConsolidationService;
import com.williamcallahan.book_recommendation_engine.service.EmbeddingService;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.repository.RedisBookMaintenanceService;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.types.MoveActionSummary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin")
public class AdminController {

    private static final Logger logger = LoggerFactory.getLogger(AdminController.class);

    private final S3CoverCleanupService s3CoverCleanupService;
    private final String configuredS3Prefix;
    private final int defaultBatchLimit;
    private final String configuredQuarantinePrefix;
    private final NewYorkTimesBestsellerScheduler newYorkTimesBestsellerScheduler;
    private final BookCacheWarmingScheduler bookCacheWarmingScheduler;
    private final ApiCircuitBreakerService apiCircuitBreakerService;
    private final BookDataConsolidationService bookDataConsolidationService;
    private final CachedBookRepository cachedBookRepository;
    private final RedisBookMaintenanceService maintenanceService;
    private final EmbeddingService embeddingService;

    public AdminController(@Autowired(required = false) S3CoverCleanupService s3CoverCleanupService,
                           @Autowired(required = false) NewYorkTimesBestsellerScheduler newYorkTimesBestsellerScheduler,
                           BookCacheWarmingScheduler bookCacheWarmingScheduler,
                           ApiCircuitBreakerService apiCircuitBreakerService,
                           BookDataConsolidationService bookDataConsolidationService,
                           @Autowired(required = false) CachedBookRepository cachedBookRepository,
                           @Autowired(required = false) RedisBookMaintenanceService maintenanceService,
                           EmbeddingService embeddingService,
                           @Value("${app.s3.cleanup.prefix:images/book-covers/}") String configuredS3Prefix,
                           @Value("${app.s3.cleanup.default-batch-limit:100}") int defaultBatchLimit,
                           @Value("${app.s3.cleanup.quarantine-prefix:images/non-covers-pages/}") String configuredQuarantinePrefix) {
        this.s3CoverCleanupService = s3CoverCleanupService;
        this.newYorkTimesBestsellerScheduler = newYorkTimesBestsellerScheduler;
        this.bookCacheWarmingScheduler = bookCacheWarmingScheduler;
        this.apiCircuitBreakerService = apiCircuitBreakerService;
        this.bookDataConsolidationService = bookDataConsolidationService;
        this.cachedBookRepository = cachedBookRepository;
        this.maintenanceService = maintenanceService;
        this.embeddingService = embeddingService;
        this.configuredS3Prefix = configuredS3Prefix;
        this.defaultBatchLimit = defaultBatchLimit;
        this.configuredQuarantinePrefix = configuredQuarantinePrefix;
    }

    /**
     * Triggers a dry run of the S3 cover cleanup process
     * The S3 prefix to scan can be overridden by a request parameter,
     * otherwise, the configured 'app.s3.cleanup.prefix' is used
     * The number of items to process can be limited by a request parameter,
     * otherwise, the configured 'app.s3.cleanup.default-batch-limit' is used
     *
     * @param prefixOptional Optional request parameter to override the S3 prefix
     * @param limitOptional Optional request parameter to override the batch processing limit
     * @return A ResponseEntity containing a plain text summary and list of flagged files
     */
    @GetMapping(value = "/s3-cleanup/dry-run", produces = MediaType.TEXT_PLAIN_VALUE)
    public CompletableFuture<ResponseEntity<String>> triggerS3CoverCleanupDryRun(
            @RequestParam(name = "prefix", required = false) String prefixOptional,
            @RequestParam(name = "limit", required = false) Integer limitOptional) {
        
        if (s3CoverCleanupService == null) {
            String errorMessage = "S3 Cover Cleanup Service is not available. S3 integration may be disabled.";
            logger.warn(errorMessage);
            return CompletableFuture.completedFuture(
                ResponseEntity.badRequest().body(errorMessage) // Changed to return String
            );
        }
        
        String prefixToUse = prefixOptional != null ? prefixOptional : configuredS3Prefix;
        int requestedLimit     = limitOptional != null ? limitOptional : defaultBatchLimit;
        int batchLimitToUse    = requestedLimit > 0 ? requestedLimit : Integer.MAX_VALUE;
        if (requestedLimit <= 0) {
            // This behavior can be adjusted; for now, let's say 0 or negative means a very large number (effectively no limit for practical purposes)
            // or stick to a sane default if that's preferred
            // The S3CoverCleanupService currently handles batchLimit > 0
            // If batchLimit is 0 or negative, it processes all
            logger.warn("Batch limit {} requested; treating as unlimited.", requestedLimit);
        }
        
        logger.info("Admin endpoint /admin/s3-cleanup/dry-run invoked. Triggering S3 Cover Cleanup Dry Run with prefix: '{}', limit: {}", prefixToUse, batchLimitToUse);

        // Invoke async service without blocking
        return s3CoverCleanupService.performDryRun(prefixToUse, batchLimitToUse)
            .thenApply(summary -> {
                logger.info("S3 Cover Cleanup dry run completed. Summary: {}", summary);
                return ResponseEntity.ok(summary.toString());
            })
            .exceptionally(e -> {
                String errorMsg = String.format(
                    "Failed to complete S3 Cover Cleanup dry run. Prefix: '%s', Limit: %d. Error: %s",
                    prefixToUse, batchLimitToUse, e.getMessage()
                );
                logger.error(errorMsg, e);
                return ResponseEntity.internalServerError()
                    .body("{\"error\": \"" + errorMsg.replace("\"", "\\\"") + "\"}");
            });
    }

    /**
     * Triggers the action of moving flagged S3 cover images to a quarantine prefix
     *
     * @param prefixOptional Optional request parameter to override the S3 source prefix
     * @param limitOptional Optional request parameter to override the batch processing limit
     * @param quarantinePrefixOptional Optional request parameter to override the quarantine prefix
     * @return A ResponseEntity containing the MoveActionSummary as JSON
     */
    @PostMapping(value = "/s3-cleanup/move-flagged", produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<ResponseEntity<MoveActionSummary>> triggerS3CoverMoveAction(
            @RequestParam(name = "prefix", required = false) String prefixOptional,
            @RequestParam(name = "limit", required = false) Integer limitOptional,
            @RequestParam(name = "quarantinePrefix", required = false) String quarantinePrefixOptional) {

        if (s3CoverCleanupService == null) {
            String errorMessage = "S3 Cover Cleanup Service is not available. S3 integration may be disabled.";
            logger.warn(errorMessage);
            return CompletableFuture.completedFuture(
                ResponseEntity.badRequest().body(new MoveActionSummary(errorMessage))
            );
        }

        String sourcePrefixToUse = prefixOptional != null ? prefixOptional : configuredS3Prefix;
        int batchLimitToUse = limitOptional != null ? limitOptional : defaultBatchLimit;
        String quarantinePrefixToUse = quarantinePrefixOptional != null ? quarantinePrefixOptional : configuredQuarantinePrefix;

        if (batchLimitToUse <= 0) {
            logger.warn("Batch limit for move action specified as {} (or defaulted to it), processing all found items.", batchLimitToUse);
        }
        if (quarantinePrefixToUse.isEmpty() || quarantinePrefixToUse.equals(sourcePrefixToUse)) {
            String errorMsg = "Invalid quarantine prefix: cannot be empty or same as source prefix.";
            logger.error(errorMsg + " Source: '{}', Quarantine: '{}'", sourcePrefixToUse, quarantinePrefixToUse);
            return CompletableFuture.completedFuture(
                ResponseEntity.badRequest().body(new MoveActionSummary(errorMsg))
            );
        }
        
        logger.info("Admin endpoint /admin/s3-cleanup/move-flagged invoked. " +
                        "Source Prefix: '{}', Limit: {}, Quarantine Prefix: '{}'",
                sourcePrefixToUse, batchLimitToUse, quarantinePrefixToUse);

        // Invoke async service without blocking
        return s3CoverCleanupService.performMoveAction(sourcePrefixToUse, batchLimitToUse, quarantinePrefixToUse)
            .thenApply(summary -> {
                logger.info("S3 Cover Cleanup Move Action completed. Summary: {}", summary);
                return ResponseEntity.ok(summary);
            })
            .exceptionally(e -> {
                String errorMessage = String.format(
                    "Failed to complete S3 Cover Cleanup Move Action. Source Prefix: '%s', Limit: %d, Quarantine Prefix: '%s'. Error: %s",
                    sourcePrefixToUse, batchLimitToUse, quarantinePrefixToUse, e.getMessage()
                );
                logger.error(errorMessage, e);
                return ResponseEntity.internalServerError()
                    .body(new MoveActionSummary(errorMessage));
            });
    }

    /**
     * Triggers the New York Times Bestseller processing job
     *
     * @return A ResponseEntity indicating the outcome of the trigger
     */
    @PostMapping(value = "/trigger-nyt-bestsellers", produces = MediaType.TEXT_PLAIN_VALUE)
    public CompletableFuture<ResponseEntity<String>> triggerNytBestsellerProcessing() {
        logger.info("Admin endpoint /admin/trigger-nyt-bestsellers invoked.");
        
        if (newYorkTimesBestsellerScheduler == null) {
            String errorMessage = "New York Times Bestseller Scheduler is not available. S3 integration may be disabled.";
            logger.warn(errorMessage);
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(errorMessage));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                newYorkTimesBestsellerScheduler.processNewYorkTimesBestsellers();
                String successMessage = "Successfully triggered New York Times Bestseller processing job.";
                logger.info(successMessage);
                return ResponseEntity.ok(successMessage);
            } catch (Exception e) {
                String errorMessage = "Failed to trigger New York Times Bestseller processing job: " + e.getMessage();
                logger.error(errorMessage, e);
                return ResponseEntity.internalServerError().body(errorMessage);
            }
        });
    }

    /**
     * Triggers the Book Cache Warming job
     *
     * @return A ResponseEntity indicating the outcome of the trigger
     */
    @PostMapping(value = "/trigger-cache-warming", produces = MediaType.TEXT_PLAIN_VALUE)
    public CompletableFuture<ResponseEntity<String>> triggerCacheWarming() {
        logger.info("Admin endpoint /admin/trigger-cache-warming invoked.");

        if (bookCacheWarmingScheduler == null) {
            String errorMessage = "Book Cache Warming Scheduler is not available.";
            logger.warn(errorMessage);
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(errorMessage));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                bookCacheWarmingScheduler.warmPopularBookCaches();
                String successMessage = "Successfully triggered book cache warming job.";
                logger.info(successMessage);
                return ResponseEntity.ok(successMessage);
            } catch (Exception e) {
                String errorMessage = "Failed to trigger book cache warming job: " + e.getMessage();
                logger.error(errorMessage, e);
                return ResponseEntity.internalServerError().body(errorMessage);
            }
        });
    }

    /**
     * Gets the current status of the API circuit breaker
     *
     * @return A ResponseEntity containing the circuit breaker status
     */
    @GetMapping(value = "/circuit-breaker/status", produces = MediaType.TEXT_PLAIN_VALUE)
    public CompletableFuture<ResponseEntity<String>> getCircuitBreakerStatus() {
        logger.info("Admin endpoint /admin/circuit-breaker/status invoked.");

        if (apiCircuitBreakerService == null) {
            String errorMessage = "API CircuitBreakerService is not available.";
            logger.warn(errorMessage);
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(errorMessage));
        }

        return apiCircuitBreakerService.getCircuitStatus()
            .thenApply(status -> {
                logger.info("Circuit breaker status retrieved: {}", status);
                return ResponseEntity.ok(status);
            })
            .exceptionally(e -> {
                String errorMessage = "Failed to get circuit breaker status: " + e.getMessage();
                logger.error(errorMessage, e);
                return ResponseEntity.internalServerError().body(errorMessage);
            });
    }

    /**
     * Manually resets the API circuit breaker to CLOSED state
     *
     * @return A ResponseEntity indicating the outcome of the reset
     */
    @PostMapping(value = "/circuit-breaker/reset", produces = MediaType.TEXT_PLAIN_VALUE)
    public CompletableFuture<ResponseEntity<String>> resetCircuitBreaker() {
        logger.info("Admin endpoint /admin/circuit-breaker/reset invoked.");

        if (apiCircuitBreakerService == null) {
            String errorMessage = "API CircuitBreakerService is not available.";
            logger.warn(errorMessage);
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(errorMessage));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                apiCircuitBreakerService.reset();
                String successMessage = "Successfully reset API circuit breaker to CLOSED state.";
                logger.info(successMessage);
                return ResponseEntity.ok(successMessage);
            } catch (Exception e) {
                String errorMessage = "Failed to reset circuit breaker: " + e.getMessage();
                logger.error(errorMessage, e);
                return ResponseEntity.internalServerError().body(errorMessage);
            }
        });
    }

    /**
     * Triggers the book data consolidation process in Redis
     *
     * @param dryRun Optional request parameter to perform a dry run without actual changes. Defaults to true
     * @return A ResponseEntity containing the consolidation summary
     */
    @PostMapping(value = "/data/consolidate-books", produces = MediaType.TEXT_PLAIN_VALUE)
    public CompletableFuture<ResponseEntity<String>> triggerBookDataConsolidation(
            @RequestParam(name = "dryRun", defaultValue = "true") boolean dryRun) {
        
        logger.info("Admin endpoint /admin/data/consolidate-books invoked. Dry run: {}", dryRun);

        if (bookDataConsolidationService == null) {
            String errorMessage = "BookDataConsolidationService is not available.";
            logger.warn(errorMessage);
            return CompletableFuture.completedFuture(ResponseEntity.status(503).body(errorMessage));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                bookDataConsolidationService.consolidateBookDataAsync(dryRun);
                String ackMessage = "Book data consolidation process started. Dry run: " + dryRun + ". Check logs for completion status and summary.";
                logger.info(ackMessage);
                return ResponseEntity.ok(ackMessage);
            } catch (Exception e) {
                String errorMessage = String.format("Failed to START book data consolidation. Dry run: %b. Error: %s", dryRun, e.getMessage());
                logger.error(errorMessage, e);
                return ResponseEntity.internalServerError().body(errorMessage);
            }
        });
    }

    /**
     * Diagnoses Redis cache integrity to identify corrupted data
     *
     * @return A ResponseEntity containing cache integrity statistics
     */
    @GetMapping(value = "/cache/diagnose", produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<ResponseEntity<Map<String,Integer>>> diagnoseCacheIntegrity() {
        logger.info("Admin endpoint /admin/cache/diagnose invoked.");
        
        if (maintenanceService == null) {
            String errorMessage = "Redis Book Maintenance Service is not available. Redis integration may be disabled.";
            logger.warn(errorMessage);
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(Collections.emptyMap()));
        }
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                Map<String, Integer> stats = maintenanceService.diagnoseCacheIntegrity();
                logger.info("Cache integrity diagnosis completed: {}", stats);
                return ResponseEntity.ok(stats);
            } catch (Exception e) {
                String errorMessage = "Failed to diagnose cache integrity: " + e.getMessage();
                logger.error(errorMessage, e);
                // Explicitly type the error response to match the success response type for the supplier
                Map<String, Integer> errorMap = Collections.emptyMap();
                return ResponseEntity.internalServerError().body(errorMap);
            }
        });
    }

    /**
     * Cleans up corrupted cache entries
     *
     * @param dryRun Optional request parameter to perform a dry run without actual cleanup; defaults to true
     * @return A ResponseEntity containing the cleanup summary
     */
    @PostMapping(value = "/cache/cleanup", produces = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<ResponseEntity<?>> cleanupCorruptedCache(
            @RequestParam(name = "dryRun", defaultValue = "true") boolean dryRun) {
        
        logger.info("Admin endpoint /admin/cache/cleanup invoked. Dry run: {}", dryRun);
        
        if (maintenanceService == null) { // Changed to maintenanceService
            String errorMessage = "Redis Book Maintenance Service is not available. Redis integration may be disabled.";
            logger.warn(errorMessage);
            return CompletableFuture.completedFuture(
                ResponseEntity.badRequest().body(new MoveActionSummary(errorMessage))
            );
        }
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                int repairedCount = maintenanceService.repairCorruptedCache(dryRun); // Changed
                java.util.Map<String, Object> result = new java.util.HashMap<>();
                result.put("repairedCount", repairedCount);
                result.put("dryRun", dryRun);
                result.put("message", dryRun ? 
                    "Dry run completed. " + repairedCount + " corrupted keys identified for repair." :
                    "Repair completed. " + repairedCount + " corrupted keys repaired.");
                
                logger.info("Cache repair completed. Keys processed: {}, Dry run: {}", repairedCount, dryRun);
                return ResponseEntity.ok(result);
            } catch (Exception e) {
                String errorMessage = String.format("Failed to repair corrupted cache. Dry run: %b. Error: %s", dryRun, e.getMessage());
                logger.error(errorMessage, e);
                // Explicitly type the error response if needed, here it's String, method returns ResponseEntity<?>
                return ResponseEntity.internalServerError().body((Object)("{\"error\": \"" + errorMessage.replace("\"", "\\\"") + "\"}"));
            }
        });
    }

    /**
     * Generates embeddings for books that don't have them
     * Useful for batch processing existing books
     */
    @PostMapping("/embeddings/generate")
    public CompletableFuture<ResponseEntity<String>> generateMissingEmbeddings(
            @RequestParam(value = "limit", defaultValue = "100") int limit,
            @RequestParam(value = "dryRun", defaultValue = "true") boolean dryRun) {
        
        if (cachedBookRepository == null) {
            return CompletableFuture.completedFuture(
                ResponseEntity.badRequest().body("Repository service is not available.")
            );
        }
        
        logger.info("Starting embedding generation. Limit: {}, Dry run: {}", limit, dryRun);
        
        // Use reactive approach from the beginning
        return Mono.fromFuture(cachedBookRepository.findAllAsync())
            .map(allBooks -> {
                var books = new ArrayList<CachedBook>();
                for (CachedBook book : allBooks) {
                    if (embeddingService.shouldGenerateEmbedding(book)) {
                        books.add(book);
                        if (books.size() >= limit) break;
                    }
                }
                return books;
            })
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(books -> {
                if (books.isEmpty()) {
                    String message = "No books found needing embedding generation.";
                    logger.info(message);
                    return Mono.just(ResponseEntity.ok(message));
                }
                
                if (dryRun) {
                    String message = String.format("Dry run: Found %d books that need embeddings generated.", books.size());
                    logger.info(message);
                    return Mono.just(ResponseEntity.ok(message));
                }
                
                AtomicInteger processedCount = new AtomicInteger(0);
                AtomicInteger errorCount = new AtomicInteger(0);
                int totalBooksToProcess = books.size();

                // Use reactive streams with controlled concurrency
                return Flux.fromIterable(books)
                    .flatMap(book -> embeddingService.generateEmbeddingForBook(book)
                        .flatMap(embedding -> {
                            if (embedding != null) {
                                book.setEmbedding(embedding);
                                // Use async save properly
                                return Mono.fromFuture(cachedBookRepository.saveAsync(book))
                                    .map(savedBook -> {
                                        processedCount.incrementAndGet();
                                        return savedBook;
                                    })
                                    .onErrorResume(ex -> {
                                        logger.warn("Failed to save book {} after embedding generation: {}", book.getId(), ex.getMessage());
                                        errorCount.incrementAndGet();
                                        return Mono.empty();
                                    });
                            }
                            return Mono.empty();
                        })
                        .doOnNext(processedBook -> {
                            int currentProcessed = processedCount.get();
                            if (currentProcessed % 10 == 0 && currentProcessed > 0) {
                                logger.info("Generated embeddings for {} books...", currentProcessed);
                            }
                        })
                        .onErrorResume(e -> {
                            logger.warn("Error during embedding generation stream for a book: {}", e.getMessage());
                            errorCount.incrementAndGet();
                            return Mono.empty();
                        }),
                        10 // Concurrency limit
                    )
                    .collectList()
                    .map(processedBooks -> {
                        String message = String.format("Embedding generation completed. Processed: %d, Errors: %d, Total found: %d",
                            processedCount.get(), errorCount.get(), totalBooksToProcess);
                        logger.info(message);
                        return ResponseEntity.ok(message);
                    });
            })
            .onErrorResume(e -> {
                String errorMessage = String.format("Failed to generate embeddings. Limit: %d, Dry run: %b. Error: %s", 
                    limit, dryRun, e.getMessage());
                logger.error(errorMessage, e);
                return Mono.just(ResponseEntity.internalServerError().body(errorMessage));
            })
            .toFuture();
    }
}
