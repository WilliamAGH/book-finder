/**
 * Service that orchestrates the migration of book data from S3 JSON files to Redis
 *
 * @author William Callahan
 *
 * Features:
 * - Migrates Google Books data from S3 to Redis JSON
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
import com.williamcallahan.book_recommendation_engine.repository.RedisBookSearchService;
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
    private static final int BATCH_SIZE = 10;
    private static final int CIRCUIT_BREAKER_THRESHOLD = 10;
    private static final Duration CIRCUIT_BREAKER_RESET_TIMEOUT = Duration.ofMinutes(5);

    private final S3Service s3Service;
    private final RedisJsonService redisJsonService;
    private final ObjectMapper objectMapper;
    private final CachedBookRepository cachedBookRepository;
    private final RedisBookSearchService redisBookSearchService;
    private final RedisBookIndexManager indexManager;
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
                                @Qualifier("migrationTaskExecutor") AsyncTaskExecutor migrationTaskExecutor) {
        this.s3Service = s3Service;
        this.redisJsonService = redisJsonService;
        this.objectMapper = objectMapper;
        this.cachedBookRepository = cachedBookRepository;
        this.redisBookSearchService = redisBookSearchService;
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
                            String s3Isbn13 = extractIsbn13(unifiedDocument);
                            String s3Isbn10 = extractIsbn10(unifiedDocument);
                            String s3GoogleId = extractGoogleBooksId(unifiedDocument);
                            String title = extractTitle(unifiedDocument);

                            String finalRecordUuid;
                            String redisKey;
                            Map<String, Object> existingData = null;
                            
                            // Use the enhanced RedisBookSearchService to find existing records
                            Optional<RedisBookSearchService.BookSearchResult> existingRecordOpt = 
                                redisBookSearchService.findExistingBook(s3Isbn13, s3Isbn10, s3GoogleId);
                            
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
        int batchNumber = (migrationProgress.getProcessed() + migrationProgress.getFailed() + migrationProgress.getSkipped()) / BATCH_SIZE + 1;
        log.info("Processing batch {} ({} files): {}", batchNumber, batch.size(), 
                batch.stream().limit(3).collect(Collectors.joining(", ")) + 
                (batch.size() > 3 ? " ..." : ""));
        
        List<CompletableFuture<Void>> fileFutures = batch.stream()
                .map(this::processFileAsync)
                .collect(Collectors.toList());
        
        return CompletableFuture.allOf(fileFutures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    log.info("Completed batch {} - Progress: {}/{} files", batchNumber, 
                            migrationProgress.getProcessed() + migrationProgress.getFailed() + migrationProgress.getSkipped(),
                            migrationProgress.getTotal());
                    migrationProgress.logProgress();
                });
    }
    
    /**
     * Process a single file asynchronously with retry
     */
    private CompletableFuture<Void> processFileAsync(String s3Key) {
        return CompletableFuture.runAsync(() -> {
            try {
                processGoogleBooksFile(s3Key);
            } catch (Exception e) {
                log.error("Failed to process file {}: {}", s3Key, e.getMessage());
                migrationProgress.incrementFailed();
                errorAggregator.addError("processFile", s3Key, e.getClass().getSimpleName(), e.getMessage(), e);
                // Don't propagate error to allow other files to continue
            }
        }, migrationTaskExecutor);
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
        log.info("Processing S3 file: {}", s3Key);
        try (InputStream rawIs = s3Service.getObjectContent(s3Key).join()) {
            PushbackInputStream is = new PushbackInputStream(rawIs, 1);
            int firstByte = is.read();
            if (firstByte == -1) {
                log.warn("InputStream empty for S3 key {}. Skipping.", s3Key);
                migrationProgress.incrementSkipped();
                return;
            }
            is.unread(firstByte);
            
            String destinationS3Key = null;
            Map.Entry<InputStream, Boolean> streamEntry = handleCompressedInputStream(is, s3Key);
            try (InputStream streamToRead = streamEntry.getKey()) {
                Map<String, Object> bookData = objectMapper.readValue(streamToRead, new TypeReference<Map<String, Object>>() {});

                String s3Isbn13 = UniversalBookDataExtractor.extractIsbn13(bookData);
                String s3Isbn10 = UniversalBookDataExtractor.extractIsbn10(bookData);
                String s3GoogleId = UniversalBookDataExtractor.extractGoogleBooksId(bookData);

                String finalRecordUuid;
                String redisKey;
                Map<String, Object> existingData = null;
                
                // Direct synchronous search for existing records with timing
                log.info("REDIS_DEBUG: Starting search for existing book with ISBN-13: {}, ISBN-10: {}, Google ID: {}", s3Isbn13, s3Isbn10, s3GoogleId);
                long searchStartTime = System.currentTimeMillis();
                Optional<RedisBookSearchService.BookSearchResult> existingRecordOpt;
                try {
                    existingRecordOpt = redisBookSearchService.findExistingBook(s3Isbn13, s3Isbn10, s3GoogleId);
                } catch (Exception e) {
                    log.warn("Redis search failed for identifiers (ISBN-13: {}, ISBN-10: {}, Google ID: {}): {}", 
                            s3Isbn13, s3Isbn10, s3GoogleId, e.getMessage());
                    existingRecordOpt = Optional.empty();
                }
                long searchEndTime = System.currentTimeMillis();
                log.info("REDIS_DEBUG: Completed search in {}ms. Found existing record: {}", 
                         (searchEndTime - searchStartTime), existingRecordOpt.isPresent());
                
                if (existingRecordOpt.isPresent()) {
                    RedisBookSearchService.BookSearchResult existingRecord = existingRecordOpt.get();
                    finalRecordUuid = existingRecord.getUuid();
                    redisKey = existingRecord.getRedisKey();
                    
                    // Convert CachedBook to Map for merging
                    CachedBook existingBook = existingRecord.getBook();
                    existingData = objectMapper.convertValue(existingBook, new TypeReference<Map<String, Object>>() {});
                    
                    String bookTitle = UniversalBookDataExtractor.extractTitle(bookData);
                    log.info("Book '{}' found with existing UUID: {} (ISBN-13: {}, ISBN-10: {}, Google ID: {})", 
                            bookTitle, finalRecordUuid, s3Isbn13, s3Isbn10, s3GoogleId);
                    
                    // Ensure the UUID in the data matches the key
                    if (existingData != null) {
                        existingData.put("id", finalRecordUuid);
                    }
                } else {
                    finalRecordUuid = UuidUtil.getTimeOrderedEpoch().toString();
                    redisKey = "book:" + finalRecordUuid;
                    String bookTitle = UniversalBookDataExtractor.extractTitle(bookData);
                    log.info("Book '{}' assigned new UUID: {} (ISBN-13: {}, ISBN-10: {}, Google ID: {})", 
                            bookTitle, finalRecordUuid, s3Isbn13, s3Isbn10, s3GoogleId);
                    existingData = null;
                }

                Map<String, Object> mergedMap;
                if (existingData != null) {
                    // Merge existing data with new data from S3
                    mergedMap = mergeMaps(existingData, bookData);
                    log.debug("Merged existing record with new S3 data for key {}", redisKey);
                } else {
                    mergedMap = bookData;
                }
                // Ensure the 'id' field in the JSON content is the canonical UUIDv7
                mergedMap.put("id", finalRecordUuid); 

                String mergedJsonString = objectMapper.writeValueAsString(mergedMap);
                boolean success = redisJsonService.jsonSet(redisKey, "$", mergedJsonString);
                
                if (success) {
                    // Consolidated log showing UUID and action taken
                    String action = existingData != null ? "UPDATED" : "CREATED";
                    String bookTitle = UniversalBookDataExtractor.extractTitle(bookData);
                    log.info("Book '{}' - UUID: {} - Action: {} - Redis Key: {}", 
                            bookTitle, finalRecordUuid, action, redisKey);

                    // Update secondary indexes for fast lookup
                    CachedBook indexBook = new CachedBook();
                    indexBook.setId(finalRecordUuid);
                    indexBook.setIsbn13(s3Isbn13);
                    indexBook.setIsbn10(s3Isbn10);
                    indexBook.setGoogleBooksId(s3GoogleId);
                    Optional<CachedBook> oldBookOpt = existingRecordOpt.map(RedisBookSearchService.BookSearchResult::getBook);
                    indexManager.updateAllIndexes(indexBook, oldBookOpt);

                    // Move processed S3 object only if Redis write was successful
                    destinationS3Key = getProcessedFileDestinationKey(s3Key, googleBooksPrefix);
                } else {
                    String bookTitle = UniversalBookDataExtractor.extractTitle(bookData);
                    log.error("Book '{}' - UUID: {} - Action: FAILED - Redis Key: {}", 
                            bookTitle, finalRecordUuid, redisKey);
                    migrationProgress.incrementFailed();
                    return;
                }
            }
            
            if (destinationS3Key != null) {
                s3Service.moveObject(s3Key, destinationS3Key);
                log.debug("Moved processed S3 object {} to {}", s3Key, destinationS3Key);
            } else {
                log.warn("Could not determine destination key for {}, file not moved.", s3Key);
            }

            migrationProgress.incrementProcessed();
            if (migrationProgress.getProcessed() % 100 == 0) {
                log.info("Ingested and moved {} Google Books records so far...", migrationProgress.getProcessed());
            }
        } catch (Exception e) {
            log.error("Error processing S3 object {}: {}", s3Key, e.getMessage(), e);
            migrationProgress.incrementFailed();
            errorAggregator.addError("processGoogleBooksFile", s3Key, e.getClass().getSimpleName(), e.getMessage(), e);
            throw new RuntimeException("Failed to process file: " + s3Key, e);
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
        merged.setIsbn10(incoming.getIsbn10() != null ? incoming.getIsbn10() : existing.getIsbn10());
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
            if (!existing.containsKey(key) || existing.get(key) == null) {
                existing.put(key, newValue);
            } else {
                Object existingValue = existing.get(key);
                if (existingValue instanceof Map && newValue instanceof Map) {
                    existing.put(key, mergeMaps((Map<String, Object>) existingValue, (Map<String, Object>) newValue));
                } else if (existingValue instanceof List && newValue instanceof List) {
                    // Create a new list with all elements from both lists
                    List<Object> mergedList = new ArrayList<>((List<Object>) existingValue);
                    mergedList.addAll((List<Object>) newValue);
                    existing.put(key, mergedList);
                }
                // Skip conflicts for non-map, non-list values
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
     * Adds flattened fields from Google Books volumeInfo, saleInfo, accessInfo
     * for easier Redis queries while preserving the original nested structure
     */
    @SuppressWarnings("unchecked")
    private void addFlattenedGoogleBooksFields(Map<String, Object> unifiedDoc, Map<String, Object> googleBooksData) {
        // Extract from volumeInfo
        if (googleBooksData.containsKey("volumeInfo")) {
            Map<String, Object> volumeInfo = (Map<String, Object>) googleBooksData.get("volumeInfo");
            
            // Core fields
            if (volumeInfo.containsKey("title")) {
                unifiedDoc.put("title", volumeInfo.get("title"));
            }
            if (volumeInfo.containsKey("subtitle")) {
                unifiedDoc.put("subtitle", volumeInfo.get("subtitle"));
            }
            if (volumeInfo.containsKey("authors")) {
                unifiedDoc.put("authors", volumeInfo.get("authors"));
            }
            if (volumeInfo.containsKey("description")) {
                unifiedDoc.put("description", volumeInfo.get("description"));
            }
            if (volumeInfo.containsKey("categories")) {
                unifiedDoc.put("categories", volumeInfo.get("categories"));
            }
            if (volumeInfo.containsKey("publisher")) {
                unifiedDoc.put("publisher", volumeInfo.get("publisher"));
            }
            if (volumeInfo.containsKey("publishedDate")) {
                unifiedDoc.put("publishedDate", volumeInfo.get("publishedDate"));
            }
            if (volumeInfo.containsKey("language")) {
                unifiedDoc.put("language", volumeInfo.get("language"));
            }
            if (volumeInfo.containsKey("pageCount")) {
                unifiedDoc.put("pageCount", volumeInfo.get("pageCount"));
            }
            if (volumeInfo.containsKey("averageRating")) {
                unifiedDoc.put("averageRating", volumeInfo.get("averageRating"));
            }
            if (volumeInfo.containsKey("ratingsCount")) {
                unifiedDoc.put("ratingsCount", volumeInfo.get("ratingsCount"));
            }
            if (volumeInfo.containsKey("maturityRating")) {
                unifiedDoc.put("maturityRating", volumeInfo.get("maturityRating"));
            }
            
            // Handle imageLinks
            if (volumeInfo.containsKey("imageLinks")) {
                Map<String, Object> imageLinks = (Map<String, Object>) volumeInfo.get("imageLinks");
                if (imageLinks.containsKey("thumbnail")) {
                    unifiedDoc.put("coverImageUrl", imageLinks.get("thumbnail"));
                }
                if (imageLinks.containsKey("small")) {
                    unifiedDoc.put("coverImageSmall", imageLinks.get("small"));
                }
                if (imageLinks.containsKey("medium")) {
                    unifiedDoc.put("coverImageMedium", imageLinks.get("medium"));
                }
                if (imageLinks.containsKey("large")) {
                    unifiedDoc.put("coverImageLarge", imageLinks.get("large"));
                }
            }
            
            // Handle industry identifiers (ISBN)
            if (volumeInfo.containsKey("industryIdentifiers")) {
                List<Map<String, Object>> identifiers = (List<Map<String, Object>>) volumeInfo.get("industryIdentifiers");
                for (Map<String, Object> identifier : identifiers) {
                    String type = (String) identifier.get("type");
                    String value = (String) identifier.get("identifier");
                    if ("ISBN_10".equals(type)) {
                        unifiedDoc.put("isbn10", value);
                    } else if ("ISBN_13".equals(type)) {
                        unifiedDoc.put("isbn13", value);
                    }
                }
            }
        }
        
        // Extract from saleInfo
        if (googleBooksData.containsKey("saleInfo")) {
            Map<String, Object> saleInfo = (Map<String, Object>) googleBooksData.get("saleInfo");
            if (saleInfo.containsKey("saleability")) {
                unifiedDoc.put("saleability", saleInfo.get("saleability"));
            }
            if (saleInfo.containsKey("isEbook")) {
                unifiedDoc.put("isEbook", saleInfo.get("isEbook"));
            }
            if (saleInfo.containsKey("listPrice") && saleInfo.get("listPrice") instanceof Map) {
                Map<String, Object> listPrice = (Map<String, Object>) saleInfo.get("listPrice");
                if (listPrice.containsKey("amount")) {
                    unifiedDoc.put("listPrice", listPrice.get("amount"));
                }
            }
        }
        
        // Extract from accessInfo
        if (googleBooksData.containsKey("accessInfo")) {
            Map<String, Object> accessInfo = (Map<String, Object>) googleBooksData.get("accessInfo");
            if (accessInfo.containsKey("publicDomain")) {
                unifiedDoc.put("publicDomain", accessInfo.get("publicDomain"));
            }
            if (accessInfo.containsKey("textToSpeechPermission")) {
                unifiedDoc.put("textToSpeechPermission", accessInfo.get("textToSpeechPermission"));
            }
            if (accessInfo.containsKey("webReaderLink")) {
                unifiedDoc.put("webReaderLink", accessInfo.get("webReaderLink"));
            }
        }
        
        // Use Google Books ID as googleBooksId
        if (googleBooksData.containsKey("id")) {
            unifiedDoc.put("googleBooksId", googleBooksData.get("id"));
        }
        
        // Extract preview and info links from volumeInfo
        if (googleBooksData.containsKey("volumeInfo")) {
            Map<String, Object> volumeInfo = (Map<String, Object>) googleBooksData.get("volumeInfo");
            if (volumeInfo.containsKey("previewLink")) {
                unifiedDoc.put("previewLink", volumeInfo.get("previewLink"));
            }
            if (volumeInfo.containsKey("infoLink")) {
                unifiedDoc.put("infoLink", volumeInfo.get("infoLink"));
            }
        }
    }
    
    /**
     * Extract ISBN-13 from unified document structure
     */
    private String extractIsbn13(Map<String, Object> unifiedDoc) {
        return UniversalBookDataExtractor.extractIsbn13(unifiedDoc);
    }
    
    /**
     * Extract ISBN-10 from unified document structure
     */
    private String extractIsbn10(Map<String, Object> unifiedDoc) {
        return UniversalBookDataExtractor.extractIsbn10(unifiedDoc);
    }
    
    /**
     * Extract Google Books ID from unified document structure
     */
    private String extractGoogleBooksId(Map<String, Object> unifiedDoc) {
        return UniversalBookDataExtractor.extractGoogleBooksId(unifiedDoc);
    }
    
    /**
     * Extract title from unified document structure
     */
    private String extractTitle(Map<String, Object> unifiedDoc) {
        return UniversalBookDataExtractor.extractTitle(unifiedDoc);
    }
    
    
    /**
     * Add flattened fields from OpenLibrary data
     */
    @SuppressWarnings("unchecked")
    private void addFlattenedOpenLibraryFields(Map<String, Object> unifiedDoc, Map<String, Object> openLibraryData) {
        // Extract title
        if (openLibraryData.containsKey("title")) {
            unifiedDoc.put("title", openLibraryData.get("title"));
        }
        
        // Extract authors (can be array of strings or objects)
        if (openLibraryData.containsKey("authors")) {
            Object authorsObj = openLibraryData.get("authors");
            if (authorsObj instanceof List) {
                List<?> authorsList = (List<?>) authorsObj;
                List<String> authorNames = new ArrayList<>();
                for (Object author : authorsList) {
                    if (author instanceof Map) {
                        Map<String, Object> authorMap = (Map<String, Object>) author;
                        if (authorMap.containsKey("name")) {
                            authorNames.add((String) authorMap.get("name"));
                        }
                    } else if (author instanceof String) {
                        authorNames.add((String) author);
                    }
                }
                unifiedDoc.put("authors", authorNames);
            }
        }
        
        // Extract description
        if (openLibraryData.containsKey("description")) {
            Object desc = openLibraryData.get("description");
            if (desc instanceof String) {
                unifiedDoc.put("description", desc);
            } else if (desc instanceof Map) {
                Map<String, Object> descMap = (Map<String, Object>) desc;
                if (descMap.containsKey("value")) {
                    unifiedDoc.put("description", descMap.get("value"));
                }
            }
        }
        
        // Extract ISBNs
        if (openLibraryData.containsKey("isbn_13")) {
            List<?> isbn13List = (List<?>) openLibraryData.get("isbn_13");
            if (isbn13List != null && !isbn13List.isEmpty()) {
                unifiedDoc.put("isbn13", isbn13List.get(0).toString());
            }
        }
        if (openLibraryData.containsKey("isbn_10")) {
            List<?> isbn10List = (List<?>) openLibraryData.get("isbn_10");
            if (isbn10List != null && !isbn10List.isEmpty()) {
                unifiedDoc.put("isbn10", isbn10List.get(0).toString());
            }
        }
        
        // Extract covers
        if (openLibraryData.containsKey("covers")) {
            List<?> covers = (List<?>) openLibraryData.get("covers");
            if (covers != null && !covers.isEmpty()) {
                String coverId = covers.get(0).toString();
                unifiedDoc.put("openLibraryCoverUrl", "https://covers.openlibrary.org/b/id/" + coverId + "-L.jpg");
                unifiedDoc.put("coverImageUrl", "https://covers.openlibrary.org/b/id/" + coverId + "-L.jpg");
            }
        }
        
        // Extract publication date
        if (openLibraryData.containsKey("publish_date")) {
            unifiedDoc.put("publishedDate", openLibraryData.get("publish_date"));
        }
        
        // Extract publisher
        if (openLibraryData.containsKey("publishers")) {
            List<?> publishers = (List<?>) openLibraryData.get("publishers");
            if (publishers != null && !publishers.isEmpty()) {
                unifiedDoc.put("publisher", publishers.get(0).toString());
            }
        }
        
        // Extract page count
        if (openLibraryData.containsKey("number_of_pages")) {
            unifiedDoc.put("pageCount", openLibraryData.get("number_of_pages"));
        }
        
        // Extract subjects as categories
        if (openLibraryData.containsKey("subjects")) {
            unifiedDoc.put("categories", openLibraryData.get("subjects"));
        }
        
        // Store OpenLibrary ID
        if (openLibraryData.containsKey("key")) {
            String key = (String) openLibraryData.get("key");
            unifiedDoc.put("openLibraryId", key);
        }
    }
    
    /**
     * Add flattened fields from NYT bestseller data
     */
    private void addFlattenedNYTFields(Map<String, Object> unifiedDoc, Map<String, Object> nytData) {
        // Extract title
        if (nytData.containsKey("title")) {
            unifiedDoc.put("title", nytData.get("title"));
        }
        
        // Extract author
        if (nytData.containsKey("author")) {
            String author = (String) nytData.get("author");
            unifiedDoc.put("authors", List.of(author));
        }
        
        // Extract description
        if (nytData.containsKey("description")) {
            unifiedDoc.put("description", nytData.get("description"));
        }
        
        // Extract ISBNs
        if (nytData.containsKey("primary_isbn13")) {
            unifiedDoc.put("isbn13", nytData.get("primary_isbn13"));
        }
        if (nytData.containsKey("primary_isbn10")) {
            unifiedDoc.put("isbn10", nytData.get("primary_isbn10"));
        }
        
        // Extract publisher
        if (nytData.containsKey("publisher")) {
            unifiedDoc.put("publisher", nytData.get("publisher"));
        }
        
        // Store NYT bestseller info in structured format
        Map<String, Object> nytInfo = new HashMap<>();
        if (nytData.containsKey("rank")) {
            nytInfo.put("rank", nytData.get("rank"));
        }
        if (nytData.containsKey("weeks_on_list")) {
            nytInfo.put("weeks_on_list", nytData.get("weeks_on_list"));
        }
        if (nytData.containsKey("list_name")) {
            nytInfo.put("list_name", nytData.get("list_name"));
        }
        if (nytData.containsKey("bestsellers_date")) {
            nytInfo.put("bestseller_date", nytData.get("bestsellers_date"));
        }
        
        if (!nytInfo.isEmpty()) {
            unifiedDoc.put("nyt_bestseller_info", nytInfo);
        }
        
        // Extract cover image
        if (nytData.containsKey("book_image")) {
            unifiedDoc.put("coverImageUrl", nytData.get("book_image"));
        }
        
        // Extract links
        if (nytData.containsKey("amazon_product_url")) {
            unifiedDoc.put("purchaseLink", nytData.get("amazon_product_url"));
        }
        if (nytData.containsKey("book_review_link")) {
            unifiedDoc.put("reviewLink", nytData.get("book_review_link"));
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
        
        public void addError(String operation, String key, String errorType, String message, Exception exception) {
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
            
            public ErrorDetail(String operation, String key, String errorType, String message, Exception exception) {
                this.operation = operation;
                this.key = key;
                this.errorType = errorType;
                this.message = message;
                this.timestamp = Instant.now();
                this.stackTrace = exception != null ? getStackTraceString(exception) : null;
            }
            
            private static String getStackTraceString(Exception e) {
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
