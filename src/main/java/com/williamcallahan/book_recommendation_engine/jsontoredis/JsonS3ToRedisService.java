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
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;

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
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("unused")
@Service("jsonS3ToRedis_JsonS3ToRedisService")
@Profile("jsontoredis")
public class JsonS3ToRedisService {

    private static final Logger log = LoggerFactory.getLogger(JsonS3ToRedisService.class);
    private static final Pattern JSON_FILE_PATTERN = Pattern.compile(".*\\.json$", Pattern.CASE_INSENSITIVE);

    private final S3Service s3Service;
    private final RedisJsonService redisJsonService;
    private final ObjectMapper objectMapper;
    private final CachedBookRepository cachedBookRepository;
    private final AsyncTaskExecutor mvcTaskExecutor;

    // S3 paths will be accessed via s3Service getters
    private final String googleBooksPrefix;
    private final String nytBestsellersKey;

    public JsonS3ToRedisService(@Qualifier("jsonS3ToRedis_S3Service") S3Service s3Service,
                                @Qualifier("jsonS3ToRedis_RedisJsonService") RedisJsonService redisJsonService,
                                ObjectMapper objectMapper,
                                CachedBookRepository cachedBookRepository,
                                @Qualifier("mvcTaskExecutor") AsyncTaskExecutor mvcTaskExecutor) {
        this.s3Service = s3Service;
        this.redisJsonService = redisJsonService;
        this.objectMapper = objectMapper;
        this.cachedBookRepository = cachedBookRepository;
        this.mvcTaskExecutor = mvcTaskExecutor;
        this.googleBooksPrefix = s3Service.getGoogleBooksPrefix();
        this.nytBestsellersKey = s3Service.getNytBestsellersKey();
        log.info("JsonS3ToRedisService initialized. Google Books S3 Prefix: {}, NYT Bestsellers S3 Key: {}", googleBooksPrefix, nytBestsellersKey);
    }

    /**
     * Orchestrates the entire migration process from S3 to Redis
     */
    public CompletableFuture<Void> performMigrationAsync() {
        log.info("Starting S3 JSON to Redis migration process asynchronously...");
        return ingestGoogleBooksDataAsync()
            .thenCompose(v -> mergeNytBestsellersDataAsync())
            .whenComplete((v, ex) -> {
                if (ex != null) {
                    log.error("S3 JSON to Redis migration process failed.", ex);
                } else {
                    log.info("S3 JSON to Redis migration process finished.");
                    log.info("REMINDER: Manually create/update your RediSearch index(es) after migration if applicable.");
                }
            });
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

        bookKeys.stream()
                .filter(key -> JSON_FILE_PATTERN.matcher(key).matches()) // Ensure it's a .json file
                .filter(key -> !key.startsWith(googleBooksPrefix + "in-redis/")) // Skip files already processed and moved
                .forEach(s3Key -> {
                    log.debug("Processing S3 object: {}", s3Key);
                    try (InputStream is = s3Service.getObjectContent(s3Key).join()) {
                        if (is == null) {
                            log.warn("InputStream is null for S3 key {}. Skipping.", s3Key);
                            return;
                        }
                        String destinationS3Key = null; // Declare destinationS3Key outside the inner try
                        Map.Entry<InputStream, Boolean> streamEntry = handleCompressedInputStream(is, s3Key);
                        try (InputStream streamToRead = streamEntry.getKey()) {
                            Map<String, Object> bookData = objectMapper.readValue(streamToRead, new TypeReference<Map<String, Object>>() {});

                            String s3Isbn13 = extractIsbn13FromGoogleBooks(bookData);
                            String s3Isbn10 = extractIsbn10FromGoogleBooks(bookData);
                            String s3GoogleId = extractGoogleBooksIdFromGoogleBooks(bookData);

                            String finalRecordUuid;
                            String redisKey;
                            String existingJson = null;
                            java.util.Optional<CachedBook> existingCanonicalBookOpt = java.util.Optional.empty();

                            if (s3Isbn13 != null && !s3Isbn13.isEmpty()) {
                                existingCanonicalBookOpt = cachedBookRepository.findByIsbn13(s3Isbn13);
                                if (existingCanonicalBookOpt.isPresent()) {
                                    log.info("Found existing canonical record for S3 key {} using ISBN-13: {}. Canonical UUIDv7 ID: {}", s3Key, s3Isbn13, existingCanonicalBookOpt.get().getId());
                                }
                            }
                            if (existingCanonicalBookOpt.isEmpty() && s3Isbn10 != null && !s3Isbn10.isEmpty()) {
                                existingCanonicalBookOpt = cachedBookRepository.findByIsbn10(s3Isbn10);
                                if (existingCanonicalBookOpt.isPresent()) {
                                    log.info("Found existing canonical record for S3 key {} using ISBN-10: {}. Canonical UUIDv7 ID: {}", s3Key, s3Isbn10, existingCanonicalBookOpt.get().getId());
                                }
                            }
                            if (existingCanonicalBookOpt.isEmpty() && s3GoogleId != null && !s3GoogleId.isEmpty()) {
                                existingCanonicalBookOpt = cachedBookRepository.findByGoogleBooksId(s3GoogleId);
                                if (existingCanonicalBookOpt.isPresent()) {
                                    log.info("Found existing canonical record for S3 key {} using GoogleBooksID: {}. Canonical UUIDv7 ID: {}", s3Key, s3GoogleId, existingCanonicalBookOpt.get().getId());
                                }
                            }

                            if (existingCanonicalBookOpt.isPresent()) {
                                finalRecordUuid = existingCanonicalBookOpt.get().getId();
                                redisKey = "book:" + finalRecordUuid;
                                // Fetch the JSON content of the canonical record for merging
                                String rawJson = redisJsonService.jsonGet(redisKey, "$");
                                if (rawJson != null && !rawJson.trim().isEmpty() && !rawJson.equalsIgnoreCase("null")) {
                                   // RedisJSON often returns an array for path '$', even for a single object.
                                   JsonNode rootNode = objectMapper.readTree(rawJson);
                                   if (rootNode.isArray() && rootNode.size() > 0) {
                                       existingJson = rootNode.get(0).toString();
                                   } else if (rootNode.isObject()) {
                                       existingJson = rootNode.toString();
                                   } else {
                                       log.warn("Unexpected JSON structure from redisJsonService.jsonGet for key {}: {}", redisKey, rawJson);
                                   }
                                }
                            } else {
                                finalRecordUuid = UuidUtil.getTimeOrderedEpoch().toString();
                                redisKey = "book:" + finalRecordUuid;
                                log.info("No existing canonical record found for S3 key {}. Assigning new UUIDv7 ID: {}. Target Redis key: {}", s3Key, finalRecordUuid, redisKey);
                                existingJson = null; // No existing record to merge from this key
                            }
                            
                            log.info("Processing S3 object {} for Redis. Final UUIDv7 ID for record: {}. Target Redis key: {}", s3Key, finalRecordUuid, redisKey);

                            Map<String, Object> mergedMap;
                            if (existingJson != null) {
                                Map<String, Object> existingMap = objectMapper.readValue(existingJson, new TypeReference<Map<String, Object>>() {});
                                mergedMap = mergeMaps(existingMap, bookData);
                            } else {
                                mergedMap = bookData;
                            }
                            // Ensure the 'id' field in the JSON content is the canonical UUIDv7
                            mergedMap.put("id", finalRecordUuid); 

                            String mergedJsonString = objectMapper.writeValueAsString(mergedMap);
                            redisJsonService.jsonSet(redisKey, "$", mergedJsonString);
                            
                            // Move processed S3 object
                            destinationS3Key = getProcessedFileDestinationKey(s3Key, googleBooksPrefix);
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

        log.info("--- Phase 1 Complete: Ingested {} Google Books records. Skipped {} records due to no identifier. ---", processedCount.get(), skippedNoIdCount.get());
    }

    private CompletableFuture<Void> ingestGoogleBooksDataAsync() {
        return CompletableFuture.runAsync(this::ingestGoogleBooksDataSync, mvcTaskExecutor);
    }

    /**
     * Extracts ISBN-13 from Google Books industryIdentifiers list
     * 
     * @param bookData The Google Books data map
     * @return The ISBN-13 if found, null otherwise
     */
    private String extractIsbn13FromGoogleBooks(Map<String, Object> bookData) {
        return extractIdentifierFromGoogleBooks(bookData, "ISBN_13");
    }

    private String extractIsbn10FromGoogleBooks(Map<String, Object> bookData) {
        return extractIdentifierFromGoogleBooks(bookData, "ISBN_10");
    }

    private String extractGoogleBooksIdFromGoogleBooks(Map<String, Object> bookData) {
        // Google Books ID is usually at the top level 'id' field in the JSON from S3
        if (bookData != null && bookData.get("id") instanceof String) {
            return (String) bookData.get("id");
        }   
        return null; // Or attempt to find it in volumeInfo if structure varies
    }

    @SuppressWarnings("unchecked")
    private String extractIdentifierFromGoogleBooks(Map<String, Object> bookData, String type) {
        if (bookData != null && bookData.containsKey("volumeInfo")) {
            Map<String, Object> volumeInfo = (Map<String, Object>) bookData.get("volumeInfo");
            if (volumeInfo != null && volumeInfo.containsKey("industryIdentifiers")) {
                try {
                    List<Map<String, String>> identifiers = (List<Map<String, String>>) volumeInfo.get("industryIdentifiers");
                    if (identifiers != null) {
                        for (Map<String, String> identifierObj : identifiers) {
                            if (type.equals(identifierObj.get("type"))) {
                                return identifierObj.get("identifier");
                            }
                        }
                    }
                } catch (ClassCastException e) {
                    log.warn("Could not cast industryIdentifiers for book data: {}", bookData.get("id"), e);
                }
            }
        }
        return null;
    }
    
    /**
     * Phase 2: Merges NYT Bestsellers data into existing Redis records
     * Adds bestseller information to books that already exist in Redis
     * Uses ISBN-13 to match NYT data with Google Books data
     * Adds metadata including bestseller date, rank, and buy links
     */
    private void mergeNytBestsellersDataSync() {
        log.info("--- Phase 2: Merging NYT Bestsellers data from S3 key: {} ---", nytBestsellersKey);
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger notFoundCount = new AtomicInteger(0);
        AtomicInteger skippedNoIsbnCount = new AtomicInteger(0);
        
        Map<String, Object> nytData = null; // Declare outside try block

        try (InputStream s3InputStream = s3Service.getObjectContent(nytBestsellersKey).join()) {
            if (s3InputStream == null) {
                log.error("InputStream is null for NYT Bestsellers S3 key {}. Cannot proceed with merge phase.", nytBestsellersKey);
                return;
            }

            Map.Entry<InputStream, Boolean> nytStreamEntry = handleCompressedInputStream(s3InputStream, nytBestsellersKey);
            try (InputStream finalNytInputStream = nytStreamEntry.getKey()) {
                nytData = objectMapper.readValue(finalNytInputStream, new TypeReference<Map<String, Object>>() {});
            } // finalNytInputStream is closed here

            if (nytData != null && nytData.containsKey("results")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> results = (Map<String, Object>) nytData.get("results");
                if (results != null && results.containsKey("lists")) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> lists = (List<Map<String, Object>>) results.get("lists");
                    String bestsellerDate = (String) results.get("bestsellers_date");

                    for (Map<String, Object> bookList : lists) {
                        if (bookList.containsKey("books")) {
                            @SuppressWarnings("unchecked")
                            List<Map<String, Object>> books = (List<Map<String, Object>>) bookList.get("books");
                            for (Map<String, Object> nytBook : books) {
                                String nytIsbn13 = extractIsbn13FromNytBooks(nytBook);

                                if (nytIsbn13 == null || nytIsbn13.isEmpty()) {
                                    log.warn("No ISBN-13 found for a NYT bestseller entry (Title: {}). Skipping merge.", nytBook.get("title"));
                                    skippedNoIsbnCount.incrementAndGet();
                                    continue;
                                }

                                String redisKey = "book:" + nytIsbn13;
                                // Retrieve existing JSON string for the key
                                String existingJsonString = redisJsonService.jsonGet(redisKey, "$"); // Reverted to use "$"

                                if (existingJsonString != null && !existingJsonString.trim().isEmpty() && !existingJsonString.equalsIgnoreCase("null")) {
                                    try {
                                        com.fasterxml.jackson.databind.JsonNode rootNodeNyt = objectMapper.readTree(existingJsonString);
                                        Map<String, Object> existingBookData;
                                        if (rootNodeNyt.isArray() && rootNodeNyt.size() > 0) {
                                            com.fasterxml.jackson.databind.JsonNode actualObjectNodeNyt = rootNodeNyt.get(0);
                                            existingBookData = objectMapper.convertValue(actualObjectNodeNyt, new TypeReference<Map<String, Object>>() {});
                                        } else {
                                             log.warn("Expected JSON array from redisJsonService.jsonGet for NYT key {}, but got: {}. Treating as new data.", redisKey, existingJsonString);
                                             // If it's not an array or an empty one, we can't merge.
                                             // Depending on desired behavior, could skip or log. Here, we'll effectively skip merging by not initializing existingBookData from it.
                                             // Or, if the non-array response IS the object, handle that:
                                             // existingBookData = objectMapper.readValue(existingJsonString, new TypeReference<Map<String, Object>>() {});
                                             // For now, sticking to the array-unwrapping logic. If it's not an array, this path won't proceed to merge.
                                             // To ensure it proceeds if it's a single object not in an array:
                                             if (rootNodeNyt.isObject()) {
                                                 existingBookData = objectMapper.convertValue(rootNodeNyt, new TypeReference<Map<String, Object>>() {});
                                             } else {
                                                // If not an array and not an object, cannot proceed with this record for merging.
                                                log.error("Cannot parse existing JSON for NYT key {} as object or array of objects: {}", redisKey, existingJsonString);
                                                notFoundCount.incrementAndGet(); // Or a different counter for parse errors
                                                continue; // Skip this NYT book
                                             }
                                        }
                                        
                                        // Proceed with merge only if existingBookData was successfully parsed
                                        Map<String, Object> nytInfo = Map.of(
                                                "bestseller_date", bestsellerDate != null ? bestsellerDate : "unknown",
                                                "list_name", bookList.getOrDefault("list_name", "unknown"),
                                                "rank", nytBook.getOrDefault("rank", 0),
                                                "rank_last_week", nytBook.getOrDefault("rank_last_week", 0),
                                                "weeks_on_list", nytBook.getOrDefault("weeks_on_list", 0),
                                                "description_nyt", nytBook.getOrDefault("description", ""),
                                                "publisher_nyt", nytBook.getOrDefault("publisher", ""),
                                                "buy_links", nytBook.getOrDefault("buy_links", List.of()),
                                                "book_image_nyt", nytBook.getOrDefault("book_image", "")
                                        );

                                        existingBookData.put("nyt_bestseller_info", nytInfo);
                                        String mergedJsonString = objectMapper.writeValueAsString(existingBookData);
                                        redisJsonService.jsonSet(redisKey, "$", mergedJsonString);
                                        processedCount.incrementAndGet();
                                        if (processedCount.get() % 50 == 0) {
                                            log.info("Merged {} NYT records so far...", processedCount.get());
                                        }
                                    } catch (Exception e) {
                                        log.error("Error merging NYT data for Redis key {}: {}", redisKey, e.getMessage(), e);
                                    }
                                } else {
                                    notFoundCount.incrementAndGet();
                                    log.debug("NYT bestseller with ISBN {} (Title: {}) not found in Redis. Skipping merge.", nytIsbn13, nytBook.get("title"));
                                }
                            }
                        }
                    }
                } else {
                     log.warn("NYT JSON data 'results' does not contain 'lists'. Structure: {}", results != null ? results.keySet() : "null");
                }
            } else {
                log.warn("NYT JSON data does not contain 'results' key or is null. Top-level keys: {}", nytData != null ? nytData.keySet() : "null");
            }
        } catch (Exception e) {
            log.error("Error fetching or processing NYT data from S3 key {}: {}", nytBestsellersKey, e.getMessage(), e);
            // Propagate the error to indicate failure of this phase
            throw new RuntimeException("Failed to merge NYT bestseller data due to: " + e.getMessage(), e);
        }

        // After processing the NYT file, move it
        if (processedCount.get() > 0 || (nytData != null && !nytData.isEmpty())) {
            // Determine the base prefix for NYT data. nytBestsellersKey might be "nyt-bestsellers/latest.json"
            // So, the base prefix would be "nyt-bestsellers/"
            String nytBasePrefix = "";
            int lastSlash = nytBestsellersKey.lastIndexOf('/');
            if (lastSlash > -1) {
                nytBasePrefix = nytBestsellersKey.substring(0, lastSlash + 1);
            }
            
            String destinationNytKey = getProcessedFileDestinationKey(nytBestsellersKey, nytBasePrefix);
            if (destinationNytKey != null) {
                try {
                    s3Service.moveObject(nytBestsellersKey, destinationNytKey);
                    log.info("Moved processed NYT S3 object {} to {}", nytBestsellersKey, destinationNytKey);
                } catch (Exception e) {
                    log.error("Error moving processed NYT S3 object {}: {}", nytBestsellersKey, e.getMessage(), e);
                }
            } else {
                log.warn("Could not determine destination key for NYT file {}, file not moved.", nytBestsellersKey);
            }
        }

        log.info("--- Phase 2 Complete: Merged {} NYT bestseller entries. {} NYT entries not found in Redis. {} NYT entries skipped due to no ISBN. ---",
                processedCount.get(), notFoundCount.get(), skippedNoIsbnCount.get());
    }

    private CompletableFuture<Void> mergeNytBestsellersDataAsync() {
        return CompletableFuture.runAsync(this::mergeNytBestsellersDataSync, mvcTaskExecutor);
    }

    /**
     * Extracts ISBN-13 from NYT Bestsellers book data
     * 
     * @param nytBookData The NYT book data map
     * @return The ISBN-13 if found, null otherwise
     */
    @SuppressWarnings("unchecked")
    private String extractIsbn13FromNytBooks(Map<String, Object> nytBookData) {
        if (nytBookData != null && nytBookData.containsKey("isbns")) {
            try {
                List<Map<String, String>> isbns = (List<Map<String, String>>) nytBookData.get("isbns");
                if (isbns != null) {
                    for (Map<String, String> isbnEntry : isbns) {
                        String isbn13 = isbnEntry.get("isbn13");
                        if (isbn13 != null && !isbn13.isEmpty()) {
                            return isbn13;
                        }
                        // The example also checked for primary_isbn13 within the list entry,
                        // but NYT API usually has isbn13 directly.
                        // String primaryIsbn13 = isbnEntry.get("primary_isbn13");
                        // if (primaryIsbn13 != null && !primaryIsbn13.isEmpty()) {
                        //    return primaryIsbn13;
                        // }
                    }
                }
            } catch (ClassCastException e) {
                 log.warn("Could not cast 'isbns' to List<Map<String, String>> for NYT book: {}", nytBookData.get("title"), e);
            }
        }
        // Fallback for primary_isbn13 at top level, though less common in NYT structure
        // String primaryIsbn13 = (String) nytBookData.get("primary_isbn13");
        // if (primaryIsbn13 != null && !primaryIsbn13.isEmpty()) {
        //     return primaryIsbn13;
        // }
        return null;
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
     * Recursively merges incoming book data into existing data without overwriting existing values
     *
     * @param existing The existing book data
     * @param incoming The incoming book data to merge
     * @return The merged book data
     */
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
}
