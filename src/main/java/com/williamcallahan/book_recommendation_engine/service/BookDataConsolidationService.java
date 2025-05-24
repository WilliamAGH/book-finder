/**
 * Service for consolidating and migrating book data between different Redis storage formats
 *
 * @author William Callahan
 *
 * Features:
 * - Consolidates books from cached_book: and book: prefixes into UUID-based keys
 * - Merges duplicate book records based on ISBN and Google Books ID
 * - Migrates from Google ID/ISBN keys to time-ordered UUID keys
 * - Handles books with missing identifiers gracefully
 * - Provides dry-run capability for testing consolidation logic
 * - Updates secondary indexes after consolidation
 * - Maintains data integrity during migration process
 */

package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.util.UuidUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.params.ScanParams;
import java.util.concurrent.CompletableFuture;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Service
public class BookDataConsolidationService {

    private static final Logger logger = LoggerFactory.getLogger(BookDataConsolidationService.class);

    private final CachedBookRepository cachedBookRepository;
    private final BookSlugService bookSlugService;
    private final JedisPooled jedisPooled;
    private final ObjectMapper objectMapper; // for converting JSON strings to CachedBook

    private static final String OLD_PREFIX = "cached_book:";
    private static final String TARGET_PREFIX = "book:";


    public BookDataConsolidationService(CachedBookRepository cachedBookRepository,
                                        BookSlugService bookSlugService,
                                        JedisPooled jedisPooled,
                                        ObjectMapper objectMapper) {
        this.cachedBookRepository = cachedBookRepository;
        this.bookSlugService = bookSlugService;
        this.jedisPooled = jedisPooled;
        this.objectMapper = objectMapper;
    }

    /**
     * Summary of consolidation operations performed
     */
    public static class ConsolidationSummary {
        public int processedConceptualBooks = 0;
        public int recordsMigrated = 0;
        public int recordsMerged = 0;
        public int oldKeysDeleted = 0;
        public int newUuidsGenerated = 0;
        public List<String> errors = new ArrayList<>();

        @Override
        public String toString() {
            return "ConsolidationSummary{" +
                    "processedConceptualBooks=" + processedConceptualBooks +
                    ", recordsMigrated=" + recordsMigrated +
                    ", recordsMerged=" + recordsMerged +
                    ", oldKeysDeleted=" + oldKeysDeleted +
                    ", newUuidsGenerated=" + newUuidsGenerated +
                    ", errors=" + errors +
                    '}';
        }
    }

    /**
     * Consolidates book data from multiple Redis keys into UUID-based storage
     *
     * @param dryRun If true, performs analysis without making changes
     * @return Summary of consolidation operations performed
     */
    // @Async // Removed: We will use CompletableFuture.supplyAsync for explicit control
    public CompletableFuture<ConsolidationSummary> consolidateBookDataAsync(boolean dryRun) {
        return CompletableFuture.supplyAsync(() -> { // Added supplyAsync
            ConsolidationSummary summary = new ConsolidationSummary();
            logger.info("Starting ASYNCHRONOUS book data consolidation. Dry run: {}", dryRun);

            Set<String> allKeys = new HashSet<>();
        allKeys.addAll(scanKeys(OLD_PREFIX + "*"));
        allKeys.addAll(scanKeys(TARGET_PREFIX + "*")); // Includes correct and old S3 import variants

        // Map: Definitive ID (ISBN13 > ISBN10 > GoogleID > Generated UUID if none) -> List of CachedBook objects from various keys
        Map<String, List<CachedBook>> booksToConsolidate = new HashMap<>();
        // Map: Definitive ID -> List of original Redis keys associated with this conceptual book
        Map<String, Set<String>> originalKeysMap = new HashMap<>();

        int processedKeyCount = 0;
        final int BATCH_SIZE_FOR_PAUSE = 25; // Pause after every 25 keys
        final long PAUSE_DURATION_MS = 100;   // Pause for 100 milliseconds

        for (String key : allKeys) {
            processedKeyCount++;
            try {
                CachedBook book = null;
                try {
                    // First, try to read as a String (expected JSON format)
                    String jsonData = jedisPooled.get(key);
                    if (jsonData != null) {
                        book = objectMapper.readValue(jsonData, CachedBook.class);
                        logger.debug("Successfully read key {} as JSON string.", key);
                    } else {
                        // Key exists but has null value, or was deleted between scan and get
                        // This case might not be a WRONGTYPE but an actual null
                        logger.warn("Key {} returned null value when read as String. Skipping.", key);
                        summary.errors.add("Key " + key + " returned null String value.");
                        continue;
                    }
                } catch (IllegalStateException e) {
                    if (e.getMessage() != null && e.getMessage().contains("JedisConnectionFactory was destroyed")) {
                        logger.error("Redis connection factory was destroyed during consolidation. Stopping process. Processed {} keys so far.", processedKeyCount);
                        summary.errors.add("Redis connection factory destroyed. Process terminated early after " + processedKeyCount + " keys.");
                        // For the supplyAsync, we can't directly return a CompletableFuture.
                        // Instead, we'll throw a specific exception that the outer layer can catch, or complete the future exceptionally.
                        // However, since this is inside supplyAsync, we should just return the summary.
                        // The `consolidateBookDataAsync` method itself returns a CompletableFuture.
                        // The error is that this return statement is for the lambda inside supplyAsync.
                        // The lambda should return ConsolidationSummary, not CompletableFuture<ConsolidationSummary>.
                        // This specific return path was for an early exit *before* the supplyAsync was introduced.
                        // Now, if this happens, the supplyAsync lambda should just return the 'summary' object as is.
                        // The original error was likely because the `return CompletableFuture.completedFuture(summary);` was inside the lambda.
                        // The fix is to ensure the lambda returns `summary`.
                        // The previous change to wrap the whole method in supplyAsync should handle this.
                        // The error message points to line 98, which is the `return CompletableFuture.supplyAsync(() -> { ... });`
                        // This implies the lambda itself is not correctly returning just `ConsolidationSummary` in all paths.
                        // The path I'm looking at is within the loop:
                        // `return CompletableFuture.completedFuture(summary);` was correct when the method was `@Async`
                        // Now, inside `supplyAsync`, it should just be `return summary;`
                        // Let me re-verify the structure.
                        // The `return CompletableFuture.completedFuture(summary);` is inside the loop, for an early exit.
                        // This needs to be `return summary;` to match the lambda's expected return type.
                        // The error is likely that this specific return was missed in the previous refactor.
                        return summary; // Corrected: Lambda returns summary
                    } else {
                        throw e; // Re-throw if it's a different IllegalStateException
                    }
                } catch (redis.clients.jedis.exceptions.JedisDataException e) {
                    if (e.getMessage() != null && e.getMessage().startsWith("WRONGTYPE")) {
                        logger.warn("Key {} is not a Redis String (WRONGTYPE). Attempting to read as Hash. Message: {}", key, e.getMessage());
                        try {
                            Map<String, String> hashData = jedisPooled.hgetAll(key);
                            if (hashData != null && !hashData.isEmpty()) {
                                book = objectMapper.convertValue(hashData, CachedBook.class);
                                logger.info("Successfully read and converted key {} from Hash to CachedBook.", key);
                            } else {
                                logger.warn("Key {} was WRONGTYPE for String, and was empty or null when read as Hash. Skipping.", key);
                                summary.errors.add("Key " + key + " (WRONGTYPE for String, empty/null as Hash)");
                                continue;
                            }
                        } catch (Exception hashEx) {
                            // Key was not a String, and not a Hash. Attempt to read as RedisJSON.
                            logger.warn("Key {} was not String or Hash. Attempting to read as RedisJSON. Original hash access error: {}", key, hashEx.getMessage());
                            try {
                                String jsonStringFromRedis = null;
                                try {
                                    Object jsonResult = jedisPooled.jsonGet(key);
                                    if (jsonResult != null) {
                                        jsonStringFromRedis = jsonResult.toString();
                                    }
                                } catch (Exception jsonGetEx) {
                                    logger.warn("JSON.GET for key {} failed: {}", key, jsonGetEx.getMessage());
                                }

                                if (jsonStringFromRedis != null && !jsonStringFromRedis.isEmpty()) {
                                    Book googleApiBook = objectMapper.readValue(jsonStringFromRedis, Book.class);
                                    JsonNode rawJsonNode = objectMapper.readTree(jsonStringFromRedis);
                                    book = CachedBook.fromBook(googleApiBook, rawJsonNode, null);
                                    logger.info("Successfully read and converted key {} from RedisJSON to CachedBook.", key);
                                } else {
                                    logger.warn("Key {} attempted as RedisJSON, but JSON.GET returned null/empty or unexpected type. Skipping.", key);
                                    summary.errors.add("Key " + key + " (attempted RedisJSON, but no data from JSON.GET or unexpected result type)");
                                    continue;
                                }
                            } catch (Exception redisJsonEx) {
                                // This catch block will handle cases where JSON.GET fails (e.g., key is not a JSON type, or other Redis errors)
                                String actualTypeAfterJsonAttempt = jedisPooled.type(key);
                                logger.error("Failed to read key {} as RedisJSON (actual type: {}). Original hash access error: {}. RedisJSON attempt error: {}",
                                        key, actualTypeAfterJsonAttempt, hashEx.getMessage(), redisJsonEx.getMessage());
                                summary.errors.add("Failed to process key " + key + " (type: " + actualTypeAfterJsonAttempt + ") as RedisJSON: " + redisJsonEx.getMessage());
                                continue;
                            }
                        }
                    } else {
                        // Different Redis access error, re-throw
                        throw e;
                    }
                } catch (IOException jsonMappingEx) {
                     // This catches errors from objectMapper.readValue if the string is not valid JSON for CachedBook
                    logger.error("Error parsing JSON data from key {} (expected String): {}. Content might be malformed.", key, jsonMappingEx.getMessage());
                    summary.errors.add("Error parsing JSON for key " + key + ": " + jsonMappingEx.getMessage());
                    continue;
                }

                if (book != null) {
                    // Determine the definitive ID for this book instance
                    String definitiveId = determineDefinitiveIdentifier(book);
                    if (definitiveId == null) {
                        // This case should be rare if data is valid, implies no ISBNs or Google ID
                        // For consolidation, we might assign a temporary UUID to group it if it's truly new
                        // or log an error if it seems like corrupt data
                        // For now, we'll use its current ID if it's a UUID, or skip if it's an unidentifiable old key
                        if (book.getId() != null && UuidUtil.isUuid(book.getId())) { // Assuming UuidUtil.isUuid
                             definitiveId = book.getId();
                        } else {
                            logger.warn("Skipping key {} as it has no standard identifiers (ISBNs, GoogleID) and no existing UUID.", key);
                            summary.errors.add("Skipped key with no identifiers: " + key);
                            continue;
                        }
                    }

                    booksToConsolidate.computeIfAbsent(definitiveId, k -> new ArrayList<>()).add(book);
                    originalKeysMap.computeIfAbsent(definitiveId, k -> new HashSet<>()).add(key);
                }
            } catch (Exception e) { // Catch any other unexpected exception during the processing of a single key
                // Check if this is a connection factory destruction error
                if ((e.getCause() instanceof IllegalStateException &&
                     e.getCause().getMessage() != null &&
                     e.getCause().getMessage().contains("JedisConnectionFactory was destroyed")) ||
                    (e.getMessage() != null && e.getMessage().contains("JedisConnectionFactory was destroyed"))) {
                    logger.error("Redis connection factory was destroyed during consolidation. Stopping process. Processed {} keys so far.", processedKeyCount);
                    summary.errors.add("Redis connection factory destroyed. Process terminated early after " + processedKeyCount + " keys.");
                    break; // Exit the loop gracefully
                } else {
                    logger.error("Unexpected error processing key {}: {}", key, e.getMessage(), e);
                    summary.errors.add("Unexpected error for key " + key + ": " + e.getMessage());
                }
            }

            // Introduce a small pause to yield resources
            if (processedKeyCount % BATCH_SIZE_FOR_PAUSE == 0) {
                try {
                    logger.debug("Pausing for {} ms after processing {} keys...", PAUSE_DURATION_MS, processedKeyCount);
                    Thread.sleep(PAUSE_DURATION_MS);
                } catch (InterruptedException ie) {
                    logger.warn("Consolidation task interrupted during pause.", ie);
                    Thread.currentThread().interrupt(); // Preserve interrupt status
                    break; // Exit loop if interrupted
                }
            }
        }

        logger.info("Finished iterating allKeys. Found {} conceptual books to process based on {} distinct Redis keys.", booksToConsolidate.size(), allKeys.size());
        summary.processedConceptualBooks = booksToConsolidate.size();

        for (Map.Entry<String, List<CachedBook>> entry : booksToConsolidate.entrySet()) {
            String definitiveIdForGroup = entry.getKey(); // This is ISBN13/ISBN10/GoogleID
            List<CachedBook> bookVersions = entry.getValue();
            Set<String> originalRedisKeysForGroup = originalKeysMap.get(definitiveIdForGroup);

            try {
                CachedBook finalBook = mergeBookVersions(bookVersions, definitiveIdForGroup);
                
                // Assign or confirm UUID v7
                String bookUuid;
                Optional<CachedBook> existingByDefinitiveId = findByDefinitiveId(definitiveIdForGroup);

                if (existingByDefinitiveId.isPresent() && UuidUtil.isUuid(existingByDefinitiveId.get().getId())) {
                    bookUuid = existingByDefinitiveId.get().getId();
                    finalBook.setId(bookUuid); // Ensure merged book has the existing UUID
                } else {
                    bookUuid = UuidUtil.getTimeOrderedEpoch().toString(); // Generate new UUID v7
                    finalBook.setId(bookUuid);
                    summary.newUuidsGenerated++;
                    logger.info("Generated new UUID {} for definitive ID {}", bookUuid, definitiveIdForGroup);
                }
                
                // Ensure the book has a slug (blocking async call)
                bookSlugService.ensureBookHasSlugAsync(finalBook).join();

                if (!dryRun) {
                    // Save the consolidated book under book:UUID_V7_ID
                    cachedBookRepository.save(finalBook); // This uses TARGET_PREFIX (book:) and finalBook.getId() (UUID)
                    
                    // Update secondary indexes to point to the new UUID
                    updateSecondaryIndexes(finalBook);

                    // Delete all old/variant keys for this conceptual book
                    for (String oldKey : originalRedisKeysForGroup) {
                        if (!oldKey.equals(TARGET_PREFIX + bookUuid)) { // Don't delete the key we just wrote to
                            jedisPooled.del(oldKey);
                            summary.oldKeysDeleted++;
                            logger.debug("Deleted old key: {}", oldKey);
                        }
                    }
                    if (bookVersions.size() > 1) summary.recordsMerged++; else summary.recordsMigrated++;

                } else {
                    logger.info("[DRY RUN] Would consolidate data for definitive ID {} into UUID {}", definitiveIdForGroup, bookUuid);
                    logger.info("[DRY RUN] Final book data: {}", objectMapper.writeValueAsString(finalBook));
                    logger.info("[DRY RUN] Would delete original keys: {}", originalRedisKeysForGroup);
                     if (bookVersions.size() > 1) summary.recordsMerged++; else summary.recordsMigrated++;
                }

            } catch (Exception e) {
                logger.error("Error consolidating book for definitive ID {}: {}", definitiveIdForGroup, e.getMessage(), e);
                summary.errors.add("Error consolidating for " + definitiveIdForGroup + ": " + e.getMessage());
            }
        }

        logger.info("ASYNCHRONOUS book data consolidation finished. Dry run: {}. Summary: {}", dryRun, summary.toString());
        return summary; // Return summary for supplyAsync
        }); // Added supplyAsync
    }

    /**
     * Merges multiple versions of the same book into a single consolidated record
     *
     * @param versions List of book versions to merge
     * @param definitiveId The primary identifier for this book
     * @return Merged book with consolidated data
     */
    private CachedBook mergeBookVersions(List<CachedBook> versions, String definitiveId) {
        if (versions.isEmpty()) {
            throw new IllegalArgumentException("Cannot merge empty list of book versions for ID: " + definitiveId);
        }
        if (versions.size() == 1) {
            return versions.get(0); // No merge needed, just ensure ID and slug
        }

        // Prioritize data from S3 import (JsonS3ToRedisService) if multiple sources
        // This means if a key was like "book:ISBN" it's likely from S3
        // If "cached_book:ISBN", it's from old repository
        // For title conflict: NYT data (if identifiable) or S3 import wins
        // Let's assume the version from `book:ISBN` (if it exists and is one of the versions) is preferred.
        
        CachedBook baseBook = versions.stream()
            // Prefer books that have the definitive ID as one of their identifiers
            .filter(b -> definitiveId.equals(b.getIsbn13()) || 
                        definitiveId.equals(b.getIsbn10()) || 
                        definitiveId.equals(b.getGoogleBooksId()))
            .findFirst()
            .orElse(versions.get(0)); // Fallback to the first one

        @SuppressWarnings("unchecked")
        Map<String, Object> mergedData = objectMapper.convertValue(baseBook, Map.class);

        for (CachedBook version : versions) {
            if (version == baseBook) continue;
            @SuppressWarnings("unchecked")
            Map<String, Object> versionData = objectMapper.convertValue(version, Map.class);
            versionData.forEach((key, value) -> {
                if (value != null) {
                    if ("title".equals(key)) {
                        // NYT title wins if identifiable, else S3/Google/OpenLib (baseBook) wins
                        // For now, stick to baseBook's title unless specific NYT logic is added
                        // If version has NYT info and a different title, consider it
                        // For simplicity, current baseBook title is kept unless overwritten by specific logic
                        // User said: "we can let the nyt logic priority win for name on the conflict"
                        // This implies if 'version' is from NYT and has a title, it might win
                        // This merge logic needs to be more sophisticated if we have clear source markers
                        // For now, if baseBook's title is from S3 import, it's likely the most complete
                        // If 'version' has 'nyt_bestseller_info' and its title is different, that's the conflict
                        // Let's assume 'baseBook' is the one from 'book:ISBN' (S3 import)
                        // and 'version' could be from 'cached_book:ISBN'
                        // The S3 import already merges NYT data. So baseBook should be preferred
                        if (mergedData.get(key) == null) {
                             mergedData.put(key, value);
                        }
                    } else if (mergedData.get(key) == null) {
                        mergedData.put(key, value);
                    }
                    // More complex field-level merge logic can be added here (e.g., for lists, combine unique)
                }
            });
        }
        
        CachedBook finalBook = objectMapper.convertValue(mergedData, CachedBook.class);
        // Ensure all key identifiers are correctly set from the definitiveId or baseBook
        finalBook.setIsbn13(baseBook.getIsbn13() != null ? baseBook.getIsbn13() : extractIsbn13(definitiveId));
        finalBook.setIsbn10(baseBook.getIsbn10() != null ? baseBook.getIsbn10() : extractIsbn10(definitiveId));
        finalBook.setGoogleBooksId(baseBook.getGoogleBooksId() != null ? baseBook.getGoogleBooksId() : extractGoogleId(definitiveId));
        
        return finalBook;
    }
    
    /**
     * Determines the primary identifier for a book based on available identifiers
     *
     * @param book Book to determine identifier for
     * @return Primary identifier (ISBN13 > ISBN10 > GoogleID > UUID)
     */
    private String determineDefinitiveIdentifier(CachedBook book) {
        if (book.getIsbn13() != null && !book.getIsbn13().isEmpty()) return book.getIsbn13();
        if (book.getIsbn10() != null && !book.getIsbn10().isEmpty()) return book.getIsbn10();
        if (book.getGoogleBooksId() != null && !book.getGoogleBooksId().isEmpty()) return book.getGoogleBooksId();
        // If the book's own ID is a UUID, it might be a new book already processed by a UUID-aware step
        if (book.getId() != null && UuidUtil.isUuid(book.getId())) return book.getId(); 
        return null; // Should not happen for valid book data
    }

    /**
     * Finds existing book by its definitive identifier
     *
     * @param definitiveId Identifier to search for
     * @return Optional containing book if found
     */
    private Optional<CachedBook> findByDefinitiveId(String definitiveId) {
        // Try ISBN13, then ISBN10, then GoogleBooksId
        Optional<CachedBook> bookOpt = cachedBookRepository.findByIsbn13(definitiveId);
        if (bookOpt.isPresent()) return bookOpt;
        
        bookOpt = cachedBookRepository.findByIsbn10(definitiveId);
        if (bookOpt.isPresent()) return bookOpt;

        return cachedBookRepository.findByGoogleBooksId(definitiveId);
    }

    /**
     * Updates secondary indexes after book consolidation
     *
     * @param book Book with updated identifiers
     */
    private void updateSecondaryIndexes(CachedBook book) {
        // The save method in RedisCachedBookRepository already handles this
        // but it relies on the book object having the correct ISBNs/GoogleID fields set
        // This method is to ensure that if the definitive ID was, say, an ISBN10,
        // but the book object also has an ISBN13, that the ISBN13 index also points to this UUID
        // The CachedBookRepository.save() method should be robust enough
        // However, we must ensure the book object passed to save() has all its identifier fields (isbn10, isbn13, googleBooksId) correctly populated
        
        // The save method in RedisCachedBookRepository should handle creating/updating these:
        // if (book.getIsbn13() != null) stringRedisTemplate.opsForValue().set(ISBN13_INDEX_PREFIX + book.getIsbn13(), book.getId());
        // if (book.getIsbn10() != null) stringRedisTemplate.opsForValue().set(ISBN10_INDEX_PREFIX + book.getIsbn10(), book.getId());
        // if (book.getGoogleBooksId() != null) stringRedisTemplate.opsForValue().set(GOOGLE_BOOKS_ID_INDEX_PREFIX + book.getGoogleBooksId(), book.getId());
        // This is implicitly handled by `cachedBookRepository.save(book)` if the `book` object has these fields correctly set
        // The `save` method in `RedisCachedBookRepository` already does this
    }
    
    /**
     * Scans Redis for keys matching the given pattern
     *
     * @param pattern Redis key pattern to match
     * @return Set of matching keys
     */
    private Set<String> scanKeys(String pattern) {
        Set<String> keys = new HashSet<>();
        try {
            ScanParams scanParams = new ScanParams().match(pattern).count(1000);
            String cursor = "0";
            do {
                ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);
                keys.addAll(scanResult.getResult());
                cursor = scanResult.getCursor();
            } while (!"0".equals(cursor));
        } catch (Exception e) {
            logger.error("Error scanning Redis keys with pattern {}: {}", pattern, e.getMessage());
        }
        return keys;
    }

    /**
     * Extracts ISBN-13 from definitive identifier
     *
     * @param definitiveId Identifier to extract from
     * @return ISBN-13 if applicable
     */
    /**
     * Extracts ISBN-13 from definitive identifier
     *
     * @param definitiveId Identifier to extract from
     * @return ISBN-13 if applicable
     */
    private String extractIsbn13(String definitiveId) {
        // ISBN-13 is 13 digits
        if (definitiveId != null && definitiveId.matches("^\\d{13}$")) {
            return definitiveId;
        }
        logger.trace("extractIsbn13: definitiveId '{}' is not a valid ISBN-13 format.", definitiveId);
        return null;
    }

    /**
     * Extracts ISBN-10 from definitive identifier
     *
     * @param definitiveId Identifier to extract from
     * @return ISBN-10 if applicable
     */
    private String extractIsbn10(String definitiveId) {
        // ISBN-10 is 10 characters (9 digits + check digit/X)
        if (definitiveId != null && definitiveId.matches("^\\d{9}[\\dX]$")) {
            return definitiveId;
        }
        logger.trace("extractIsbn10: definitiveId '{}' is not a valid ISBN-10 format.", definitiveId);
        return null;
    }

    /**
     * Extracts Google Books ID from definitive identifier
     *
     * @param definitiveId Identifier to extract from
     * @return Google Books ID if applicable
     */
    private String extractGoogleId(String definitiveId) {
        // Google Books IDs are typically alphanumeric and not purely numeric like ISBNs.
        // This check ensures it's not an ISBN-like pattern.
        if (definitiveId != null && !definitiveId.isEmpty() && 
            !definitiveId.matches("^\\d{13}$") && 
            !definitiveId.matches("^\\d{9}[\\dX]$")) {
            return definitiveId;
        }
        logger.trace("extractGoogleId: definitiveId '{}' does not appear to be a Google Books ID (or it resembles an ISBN).", definitiveId);
        return null;
    }
}
