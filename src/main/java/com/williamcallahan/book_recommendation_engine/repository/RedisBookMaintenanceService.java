/**
 * Redis cache maintenance and data integrity service for book storage
 * Provides diagnostic tools, repair operations, and data format migration capabilities
 * Ensures cache health without data loss through non-destructive repair processes
 *
 * @author William Callahan
 *
 * Features:
 * - Comprehensive cache integrity diagnostics with detailed statistics
 * - Non-destructive repair operations for corrupted or malformed entries
 * - Data format migration support for schema evolution
 * - Safe cleanup operations that preserve valid book data
 */

package com.williamcallahan.book_recommendation_engine.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.service.RedisCacheService;
import com.williamcallahan.book_recommendation_engine.util.UuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Service
public class RedisBookMaintenanceService {

    private static final Logger logger = LoggerFactory.getLogger(RedisBookMaintenanceService.class);
    private static final String CACHED_BOOK_PREFIX = "book:";

    private final JedisPooled jedisPooled;
    private final ObjectMapper objectMapper;
    private final RedisCacheService redisCacheService;

    public RedisBookMaintenanceService(JedisPooled jedisPooled,
                                       ObjectMapper objectMapper,
                                       RedisCacheService redisCacheService) {
        this.jedisPooled = jedisPooled;
        this.objectMapper = objectMapper;
        this.redisCacheService = redisCacheService;
        logger.info("RedisBookMaintenanceService initialized");
    }

    /**
     * Performs a comprehensive diagnostic scan of Redis cache for data integrity issues
     * This enhanced method checks for:
     * - Valid JSON syntax
     * - Successful deserialization into a CachedBook object
     * - Presence and validity of critical identifiers (id, googleBooksId, isbn10/isbn13, title)
     *
     * @return map containing detailed statistics about the cache integrity
     */
    public Map<String, Integer> diagnoseCacheIntegrity() {
        Map<String, Integer> stats = new HashMap<>();
        stats.put("total_keys_scanned", 0);
        stats.put("valid_json_strings", 0);          // Valid JSON string, deserialized, and critical IDs present
        stats.put("valid_redisjson_objects", 0);     // Valid RedisJSON object, deserialized, and critical IDs present
        stats.put("deserialization_to_cachedbook_failed", 0); // Valid JSON syntax but couldn't map to CachedBook
        stats.put("missing_critical_identifiers", 0); // Deserialized to CachedBook but missing/invalid critical IDs
        stats.put("fully_corrupted_entries", 0);      // e.g., not JSON, null content, or other unrecoverable issues

        if (!redisCacheService.isRedisAvailableAsync().join()) {
            logger.warn("Redis not available for cache integrity diagnosis.");
            return stats;
        }

        logger.info("Starting comprehensive cache integrity diagnosis...");
        String scanPattern = CACHED_BOOK_PREFIX + "*";
        Set<String> keys = new HashSet<>();
        String cursor = "0";
        ScanParams scanParams = new ScanParams().match(scanPattern).count(1000);

        try {
            do {
                ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                keys.addAll(scanResult.getResult());
            } while (!"0".equals(cursor));
        } catch (Exception e) {
            logger.error("Error during Redis SCAN operation: {}", e.getMessage(), e);
            stats.put("scan_error", 1);
            return stats;
        }

        stats.put("total_keys_scanned", keys.size());
        logger.info("Diagnosing {} keys matching pattern '{}'", keys.size(), scanPattern);

        for (String key : keys) {
            if (key.endsWith(":lock")) { // Skip lock keys
                logger.trace("Skipping lock key: {}", key);
                continue;
            }

            boolean entryCorrupted = false;
            String rawContent = null;
            String redisType = "unknown";

            try {
                redisType = jedisPooled.type(key);
                CachedBook book = null;

                if ("string".equalsIgnoreCase(redisType)) {
                    rawContent = jedisPooled.get(key);
                    if (rawContent == null || rawContent.trim().isEmpty()) {
                        logger.warn("Key {} (string) has null or empty content.", key);
                        entryCorrupted = true;
                    } else if (!isValidJson(rawContent)) {
                        logger.warn("Key {} (string) contains invalid JSON syntax. Content snippet: {}", key, rawContent.substring(0, Math.min(100, rawContent.length())));
                        entryCorrupted = true;
                    } else {
                        try {
                            book = objectMapper.readValue(rawContent, CachedBook.class);
                        } catch (JsonProcessingException e) {
                            logger.warn("Key {} (string) is valid JSON but failed to deserialize to CachedBook: {}. Content snippet: {}", key, e.getMessage(), rawContent.substring(0, Math.min(100, rawContent.length())));
                            stats.merge("deserialization_to_cachedbook_failed", 1, Integer::sum);
                            entryCorrupted = true;
                        }
                    }
                } else if ("ReJSON-RL".equalsIgnoreCase(redisType) || "json".equalsIgnoreCase(redisType)) {
                    Object jsonResult = jedisPooled.jsonGet(key);
                    if (jsonResult == null) {
                        logger.warn("Key {} (RedisJSON) has null content.", key);
                        entryCorrupted = true;
                    } else {
                        rawContent = objectMapper.writeValueAsString(jsonResult); // For logging & parsing
                        if (!isValidJson(rawContent)) { // Should be rare for RedisJSON type but good to check
                             logger.warn("Key {} (RedisJSON) surprisingly contains invalid JSON syntax. Content: {}", key, rawContent);
                             entryCorrupted = true;
                        } else {
                            try {
                                book = objectMapper.readValue(rawContent, CachedBook.class);
                            } catch (JsonProcessingException e) {
                                logger.warn("Key {} (RedisJSON) failed to deserialize to CachedBook: {}. Content: {}", key, e.getMessage(), rawContent);
                                stats.merge("deserialization_to_cachedbook_failed", 1, Integer::sum);
                                entryCorrupted = true;
                            }
                        }
                    }
                } else if ("none".equalsIgnoreCase(redisType)) {
                    logger.warn("Key {} reported as 'none' type during scan, but was found by SCAN. Skipping.", key);
                    continue; // Skip if key disappeared or is an anomaly
                } else {
                    logger.warn("Key {} has an unexpected Redis type: {}. Marking as corrupted.", key, redisType);
                    entryCorrupted = true;
                }

                if (book != null) { // Deserialization was successful
                    boolean criticalIdsValid = true;
                    StringBuilder missingIdsLog = new StringBuilder();

                    if (book.getId() == null || book.getId().trim().isEmpty() || !UuidUtil.isUuid(book.getId())) {
                        criticalIdsValid = false;
                        missingIdsLog.append("id (null, empty, or not UUID); ");
                    }
                    if (book.getGoogleBooksId() == null || book.getGoogleBooksId().trim().isEmpty()) {
                        criticalIdsValid = false;
                        missingIdsLog.append("googleBooksId; ");
                    }
                    if (book.getTitle() == null || book.getTitle().trim().isEmpty()) {
                        criticalIdsValid = false;
                        missingIdsLog.append("title; ");
                    }
                    boolean hasIsbn = (book.getIsbn10() != null && !book.getIsbn10().trim().isEmpty()) ||
                                      (book.getIsbn13() != null && !book.getIsbn13().trim().isEmpty());
                    if (!hasIsbn) {
                        criticalIdsValid = false;
                        missingIdsLog.append("isbn10/isbn13; ");
                    }

                    if (criticalIdsValid) {
                        if ("string".equalsIgnoreCase(redisType)) {
                            stats.merge("valid_json_strings", 1, Integer::sum);
                        } else {
                            stats.merge("valid_redisjson_objects", 1, Integer::sum);
                        }
                    } else {
                        logger.warn("Key {} deserialized to CachedBook but is missing/invalid critical identifiers: {}", key, missingIdsLog.toString());
                        stats.merge("missing_critical_identifiers", 1, Integer::sum);
                        entryCorrupted = true;
                    }
                }

                if (entryCorrupted) {
                    stats.merge("fully_corrupted_entries", 1, Integer::sum);
                }

            } catch (Exception e) {
                logger.error("Unexpected error diagnosing key {}: {}", key, e.getMessage(), e);
                stats.merge("fully_corrupted_entries", 1, Integer::sum); // Count as corrupted if any error occurs
            }
        }
        logger.info("Cache integrity diagnosis complete. Stats: {}", stats);
        return stats;
    }

    /**
     * Repairs corrupted cache entries using non-destructive methods
     * Identifies and fixes malformed data while preserving all valid information
     *
     * @param dryRun when true, performs analysis only without making changes
     * @return count of keys identified for repair (dry run) or successfully repaired
     */
    public int repairCorruptedCache(boolean dryRun) {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            logger.warn("Redis not available for cache repair");
            return 0;
        }

        int repairedCount = 0;
        String scanPattern = CACHED_BOOK_PREFIX + "*";
        logger.info("Starting cache repair scan. Dry run: {}", dryRun);
        long startTime = System.currentTimeMillis();

        try {
            Set<String> keys = new HashSet<>();
            String cursor = "0";
            ScanParams scanParams = new ScanParams().match(scanPattern).count(1000);
            do {
                ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                keys.addAll(scanResult.getResult());
            } while (!"0".equals(cursor));

            logger.info("Scanned {} cache keys in {}ms", keys.size(), System.currentTimeMillis() - startTime);

            Set<String> keysToRepair = new HashSet<>();
            int processed = 0;
            int validKeys = 0;

            logger.info("Analyzing {} keys for repair needs...", keys.size());
            for (String key : keys) {
                if (key.startsWith(CACHED_BOOK_PREFIX) && !key.endsWith(":lock")) {
                    processed++;
                    boolean needsRepair = false;
                    try {
                        String actualTypeString = jedisPooled.type(key).toLowerCase();
                        if ("string".equalsIgnoreCase(actualTypeString)) {
                            String value = jedisPooled.get(key);
                            if (value == null || !isValidJson(value)) {
                                needsRepair = true; // Invalid JSON or null content
                            } else {
                                // Try to parse as CachedBook to validate structure
                                CachedBook book = objectMapper.readValue(value, CachedBook.class);
                                if (book.getId() == null || book.getId().trim().isEmpty()) {
                                    needsRepair = true; // Missing required ID field
                                }
                            }
                        } else if ("none".equalsIgnoreCase(actualTypeString)) {
                            continue; // Key doesn't exist
                        } else {
                            // For ReJSON-RL or other types, assume valid for now if they exist and are not string
                            // More detailed checks can be added if needed (e.g., try jsonGet and parse)
                            validKeys++;
                            continue;
                        }

                        if (needsRepair) {
                            keysToRepair.add(key);
                        } else {
                            validKeys++;
                        }
                    } catch (Exception e) {
                        logger.debug("Error analyzing key {} for repair: {}", key, e.getMessage());
                        // Potentially mark for repair if analysis itself fails due to bad data
                        keysToRepair.add(key);
                    }
                    if (processed % 100 == 0) {
                        logger.info("Repair analysis progress: {}/{} keys analyzed", processed, keys.size());
                    }
                }
            }

            int numKeysNeedingRepair = keysToRepair.size();
            logger.info("Analysis complete: {} valid, {} need repair out of {} processed book-prefixed keys",
                       validKeys, numKeysNeedingRepair, processed);

            if (!keysToRepair.isEmpty()) {
                if (!dryRun) {
                    logger.info("Attempting to repair {} keys with issues...", numKeysNeedingRepair);
                    repairedCount = attemptKeyRepairs(keysToRepair);
                    logger.info("Successfully attempted repair on {} keys, {} actually modified/repaired.", numKeysNeedingRepair, repairedCount);
                } else {
                    logger.info("DRY RUN: Would attempt to repair {} keys.", numKeysNeedingRepair);
                    repairedCount = numKeysNeedingRepair; // For dry run, count of keys that would be processed
                }
            } else {
                logger.info("No keys identified as needing repair - cache appears healthy based on current checks.");
            }
        } catch (Exception e) {
            logger.error("Error during cache repair process: {}", e.getMessage(), e);
        }

        long totalTime = System.currentTimeMillis() - startTime;
        logger.info("Cache repair process completed in {}ms. {} keys {}",
                   totalTime, repairedCount, dryRun ? "identified for repair (dry run)" : "processed for repair");
        return repairedCount;
    }

    private int attemptKeyRepairs(Set<String> keysToRepair) {
        int actuallyRepairedCount = 0;
        for (String key : keysToRepair) {
            try {
                String content = jedisPooled.get(key); // Assuming string type for repairable content
                if (content != null) {
                    CachedBook book = objectMapper.readValue(content, CachedBook.class);
                    boolean needsUpdate = false;
                    if (book.getId() == null || book.getId().trim().isEmpty()) {
                        String potentialId = key.substring(CACHED_BOOK_PREFIX.length());
                        if (!potentialId.isEmpty() && UuidUtil.isUuid(potentialId)) {
                            book.setId(potentialId);
                            needsUpdate = true;
                        } else {
                            logger.warn("Could not derive a valid UUID from key {} for repair", key);
                        }
                    }

                    if (needsUpdate) {
                        String repairedJson = objectMapper.writeValueAsString(book);
                        jedisPooled.set(key, repairedJson);
                        actuallyRepairedCount++;
                        logger.debug("Repaired book data at key: {}", key);
                    }
                } else {
                    logger.warn("Key {} for repair has null content. Skipping.", key);
                }
            } catch (JsonProcessingException e) {
                logger.warn("Could not parse/repair malformed JSON at key {}: {}", key, e.getMessage());
            } catch (Exception e) {
                logger.error("Error during repair attempt for key {}: {}", key, e.getMessage());
            }
        }
        return actuallyRepairedCount;
    }

    /**
     * Validates JSON string structure for basic syntax correctness
     *
     * @param jsonString string content to validate
     * @return true if valid JSON syntax, false otherwise
     */
    private boolean isValidJson(String jsonString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            return false;
        }
        try {
            objectMapper.readTree(jsonString);
            return true;
        } catch (JsonProcessingException e) {
            return false;
        }
    }
    
    /**
     * Migrates book data format to current schema without data loss
     * Performs safe schema evolution and structure validation
     *
     * @return count of entries processed during migration
     */
    public int migrateBookDataFormat() {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            return 0;
        }
        
        int migrated = 0;
        String bookPattern = CACHED_BOOK_PREFIX + "*";
        logger.info("Starting safe book data format migration (currently checks for parsable CachedBook)...");
        
        try {
            String cursor = "0";
            ScanParams scanParams = new ScanParams().match(bookPattern).count(100);
            
            do {
                ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                
                for (String key : scanResult.getResult()) {
                    if (key.endsWith(":lock")) continue; // Skip lock keys
                    try {
                        String keyType = jedisPooled.type(key);
                        if ("string".equals(keyType)) {
                            String content = jedisPooled.get(key);
                            if (content != null && (content.contains("\"googleBooksId\"") || content.contains("\"title\""))) {
                                try {
                                    CachedBook book = objectMapper.readValue(content, CachedBook.class);
                                    if (book.getId() != null && !book.getId().trim().isEmpty()) {
                                        logger.trace("Found valid book data at key: {}", key);
                                        migrated++;
                                    } else {
                                        logger.warn("Book data at key {} parsed but has null/empty ID.", key);
                                    }
                                } catch (JsonProcessingException e) {
                                    logger.warn("Found malformed JSON at key {} during migration check: {}", key, e.getMessage());
                                }
                            }
                        } else if ("ReJSON-RL".equalsIgnoreCase(keyType) || "json".equalsIgnoreCase(keyType)) {
                            logger.trace("Found RedisJSON type key {} during migration scan, assuming valid for now", key);
                            migrated++;
                        }
                    } catch (Exception e) {
                        logger.warn("Error processing key {} during format migration: {}", key, e.getMessage());
                    }
                }
            } while (!"0".equals(cursor));
            
            if (migrated > 0) {
                logger.info("Format migration scan: Processed/verified {} book data entries", migrated);
            } else {
                logger.info("Format migration scan: No book data found or processed");
            }
        } catch (Exception e) {
            logger.error("Error during format migration scan: {}", e.getMessage(), e);
        }
        return migrated;
    }
}
