/**
 * Direct Redis data access layer for book cache operations
 * Provides low-level CRUD operations and JSON serialization for CachedBook entities in Redis
 * Handles both string-based and RedisJSON format storage with automatic type detection
 *
 * @author William Callahan
 *
 * Features:
 * - Low-level CRUD operations with TTL support for Redis book cache
 * - JSON serialization and deserialization for CachedBook entities
 * - Automatic format detection between string and RedisJSON storage types
 * - Safe key scanning with lock detection and malformed data filtering
 */

package com.williamcallahan.book_recommendation_engine.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.json.Path2;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisPooled;

import java.util.Optional;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;
@Service
public class RedisBookAccessor {

    private static final Logger logger = LoggerFactory.getLogger(RedisBookAccessor.class);
    private static final String CACHED_BOOK_PREFIX = "book:";

    private final JedisPooled jedisPooled;
    private final ObjectMapper objectMapper;

    public RedisBookAccessor(JedisPooled jedisPooled, ObjectMapper objectMapper) {
        this.jedisPooled = jedisPooled;
        this.objectMapper = objectMapper;
        logger.info("RedisBookAccessor initialized");
    }

    private String getCachedBookKey(String bookId) {
        return CACHED_BOOK_PREFIX + bookId;
    }

    /**
     * Saves book JSON data to Redis with expiration time
     *
     * @param bookId     book identifier for Redis key generation
     * @param bookJson   serialized JSON representation of the book data
     * @param ttlSeconds expiration time in seconds for the Redis key
     */
    public void saveJson(String bookId, String bookJson, long ttlSeconds) {
        String key = getCachedBookKey(bookId);
        try {
            jedisPooled.setex(key, ttlSeconds, bookJson);
            logger.debug("Saved JSON for book ID {} with TTL {} seconds.", bookId, ttlSeconds);
        } catch (Exception e) {
            logger.error("Error saving JSON for book ID {}: {}", bookId, e.getMessage(), e);
        }
    }

    /**
     * Retrieves book JSON data from Redis by book identifier
     *
     * @param bookId unique book identifier
     * @return Optional containing JSON string if found, empty if not found or invalid
     */
    public Optional<String> findJsonById(String bookId) {
        String key = getCachedBookKey(bookId);
        try {
            if (!jedisPooled.exists(key)) {
                logger.debug("Book key {} does not exist.", key);
                return Optional.empty();
            }
            // Prevent reading lock keys as book data
            if (key.endsWith(":lock")) {
                logger.debug("Skipping lock key: {}", key);
                return Optional.empty();
            }
            String bookJson = jedisPooled.get(key);
            if (bookJson != null) {
                // Validate it's actually JSON and not a lock value or other short, non-JSON string
                if (bookJson.equals("locked") || bookJson.length() < 10 || !isValidJsonStructure(bookJson)) { // Basic check
                    logger.warn("Skipping non-book or malformed data for key: {}. Content snippet: {}", key, bookJson.substring(0, Math.min(bookJson.length(), 50)));
                    return Optional.empty();
                }
                return Optional.of(bookJson);
            }
            return Optional.empty();
        } catch (redis.clients.jedis.exceptions.JedisDataException jde) {
            if (jde.getMessage() != null && jde.getMessage().startsWith("WRONGTYPE")) {
                logger.warn("WRONGTYPE error for key {}, attempting to clean up.", key);
                try {
                    jedisPooled.del(key);
                    logger.info("Deleted key with wrong type: {}", key);
                } catch (Exception deleteEx) {
                    logger.error("Failed to delete wrong type key {}: {}", key, deleteEx.getMessage());
                }
            } else {
                logger.error("Jedis data exception for key {}: {}", key, jde.getMessage());
            }
            return Optional.empty();
        } catch (Exception e) {
            logger.error("Error finding JSON for book ID {}: {}", bookId, e.getMessage(), e);
            return Optional.empty();
        }
    }
    
    /**
     * Validates basic JSON structure format
     * @param jsonString string content to validate
     * @return true if appears to be valid JSON object or array structure
     */
    private boolean isValidJsonStructure(String jsonString) {
        if (jsonString == null) return false;
        String trimmed = jsonString.trim();
        return (trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"));
    }


    /**
     * Converts JSON string to CachedBook object
     *
     * @param bookJson JSON string representation of book data
     * @return Optional containing deserialized CachedBook, empty if conversion fails
     */
    public Optional<CachedBook> deserializeBook(String bookJson) {
        if (bookJson == null || bookJson.isEmpty()) {
            return Optional.empty();
        }
        try {
            CachedBook book = objectMapper.readValue(bookJson, CachedBook.class);
            return Optional.of(book);
        } catch (JsonProcessingException e) {
            logger.error("Error deserializing book JSON: {}. JSON snippet: {}", e.getMessage(), bookJson.substring(0, Math.min(bookJson.length(), 200)));
            return Optional.empty();
        }
    }

    /**
     * Converts CachedBook object to JSON string
     *
     * @param book CachedBook object to serialize
     * @return JSON string representation, null if serialization fails
     */
    public String serializeBook(CachedBook book) {
        if (book == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(book);
        } catch (JsonProcessingException e) {
            logger.error("Error serializing CachedBook with ID {}: {}", book.getId(), e.getMessage(), e);
            return null;
        }
    }

    /**
     * Removes book data from Redis cache
     *
     * @param bookId identifier of book to delete from cache
     */
    public void deleteJsonById(String bookId) {
        String key = getCachedBookKey(bookId);
        try {
            jedisPooled.del(key);
            logger.debug("Deleted JSON for book ID {}.", bookId);
        } catch (Exception e) {
            logger.error("Error deleting JSON for book ID {}: {}", bookId, e.getMessage(), e);
        }
    }

    /**
     * Verifies existence of book data in Redis cache
     *
     * @param bookId book identifier to check
     * @return true if book data exists in cache
     */
    public boolean exists(String bookId) {
        String key = getCachedBookKey(bookId);
        try {
            return jedisPooled.exists(key);
        } catch (Exception e) {
            logger.error("Error checking existence for book ID {}: {}", bookId, e.getMessage(), e);
            return false;
        }
    }
    
    public Optional<String> findJsonByIdWithRedisJsonFallback(String bookId) {
        Optional<String> stringJsonOpt = findJsonById(bookId);
        if (stringJsonOpt.isPresent()) {
            return stringJsonOpt;
        }

        // Fallback to trying as RedisJSON type
        String bookKey = getCachedBookKey(bookId);
        try {
            Object jsonResult = jedisPooled.jsonGet(bookKey, Path2.of("$"));
            if (jsonResult == null) {
                jsonResult = jedisPooled.jsonGet(bookKey); // Try root path
            }

            if (jsonResult != null) {
                String bookJsonString;
                if (jsonResult instanceof String) {
                    bookJsonString = (String) jsonResult;
                } else if (jsonResult instanceof java.util.List) {
                    java.util.List<?> resultList = (java.util.List<?>) jsonResult;
                    if (!resultList.isEmpty() && resultList.get(0) instanceof String) {
                        bookJsonString = (String) resultList.get(0);
                    } else {
                        logger.warn("RedisJSON (fallback) returned non-string list for key {}: {}", bookKey, resultList);
                        return Optional.empty();
                    }
                } else {
                    bookJsonString = objectMapper.writeValueAsString(jsonResult);
                }
                
                if (bookJsonString != null && !bookJsonString.isEmpty() && isValidJsonStructure(bookJsonString)) {
                     logger.debug("Successfully read book {} using RedisJSON fallback, returning raw JSON string.", bookId);
                    return Optional.of(bookJsonString);
                }
            }
            logger.debug("RedisJSON (fallback) returned null or empty/invalid data for key {}", bookKey);
        } catch (redis.clients.jedis.exceptions.JedisDataException jde) {
            if (jde.getMessage() != null && jde.getMessage().startsWith("WRONGTYPE")) {
                logger.debug("WRONGTYPE error for key {} during RedisJSON fallback read (likely a string key).", bookKey);
            } else {
                logger.error("JedisDataException reading RedisJSON (fallback) for key {}: {}", bookKey, jde.getMessage());
            }
        } catch (Exception e) {
            logger.warn("Unexpected error reading RedisJSON (fallback) for key {}: {}", bookKey, e.getMessage());
        }
        return Optional.empty();
    }


    /**
     * Retrieves all cached books from Redis with deserialization
     * Scans for book keys and converts valid JSON entries to CachedBook objects
     *
     * @return list of successfully deserialized CachedBook objects
     */
    public List<CachedBook> scanAndDeserializeAllBooks() {
        List<CachedBook> books = new ArrayList<>();
        Set<String> keys = scanAllBookKeys();

        if (!keys.isEmpty()) {
            logger.debug("Processing {} book keys found by SCAN.", keys.size());
            for (String key : keys) {
                String bookId = key.substring(CACHED_BOOK_PREFIX.length());
                if (bookId.isEmpty() || key.endsWith(":lock")) {
                    logger.trace("Skipping potentially invalid key from scan: {}", key);
                    continue;
                }
                
                findJsonByIdWithRedisJsonFallback(bookId).flatMap(this::deserializeBook).ifPresent(books::add);
            }
        }
        logger.debug("Finished processing keys from SCAN. Found {} CachedBook objects.", books.size());
        return books;
    }

    /**
     * Scans Redis for all book-related keys
     *
     * @return set of Redis keys matching book prefix pattern
     */
    public Set<String> scanAllBookKeys() {
        Set<String> keys = new HashSet<>();
        String scanPattern = CACHED_BOOK_PREFIX + "*";
        logger.debug("Initiating SCAN with pattern: {}", scanPattern);
        try {
            String cursor = "0";
            redis.clients.jedis.params.ScanParams scanParams = new redis.clients.jedis.params.ScanParams().match(scanPattern).count(1000);

            do {
                redis.clients.jedis.resps.ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                for (String key : scanResult.getResult()) {
                    if (!key.endsWith(":lock")) {
                        keys.add(key);
                    } else {
                        logger.trace("SCAN found a lock key, excluding: {}", key);
                    }
                }
            } while (!"0".equals(cursor));
            
            logger.debug("SCAN found {} raw keys matching pattern {}.", keys.size(), scanPattern);
        } catch (Exception e) {
            logger.error("Error during Redis SCAN operation with pattern {}: {}", scanPattern, e.getMessage(), e);
        }
        return keys;
    }
    
    /**
     * Counts total number of book entries in Redis cache
     * Uses key scanning which may be inefficient for large datasets
     *
     * @return total count of cached book keys
     */
    public long countAllBooks() {
        return scanAllBookKeys().size();
    }
}
