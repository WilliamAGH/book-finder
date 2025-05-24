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
import com.williamcallahan.book_recommendation_engine.config.GracefulShutdownConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisPooled;

import java.util.Optional;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Stream;
import java.util.concurrent.CompletableFuture;
import org.springframework.scheduling.annotation.Async;
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
        if (GracefulShutdownConfig.isShuttingDown()) {
            logger.debug("Application is shutting down, skipping save operation for book ID: {}", bookId);
            return;
        }
        
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
        if (GracefulShutdownConfig.isShuttingDown()) {
            logger.debug("Application is shutting down, skipping Redis operation for book ID: {}", bookId);
            return Optional.empty();
        }
        
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

            String type = jedisPooled.type(key); // Check type first

            if ("string".equalsIgnoreCase(type)) {
                String bookJson = jedisPooled.get(key);
                if (bookJson != null) {
                    if (bookJson.equals("locked") || bookJson.length() < 10 || !isValidJsonStructure(bookJson)) {
                        logger.warn("Skipping non-book or malformed data for key: {}. Content snippet: {}", key, bookJson.substring(0, Math.min(bookJson.length(), 50)));
                        return Optional.empty();
                    }
                    return Optional.of(bookJson);
                }
                return Optional.empty(); // String type but null value
            } else if ("ReJSON-RL".equalsIgnoreCase(type) || "json".equalsIgnoreCase(type)) {
                // It's a JSON type, findJsonById (for strings) should not handle it.
                // Let findJsonByIdWithRedisJsonFallback attempt to read it as JSON.
                logger.debug("Key {} is RedisJSON type ({}). findJsonById returning empty to allow fallback.", key, type);
                return Optional.empty();
            } else {
                // Unexpected type. Log it. Consider if deletion is appropriate or should be handled by a maintenance task.
                logger.warn("Key {} is of unexpected type: {}. Not a string or known JSON type.", key, type);
                // Optional: Delete if this state is considered unrecoverable and problematic for other operations.
                // For now, we won't delete, to be conservative and prevent data loss if type() was misleading.
                // try {
                //     jedisPooled.del(key);
                //     logger.info("Deleted key {} with unexpected type: {}", key, type);
                // } catch (Exception deleteEx) {
                //     logger.error("Failed to delete key {} with unexpected type {}: {}", key, type, deleteEx.getMessage());
                // }
                return Optional.empty();
            }
        } catch (redis.clients.jedis.exceptions.JedisDataException jde) {
            // This catch block handles cases where `jedisPooled.get(key)` might still be called
            // (e.g., if type check was somehow bypassed or if type() returned "string" for a non-string key - unlikely).
            if (jde.getMessage() != null && jde.getMessage().startsWith("WRONGTYPE")) {
                logger.warn("WRONGTYPE JedisDataException for key {} (likely a non-string type). Letting fallback handle. Message: {}", key, jde.getMessage());
                // DO NOT DELETE. The key might be a valid RedisJSON object.
            } else {
                logger.error("Jedis data exception for key {}: {}", key, jde.getMessage(), jde);
            }
            return Optional.empty();
        } catch (redis.clients.jedis.exceptions.JedisException e) {
            if (e.getMessage() != null && e.getMessage().contains("Pool not open")) {
                logger.debug("Redis pool closed during shutdown for book ID {}", bookId);
            } else {
                logger.error("Redis error finding JSON for book ID {}: {}", bookId, e.getMessage(), e);
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
        if (GracefulShutdownConfig.isShuttingDown()) {
            logger.debug("Application is shutting down, returning false for exists check on book ID: {}", bookId);
            return false;
        }
        
        String key = getCachedBookKey(bookId);
        try {
            return jedisPooled.exists(key);
        } catch (redis.clients.jedis.exceptions.JedisException e) {
            if (e.getMessage() != null && e.getMessage().contains("Pool not open")) {
                logger.debug("Redis pool closed during shutdown for book ID {}", bookId);
            } else {
                logger.error("Redis error checking existence for book ID {}: {}", bookId, e.getMessage(), e);
            }
            return false;
        } catch (Exception e) {
            logger.error("Error checking existence for book ID {}: {}", bookId, e.getMessage(), e);
            return false;
        }
    }
    
    public Optional<String> findJsonByIdWithRedisJsonFallback(String bookId) {
        if (GracefulShutdownConfig.isShuttingDown()) {
            logger.debug("Application is shutting down, skipping Redis operation for book ID: {}", bookId);
            return Optional.empty();
        }
        
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
        } catch (redis.clients.jedis.exceptions.JedisException e) {
            if (e.getMessage() != null && e.getMessage().contains("Pool not open")) {
                logger.debug("Redis pool closed during shutdown for key {}", bookKey);
            } else {
                logger.warn("Redis error reading RedisJSON (fallback) for key {}: {}", bookKey, e.getMessage());
            }
        } catch (Exception e) {
            logger.warn("Unexpected error reading RedisJSON (fallback) for key {}: {}", bookKey, e.getMessage());
        }
        return Optional.empty();
    }

    /**
     * Streams cached books from Redis without loading all into memory
     * @return Stream of CachedBook objects
     */
    public Stream<CachedBook> streamAllBooks() {
        return scanAllBookKeys().stream()
            .filter(key -> !key.endsWith(":lock"))
            .map(key -> key.substring(CACHED_BOOK_PREFIX.length()))
            .map(this::findJsonByIdWithRedisJsonFallback)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(this::deserializeBook)
            .filter(Optional::isPresent)
            .map(Optional::get);
    }

    /**
     * Retrieves all cached books from Redis with deserialization
     * Scans for book keys and converts valid JSON entries to CachedBook objects
     *
     * @deprecated Use streamAllBooks() for better memory efficiency with large datasets
     * @return list of successfully deserialized CachedBook objects
     */
    @Deprecated
    public List<CachedBook> scanAndDeserializeAllBooks() {
        logger.warn("Using deprecated scanAndDeserializeAllBooks(). Consider using streamAllBooks() for better memory efficiency.");
        return streamAllBooks().collect(java.util.stream.Collectors.toList());
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
    
    // ===== Async wrapper methods for non-blocking operations =====
    
    /**
     * Asynchronously saves book JSON data to Redis with expiration time
     *
     * @param bookId     book identifier for Redis key generation
     * @param bookJson   serialized JSON representation of the book data
     * @param ttlSeconds expiration time in seconds for the Redis key
     * @return CompletableFuture that completes when save operation finishes
     */
    @Async
    public CompletableFuture<Void> saveJsonAsync(String bookId, String bookJson, long ttlSeconds) {
        return CompletableFuture.runAsync(() -> saveJson(bookId, bookJson, ttlSeconds));
    }
    
    /**
     * Asynchronously retrieves book JSON data from Redis by book identifier
     *
     * @param bookId unique book identifier
     * @return CompletableFuture containing Optional with JSON string if found
     */
    @Async
    public CompletableFuture<Optional<String>> findJsonByIdAsync(String bookId) {
        return CompletableFuture.supplyAsync(() -> findJsonById(bookId));
    }
    
    /**
     * Asynchronously retrieves book JSON with RedisJSON fallback
     *
     * @param bookId unique book identifier
     * @return CompletableFuture containing Optional with JSON string if found
     */
    @Async
    public CompletableFuture<Optional<String>> findJsonByIdWithRedisJsonFallbackAsync(String bookId) {
        return CompletableFuture.supplyAsync(() -> findJsonByIdWithRedisJsonFallback(bookId));
    }
    
    /**
     * Asynchronously removes book data from Redis cache
     *
     * @param bookId identifier of book to delete from cache
     * @return CompletableFuture that completes when delete operation finishes
     */
    @Async
    public CompletableFuture<Void> deleteJsonByIdAsync(String bookId) {
        return CompletableFuture.runAsync(() -> deleteJsonById(bookId));
    }
    
    /**
     * Asynchronously verifies existence of book data in Redis cache
     *
     * @param bookId book identifier to check
     * @return CompletableFuture containing true if book data exists in cache
     */
    @Async
    public CompletableFuture<Boolean> existsAsync(String bookId) {
        return CompletableFuture.supplyAsync(() -> exists(bookId));
    }
    
    /**
     * Asynchronously counts total number of book entries in Redis cache
     *
     * @return CompletableFuture containing total count of cached book keys
     */
    @Async
    public CompletableFuture<Long> countAllBooksAsync() {
        return CompletableFuture.supplyAsync(this::countAllBooks);
    }
    
    /**
     * Asynchronously scans Redis for all book-related keys
     *
     * @return CompletableFuture containing set of Redis keys matching book prefix pattern
     */
    @Async
    public CompletableFuture<Set<String>> scanAllBookKeysAsync() {
        return CompletableFuture.supplyAsync(this::scanAllBookKeys);
    }
}
