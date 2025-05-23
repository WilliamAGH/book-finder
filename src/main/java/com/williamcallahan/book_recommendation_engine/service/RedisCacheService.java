/**
 * Service for interacting with Redis cache for Book objects
 * This service handles the caching of {@link com.williamcallahan.book_recommendation_engine.model.Book}
 * objects in a Redis data store. It provides mechanisms for serializing books to JSON for storage
 * and deserializing them upon retrieval. Both synchronous and reactive methods are available
 * for common cache operations such as getting, putting (caching), and evicting book entries
 * The service also includes a utility to check Redis availability before attempting operations
 * and uses a configurable Time To Live (TTL) for cached entries
 *
 * @author William Callahan
 * 
 * Features:
 * - Manages caching of Book objects in Redis
 * - Serializes and deserializes Book objects to/from JSON for Redis storage
 * - Provides synchronous and reactive methods for cache operations (get, put, evict)
 * - Includes a mechanism to check Redis availability
 * - Utilizes a configurable Time To Live (TTL) for cached entries
 * - Supports enabling/disabling caching via application configuration
 * - Uses a specific prefix for cache keys to organize book data in Redis
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.springframework.boot.convert.DurationStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class RedisCacheService {

    private static final Logger logger = LoggerFactory.getLogger(RedisCacheService.class);
    private static final String BOOK_CACHE_PREFIX = "book:";
    private static final String SEARCH_CACHE_PREFIX = "search:";

    private final AtomicBoolean redisCurrentlyAvailable = new AtomicBoolean(false);
    private volatile long lastAvailabilityCheckTimestamp = 0L;
    private static final long AVAILABILITY_CACHE_DURATION_MS = 30000; // Cache status for 30 seconds
    private final Object availabilityCheckLock = new Object(); // Lock for synchronized block

    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;
    private final Duration bookTtl;
    private final Duration affiliateTtl;

    @Value("${app.redis.cache.enabled:true}")
    private boolean redisCacheEnabled;

    public RedisCacheService(StringRedisTemplate stringRedisTemplate, 
                              ObjectMapper objectMapper,
                             @Value("${app.cache.book.ttl:24h}") String bookTtlStr,
                             @Value("${app.cache.affiliate.ttl:1h}") String affiliateTtlStr) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.objectMapper = objectMapper;
        this.bookTtl = DurationStyle.detectAndParse(bookTtlStr);
        this.affiliateTtl = DurationStyle.detectAndParse(affiliateTtlStr);
    }

    /**
     * Checks if the Redis cache is enabled via configuration and if Redis is reachable
     * Availability status is cached for a short duration to reduce PING overhead
     * @return true if Redis is enabled and considered available, false otherwise
     */
    public boolean isRedisAvailable() {
        if (!redisCacheEnabled) {
            return false;
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime - lastAvailabilityCheckTimestamp < AVAILABILITY_CACHE_DURATION_MS) {
            return redisCurrentlyAvailable.get();
        }

        // Cache is stale or this is the first check, need to actually ping Redis
        // Use a lock to prevent multiple threads from pinging simultaneously
        synchronized (availabilityCheckLock) {
            // Re-check condition inside synchronized block in case another thread updated it
            if (currentTime - lastAvailabilityCheckTimestamp < AVAILABILITY_CACHE_DURATION_MS) {
                return redisCurrentlyAvailable.get();
            }

            RedisConnection connection = null;
            try {
                RedisConnectionFactory connectionFactory = stringRedisTemplate.getConnectionFactory();
                if (connectionFactory == null) {
                    logger.warn("Redis connection factory is null.");
                    redisCurrentlyAvailable.set(false);
                    lastAvailabilityCheckTimestamp = currentTime;
                    return false;
                }
                connection = connectionFactory.getConnection();
                connection.ping(); // Throws exception on failure
                redisCurrentlyAvailable.set(true);
                logger.debug("Redis PING successful, connection is available.");
            } catch (RedisConnectionFailureException e) {
                logger.warn("Redis connection failed during PING: {}", e.getMessage());
                redisCurrentlyAvailable.set(false);
            } catch (Exception e) {
                logger.error("Unexpected error pinging Redis: {}", e.getMessage(), e);
                redisCurrentlyAvailable.set(false);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception e) {
                        logger.error("Error closing Redis connection after PING: {}", e.getMessage(), e);
                    }
                }
                lastAvailabilityCheckTimestamp = currentTime; // Update timestamp regardless of success/failure
            }
            return redisCurrentlyAvailable.get();
        }
    }

    private String getKeyForBook(String bookId) {
        return BOOK_CACHE_PREFIX + bookId;
    }

    /**
     * Retrieves a Book from Redis cache by its ID synchronously
     * Returns an empty Optional if Redis is unavailable, bookId is invalid, or book is not found
     * Logs errors during deserialization or connection issues
     * @param bookId The ID of the book to retrieve
     * @return Optional containing the Book if found, otherwise empty
     */
    public Optional<Book> getBookById(String bookId) {
        if (!isRedisAvailable() || bookId == null || bookId.isEmpty()) {
            return Optional.empty();
        }
        try {
            String bookJson = stringRedisTemplate.opsForValue().get(getKeyForBook(bookId));
            if (bookJson != null) {
                Book book = objectMapper.readValue(bookJson, Book.class);
                logger.debug("Redis cache HIT for book ID: {}", bookId);
                return Optional.of(book);
            }
            logger.debug("Redis cache MISS for book ID: {}", bookId);
        } catch (JsonProcessingException e) {
            logger.error("Error deserializing book from Redis for ID {}: {}", bookId, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error getting book from Redis for ID {}: {}", bookId, e.getMessage(), e);
        }
        return Optional.empty();
    }

    /**
     * Reactively retrieves a Book from Redis cache by its ID
     * Returns an empty Mono if Redis is unavailable, bookId is invalid, or book is not found
     * Operations are performed on a bounded elastic scheduler
     * Logs errors during deserialization or connection issues
     * @param bookId The ID of the book to retrieve
     * @return Mono emitting the Book if found, otherwise empty
     */
    public Mono<Book> getBookByIdReactive(String bookId) {
        if (!isRedisAvailable() || bookId == null || bookId.isEmpty()) {
            return Mono.empty();
        }
        return Mono.fromCallable(() -> {
                    String bookJson = stringRedisTemplate.opsForValue().get(getKeyForBook(bookId));
                    if (bookJson != null) {
                        return Optional.of(objectMapper.readValue(bookJson, Book.class));
                    }
                    return Optional.<Book>empty();
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(optionalBook -> {
                    if (optionalBook.isPresent()) {
                        logger.debug("Redis cache HIT (reactive) for book ID: {}", bookId);
                        return Mono.just(optionalBook.get());
                    }
                    logger.debug("Redis cache MISS (reactive) for book ID: {}", bookId);
                    return Mono.empty();
                })
                .onErrorResume(JsonProcessingException.class, e -> {
                    logger.error("Error deserializing book from Redis (reactive) for ID {}: {}", bookId, e.getMessage(), e);
                    return Mono.empty();
                })
                .onErrorResume(Exception.class, e -> {
                    logger.error("Error getting book from Redis (reactive) for ID {}: {}", bookId, e.getMessage(), e);
                    return Mono.empty();
                });
    }

    /**
     * Caches a Book in Redis synchronously with a default TTL
     * Does nothing if Redis is unavailable or input parameters are invalid
     * Logs errors during serialization or connection issues
     * @param bookId The ID of the book to use as the cache key
     * @param book The Book object to cache
     */
    public void cacheBook(String bookId, Book book) {
        if (!isRedisAvailable() || bookId == null || bookId.isEmpty() || book == null) {
            return;
        }
        try {
            String bookJson = objectMapper.writeValueAsString(book);
            stringRedisTemplate.opsForValue().set(getKeyForBook(bookId), bookJson, bookTtl);
            logger.debug("Cached book ID {} in Redis", bookId);
        } catch (JsonProcessingException e) {
            logger.error("Error serializing book for Redis cache for ID {}: {}", bookId, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error caching book in Redis for ID {}: {}", bookId, e.getMessage(), e);
        }
    }

    /**
     * Reactively caches a Book in Redis with a default TTL
     * Returns an empty Mono if Redis is unavailable or input parameters are invalid
     * Operations are performed on a bounded elastic scheduler
     * Logs errors during serialization or connection issues
     * @param bookId The ID of the book to use as the cache key
     * @param book The Book object to cache
     * @return Mono<Void> completing when the caching operation is finished or an error occurs
     */
    public Mono<Void> cacheBookReactive(String bookId, Book book) {
        if (!isRedisAvailable() || bookId == null || bookId.isEmpty() || book == null) {
            return Mono.empty();
        }
        return Mono.fromRunnable(() -> {
                    try {
                        String bookJson = objectMapper.writeValueAsString(book);
                        stringRedisTemplate.opsForValue().set(getKeyForBook(bookId), bookJson, bookTtl);
                        logger.debug("Cached book ID {} in Redis (reactive)", bookId);
                    } catch (JsonProcessingException e) {
                        logger.error("Error serializing book for Redis cache (reactive) for ID {}: {}", bookId, e.getMessage(), e);
                        // Optionally rethrow or handle as a failed Mono
                    } catch (Exception e) {
                        logger.error("Error caching book in Redis (reactive) for ID {}: {}", bookId, e.getMessage(), e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    /**
     * Evicts a Book from Redis cache by its ID synchronously
     * Does nothing if Redis is unavailable or bookId is invalid
     * Logs errors if eviction fails
     * @param bookId The ID of the book to evict
     */
    public void evictBook(String bookId) {
        if (!isRedisAvailable() || bookId == null || bookId.isEmpty()) {
            return;
        }
        try {
            stringRedisTemplate.delete(getKeyForBook(bookId));
            logger.debug("Evicted book ID {} from Redis", bookId);
        } catch (Exception e) {
            logger.error("Error evicting book from Redis for ID {}: {}", bookId, e.getMessage(), e);
        }
    }

    /**
     * Reactively evicts a Book from Redis cache by its ID
     * Returns an empty Mono if Redis is unavailable or bookId is invalid
     * Operations are performed on a bounded elastic scheduler
     * Logs errors if eviction fails
     * @param bookId The ID of the book to evict
     * @return Mono<Void> completing when the eviction operation is finished or an error occurs
     */
    public Mono<Void> evictBookReactive(String bookId) {
        if (!isRedisAvailable() || bookId == null || bookId.isEmpty()) {
            return Mono.empty();
        }
        return Mono.fromRunnable(() -> {
            try {
                stringRedisTemplate.delete(getKeyForBook(bookId));
                logger.debug("Evicted book ID {} from Redis (reactive)", bookId);
            } catch (Exception e) {
                logger.error("Error evicting book from Redis (reactive) for ID {}: {}", bookId, e.getMessage(), e);
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .then();
    }

    /**
     * Retrieves a String value from Redis by key
     * @param key Redis key
     * @return Optional containing value if found
     */
    public Optional<String> getCachedString(String key) {
        if (!isRedisAvailable() || key == null || key.isEmpty()) {
            return Optional.empty();
        }
        try {
            String value = stringRedisTemplate.opsForValue().get(key);
            return value != null ? Optional.of(value) : Optional.empty();
        } catch (Exception e) {
            logger.error("Error getting cache for key {}: {}", key, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Caches a String value in Redis with affiliate TTL
     * @param key Redis key
     * @param value Value to cache
     */
    public void cacheString(String key, String value) {
        if (!isRedisAvailable() || key == null || key.isEmpty() || value == null) {
            return;
        }
        try {
            stringRedisTemplate.opsForValue().set(key, value, affiliateTtl);
            logger.debug("Cached key {} in Redis", key);
        } catch (Exception e) {
            logger.error("Error caching key {} in Redis: {}", key, e.getMessage(), e);
        }
    }

    /**
     * Retrieves all book IDs from Redis cache using SCAN command
     * Uses SCAN instead of KEYS for production safety
     * @return Set of book IDs found in Redis, or empty set if Redis unavailable or error occurs
     */
    public Set<String> getAllBookIds() {
        if (!isRedisAvailable()) {
            return Collections.emptySet();
        }
        
        Set<String> bookIds = new HashSet<>();
        try {
            // Use SCAN to safely iterate through keys matching book:* pattern
            ScanOptions scanOptions = ScanOptions.scanOptions()
                    .match(BOOK_CACHE_PREFIX + "*")
                    .count(100) // Process in batches of 100
                    .build();
                    
            try (Cursor<String> cursor = stringRedisTemplate.scan(scanOptions)) {
                cursor.forEachRemaining(key -> {
                    // Remove the "book:" prefix to get the actual book ID
                    if (key.startsWith(BOOK_CACHE_PREFIX)) {
                        String bookId = key.substring(BOOK_CACHE_PREFIX.length());
                        if (!bookId.isEmpty()) {
                            bookIds.add(bookId);
                        }
                    }
                });
            }
            
            logger.debug("Retrieved {} book IDs from Redis using SCAN", bookIds.size());
            return bookIds;
        } catch (Exception e) {
            logger.error("Error scanning Redis for book keys: {}", e.getMessage(), e);
            return Collections.emptySet();
        }
    }

    // ===== SEARCH RESULT CACHING METHODS =====

    private String getKeyForSearch(String searchKey) {
        return SEARCH_CACHE_PREFIX + searchKey;
    }

    /**
     * Retrieves cached search results from Redis by search key
     * @param searchKey The search cache key
     * @return Optional containing list of book IDs if found, otherwise empty
     */
    public Optional<List<String>> getCachedSearchResults(String searchKey) {
        if (!isRedisAvailable() || searchKey == null || searchKey.isEmpty()) {
            return Optional.empty();
        }
        try {
            String searchResultsJson = stringRedisTemplate.opsForValue().get(getKeyForSearch(searchKey));
            if (searchResultsJson != null) {
                @SuppressWarnings("unchecked")
                List<String> bookIds = objectMapper.readValue(searchResultsJson, List.class);
                logger.debug("Redis search cache HIT for key: {}", searchKey);
                return Optional.of(bookIds);
            }
            logger.debug("Redis search cache MISS for key: {}", searchKey);
        } catch (JsonProcessingException e) {
            logger.error("Error deserializing search results from Redis for key {}: {}", searchKey, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error getting search results from Redis for key {}: {}", searchKey, e.getMessage(), e);
        }
        return Optional.empty();
    }

    /**
     * Reactively retrieves cached search results from Redis by search key
     * @param searchKey The search cache key
     * @return Mono emitting list of book IDs if found, otherwise empty
     */
    public Mono<List<String>> getCachedSearchResultsReactive(String searchKey) {
        if (!isRedisAvailable() || searchKey == null || searchKey.isEmpty()) {
            return Mono.empty();
        }
        return Mono.fromCallable(() -> {
                    String searchResultsJson = stringRedisTemplate.opsForValue().get(getKeyForSearch(searchKey));
                    if (searchResultsJson != null) {
                        @SuppressWarnings("unchecked")
                        List<String> bookIds = objectMapper.readValue(searchResultsJson, List.class);
                        return Optional.of(bookIds);
                    }
                    return Optional.<List<String>>empty();
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(optionalResults -> {
                    if (optionalResults.isPresent()) {
                        logger.debug("Redis search cache HIT (reactive) for key: {}", searchKey);
                        return Mono.just(optionalResults.get());
                    }
                    logger.debug("Redis search cache MISS (reactive) for key: {}", searchKey);
                    return Mono.empty();
                })
                .onErrorResume(JsonProcessingException.class, e -> {
                    logger.error("Error deserializing search results from Redis (reactive) for key {}: {}", searchKey, e.getMessage(), e);
                    return Mono.empty();
                })
                .onErrorResume(Exception.class, e -> {
                    logger.error("Error getting search results from Redis (reactive) for key {}: {}", searchKey, e.getMessage(), e);
                    return Mono.empty();
                });
    }

    /**
     * Caches search results in Redis with affiliate TTL (shorter than book TTL)
     * @param searchKey The search cache key
     * @param bookIds List of book IDs to cache
     */
    public void cacheSearchResults(String searchKey, List<String> bookIds) {
        if (!isRedisAvailable() || searchKey == null || searchKey.isEmpty() || bookIds == null) {
            return;
        }
        try {
            String searchResultsJson = objectMapper.writeValueAsString(bookIds);
            stringRedisTemplate.opsForValue().set(getKeyForSearch(searchKey), searchResultsJson, affiliateTtl);
            logger.debug("Cached search results for key {} with {} book IDs in Redis", searchKey, bookIds.size());
        } catch (JsonProcessingException e) {
            logger.error("Error serializing search results for Redis cache for key {}: {}", searchKey, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error caching search results in Redis for key {}: {}", searchKey, e.getMessage(), e);
        }
    }

    /**
     * Reactively caches search results in Redis with affiliate TTL
     * @param searchKey The search cache key
     * @param bookIds List of book IDs to cache
     * @return Mono<Void> completing when the caching operation is finished
     */
    public Mono<Void> cacheSearchResultsReactive(String searchKey, List<String> bookIds) {
        if (!isRedisAvailable() || searchKey == null || searchKey.isEmpty() || bookIds == null) {
            return Mono.empty();
        }
        return Mono.fromRunnable(() -> {
                    try {
                        String searchResultsJson = objectMapper.writeValueAsString(bookIds);
                        stringRedisTemplate.opsForValue().set(getKeyForSearch(searchKey), searchResultsJson, affiliateTtl);
                        logger.debug("Cached search results (reactive) for key {} with {} book IDs in Redis", searchKey, bookIds.size());
                    } catch (JsonProcessingException e) {
                        logger.error("Error serializing search results for Redis cache (reactive) for key {}: {}", searchKey, e.getMessage(), e);
                    } catch (Exception e) {
                        logger.error("Error caching search results in Redis (reactive) for key {}: {}", searchKey, e.getMessage(), e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }
}
