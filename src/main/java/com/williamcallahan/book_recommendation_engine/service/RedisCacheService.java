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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Optional;

@Service
public class RedisCacheService {

    private static final Logger logger = LoggerFactory.getLogger(RedisCacheService.class);
    private static final String BOOK_CACHE_PREFIX = "book:";
    // Default TTL for book entries in Redis, e.g., 24 hours
    private static final Duration BOOK_TTL = Duration.ofHours(24); // Time To Live for cached books

    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.redis.cache.enabled:true}")
    private boolean redisCacheEnabled;

    @Autowired
    public RedisCacheService(StringRedisTemplate stringRedisTemplate, ObjectMapper objectMapper) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Checks if the Redis cache is enabled via configuration and reachable by attempting a PING command
     * @return true if Redis is enabled and a connection can be established, false otherwise
     */
    public boolean isRedisAvailable() {
        if (!redisCacheEnabled) {
            return false;
        }
        RedisConnection connection = null;
        try {
            RedisConnectionFactory connectionFactory = stringRedisTemplate.getConnectionFactory();
            if (connectionFactory == null) {
                logger.warn("Redis connection factory is null.");
                return false;
            }
            connection = connectionFactory.getConnection();
            // Assuming getConnection() throws an exception if it cannot provide a connection,
            // rather than returning null. The specific null check for 'connection' might be considered dead code by some analyzers.
            connection.ping();
            return true;
        } catch (RedisConnectionFailureException e) {
            logger.warn("Redis connection failed: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            logger.error("Unexpected error pinging Redis: {}", e.getMessage(), e);
            return false;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                    logger.error("Error closing Redis connection: {}", e.getMessage(), e);
                }
            }
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
            logger.error("Error deserializing book from Redis for ID {}: {}", bookId, e.getMessage());
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
                    logger.error("Error deserializing book from Redis (reactive) for ID {}: {}", bookId, e.getMessage());
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
            stringRedisTemplate.opsForValue().set(getKeyForBook(bookId), bookJson, BOOK_TTL);
            logger.debug("Cached book ID {} in Redis", bookId);
        } catch (JsonProcessingException e) {
            logger.error("Error serializing book for Redis cache for ID {}: {}", bookId, e.getMessage());
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
                        stringRedisTemplate.opsForValue().set(getKeyForBook(bookId), bookJson, BOOK_TTL);
                        logger.debug("Cached book ID {} in Redis (reactive)", bookId);
                    } catch (JsonProcessingException e) {
                        logger.error("Error serializing book for Redis cache (reactive) for ID {}: {}", bookId, e.getMessage());
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
}
