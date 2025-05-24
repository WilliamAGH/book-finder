/**
 * Utility class for Redis operations with error handling and circuit breaker pattern
 * Provides centralized Redis exception handling and connection resilience
 * Includes timing, logging, and automatic failure recovery mechanisms
 *
 * @author William Callahan
 *
 * Features:
 * - Unified error handling for Redis operations
 * - Circuit breaker pattern to prevent cascading failures
 * - Operation timing and performance logging
 * - Automatic recovery after connection issues
 */

package com.williamcallahan.book_recommendation_engine.util;

import org.slf4j.Logger;
import redis.clients.jedis.exceptions.JedisException;
import java.util.function.Supplier;
import java.time.Instant;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Redis operation helper with error handling and circuit breaker
 */
public class RedisHelper {

    /** Circuit breaker failure threshold before opening */
    private static final int CIRCUIT_BREAKER_THRESHOLD = 10;
    private static final Duration CIRCUIT_BREAKER_RESET_TIMEOUT = Duration.ofMinutes(5);
    
    /** Tracks consecutive operation failures */
    private static final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    
    /** Circuit breaker state flag */
    private static final AtomicBoolean circuitBreakerOpen = new AtomicBoolean(false);
    
    /** Timestamp when circuit breaker was opened */
    private static Instant circuitBreakerOpenTime;
    
    // Redis key prefixes
    private static final String BOOK_PREFIX = "book:";
    private static final String SEARCH_CACHE_PREFIX = "search:";

    /**
     * Executes a Redis operation with unified error handling
     *
     * @param log         SLF4J Logger to record warnings
     * @param jedisCall   Supplier performing the Jedis call
     * @param operation   Description of the Redis operation (for logging)
     * @param fallback    Value to return on exception
     * @param <T>         Return type
     * @return Result of jedisCall.get(), or fallback if an exception occurs
     */
    public static <T> T execute(Logger log, Supplier<T> jedisCall, String operation, T fallback) {
        try {
            return jedisCall.get();
        } catch (JedisException e) {
            log.warn("Redis {} failed ({}): {}", operation, e.getClass().getSimpleName(), e.getMessage());
            return fallback;
        } catch (Exception e) {
            log.warn("Redis {} encountered error: {}", operation, e.getMessage());
            return fallback;
        }
    }

    /**
     * Executes a Redis operation with timing, logging, and a simple circuit breaker
     * @param log SLF4J Logger to record warnings and timings
     * @param operation Supplier performing the Redis call
     * @param operationName Description of the operation for logging
     * @param fallback Value to return if operation fails or circuit is open
     * @param <T> Return type
     * @return result of operation, or fallback on failure or open circuit
     */
    public static <T> T executeWithTiming(Logger log, Supplier<T> operation, String operationName, T fallback) {
        // Check if circuit is open
        if (circuitBreakerOpen.get()) {
            if (Duration.between(circuitBreakerOpenTime, Instant.now()).compareTo(CIRCUIT_BREAKER_RESET_TIMEOUT) > 0) {
                log.info("Circuit breaker reset after timeout, resuming operations");
                circuitBreakerOpen.set(false);
                consecutiveFailures.set(0);
            } else {
                log.error("Circuit breaker is open, skipping Redis operation {}", operationName);
                return fallback;
            }
        }

        Instant start = Instant.now();
        try {
            T result = operation.get();
            // Success resets failure count
            consecutiveFailures.set(0);
            log.debug("Redis operation {} completed in {} ms", operationName, Duration.between(start, Instant.now()).toMillis());
            return result;
        } catch (JedisException e) {
            int failures = consecutiveFailures.incrementAndGet();
            log.warn("Redis operation {} failed (failure {}): {}", operationName, failures, e.getMessage());
            if (failures >= CIRCUIT_BREAKER_THRESHOLD) {
                circuitBreakerOpen.set(true);
                circuitBreakerOpenTime = Instant.now();
                log.error("Circuit breaker triggered after {} consecutive failures", CIRCUIT_BREAKER_THRESHOLD);
            }
            return fallback;
        } catch (Exception e) {
            int failures = consecutiveFailures.incrementAndGet();
            log.warn("Redis operation {} encountered error (failure {}): {}", operationName, failures, e.getMessage());
            return fallback;
        }
    }
    
    // Key generation methods
    
    /**
     * Generates a Redis key for a book by ID
     * @param bookId the book identifier
     * @return the Redis key
     */
    public static String bookKey(String bookId) {
        return BOOK_PREFIX + bookId;
    }
    
    /**
     * Generates a Redis key pattern for scanning all books
     * @return the pattern for scanning book keys
     */
    public static String bookKeyPattern() {
        return BOOK_PREFIX + "*";
    }
    
    /**
     * Extracts book ID from a Redis book key
     * @param key the Redis key
     * @return the book ID
     */
    public static String extractIdFromBookKey(String key) {
        if (key != null && key.startsWith(BOOK_PREFIX)) {
            return key.substring(BOOK_PREFIX.length());
        }
        return key;
    }
    
    /**
     * Generates a Redis key for search cache
     * @param searchQuery the search query
     * @return the Redis key
     */
    public static String searchCacheKey(String searchQuery) {
        return SEARCH_CACHE_PREFIX + searchQuery;
    }
    
    /**
     * Get the book key prefix constant
     * @return the book prefix
     */
    public static String getBookPrefix() {
        return BOOK_PREFIX;
    }
}
