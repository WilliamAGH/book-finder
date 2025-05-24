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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.config.RedisEnvironmentCondition;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.springframework.boot.convert.DurationStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.task.AsyncTaskExecutor;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.resps.ScanResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Conditional(RedisEnvironmentCondition.class)
public class RedisCacheService {

    private static final Logger logger = LoggerFactory.getLogger(RedisCacheService.class);
    private static final String BOOK_CACHE_PREFIX = "book:";
    private static final String SEARCH_CACHE_PREFIX = "search:";

    private final AtomicBoolean redisCurrentlyAvailable = new AtomicBoolean(false);
    private volatile long lastAvailabilityCheckTimestamp = 0L;
    private static final long AVAILABILITY_CACHE_DURATION_MS = 30000; // Cache status for 30 seconds
    private final Object availabilityCheckLock = new Object(); // Lock for synchronized block

    private final JedisPooled jedisPooled;
    private final ObjectMapper objectMapper;
    private final Duration bookTtl;
    private final Duration affiliateTtl;
    private final AsyncTaskExecutor mvcTaskExecutor;

    @Value("${app.redis.cache.enabled:true}")
    private boolean redisCacheEnabled;

    public RedisCacheService(JedisPooled jedisPooled,
                              ObjectMapper objectMapper,
                             @Qualifier("mvcTaskExecutor") AsyncTaskExecutor mvcTaskExecutor,
                             @Value("${app.cache.book.ttl:24h}") String bookTtlStr,
                             @Value("${app.cache.affiliate.ttl:1h}") String affiliateTtlStr) {
        this.jedisPooled = jedisPooled;
        this.objectMapper = objectMapper;
        this.mvcTaskExecutor = mvcTaskExecutor;
        this.bookTtl = DurationStyle.detectAndParse(bookTtlStr);
        this.affiliateTtl = DurationStyle.detectAndParse(affiliateTtlStr);
    }

    /**
     * Asynchronously checks if the Redis cache is enabled via configuration and if Redis is reachable.
     * Availability status is cached for a short duration to reduce PING overhead.
     * @return CompletableFuture<Boolean> resolving to true if Redis is enabled and available, false otherwise.
     */
    public CompletableFuture<Boolean> isRedisAvailableAsync() {
        if (!redisCacheEnabled) {
            return CompletableFuture.completedFuture(false);
        }

        long currentTime = System.currentTimeMillis();
        if (currentTime - lastAvailabilityCheckTimestamp < AVAILABILITY_CACHE_DURATION_MS) {
            return CompletableFuture.completedFuture(redisCurrentlyAvailable.get());
        }

        return CompletableFuture.supplyAsync(() -> {
            synchronized (availabilityCheckLock) {
                long recheckTime = System.currentTimeMillis();
                if (recheckTime - lastAvailabilityCheckTimestamp < AVAILABILITY_CACHE_DURATION_MS) {
                    return redisCurrentlyAvailable.get();
                }

                try {
                    jedisPooled.ping(); 
                    redisCurrentlyAvailable.set(true);
                    logger.debug("Redis PING successful, connection is available.");
                } catch (JedisConnectionException e) {
                    logger.warn("Redis connection failed during PING: {}", e.getMessage());
                    redisCurrentlyAvailable.set(false);
                } catch (Exception e) {
                    logger.error("Unexpected error pinging Redis: {}", e.getMessage(), e);
                    redisCurrentlyAvailable.set(false);
                } finally {
                    lastAvailabilityCheckTimestamp = recheckTime; 
                }
                return redisCurrentlyAvailable.get();
            }
        }, this.mvcTaskExecutor);
    }

    /**
     * Synchronous wrapper for isRedisAvailableAsync for internal use or where blocking is acceptable.
     * @deprecated Prefer asynchronous checks where possible.
     * @return true if Redis is enabled and considered available, false otherwise
     */
    @Deprecated
    public boolean isRedisAvailable() {
        logger.warn("Deprecated synchronous isRedisAvailable called; use isRedisAvailableAsync instead.");
        try {
            return isRedisAvailableAsync().get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Error checking Redis availability (sync wrapper): {}", e.getMessage());
            Thread.currentThread().interrupt(); 
            return false;
        }
    }

    private String getKeyForBook(String bookId) {
        return BOOK_CACHE_PREFIX + bookId;
    }

    public CompletableFuture<Optional<Book>> getBookByIdAsync(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return isRedisAvailableAsync().thenComposeAsync(available -> {
            if (!available) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return CompletableFuture.supplyAsync(() -> {
                try {
                    String bookJson = jedisPooled.get(getKeyForBook(bookId));
                    if (bookJson != null) {
                        Book book = objectMapper.readValue(bookJson, Book.class);
                        logger.debug("Redis cache HIT for book ID: {}", bookId);
                        return Optional.of(book);
                    }
                    logger.debug("Redis cache MISS for book ID: {}", bookId);
                    return Optional.empty();
                } catch (Exception e) {
                    logger.error("Error getting book from Redis for ID {}: {}", bookId, e.getMessage(), e);
                    return Optional.empty();
                }
            }, this.mvcTaskExecutor);
        });
    }

    public Mono<Book> getBookByIdReactive(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return Mono.empty();
        }
        return Mono.fromFuture(isRedisAvailableAsync())
            .flatMap(available -> {
                if (!available) {
                    return Mono.empty();
                }
                return Mono.fromCallable(() -> {
                    String bookJson = jedisPooled.get(getKeyForBook(bookId));
                    if (bookJson != null) {
                        return Optional.of(objectMapper.readValue(bookJson, Book.class));
                    }
                    return Optional.<Book>empty();
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(optionalBook -> optionalBook.map(Mono::just).orElseGet(Mono::empty))
                .onErrorResume(e -> {
                    logger.error("Error getting book from Redis (reactive) for ID {}: {}", bookId, e.getMessage(), e);
                    return Mono.empty();
                });
            });
    }

    public CompletableFuture<Void> cacheBookAsync(String bookId, Book book) {
        if (bookId == null || bookId.isEmpty() || book == null) {
            return CompletableFuture.completedFuture(null);
        }
        return isRedisAvailableAsync().thenComposeAsync(available -> {
            if (!available) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.runAsync(() -> {
                try {
                    String bookJson = objectMapper.writeValueAsString(book);
                    jedisPooled.setex(getKeyForBook(bookId), bookTtl.toSeconds(), bookJson);
                    logger.debug("Cached book ID {} in Redis", bookId);
                } catch (Exception e) {
                    logger.error("Error caching book in Redis for ID {}: {}", bookId, e.getMessage(), e);
                }
            }, this.mvcTaskExecutor);
        });
    }

    public Mono<Void> cacheBookReactive(String bookId, Book book) {
        if (bookId == null || bookId.isEmpty() || book == null) {
            return Mono.empty();
        }
        return Mono.fromFuture(isRedisAvailableAsync())
            .flatMap(available -> {
                if (!available) {
                    return Mono.empty();
                }
                return Mono.fromRunnable(() -> {
                    try {
                        String bookJson = objectMapper.writeValueAsString(book);
                        jedisPooled.setex(getKeyForBook(bookId), bookTtl.toSeconds(), bookJson);
                        logger.debug("Cached book ID {} in Redis (reactive)", bookId);
                    } catch (Exception e) {
                         logger.error("Error caching book in Redis (reactive) for ID {}: {}", bookId, e.getMessage(), e);
                         throw new RuntimeException(e); // Propagate to onErrorResume
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(e -> {
                    // Already logged, just complete empty
                    return Mono.empty();
                })
                .then();
            });
    }

    public CompletableFuture<Void> evictBookAsync(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return isRedisAvailableAsync().thenComposeAsync(available -> {
            if (!available) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.runAsync(() -> {
                try {
                    jedisPooled.del(getKeyForBook(bookId));
                    logger.debug("Evicted book ID {} from Redis", bookId);
                } catch (Exception e) {
                    logger.error("Error evicting book from Redis for ID {}: {}", bookId, e.getMessage(), e);
                }
            }, this.mvcTaskExecutor);
        });
    }

    public Mono<Void> evictBookReactive(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return Mono.empty();
        }
        return Mono.fromFuture(isRedisAvailableAsync())
            .flatMap(available -> {
                if (!available) {
                    return Mono.empty();
                }
                return Mono.fromRunnable(() -> {
                    try {
                        jedisPooled.del(getKeyForBook(bookId));
                        logger.debug("Evicted book ID {} from Redis (reactive)", bookId);
                    } catch (Exception e) {
                        logger.error("Error evicting book from Redis (reactive) for ID {}: {}", bookId, e.getMessage(), e);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
            });
    }

    public CompletableFuture<Optional<String>> getCachedStringAsync(String key) {
        if (key == null || key.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return isRedisAvailableAsync().thenComposeAsync(available -> {
            if (!available) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return CompletableFuture.supplyAsync(() -> {
                try {
                    String value = jedisPooled.get(key);
                    return Optional.ofNullable(value);
                } catch (Exception e) {
                    logger.error("Error getting cache for key {}: {}", key, e.getMessage(), e);
                    return Optional.empty();
                }
            }, this.mvcTaskExecutor);
        });
    }

    public CompletableFuture<Void> cacheStringAsync(String key, String value) {
        if (key == null || key.isEmpty() || value == null) {
            return CompletableFuture.completedFuture(null);
        }
        return isRedisAvailableAsync().thenComposeAsync(available -> {
            if (!available) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.runAsync(() -> {
                try {
                    jedisPooled.setex(key, affiliateTtl.toSeconds(), value);
                    logger.debug("Cached key {} in Redis", key);
                } catch (Exception e) {
                    logger.error("Error caching key {} in Redis: {}", key, e.getMessage(), e);
                }
            }, this.mvcTaskExecutor);
        });
    }

    private String getKeyForSearch(String searchKey) {
        return SEARCH_CACHE_PREFIX + searchKey;
    }

    public CompletableFuture<Optional<List<String>>> getCachedSearchResultsAsync(String searchKey) {
        if (searchKey == null || searchKey.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return isRedisAvailableAsync().thenComposeAsync(available -> {
            if (!available) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            return CompletableFuture.supplyAsync(() -> {
                try {
                    String searchResultsJson = jedisPooled.get(getKeyForSearch(searchKey));
                    if (searchResultsJson != null) {
                        @SuppressWarnings("unchecked")
                        List<String> bookIds = objectMapper.readValue(searchResultsJson, List.class);
                        logger.debug("Redis search cache HIT for key: {}", searchKey);
                        return Optional.of(bookIds);
                    }
                    logger.debug("Redis search cache MISS for key: {}", searchKey);
                    return Optional.empty();
                } catch (Exception e) {
                    logger.error("Error getting search results from Redis for key {}: {}", searchKey, e.getMessage(), e);
                    return Optional.empty();
                }
            }, this.mvcTaskExecutor);
        });
    }

    public Mono<List<String>> getCachedSearchResultsReactive(String searchKey) {
        if (searchKey == null || searchKey.isEmpty()) {
            return Mono.empty();
        }
        return Mono.fromFuture(isRedisAvailableAsync())
            .flatMap(available -> {
                if (!available) {
                    return Mono.empty();
                }
                return Mono.fromCallable(() -> {
                    String searchResultsJson = jedisPooled.get(getKeyForSearch(searchKey));
                    if (searchResultsJson != null) {
                        @SuppressWarnings("unchecked")
                        List<String> bookIds = objectMapper.readValue(searchResultsJson, List.class);
                        return Optional.of(bookIds);
                    }
                    return Optional.<List<String>>empty();
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(optionalResults -> optionalResults.map(Mono::just).orElseGet(Mono::empty))
                .onErrorResume(e -> {
                    logger.error("Error getting search results from Redis (reactive) for key {}: {}", searchKey, e.getMessage(), e);
                    return Mono.empty();
                });
            });
    }

    public CompletableFuture<Void> cacheSearchResultsAsync(String searchKey, List<String> bookIds) {
        if (searchKey == null || searchKey.isEmpty() || bookIds == null) {
            return CompletableFuture.completedFuture(null);
        }
        return isRedisAvailableAsync().thenComposeAsync(available -> {
            if (!available) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.runAsync(() -> {
                try {
                    String searchResultsJson = objectMapper.writeValueAsString(bookIds);
                    jedisPooled.setex(getKeyForSearch(searchKey), affiliateTtl.toSeconds(), searchResultsJson);
                    logger.debug("Cached search results for key {} with {} book IDs in Redis", searchKey, bookIds.size());
                } catch (Exception e) {
                    logger.error("Error caching search results in Redis for key {}: {}", searchKey, e.getMessage(), e);
                }
            }, this.mvcTaskExecutor);
        });
    }

    public Mono<Void> cacheSearchResultsReactive(String searchKey, List<String> bookIds) {
        logger.trace("Attempting reactive SET for search results with key: {}", searchKey);
        if (searchKey == null || searchKey.isEmpty() || bookIds == null) {
            return Mono.empty();
        }
        return Mono.fromFuture(isRedisAvailableAsync())
            .flatMap(available -> {
                if (!available) {
                    if (searchKey != null && !searchKey.isEmpty()) logger.debug("Redis not available or parameters invalid, skipping cacheSearchResultsReactive for key: {}", searchKey);
                    return Mono.empty();
                }
                String redisKey = getKeyForSearch(searchKey);
                logger.trace("Attempting reactive Redis SET for search results key: {} with TTL: {}", redisKey, affiliateTtl);
                return Mono.fromRunnable(() -> {
                    try {
                        String searchResultsJson = objectMapper.writeValueAsString(bookIds);
                        jedisPooled.setex(redisKey, affiliateTtl.toSeconds(), searchResultsJson);
                        logger.debug("Cached search results (reactive) for key {} with {} book IDs in Redis", redisKey, bookIds.size());
                    } catch (Exception e) {
                        logger.error("Error caching search results in Redis (reactive) for key {}: {}", redisKey, e.getMessage(), e);
                        throw new RuntimeException(e); // Propagate to onErrorResume
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(e -> {
                    // Already logged
                    return Mono.empty();
                })
                .then();
            });
    }

    public CompletableFuture<Set<String>> getAllBookIdsAsync() {
        return isRedisAvailableAsync().thenComposeAsync(available -> {
            if (!available) {
                return CompletableFuture.completedFuture(Collections.emptySet());
            }
            return CompletableFuture.supplyAsync(() -> {
                Set<String> bookIds = new HashSet<>();
                try {
                    String cursor = "0";
                    do {
                        ScanResult<String> scanResult = jedisPooled.scan(cursor, new redis.clients.jedis.params.ScanParams()
                                .match(BOOK_CACHE_PREFIX + "*")
                                .count(100));
                        
                        for (String key : scanResult.getResult()) {
                            if (key.startsWith(BOOK_CACHE_PREFIX)) {
                                String bookId = key.substring(BOOK_CACHE_PREFIX.length());
                                if (!bookId.isEmpty()) {
                                    bookIds.add(bookId);
                                }
                            }
                        }
                        cursor = scanResult.getCursor();
                    } while (!"0".equals(cursor));
                    
                    logger.debug("Retrieved {} book IDs from Redis using SCAN (async)", bookIds.size());
                    return bookIds;
                } catch (Exception e) {
                    logger.error("Error scanning Redis for book keys (async): {}", e.getMessage(), e);
                    return Collections.emptySet();
                }
            }, this.mvcTaskExecutor); // Use the class-level executor
        });
    }

    @Deprecated
    public Optional<Book> getBookById(String bookId) {
        logger.warn("Deprecated synchronous getBookById called; use getBookByIdAsync or reactive method instead.");
        return getBookByIdAsync(bookId).join();
    }

    @Deprecated
    public void cacheBook(String bookId, Book book) {
        logger.warn("Deprecated synchronous cacheBook called; use cacheBookAsync instead.");
        cacheBookAsync(bookId, book).join();
    }

    @Deprecated
    public void evictBook(String bookId) {
        logger.warn("Deprecated synchronous evictBook called; use evictBookAsync instead.");
        evictBookAsync(bookId).join();
    }

    @Deprecated
    public Optional<String> getCachedString(String key) {
        logger.warn("Deprecated synchronous getCachedString called; use getCachedStringAsync instead.");
        return getCachedStringAsync(key).join();
    }

    @Deprecated
    public void cacheString(String key, String value) {
        logger.warn("Deprecated synchronous cacheString called; use cacheStringAsync instead.");
        cacheStringAsync(key, value).join();
    }

    @Deprecated
    public Optional<List<String>> getCachedSearchResults(String searchKey) {
        logger.warn("Deprecated synchronous getCachedSearchResults called; use getCachedSearchResultsAsync or reactive instead.");
        return getCachedSearchResultsAsync(searchKey).join();
    }

    @Deprecated
    public void cacheSearchResults(String searchKey, List<String> bookIds) {
        logger.warn("Deprecated synchronous cacheSearchResults called; use cacheSearchResultsAsync instead.");
        cacheSearchResultsAsync(searchKey, bookIds).join();
    }
}
