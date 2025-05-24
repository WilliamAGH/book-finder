/**
 * Facade service that provides a unified interface for book caching operations
 * Acts as a central entry point for all book cache-related functionality
 * Coordinates between synchronous and reactive cache services
 *
 * @author William Callahan
 *
 * Features:
 * - Unified interface for book caching operations
 * - Coordinates between synchronous and reactive cache services
 * - Manages cache lifecycle and non-blocking operations
 * - Provides CompletableFuture-based async methods for public APIs
 */

package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.service.cache.BookReactiveCacheService;
import com.williamcallahan.book_recommendation_engine.service.cache.BookSyncCacheService;
import com.williamcallahan.book_recommendation_engine.service.similarity.BookSimilarityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Service
public class BookCacheFacadeService {

    private static final Logger logger = LoggerFactory.getLogger(BookCacheFacadeService.class);

    private final BookSyncCacheService bookSyncCacheService;
    private final BookReactiveCacheService bookReactiveCacheService;
    private final BookSimilarityService bookSimilarityService;
    private final CacheManager cacheManager;
    private final RedisCacheService redisCacheService;
    private final CachedBookRepository cachedBookRepository;

    @Value("${app.cache.enabled:true}")
    private boolean dbCacheEnabled; // Renamed for clarity, as this primarily gates DB cache part

    public BookCacheFacadeService(BookSyncCacheService bookSyncCacheService,
                                  BookReactiveCacheService bookReactiveCacheService,
                                  BookSimilarityService bookSimilarityService,
                                  CacheManager cacheManager,
                                  RedisCacheService redisCacheService,
                                  @Autowired(required = false) CachedBookRepository cachedBookRepository) {
        this.bookSyncCacheService = bookSyncCacheService;
        this.bookReactiveCacheService = bookReactiveCacheService;
        this.bookSimilarityService = bookSimilarityService;
        this.cacheManager = cacheManager;
        this.redisCacheService = redisCacheService;
        this.cachedBookRepository = cachedBookRepository;

        if (this.cachedBookRepository == null ||
            cachedBookRepository instanceof com.williamcallahan.book_recommendation_engine.repository.NoOpCachedBookRepository) {
            this.dbCacheEnabled = false;
            logger.info("BookCacheFacadeService: Database cache (CachedBookRepository) is not available or is NoOp. DB cache operations will be skipped.");
        }
    }

    @Cacheable(value = "books", key = "#id", unless = "#result.get() == null") // Adjusted for CompletableFuture<Book>
    public CompletableFuture<Book> getBookById(String id) {
        logger.debug("BookCacheFacadeService.getBookById (async) called for ID: {}", id);
        // Spring Cache handles CompletableFuture by caching the result once completed.
        return bookSyncCacheService.getBookByIdAsync(id);
    }

    public CompletableFuture<List<Book>> getBooksByIsbn(String isbn) {
        logger.debug("BookCacheFacadeService.getBooksByIsbn (async) called for ISBN: {}", isbn);
        return bookSyncCacheService.getBooksByIsbnAsync(isbn);
    }

    public CompletableFuture<List<Book>> searchBooks(String query, int startIndex, int maxResults) {
        logger.info("BookCacheFacadeService.searchBooks (async) with query: {}, startIndex: {}, maxResults: {}", query, startIndex, maxResults);
        return bookSyncCacheService.searchBooksAsync(query, startIndex, maxResults);
    }

    public Mono<Book> getBookByIdReactive(String id) {
        logger.info("BookCacheFacadeService.getBookByIdReactive called for ID: {}", id);
        return bookReactiveCacheService.getBookByIdReactive(id);
    }

    public Mono<Book> getBookByIdReactiveFromCacheOnly(String id) {
        logger.debug("BookCacheFacadeService.getBookByIdReactiveFromCacheOnly called for ID: {}", id);
        return bookReactiveCacheService.getBookByIdReactiveFromCacheOnly(id);
    }

    public Mono<List<Book>> getBooksByIsbnReactive(String isbn) {
        logger.debug("BookCacheFacadeService.getBooksByIsbnReactive called for ISBN: {}", isbn);
        return bookReactiveCacheService.getBooksByIsbnReactive(isbn);
    }

    public Mono<List<Book>> searchBooksReactive(String query, int startIndex, int maxResults, Integer publishedYear, String langCode, String orderBy) {
        logger.info("BookCacheFacadeService.searchBooksReactive called with query: {}, startIndex: {}, maxResults: {}", query, startIndex, maxResults);
        return bookReactiveCacheService.searchBooksReactive(query, startIndex, maxResults, publishedYear, langCode, orderBy);
    }

    public Mono<Void> cacheBookReactive(Book book) {
        logger.debug("BookCacheFacadeService.cacheBookReactive called for book ID: {}", book != null ? book.getId() : "null");
        return bookReactiveCacheService.cacheBookReactive(book);
    }

    public CompletableFuture<List<Book>> getSimilarBooks(String bookId, int count) {
        logger.info("BookCacheFacadeService.getSimilarBooks (async) called for bookId: {}, count: {}", bookId, count);
        return bookSimilarityService.getSimilarBooksReactive(bookId, count).toFuture();
    }

    public Mono<List<Book>> getSimilarBooksReactive(String bookId, int count) {
        logger.info("BookCacheFacadeService.getSimilarBooksReactive called for bookId: {}, count: {}", bookId, count);
        return bookSimilarityService.getSimilarBooksReactive(bookId, count);
    }

    public CompletableFuture<Set<String>> getAllCachedBookIds() {
        CompletableFuture<Set<String>> dbFuture = CompletableFuture.completedFuture(Collections.emptySet());
        if (dbCacheEnabled) {
            dbFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    logger.info("Fetching all distinct Google Books IDs from DB cache.");
                    Set<String> ids = cachedBookRepository.findAllDistinctGoogleBooksIds();
                    return (ids != null && !ids.isEmpty()) ? ids : Collections.emptySet();
                } catch (Exception e) {
                    logger.warn("Error fetching from DB cache: {}", e.getMessage());
                    return Collections.emptySet();
                }
            });
        }

        return dbFuture.thenCompose(dbIds -> {
            if (!dbIds.isEmpty()) {
                logger.info("Retrieved {} IDs from DB cache.", dbIds.size());
                return CompletableFuture.completedFuture(dbIds);
            }
            logger.debug("DB cache empty or unavailable, trying Redis cache.");
            return redisCacheService.getAllBookIdsAsync().thenCompose(redisIds -> {
                if (!redisIds.isEmpty()) {
                    logger.info("Retrieved {} IDs from Redis cache.", redisIds.size());
                    return CompletableFuture.completedFuture(redisIds);
                }
                logger.debug("Redis cache empty or unavailable, trying Spring cache.");
                return getSpringCacheBookIds();
            });
        });
    }

    private CompletableFuture<Set<String>> getSpringCacheBookIds() {
        return CompletableFuture.supplyAsync(() -> {
            Cache booksCache = cacheManager.getCache("books");
            if (booksCache == null) {
                logger.debug("Spring Cache 'books' is not available.");
                return Collections.emptySet();
            }
            Object nativeCache = booksCache.getNativeCache();
            if (nativeCache instanceof com.github.benmanes.caffeine.cache.Cache) {
                @SuppressWarnings("unchecked")
                com.github.benmanes.caffeine.cache.Cache<Object, Object> caffeineCache =
                        (com.github.benmanes.caffeine.cache.Cache<Object, Object>) nativeCache;
                Set<String> bookIds = new HashSet<>();
                caffeineCache.asMap().keySet().forEach(key -> {
                    if (key instanceof String) {
                        bookIds.add((String) key);
                    }
                });
                logger.debug("Retrieved {} IDs from Spring (Caffeine) cache.", bookIds.size());
                return bookIds;
            }
            logger.debug("Spring Cache native cache is not Caffeine: {}", nativeCache != null ? nativeCache.getClass().getName() : "null");
            return Collections.emptySet();
        });
    }

    public CompletableFuture<Boolean> isBookInCache(String id) {
        if (id == null || id.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }
        Cache booksSpringCache = cacheManager.getCache("books");
        if (booksSpringCache != null && booksSpringCache.get(id) != null) {
            return CompletableFuture.completedFuture(true);
        }

        return redisCacheService.isRedisAvailableAsync()
            .thenCompose(isRedisAvailable -> {
                if (!isRedisAvailable) {
                    // Redis not available, check DB cache if enabled
                    if (dbCacheEnabled) {
                        return CompletableFuture.supplyAsync(() -> {
                            try {
                                return cachedBookRepository.findByGoogleBooksId(id).isPresent();
                            } catch (Exception e) {
                                logger.warn("Error checking DB cache for book ID {} (Redis unavailable): {}", id, e.getMessage());
                                return false;
                            }
                        });
                    }
                    return CompletableFuture.completedFuture(false); // Redis unavailable, DB not enabled or not checked
                }

                // Redis is available, check if book is in Redis
                return redisCacheService.getBookByIdAsync(id)
                    .thenCompose(optionalBook -> {
                        if (optionalBook.isPresent()) {
                            return CompletableFuture.completedFuture(true); // Book found in Redis
                        }
                        // Book not in Redis, check DB cache if enabled
                        if (dbCacheEnabled) {
                            return CompletableFuture.supplyAsync(() -> {
                                try {
                                    return cachedBookRepository.findByGoogleBooksId(id).isPresent();
                                } catch (Exception e) {
                                    logger.warn("Error checking DB cache for book ID {} (not in Redis): {}", id, e.getMessage());
                                    return false;
                                }
                            });
                        }
                        return CompletableFuture.completedFuture(false); // Not in Redis, DB not enabled or not checked
                    });
            });
    }

    public CompletableFuture<Optional<Book>> getCachedBook(String id) {
        if (id == null || id.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Book ID cannot be null or empty."));
        }
        Cache booksSpringCache = cacheManager.getCache("books");
        if (booksSpringCache != null) {
            Book cachedValue = booksSpringCache.get(id, Book.class);
            if (cachedValue != null) return CompletableFuture.completedFuture(Optional.of(cachedValue));
        }

        return redisCacheService.isRedisAvailableAsync()
            .thenCompose(isAvailable -> {
                if (!isAvailable) return CompletableFuture.completedFuture(Optional.<Book>empty());
                return redisCacheService.getBookByIdAsync(id);
            })
            .thenCompose(redisBook -> {
                if (redisBook.isPresent()) return CompletableFuture.completedFuture(redisBook);
                if (dbCacheEnabled) {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            return cachedBookRepository.findByGoogleBooksId(id)
                                    .map(com.williamcallahan.book_recommendation_engine.model.CachedBook::toBook);
                        } catch (Exception e) {
                            logger.warn("Error reading from DB cache for ID {}: {}", id, e.getMessage());
                            return Optional.empty();
                        }
                    });
                }
                return CompletableFuture.completedFuture(Optional.empty());
            });
    }

    public CompletableFuture<Void> cacheBook(Book book) {
        if (book == null || book.getId() == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Book and Book ID must not be null."));
        }
        return CompletableFuture.runAsync(() -> {
            Cache booksSpringCache = cacheManager.getCache("books");
            if (booksSpringCache != null) {
                booksSpringCache.put(book.getId(), book);
            }
            // Delegate to reactive for DB and Redis. Subscribe is non-blocking.
            bookReactiveCacheService.cacheBookReactive(book).subscribe(
                null,
                error -> logger.error("Error during reactive caching via facade for book ID {}: {}", book.getId(), error.getMessage())
            );
            logger.info("Initiated caching for book ID: {}", book.getId());
        });
    }

    public CompletableFuture<Void> evictBook(String id) {
        if (id == null || id.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Book ID cannot be null or empty."));
        }
        Cache booksSpringCache = cacheManager.getCache("books");
        if (booksSpringCache != null) {
            booksSpringCache.evictIfPresent(id);
        }

        CompletableFuture<Void> redisEviction = redisCacheService.isRedisAvailableAsync()
            .thenCompose(isAvailable -> {
                if (isAvailable) {
                    return redisCacheService.evictBookAsync(id);
                }
                return CompletableFuture.completedFuture(null); // Do nothing if Redis is not available
            });

        CompletableFuture<Void> dbEviction = CompletableFuture.runAsync(() -> {
            if (dbCacheEnabled) {
                try {
                    cachedBookRepository.findByGoogleBooksId(id).ifPresent(cb -> cachedBookRepository.deleteByIdAsync(cb.getId()).join());
                    logger.info("Evicted book ID {} from DB cache.", id);
                } catch (Exception e) {
                    logger.error("Error evicting book ID {} from DB cache: {}", id, e.getMessage());
                }
            }
        });

        return CompletableFuture.allOf(redisEviction, dbEviction)
            .thenRun(() -> logger.info("Completed eviction for book ID {} from all caches.", id))
            .exceptionally(ex -> {
                logger.error("Exception during combined eviction for book ID {}: {}", id, ex.getMessage());
                return null;
            });
    }

    public CompletableFuture<Void> clearAll() {
        Cache booksSpringCache = cacheManager.getCache("books");
        if (booksSpringCache != null) {
            booksSpringCache.clear();
        }

        CompletableFuture<Void> dbClear = CompletableFuture.runAsync(() -> {
            if (dbCacheEnabled) {
                try {
                    cachedBookRepository.deleteAll();
                    logger.info("Cleared all entries from DB cache.");
                } catch (Exception e) {
                    logger.error("Error clearing DB cache: {}", e.getMessage());
                }
            }
        });
        // Note: Redis wide clear is usually an admin task. Not clearing all Redis keys here.
        return dbClear.thenRun(() -> logger.info("Cleared application-managed caches (Spring, DB)."));
    }

    public CompletableFuture<Void> updateBook(Book book) {
        if (book == null || book.getId() == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Book and Book ID must not be null."));
        }
        return evictBook(book.getId())
            .thenCompose(v -> cacheBook(book))
            .thenRun(() -> logger.info("Updated book ID: {} in all caches.", book.getId()));
    }

    public CompletableFuture<Void> removeBook(String id) {
        return evictBook(id)
            .thenRun(() -> logger.info("Removed book ID: {} from all caches.", id));
    }
}
