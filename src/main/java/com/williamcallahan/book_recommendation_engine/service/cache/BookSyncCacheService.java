/**
 * Asynchronous service for handling book caching operations with non-blocking I/O
 * This service provides CompletableFuture-based programming support for cache operations
 * It handles:
 * - Non-blocking book retrieval and storage operations
 * - Asynchronous search functionality with reactive results
 * - Async cache update and invalidation operations
 * - Integration with asynchronous data sources and external APIs
 * - CompletableFuture-based method invocation patterns for cache operations
 * - Comprehensive async error handling and retry mechanisms
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service.cache;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.service.RedisCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class BookSyncCacheService {

    private static final Logger logger = LoggerFactory.getLogger(BookSyncCacheService.class);

    private final RedisCacheService redisCacheService;
    private final CachedBookRepository cachedBookRepository; // Optional
    private final BookReactiveCacheService bookReactiveCacheService; // For delegation
    private final AsyncTaskExecutor mvcTaskExecutor;

    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled;

    public BookSyncCacheService(RedisCacheService redisCacheService,
                                BookReactiveCacheService bookReactiveCacheService,
                                @Qualifier("mvcTaskExecutor") AsyncTaskExecutor mvcTaskExecutor,
                                @Autowired(required = false) CachedBookRepository cachedBookRepository) {
        this.redisCacheService = redisCacheService;
        this.cachedBookRepository = cachedBookRepository;
        this.bookReactiveCacheService = bookReactiveCacheService;
        this.mvcTaskExecutor = mvcTaskExecutor;

        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false; // If DB is not there, main cacheEnabled might be affected
                                     // This logic might need refinement based on overall cache strategy
            logger.info("BookSyncCacheService: Database cache (CachedBookRepository) is not available.");
        }
    }

    /**
     * Retrieves a book by its Google Books ID using a multi-level asynchronous cache approach
     * This method is called by the Facade which might have @Cacheable
     * Cache hierarchy: Spring Cache (handled by facade) → Redis → Database
     * If a book is found in a slower cache (Redis, Database), faster caches are populated
     * Returns null if the book is not found in any of these cache layers
     *
     * @param id The Google Books ID of the book to retrieve
     * @return CompletableFuture<Book> containing the Book object if found in any cache, otherwise null
     */
    public CompletableFuture<Book> getBookByIdAsync(String id) {
        logger.debug("BookSyncCacheService.getBookByIdAsync called for ID: {}", id);

        // 1. Check Redis cache asynchronously
        return redisCacheService.getBookByIdAsync(id)
            .thenCompose(redisBookOpt -> {
                if (redisBookOpt.isPresent()) {
                    Book redisBook = redisBookOpt.get();
                    logger.debug("Redis cache hit for book ID (async): {}", id);
                    return CompletableFuture.completedFuture(redisBook);
                }

                // 2. Check database cache (if enabled and available)
                if (cacheEnabled && cachedBookRepository != null) {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            Optional<CachedBook> dbCachedBookOpt = cachedBookRepository.findByGoogleBooksId(id);
                            if (dbCachedBookOpt.isPresent()) {
                                Book dbBook = dbCachedBookOpt.get().toBook();
                                logger.debug("Database cache hit for book ID (async): {}", id);
                                // Populate Redis if found in DB and not in Redis (async)
                                redisCacheService.cacheBookAsync(id, dbBook)
                                    .whenComplete((result, ex) -> {
                                        if (ex != null) {
                                            logger.warn("Failed to populate Redis cache for book ID {} from DB: {}", id, ex.getMessage());
                                        }
                                    });
                                return dbBook;
                            }
                            return null;
                        } catch (Exception e) {
                            logger.warn("Error accessing database cache for book ID {} (async): {}", id, e.getMessage());
                            return null;
                        }
                    }, mvcTaskExecutor);
                }
                
                logger.debug("Book ID {} not found in BookSyncCacheService caches (Redis, DB). Returning null.", id);
                return CompletableFuture.completedFuture(null);
            });
    }

    /**
     * Asynchronously retrieves books by ISBN
     * Delegates to the reactive version
     *
     * @param isbn The book ISBN
     * @return CompletableFuture<List<Book>> containing books matching the ISBN, or an empty list
     */
    public CompletableFuture<List<Book>> getBooksByIsbnAsync(String isbn) {
        logger.debug("BookSyncCacheService.getBooksByIsbnAsync called for ISBN: {}", isbn);
        // This delegates to the reactive service, which will handle its own caching strategy.
        return bookReactiveCacheService.getBooksByIsbnReactive(isbn)
                                     .toFuture()
                                     .handle((result, ex) -> {
                                         if (ex != null) {
                                             logger.warn("Error retrieving books by ISBN {} (async): {}", isbn, ex.getMessage());
                                             return Collections.<Book>emptyList();
                                         }
                                         return result != null ? result : Collections.<Book>emptyList();
                                     });
    }

    /**
     * Asynchronously searches for books
     * Delegates to the reactive version
     *
     * @param query The search query
     * @param startIndex The start index for pagination
     * @param maxResults The maximum number of results
     * @return CompletableFuture<List<Book>> containing books matching the search criteria, or an empty list
     */
    public CompletableFuture<List<Book>> searchBooksAsync(String query, int startIndex, int maxResults) {
        logger.info("BookSyncCacheService.searchBooksAsync with query: {}, startIndex: {}, maxResults: {}", query, startIndex, maxResults);
        // This delegates to the reactive service
        // The reactive searchBooksReactive should handle its own caching (e.g. search results cache)
        return bookReactiveCacheService.searchBooksReactive(query, startIndex, maxResults, null, null, "relevance")
                                     .toFuture()
                                     .handle((result, ex) -> {
                                         if (ex != null) {
                                             logger.warn("Error searching books (async) for query '{}': {}", query, ex.getMessage());
                                             return Collections.<Book>emptyList();
                                         }
                                         return result != null ? result : Collections.<Book>emptyList();
                                     });
    }

    // In-memory cache helper methods (populateInMemoryCache, evictFromInMemoryCache, clearInMemoryCache, 
    // isBookInInMemoryCache, getBookFromInMemoryCache) are removed as this service no longer manages a direct in-memory cache.
    // Spring Cache (e.g., Caffeine via @Cacheable on BookCacheFacadeService) handles this role.

    // Backward compatibility synchronous methods - deprecated in favor of async versions

    /**
     * @deprecated Use getBookByIdAsync() instead for better performance and non-blocking behavior.
     *             This synchronous wrapper is provided for backward compatibility and will be removed in a future version.
     *             Note: This method blocks the calling thread and should be avoided in async contexts.
     */
    @Deprecated
    public Book getBookById(String id) {
        logger.warn("Deprecated method BookSyncCacheService.getBookById called for ID: {}. Consider migrating to getBookByIdAsync.", id);
        try {
            // Use get() with timeout instead of join() for better timeout control
            return getBookByIdAsync(id).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Error getting book by ID {} synchronously: {}", id, e.getMessage());
            return null;
        }
    }

    /**
     * @deprecated Use getBooksByIsbnAsync() instead for better performance and non-blocking behavior.
     *             This synchronous wrapper is provided for backward compatibility and will be removed in a future version.
     *             Note: This method blocks the calling thread and should be avoided in async contexts.
     */
    @Deprecated
    public List<Book> getBooksByIsbn(String isbn) {
        logger.warn("Deprecated method BookSyncCacheService.getBooksByIsbn called for ISBN: {}. Consider migrating to getBooksByIsbnAsync.", isbn);
        try {
            // Use get() with timeout instead of join() for better timeout control
            return getBooksByIsbnAsync(isbn).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Error getting books by ISBN {} synchronously: {}", isbn, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * @deprecated Use searchBooksAsync() instead for better performance and non-blocking behavior.
     *             This synchronous wrapper is provided for backward compatibility and will be removed in a future version.
     *             Note: This method blocks the calling thread and should be avoided in async contexts.
     */
    @Deprecated
    public List<Book> searchBooks(String query, int startIndex, int maxResults) {
        logger.warn("Deprecated method BookSyncCacheService.searchBooks called for query: '{}'. Consider migrating to searchBooksAsync.", query);
        try {
            // Use get() with timeout instead of join() for better timeout control
            return searchBooksAsync(query, startIndex, maxResults).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Error searching books for query '{}' synchronously: {}", query, e.getMessage());
            return Collections.emptyList();
        }
    }
}
