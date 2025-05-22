/**
 * Synchronous service for handling book caching operations with traditional blocking I/O
 * This service provides traditional synchronous programming support for cache operations
 * It handles:
 * - Blocking book retrieval and storage operations
 * - Synchronous search functionality with immediate results
 * - Traditional cache update and invalidation operations
 * - Integration with synchronous data sources and external APIs
 * - Direct method invocation patterns for cache operations
 * - Simple error handling and retry mechanisms
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Service
public class BookSyncCacheService {

    private static final Logger logger = LoggerFactory.getLogger(BookSyncCacheService.class);

    private final RedisCacheService redisCacheService;
    private final CachedBookRepository cachedBookRepository; // Optional
    private final BookReactiveCacheService bookReactiveCacheService; // For delegation

    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled;

    public BookSyncCacheService(RedisCacheService redisCacheService,
                                BookReactiveCacheService bookReactiveCacheService,
                                @Autowired(required = false) CachedBookRepository cachedBookRepository) {
        this.redisCacheService = redisCacheService;
        this.cachedBookRepository = cachedBookRepository;
        this.bookReactiveCacheService = bookReactiveCacheService;

        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false; // If DB is not there, main cacheEnabled might be affected
                                     // This logic might need refinement based on overall cache strategy
            logger.info("BookSyncCacheService: Database cache (CachedBookRepository) is not available.");
        }
    }

    /**
     * Retrieves a book by its Google Books ID using a multi-level synchronous cache approach
     * This method is called by the Facade which might have @Cacheable
     * Cache hierarchy: In-memory (ConcurrentHashMap), Redis, Database
     * If a book is found in a slower cache (Redis, Database), faster caches are populated
     * Returns null if the book is not found in any of these synchronous cache layers
     *
     * @param id The Google Books ID of the book to retrieve
     * @return The Book object if found in any synchronous cache, otherwise null
     */
    public Book getBookById(String id) {
        logger.debug("BookSyncCacheService.getBookById (sync) called for ID: {}", id);

        // 1. Check Redis cache
        Optional<Book> redisBookOpt = redisCacheService.getBookById(id);
        if (redisBookOpt.isPresent()) {
            Book redisBook = redisBookOpt.get();
            logger.debug("Redis cache hit for book ID (sync): {}", id);
            // The Spring Cache on the facade will handle in-memory caching
            return redisBook;
        }

        // 2. Check database cache (if enabled and available)
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> dbCachedBookOpt = cachedBookRepository.findByGoogleBooksId(id);
                if (dbCachedBookOpt.isPresent()) {
                    Book dbBook = dbCachedBookOpt.get().toBook();
                    logger.debug("Database cache hit for book ID (sync): {}", id);
                    // The Spring Cache on the facade will handle in-memory caching
                    // Populate Redis if found in DB and not in Redis
                    redisCacheService.cacheBook(id, dbBook); // Also populate Redis
                    return dbBook;
                }
            } catch (Exception e) {
                logger.warn("Error accessing database cache for book ID {} (sync): {}", id, e.getMessage());
                // Proceed as if DB cache miss
            }
        }
        
        logger.debug("Book ID {} not found in BookSyncCacheService synchronous caches (In-Memory, Redis, DB). Returning null.", id);
        return null;
    }

    /**
     * Synchronously retrieves books by ISBN
     * Delegates to the reactive version and blocks
     *
     * @param isbn The book ISBN
     * @return A List of Book objects matching the ISBN, or an empty list
     */
    public List<Book> getBooksByIsbn(String isbn) {
        logger.debug("BookSyncCacheService.getBooksByIsbn (sync) called for ISBN: {}", isbn);
        // This delegates to the reactive service, which will handle its own caching strategy.
        return bookReactiveCacheService.getBooksByIsbnReactive(isbn)
                                     .blockOptional().orElse(Collections.emptyList());
    }

    /**
     * Synchronously searches for books
     * Delegates to the reactive version and blocks
     *
     * @param query The search query
     * @param startIndex The start index for pagination
     * @param maxResults The maximum number of results
     * @return A List of Book objects matching the search criteria, or an empty list
     */
    public List<Book> searchBooks(String query, int startIndex, int maxResults) {
        logger.info("BookSyncCacheService.searchBooks (sync) with query: {}, startIndex: {}, maxResults: {}", query, startIndex, maxResults);
        // This delegates to the reactive service
        // The reactive searchBooksReactive should handle its own caching (e.g. search results cache)
        return bookReactiveCacheService.searchBooksReactive(query, startIndex, maxResults, null, null, "relevance")
                                     .blockOptional().orElse(Collections.emptyList());
    }
    // In-memory cache helper methods (populateInMemoryCache, evictFromInMemoryCache, clearInMemoryCache, 
    // isBookInInMemoryCache, getBookFromInMemoryCache) are removed as this service no longer manages a direct in-memory cache.
    // Spring Cache (e.g., Caffeine via @Cacheable on BookCacheFacadeService) handles this role.
}
