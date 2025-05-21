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
import java.util.concurrent.ConcurrentHashMap;

@Service
public class BookSyncCacheService {

    private static final Logger logger = LoggerFactory.getLogger(BookSyncCacheService.class);

    private final ConcurrentHashMap<String, Book> bookDetailCache = new ConcurrentHashMap<>();
    private final RedisCacheService redisCacheService;
    private final CachedBookRepository cachedBookRepository; // Optional
    private final BookReactiveCacheService bookReactiveCacheService; // For delegation

    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled;

    @Autowired
    public BookSyncCacheService(RedisCacheService redisCacheService,
                                @Autowired(required = false) CachedBookRepository cachedBookRepository,
                                BookReactiveCacheService bookReactiveCacheService) {
        this.redisCacheService = redisCacheService;
        this.cachedBookRepository = cachedBookRepository;
        this.bookReactiveCacheService = bookReactiveCacheService;

        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false; // If DB is not there, main cacheEnabled might be affected.
                                     // This logic might need refinement based on overall cache strategy.
            logger.info("BookSyncCacheService: Database cache (CachedBookRepository) is not available.");
        }
    }

    /**
     * Retrieves a book by its Google Books ID using a multi-level synchronous cache approach.
     * This method is called by the Facade which might have @Cacheable.
     * Cache hierarchy: In-memory (ConcurrentHashMap), Redis, Database.
     * If a book is found in a slower cache (Redis, Database), faster caches are populated.
     * Returns null if the book is not found in any of these synchronous cache layers.
     *
     * @param id The Google Books ID of the book to retrieve.
     * @return The Book object if found in any synchronous cache, otherwise null.
     */
    public Book getBookById(String id) {
        logger.debug("BookSyncCacheService.getBookById (sync) called for ID: {}", id);

        // 1. Check in-memory cache (bookDetailCache)
        Book book = bookDetailCache.get(id);
        if (book != null) {
            logger.debug("In-memory cache hit for book ID (sync): {}", id);
            return book;
        }

        // 2. Check Redis cache
        Optional<Book> redisBookOpt = redisCacheService.getBookById(id);
        if (redisBookOpt.isPresent()) {
            Book redisBook = redisBookOpt.get();
            logger.debug("Redis cache hit for book ID (sync): {}", id);
            bookDetailCache.put(id, redisBook); // Populate faster in-memory cache
            return redisBook;
        }

        // 3. Check database cache (if enabled and available)
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> dbCachedBookOpt = cachedBookRepository.findByGoogleBooksId(id);
                if (dbCachedBookOpt.isPresent()) {
                    Book dbBook = dbCachedBookOpt.get().toBook();
                    logger.debug("Database cache hit for book ID (sync): {}", id);
                    // Populate faster caches
                    bookDetailCache.put(id, dbBook);
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
     * Synchronously retrieves books by ISBN.
     * Delegates to the reactive version and blocks.
     *
     * @param isbn The book ISBN.
     * @return A List of Book objects matching the ISBN, or an empty list.
     */
    public List<Book> getBooksByIsbn(String isbn) {
        logger.debug("BookSyncCacheService.getBooksByIsbn (sync) called for ISBN: {}", isbn);
        // This delegates to the reactive service, which will handle its own caching strategy.
        return bookReactiveCacheService.getBooksByIsbnReactive(isbn)
                                     .blockOptional().orElse(Collections.emptyList());
    }

    /**
     * Synchronously searches for books.
     * Delegates to the reactive version and blocks.
     *
     * @param query The search query.
     * @param startIndex The start index for pagination.
     * @param maxResults The maximum number of results.
     * @return A List of Book objects matching the search criteria, or an empty list.
     */
    public List<Book> searchBooks(String query, int startIndex, int maxResults) {
        logger.info("BookSyncCacheService.searchBooks (sync) with query: {}, startIndex: {}, maxResults: {}", query, startIndex, maxResults);
        // This delegates to the reactive service.
        // The reactive searchBooksReactive should handle its own caching (e.g. search results cache).
        return bookReactiveCacheService.searchBooksReactive(query, startIndex, maxResults, null, null, "relevance")
                                     .blockOptional().orElse(Collections.emptyList());
    }
    
    // Helper method to populate in-memory cache, potentially called by other services or facade
    public void populateInMemoryCache(String id, Book book) {
        if (id != null && book != null) {
            bookDetailCache.put(id, book);
        }
    }

    // Helper method to evict from in-memory cache
    public void evictFromInMemoryCache(String id) {
        if (id != null) {
            bookDetailCache.remove(id);
        }
    }
    
    // Helper method to clear in-memory cache
    public void clearInMemoryCache() {
        bookDetailCache.clear();
    }
     public boolean isBookInInMemoryCache(String id) {
        return id != null && bookDetailCache.containsKey(id);
    }

    public Optional<Book> getBookFromInMemoryCache(String id) {
        return Optional.ofNullable(bookDetailCache.get(id));
    }
}
