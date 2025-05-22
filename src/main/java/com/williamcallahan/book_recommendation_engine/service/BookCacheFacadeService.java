/**
 * Facade service that provides a unified interface for book caching operations
 * This service acts as a central entry point for all book cache-related functionality
 * It handles:
 * - Coordinating between synchronous and reactive cache services
 * - Providing consistent API for both sync and async book operations
 * - Managing cache lifecycle including eviction and maintenance
 * - Delegating similarity-based recommendations to specialized services
 * - Abstracting the complexity of multiple cache layers and strategies
 * - Ensuring proper cache consistency across different service implementations
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.cache.BookReactiveCacheService;
import com.williamcallahan.book_recommendation_engine.service.cache.BookSyncCacheService;
// import com.williamcallahan.book_recommendation_engine.service.cache.CacheMaintenanceService; // Not directly used by facade's public methods
import com.williamcallahan.book_recommendation_engine.service.similarity.BookSimilarityService;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository; // For getAllCachedBookIds
// import com.williamcallahan.book_recommendation_engine.service.RedisCacheService; // Used via sub-services or directly if needed
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Collections;
// import java.util.HashSet; // Not used
import java.util.List;
import java.util.Optional;
import java.util.Set;
// import java.util.stream.Collectors; // Not used

@Service
public class BookCacheFacadeService {

    private static final Logger logger = LoggerFactory.getLogger(BookCacheFacadeService.class);

    private final BookSyncCacheService bookSyncCacheService;
    private final BookReactiveCacheService bookReactiveCacheService;
    // CacheMaintenanceService is mostly event-driven or scheduled, not directly called by facade for public API usually.
    private final BookSimilarityService bookSimilarityService;
    private final CacheManager cacheManager;
    private final RedisCacheService redisCacheService; // Kept for direct use in utility methods
    private final CachedBookRepository cachedBookRepository; // Optional, for getAllCachedBookIds & direct DB actions

    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled; // To mirror logic from original BookCacheService

    @Autowired
    public BookCacheFacadeService(BookSyncCacheService bookSyncCacheService,
                                  BookReactiveCacheService bookReactiveCacheService,
                                  // CacheMaintenanceService cacheMaintenanceService, // Removed if not directly called
                                  BookSimilarityService bookSimilarityService,
                                  CacheManager cacheManager,
                                  RedisCacheService redisCacheService,
                                  @Autowired(required = false) CachedBookRepository cachedBookRepository) {
        this.bookSyncCacheService = bookSyncCacheService;
        this.bookReactiveCacheService = bookReactiveCacheService;
        // this.cacheMaintenanceService = cacheMaintenanceService;
        this.bookSimilarityService = bookSimilarityService;
        this.cacheManager = cacheManager;
        this.redisCacheService = redisCacheService;
        this.cachedBookRepository = cachedBookRepository;

        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false;
            logger.info("BookCacheFacadeService: Database cache (CachedBookRepository) is not available.");
        }
    }

    // Synchronous API methods delegating to BookSyncCacheService
    // The @Cacheable annotation can remain here on the facade
    @Cacheable(value = "books", key = "#id", unless = "#result == null")
    public Book getBookById(String id) {
        logger.debug("BookCacheFacadeService.getBookById (sync) called for ID: {}", id);
        return bookSyncCacheService.getBookById(id);
    }

    public List<Book> getBooksByIsbn(String isbn) {
        logger.debug("BookCacheFacadeService.getBooksByIsbn (sync) called for ISBN: {}", isbn);
        return bookSyncCacheService.getBooksByIsbn(isbn);
    }

    public List<Book> searchBooks(String query, int startIndex, int maxResults) {
        logger.info("BookCacheFacadeService.searchBooks (sync) with query: {}, startIndex: {}, maxResults: {}", query, startIndex, maxResults);
        return bookSyncCacheService.searchBooks(query, startIndex, maxResults);
    }

    // Reactive API methods delegating to BookReactiveCacheService
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

    // Similarity methods delegating to BookSimilarityService
    public List<Book> getSimilarBooks(String bookId, int count) {
        logger.info("BookCacheFacadeService.getSimilarBooks called for bookId: {}, count: {}", bookId, count);
        return bookSimilarityService.getSimilarBooks(bookId, count);
    }

    public Mono<List<Book>> getSimilarBooksReactive(String bookId, int count) {
        logger.info("BookCacheFacadeService.getSimilarBooksReactive called for bookId: {}, count: {}", bookId, count);
        return bookSimilarityService.getSimilarBooksReactive(bookId, count);
    }

    // Cache management/utility methods

    public Set<String> getAllCachedBookIds() {
        // Logic from original BookCacheService
        if (cacheEnabled && cachedBookRepository != null &&
            !(cachedBookRepository instanceof com.williamcallahan.book_recommendation_engine.repository.NoOpCachedBookRepository)) {
            try {
                logger.info("BookCacheFacadeService: Fetching all distinct Google Books IDs from active persistent CachedBookRepository.");
                Set<String> ids = cachedBookRepository.findAllDistinctGoogleBooksIds();
                logger.info("BookCacheFacadeService: Retrieved {} distinct book IDs from persistent repository.", ids != null ? ids.size() : 0);
                return ids != null ? ids : Collections.emptySet();
            } catch (Exception e) {
                logger.error("BookCacheFacadeService: Error fetching all distinct Google Books IDs from persistent repository: {}", e.getMessage(), e);
            }
        }
        // Fallback to in-memory cache keys if DB fails or is not primary source for this
        // BookSyncCacheService holds the L1 in-memory cache now.
        // This part needs careful thought: if bookDetailCache is shared, one service can expose its keys.
        // For now, assuming BookSyncCacheService can provide its keys.
        // This might require adding a method to BookSyncCacheService like `getInMemoryCachedBookIds()`
        // logger.warn("BookCacheFacadeService: getAllCachedBookIds - DB not available or error, attempting to get from sync in-memory (not fully implemented yet).");
        // TODO: Revisit how to get keys from Spring Cache if needed, or rely solely on DB for this method.
        // Set<String> inMemoryIds = new HashSet<>(bookSyncCacheService.getInMemoryCachedBookIds()); // Assuming such method exists
        // return inMemoryIds;
        return Collections.emptySet(); // Placeholder until in-memory key access is defined
    }

    public boolean isBookInCache(String id) {
        if (id == null || id.isEmpty()) return false;
        // Check Spring Cache (often managed at Facade level or by sync service)
        org.springframework.cache.Cache booksCache = cacheManager.getCache("books");
        if (booksCache != null && booksCache.get(id) != null) return true;

        // Check Redis
        if (redisCacheService.getBookById(id).isPresent()) return true;
        
        // Check Database Cache (if enabled)
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                if (cachedBookRepository.findByGoogleBooksId(id).isPresent()) return true;
            } catch (Exception e) {
                logger.warn("Error checking persistent cache for book ID {}: {}", id, e.getMessage());
            }
        }
        return false;
    }

    public Optional<Book> getCachedBook(String id) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Book ID cannot be null or empty for getCachedBook");
        }
        // Try Spring Cache
        org.springframework.cache.Cache booksSpringCache = cacheManager.getCache("books");
        if (booksSpringCache != null) {
            Book cachedValue = booksSpringCache.get(id, Book.class);
            if (cachedValue != null) return Optional.of(cachedValue);
        }

        // Try Redis
        Optional<Book> redisBook = redisCacheService.getBookById(id);
        if (redisBook.isPresent()) return redisBook;

        // Try Database Cache
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                return cachedBookRepository.findByGoogleBooksId(id).map(com.williamcallahan.book_recommendation_engine.model.CachedBook::toBook);
            } catch (Exception e) {
                logger.warn("Error reading from persistent cache for getCachedBook (ID {}): {}", id, e.getMessage());
            }
        }
        return Optional.empty();
    }

    public void cacheBook(Book book) {
        if (book == null || book.getId() == null) {
            throw new IllegalArgumentException("Book and Book ID must not be null for cacheBook");
        }
        // Populate Spring Cache
        org.springframework.cache.Cache booksSpringCache = cacheManager.getCache("books");
        if (booksSpringCache != null) {
            booksSpringCache.put(book.getId(), book);
        }

        // Delegate to reactive for DB and Redis (as it handles complex logic like merge, embedding)
        bookReactiveCacheService.cacheBookReactive(book).subscribe(
            null, // onComplete
            error -> logger.error("Error during reactive caching via facade for book ID {}: {}", book.getId(), error.getMessage())
        );
        // If a purely synchronous Redis put is needed without DB interaction, call redisCacheService directly
        // redisCacheService.cacheBook(book.getId(), book); 
        logger.info("BookCacheFacadeService: Initiated caching for book ID: {}", book.getId());
    }

    public void evictBook(String id) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Book ID cannot be null or empty for evictBook");
        }
        // Evict from Spring Cache
        org.springframework.cache.Cache booksSpringCache = cacheManager.getCache("books");
        if (booksSpringCache != null) {
            booksSpringCache.evictIfPresent(id);
        }

        // Evict from Redis
        redisCacheService.evictBook(id); // Assuming sync evict is fine, or use reactive

        // Evict from Database
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                cachedBookRepository.findByGoogleBooksId(id).ifPresent(cachedBook -> {
                    cachedBookRepository.deleteById(cachedBook.getId());
                    logger.info("Evicted book with Google ID {} (DB ID {}) from persistent cache.", id, cachedBook.getId());
                });
            } catch (Exception e) {
                logger.error("Error evicting book ID {} from persistent cache: {}", id, e.getMessage());
            }
        }
        logger.info("BookCacheFacadeService: Evicted book ID {} from caches.", id);
    }

    public void clearAll() {
        // Clear Spring Cache
        org.springframework.cache.Cache booksSpringCache = cacheManager.getCache("books");
        if (booksSpringCache != null) {
            booksSpringCache.clear();
        }
        
        // Clear Database Cache
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                cachedBookRepository.deleteAll();
                logger.info("Cleared all entries from persistent cache.");
            } catch (Exception e) {
                logger.error("Error clearing persistent cache: {}", e.getMessage());
            }
        }
        
        // Note: Redis wide clear is usually a separate admin task.
        // If only book:* keys need clearing, RedisCacheService would need a specific method.
        logger.info("BookCacheFacadeService: Cleared application-managed caches (in-memory, Spring, DB). Redis not cleared by this method directly.");
    }
    
    public void updateBook(Book book) {
        if (book == null || book.getId() == null) {
            throw new IllegalArgumentException("Book and Book ID must not be null for updateBook");
        }
        // Evict first to ensure stale data is gone from all simple caches
        evictBook(book.getId()); 
        // Then cache the new version. cacheBookReactive handles complex merge/DB logic.
        cacheBook(book);
        logger.info("BookCacheFacadeService: Updated book ID: {} in all caches", book.getId());
    }

    public void removeBook(String id) { // Same as evictBook for this structure
        evictBook(id);
        logger.info("BookCacheFacadeService: Removed book ID: {} from all caches", id);
    }
}
