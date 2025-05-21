/**
 * Service responsible for cache maintenance operations and scheduled cleanup
 * This service handles all automated cache management tasks and maintenance operations
 * It handles:
 * - Scheduled cache expiration and cleanup operations
 * - Event-driven cache invalidation based on data changes
 * - Cache consistency maintenance across different storage layers
 * - Periodic cache statistics and health monitoring
 * - Automated cache warming and optimization processes
 * - Responding to system events that require cache updates
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service.cache;

import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.service.RedisCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class CacheMaintenanceService {

    private static final Logger logger = LoggerFactory.getLogger(CacheMaintenanceService.class);

    private final ConcurrentHashMap<String, Book> bookDetailCache; // Shared L1 cache
    private final CachedBookRepository cachedBookRepository; // Optional
    private final CacheManager cacheManager;
    private final RedisCacheService redisCacheService;

    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled;

    @Autowired
    public CacheMaintenanceService(
            ConcurrentHashMap<String, Book> bookDetailCache, // Injected
            @Autowired(required = false) CachedBookRepository cachedBookRepository,
            CacheManager cacheManager,
            RedisCacheService redisCacheService) {
        this.bookDetailCache = bookDetailCache;
        this.cachedBookRepository = cachedBookRepository;
        this.cacheManager = cacheManager;
        this.redisCacheService = redisCacheService;

        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false;
             logger.info("CacheMaintenanceService: Database cache (CachedBookRepository) is not available.");
        }
    }

    @EventListener
    public void handleBookCoverUpdate(BookCoverUpdatedEvent event) {
        if (event.getGoogleBookId() == null) {
            logger.warn("BookCoverUpdatedEvent received with null googleBookId.");
            return;
        }
        String bookId = event.getGoogleBookId();
        String newCoverUrl = event.getNewCoverUrl();
        if (newCoverUrl == null || newCoverUrl.isEmpty()) {
            logger.warn("BookCoverUpdatedEvent received with null/empty cover URL for book ID {}.", bookId);
            return;
        }

        // Update L1 in-memory cache
        Book cachedBook = bookDetailCache.get(bookId);
        if (cachedBook != null) {
            cachedBook.setCoverImageUrl(newCoverUrl);
            logger.debug("Updated cover URL in memory cache for book ID: {}", bookId);
        } else {
            logger.debug("Book ID {} not found in memory cache for cover update, will be refreshed on next request", bookId);
        }

        // Update Database Cache (Reactive)
        if (cacheEnabled && cachedBookRepository != null) {
            Mono.fromCallable(() -> cachedBookRepository.findByGoogleBooksId(bookId))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(optionalCachedBook -> {
                    if (optionalCachedBook.isPresent()) {
                        CachedBook dbBook = optionalCachedBook.get();
                        dbBook.setCoverImageUrl(newCoverUrl);
                        return Mono.fromCallable(() -> cachedBookRepository.save(dbBook));
                    }
                    return Mono.empty();
                })
                .subscribe(
                    updatedBook -> logger.info("Updated cover URL in database for book ID: {}", bookId),
                    error -> logger.error("Error updating cover URL in database for book ID {}: {}", bookId, error.getMessage())
                );
        }

        // Evict from Spring Cache
        Optional.ofNullable(cacheManager.getCache("books")).ifPresent(c -> {
            logger.debug("Evicting book ID {} from Spring 'books' cache due to cover update.", bookId);
            c.evict(bookId);
        });

        // Evict from Redis Cache (Reactive)
        // For simplicity, evict. If the book is requested again, it will be re-fetched and re-cached with new cover.
        redisCacheService.evictBookReactive(bookId).subscribe(
            null, // onComplete
            error -> logger.error("Error evicting book ID {} from Redis during cover update: {}", bookId, error.getMessage())
        );
        
        logger.info("Processed cache updates for book ID {} due to BookCoverUpdatedEvent.", bookId);
    }

    /**
     * Scheduled task to clean expired entries from Spring's "books" cache.
     * This method is annotated with @CacheEvict to clear all entries from the "books" cache.
     * Runs daily at midnight as per the cron expression.
     */
    @CacheEvict(value = "books", allEntries = true)
    @Scheduled(cron = "0 0 0 * * ?")
    public void cleanExpiredSpringCacheEntries() {
        // The @CacheEvict annotation handles the Spring cache "books"
        logger.info("CacheMaintenanceService: Cleaned expired entries from Spring 'books' cache via @CacheEvict.");
        // If other Spring-managed caches need similar eviction, they'd need their own @CacheEvict or manual logic.
    }
    
    // Other cache maintenance tasks (e.g., cleaning bookDetailCache based on some policy,
    // or more complex Redis cleanup) could be added here.
}
