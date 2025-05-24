/**
 * Scheduler for proactively warming caches with popular book data
 * - Reduces API latency by pre-caching frequently accessed books
 * - Runs during off-peak hours to minimize impact on performance
 * - Monitors API usage to stay within rate limits
 * - Prioritizes recently viewed and popular books
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookCacheFacadeService;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.concurrent.CompletableFuture;

/**
 * Scheduler for warming book caches to minimize Google Books API calls
 * - Proactively caches popular books during low-traffic periods
 * - Prioritizes recently viewed and trending books
 * - Uses rate-limited execution to avoid overloading the API
 * - Provides configurable cache warming behavior via properties
 */
@Component
@Configuration
@EnableScheduling
public class BookCacheWarmingScheduler {

    private static final Logger logger = LoggerFactory.getLogger(BookCacheWarmingScheduler.class);
    
    private final BookCacheFacadeService bookCacheFacadeService;
    private final GoogleBooksService googleBooksService;
    private final RecentlyViewedService recentlyViewedService;
    
    // Keep track of which books have been warmed recently to avoid duplicates
    private final Set<String> recentlyWarmedBooks = ConcurrentHashMap.newKeySet();
    
    // Configurable properties
    @Value("${app.cache.warming.enabled:true}")
    private boolean cacheWarmingEnabled;
    
    @Value("${app.cache.warming.rate-limit-per-minute:3}")
    private int rateLimit;
    
    @Value("${app.cache.warming.max-books-per-run:10}")
    private int maxBooksPerRun;
    
    @Value("${app.cache.warming.recently-viewed-days:7}")
    private int recentlyViewedDays;

    public BookCacheWarmingScheduler(BookCacheFacadeService bookCacheFacadeService,
                                     @Autowired(required = false) GoogleBooksService googleBooksService,
                                     RecentlyViewedService recentlyViewedService) {
        this.bookCacheFacadeService = bookCacheFacadeService;
        this.googleBooksService = googleBooksService;
        this.recentlyViewedService = recentlyViewedService;
    }

    /**
     * Scheduled task that runs during off-peak hours (e.g., 3 AM)
     * to warm caches for popular books
     */
    @Scheduled(cron = "${app.cache.warming.cron:0 0 3 * * ?}")
    public void warmPopularBookCaches() {
        if (!cacheWarmingEnabled) {
            logger.debug("Book cache warming is disabled");
            return;
        }

        if (googleBooksService == null) {
            logger.warn("GoogleBooksService is not available, skipping cache warming");
            return;
        }

        logger.info("Starting scheduled book cache warming");

        // Asynchronously fetch recently viewed books and warm cache
        recentlyViewedService.getRecentlyViewedBooksAsync()
            .thenApply(books -> books.stream()
                .map(Book::getId)
                .filter(id -> id != null && !id.isEmpty() && !recentlyWarmedBooks.contains(id))
                .collect(Collectors.toList()))
            .thenAcceptAsync(bookIdsToWarm -> {
                // Clean up tracking set if it gets too large
                if (recentlyWarmedBooks.size() > 500) {
                    recentlyWarmedBooks.clear();
                }
                if (bookIdsToWarm.isEmpty()) {
                    logger.info("No books to warm in cache");
                    return;
                }
                AtomicInteger warmedCount = new AtomicInteger(0);
                AtomicInteger existingCount = new AtomicInteger(0);
                ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
                long delayMillis = (60L * 1000L) / rateLimit;
                int booksToWarmCount = Math.min(bookIdsToWarm.size(), maxBooksPerRun);

                for (int i = 0; i < booksToWarmCount; i++) {
                    final String bookId = bookIdsToWarm.get(i);
                    executor.schedule(() -> {
                        try {
                            boolean inCache = bookCacheFacadeService.isBookInCache(bookId).join();
                            if (inCache) {
                                existingCount.incrementAndGet();
                                logger.debug("Book {} already in cache, skipping warming", bookId);
                            } else {
                                logger.info("Warming cache for book ID: {}", bookId);
                                googleBooksService.getBookById(bookId)
                                    .thenAccept(book -> {
                                        if (book != null) {
                                            warmedCount.incrementAndGet();
                                            logger.info("Successfully warmed cache for book: {}", book.getTitle() != null ? book.getTitle() : bookId);
                                        }
                                    })
                                    .exceptionally(ex -> {
                                        logger.error("Error warming cache for book {}: {}", bookId, ex.getMessage());
                                        return null;
                                    });
                                recentlyWarmedBooks.add(bookId);
                            }
                        } catch (Exception e) {
                            logger.error("Error in cache warming task for book {}: {}", bookId, e.getMessage());
                        }
                    }, i * delayMillis, TimeUnit.MILLISECONDS);
                }
                executor.shutdown();
                CompletableFuture.runAsync(() -> {
                    try {
                        executor.awaitTermination(booksToWarmCount * delayMillis + 10000L, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        logger.error("Interrupted while waiting for cache warming tasks to finish", e);
                        Thread.currentThread().interrupt();
                    }
                    logger.info("Book cache warming completed. Warmed: {}, Already in cache: {}, Total: {}",
                        warmedCount.get(), existingCount.get(), warmedCount.get() + existingCount.get());
                });
            })
            .exceptionally(e -> {
                logger.error("Error during asynchronous book cache warming: {}", e.getMessage(), e);
                return null;
            });
    }
}
