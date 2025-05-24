/**
 * Scheduler for proactively warming caches with popular book data
 * Reduces API latency by pre-caching frequently accessed books
 * Runs during off-peak hours to minimize impact on performance
 *
 * @author William Callahan
 *
 * Features:
 * - Reduces API latency by pre-caching frequently accessed books
 * - Runs during off-peak hours to minimize impact on performance
 * - Monitors API usage to stay within rate limits
 * - Prioritizes recently viewed and popular books
 */

package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookCacheFacadeService;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Scheduler for proactively warming caches with popular book data
 * Reduces API latency by pre-caching frequently accessed books
 * Runs during off-peak hours to minimize impact on performance
 *
 * @author William Callahan
 *
 * Features:
 * - Reduces API latency by pre-caching frequently accessed books
 * - Runs during off-peak hours to minimize impact on performance
 * - Monitors API usage to stay within rate limits
 * - Prioritizes recently viewed and popular books
 */

@Component
@Configuration
@EnableScheduling
@ConditionalOnBean(GoogleBooksService.class)
public class BookCacheWarmingScheduler {

    private static final Logger logger = LoggerFactory.getLogger(BookCacheWarmingScheduler.class);
    
    private final BookCacheFacadeService bookCacheFacadeService;
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
                                     // GoogleBooksService googleBooksService, // Parameter is not used
                                     RecentlyViewedService recentlyViewedService) {
        this.bookCacheFacadeService = bookCacheFacadeService;
        // this.googleBooksService = googleBooksService; // Field is not used
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
        logger.info("Starting scheduled book cache warming");
        List<Book> books;
        try {
            books = recentlyViewedService.getRecentlyViewedBooksAsync().join();
        } catch (Exception e) {
            logger.error("Error fetching recently viewed books: {}", e.getMessage(), e);
            return;
        }
        List<String> bookIdsToWarm = books.stream()
            .map(Book::getId)
            .filter(id -> id != null && !id.isEmpty() && !recentlyWarmedBooks.contains(id))
            .collect(Collectors.toList());
        if (bookIdsToWarm.isEmpty()) {
            logger.info("No books to warm in cache");
            return;
        }
        if (recentlyWarmedBooks.size() > 500) {
            recentlyWarmedBooks.clear();
        }
        int booksToWarmCount = Math.min(bookIdsToWarm.size(), maxBooksPerRun);
        AtomicInteger warmedCount = new AtomicInteger(0);
        for (int i = 0; i < booksToWarmCount; i++) {
            String bookId = bookIdsToWarm.get(i);
            try {
                Book book = bookCacheFacadeService.getBookById(bookId).join();
                if (book != null) {
                    warmedCount.incrementAndGet();
                    recentlyWarmedBooks.add(bookId);
                    logger.info("Successfully warmed/retrieved cache for book: {} (ID: {})",
                        book.getTitle() != null ? book.getTitle() : "N/A", bookId);
                } else {
                    logger.warn("Cache warming: Book with ID {} could not be fetched or found by facade (returned null).", bookId);
                }
            } catch (Exception e) {
                logger.error("Error warming cache for book {}: {}", bookId, e.getMessage(), e);
            }
        }
        logger.info("Book cache warming completed. Processed attempts: {}. Successful warm/fetch: {}.",
            booksToWarmCount, warmedCount.get());
    }
}
