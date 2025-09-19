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
import com.williamcallahan.book_recommendation_engine.service.ApiRequestMonitor;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Scheduler for book data pre-fetching (cache warming functionality disabled)
 * - Previously cached popular books during low-traffic periods
 * - Still prioritizes recently viewed and trending books for fetching
 * - Uses rate-limited execution to avoid overloading the API
 * - Provides configurable behavior via properties
 * Note: Cache warming has been disabled as cache services have been removed
 */
@Configuration
@EnableScheduling
public class BookCacheWarmingScheduler {

    private static final Logger logger = LoggerFactory.getLogger(BookCacheWarmingScheduler.class);
    
    private final GoogleBooksService googleBooksService;
    private final RecentlyViewedService recentlyViewedService;
    private final ApplicationContext applicationContext;
    
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

    public BookCacheWarmingScheduler(GoogleBooksService googleBooksService,
                                     RecentlyViewedService recentlyViewedService,
                                     ApplicationContext applicationContext) {
        this.googleBooksService = googleBooksService;
        this.recentlyViewedService = recentlyViewedService;
        this.applicationContext = applicationContext;
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
        
        // Get books to warm (recently viewed, popular, etc.)
        List<String> bookIdsToWarm = getBookIdsToWarm();
        
        // Clean up our tracking set if it gets too large
        if (recentlyWarmedBooks.size() > 500) {
            recentlyWarmedBooks.clear();
        }
        
        // If no books to warm, we're done
        if (bookIdsToWarm.isEmpty()) {
            logger.info("No books to warm in cache");
            return;
        }
        
        // Rate-limited warming to avoid overwhelming the API
        AtomicInteger warmedCount = new AtomicInteger(0);
        AtomicInteger existingCount = new AtomicInteger(0);
        
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        
        try {
            // Calculate delay between books based on rate limit
            final long delayMillis = (60 * 1000) / rateLimit;
            
            // Check current API call count from metrics if possible
            // Inject ApiRequestMonitor if available
            int currentHourlyRequests = 0;
            try {
                ApiRequestMonitor apiRequestMonitor = applicationContext.getBean(ApiRequestMonitor.class);
                currentHourlyRequests = apiRequestMonitor.getCurrentHourlyRequests();
                logger.info("Current hourly API request count: {}. Will adjust cache warming accordingly.", currentHourlyRequests);
            } catch (Exception e) {
                logger.warn("Could not get ApiRequestMonitor metrics: {}", e.getMessage());
            }
            
            // Calculate how many books we can warm based on current API usage
            // We want to leave some headroom for user requests
            int hourlyLimit = rateLimit * 60; // Max requests per hour based on rate limit
            int requestBudget = Math.max(0, hourlyLimit / 2 - currentHourlyRequests); // Use at most half the remaining budget
            int booksToWarm = Math.min(Math.min(bookIdsToWarm.size(), maxBooksPerRun), requestBudget);
            
            logger.info("Warming {} books based on rate limit {} per minute and current API usage", 
                    booksToWarm, rateLimit);
            
            for (int i = 0; i < booksToWarm; i++) {
                final String bookId = bookIdsToWarm.get(i);
                
                // Schedule each book to be warmed with a delay
                executor.schedule(() -> {
                    try {
                        // Note: Cache warming functionality has been disabled as the cache service has been removed
                        logger.info("Attempting to warm book ID: {} (cache functionality disabled)", bookId);
                        googleBooksService.getBookById(bookId)
                            .thenAccept(book -> {
                                if (book != null) {
                                    warmedCount.incrementAndGet();
                                    logger.info("Successfully fetched book for warming: {}",
                                            book.getTitle() != null ? book.getTitle() : bookId);
                                } else {
                                    logger.debug("No book found for ID: {}", bookId);
                                }
                            })
                            .exceptionally(ex -> {
                                logger.error("Error fetching book {}: {}", bookId, ex.getMessage());
                                return null;
                            });

                        // Track that we've processed this book
                        recentlyWarmedBooks.add(bookId);
                    } catch (Exception e) {
                        logger.error("Error in cache warming task for book {}: {}", bookId, e.getMessage());
                    }
                }, i * delayMillis, TimeUnit.MILLISECONDS);
            }
            
            // Make sure all tasks have a chance to complete
            executor.shutdown();
            executor.awaitTermination(maxBooksPerRun * delayMillis + 10000, TimeUnit.MILLISECONDS);
            
            logger.info("Book cache warming completed. Warmed: {}, Already in cache: {}, Total: {}", 
                    warmedCount.get(), existingCount.get(), 
                    warmedCount.get() + existingCount.get());
            
        } catch (Exception e) {
            logger.error("Error during book cache warming: {}", e.getMessage());
        } finally {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
        }
    }
    
    /**
     * Get a list of book IDs to warm in the cache, based on recently viewed books.
     * This currently prioritizes recently viewed books that have not been warmed recently.
     * (Future consideration: popular/trending books or those specifically marked for warming could be added.)
     * 
     * @return List of book IDs to warm
     */
    private List<String> getBookIdsToWarm() {
        List<String> result = new ArrayList<>();
        
        // 1. Recently viewed books
        List<String> recentlyViewedIds = recentlyViewedService.getRecentlyViewedBooks().stream()
            .map(Book::getId)
            .filter(id -> id != null && !id.isEmpty())
            .collect(Collectors.toList());
        for (String id : recentlyViewedIds) {
            if (!recentlyWarmedBooks.contains(id)) {
                result.add(id);
            }
        }
        
        return result;
    }
}
