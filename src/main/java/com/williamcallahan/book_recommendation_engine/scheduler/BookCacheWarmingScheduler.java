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

import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.service.ApiRequestMonitor;
import com.williamcallahan.book_recommendation_engine.service.BookIdentifierResolver;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.util.BookDomainMapper;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;

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
@Slf4j
public class BookCacheWarmingScheduler {

        
    private final RecentlyViewedService recentlyViewedService;
    private final ApplicationContext applicationContext;
    private final BookQueryRepository bookQueryRepository;
    private final BookIdentifierResolver bookIdentifierResolver;
    
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

    public BookCacheWarmingScheduler(RecentlyViewedService recentlyViewedService,
                                     ApplicationContext applicationContext,
                                     BookQueryRepository bookQueryRepository,
                                     BookIdentifierResolver bookIdentifierResolver) {
        this.recentlyViewedService = recentlyViewedService;
        this.applicationContext = applicationContext;
        this.bookQueryRepository = bookQueryRepository;
        this.bookIdentifierResolver = bookIdentifierResolver;
    }

    /**
     * Scheduled task that runs during off-peak hours (e.g., 3 AM)
     * to warm caches for popular books
     */
    @Scheduled(cron = "${app.cache.warming.cron:0 0 3 * * ?}")
    public void warmPopularBookCaches() {
        if (!cacheWarmingEnabled) {
            log.debug("Book cache warming is disabled");
            return;
        }

        log.info("Starting scheduled book cache warming");
        
        // Get books to warm (recently viewed, popular, etc.)
        List<String> bookIdsToWarm = getBookIdsToWarm();
        
        // Clean up our tracking set if it gets too large
        if (recentlyWarmedBooks.size() > 500) {
            recentlyWarmedBooks.clear();
        }
        
        // If no books to warm, we're done
        if (bookIdsToWarm.isEmpty()) {
            log.info("No books to warm in cache");
            return;
        }
        
        // Rate-limited warming to avoid overwhelming the API
        AtomicInteger warmedCount = new AtomicInteger(0);
        AtomicInteger existingCount = new AtomicInteger(0);
        
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        try {
            // Validate rate limit configuration
            if (rateLimit <= 0) {
                log.warn("Invalid rateLimit configuration: {}. Skipping cache warming run.", rateLimit);
                return;
            }

            // Calculate delay between books based on rate limit
            final long delayMillis = (60_000L) / rateLimit;
            
            // Check current API call count from metrics if possible
            // Inject ApiRequestMonitor if available
            int currentHourlyRequests = 0;
            try {
                ApiRequestMonitor apiRequestMonitor = applicationContext.getBean(ApiRequestMonitor.class);
                currentHourlyRequests = apiRequestMonitor.getCurrentHourlyRequests();
                log.info("Current hourly API request count: {}. Will adjust cache warming accordingly.", currentHourlyRequests);
            } catch (Exception e) {
                log.warn("Could not get ApiRequestMonitor metrics: {}", e.getMessage());
            }
            
            // Calculate how many books we can warm based on current API usage
            // We want to leave some headroom for user requests
            int hourlyLimit = rateLimit * 60; // Max requests per hour based on rate limit
            int requestBudget = PagingUtils.atLeast(hourlyLimit / 2 - currentHourlyRequests, 0); // Use at most half the remaining budget
            int booksToWarm = Math.min(Math.min(bookIdsToWarm.size(), maxBooksPerRun), requestBudget);
            
            log.info("Warming {} books based on rate limit {} per minute and current API usage", 
                    booksToWarm, rateLimit);
            
            for (int i = 0; i < booksToWarm; i++) {
                final String bookId = bookIdsToWarm.get(i);
                
                // Schedule each book to be warmed with a delay
                executor.schedule(() -> {
                    try {
                        // Note: Cache warming functionality has been disabled as the cache service has been removed
                        log.info("Attempting to warm book ID: {} (cache functionality disabled)", bookId);
                        fetchBookForWarming(bookId)
                            .thenAccept(book -> {
                                if (book != null) {
                                    warmedCount.incrementAndGet();
                                    log.info("Successfully fetched book for warming: {}",
                                            book.getTitle() != null ? book.getTitle() : bookId);
                                } else {
                                    log.debug("No book found for ID: {}", bookId);
                                }
                            })
                            .exceptionally(ex -> {
                                LoggingUtils.error(log, ex, "Error fetching book {}", bookId);
                                return null;
                            });

                        // Track that we've processed this book
                        recentlyWarmedBooks.add(bookId);
                    } catch (Exception e) {
                        LoggingUtils.error(log, e, "Error in cache warming task for book {}", bookId);
                    }
                }, i * delayMillis, TimeUnit.MILLISECONDS);
            }
            
            // Make sure all tasks have a chance to complete
            executor.shutdown();
            executor.awaitTermination(maxBooksPerRun * delayMillis + 10000, TimeUnit.MILLISECONDS);
            
            log.info("Book cache warming completed. Warmed: {}, Already in cache: {}, Total: {}", 
                    warmedCount.get(), existingCount.get(), 
                    warmedCount.get() + existingCount.get());
            
        } catch (Exception e) {
            LoggingUtils.error(log, e, "Error during book cache warming");
        } finally {
            if (!executor.isTerminated()) {
                executor.shutdownNow();
            }
        }
    }

    private CompletionStage<Book> fetchBookForWarming(String bookId) {
        return CompletableFuture.completedFuture(resolveBookForWarming(bookId));
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

        int lookupLimit = Math.max(maxBooksPerRun * 2, 20);
        List<String> recentlyViewedIds = recentlyViewedService.getRecentlyViewedBookIds(lookupLimit);
        for (String id : recentlyViewedIds) {
            if (id != null && !id.isBlank() && !recentlyWarmedBooks.contains(id)) {
                result.add(id);
            }
        }

        return result;
    }

    private Book resolveBookForWarming(String identifier) {
        if (!ValidationUtils.hasText(identifier)) {
            return null;
        }
        String trimmed = identifier.trim();

        Optional<BookDetail> bySlug = bookQueryRepository.fetchBookDetailBySlug(trimmed);
        if (bySlug.isPresent()) {
            return BookDomainMapper.fromDetail(bySlug.get());
        }

        Optional<UUID> maybeUuid = bookIdentifierResolver.resolveToUuid(trimmed);
        if (maybeUuid.isEmpty()) {
            return null;
        }

        UUID uuid = maybeUuid.get();
        return bookQueryRepository.fetchBookDetail(uuid)
            .map(BookDomainMapper::fromDetail)
            .or(() -> bookQueryRepository.fetchBookCard(uuid).map(BookDomainMapper::fromCard))
            .orElse(null);
    }
}
