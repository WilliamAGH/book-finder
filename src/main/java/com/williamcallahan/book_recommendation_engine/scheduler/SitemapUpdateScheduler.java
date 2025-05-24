/**
 * Scheduler for automating sitemap maintenance tasks
 *
 * @author William Callahan
 *
 * Features:
 * - Periodically updates accumulated book IDs in S3 for sitemap generation
 * - Executes on a configurable schedule (hourly by default)
 * - Handles exceptions gracefully to prevent disruption of scheduled operations
 * - Logs detailed information about task execution and status
 * - Integrates with BookSitemapService for actual update operations
 */

package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.service.BookSitemapService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
@Component
public class SitemapUpdateScheduler {

    private final Logger logger = LoggerFactory.getLogger(SitemapUpdateScheduler.class); // Made non-static and non-final for testing
    private final BookSitemapService bookSitemapService;

    /**
     * Constructs the scheduler with required dependencies
     * 
     * @param bookSitemapService Service for generating and managing sitemaps
     */
    public SitemapUpdateScheduler(BookSitemapService bookSitemapService) {
        this.bookSitemapService = bookSitemapService;
    }

    /**
     * Scheduled task to update the accumulated book IDs in S3 for the sitemap
     * 
     * - Runs every hour at the beginning of the hour
     * - Updates book ID list used for sitemap generation
     * - Ensures search engines have latest content information
     * - Uses cron expression: second, minute, hour, day, month, weekday
     * - "0 0 * * * *" means at minute 0 of every hour
     */
    @Scheduled(cron = "0 0 * * * *") // Runs hourly
    // For testing, you might use a shorter interval like: @Scheduled(fixedRate = 60000) // Every minute
    public void scheduleSitemapBookIdUpdate() {
        logger.info("Scheduler triggered: Updating accumulated book IDs in S3.");
        try {
            bookSitemapService.updateAccumulatedBookIdsInS3();
            logger.info("Scheduler finished: Accumulated book ID update process completed.");
        } catch (Exception e) {
            logger.error("Error during scheduled sitemap book ID update:", e);
        }
    }
}
