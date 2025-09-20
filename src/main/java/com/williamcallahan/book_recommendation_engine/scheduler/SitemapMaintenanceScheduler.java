package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.SitemapService;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.BookSitemapItem;
import com.williamcallahan.book_recommendation_engine.service.image.S3BookCoverService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * Scheduler that periodically warms sitemap data from Postgres and verifies S3-backed cover assets.
 */
@Component
public class SitemapMaintenanceScheduler {

    private static final Logger logger = LoggerFactory.getLogger(SitemapMaintenanceScheduler.class);
    private static final int COVER_SAMPLE_LIMIT = 25;

    private final SitemapService sitemapService;
    private final S3BookCoverService s3BookCoverService;

    public SitemapMaintenanceScheduler(SitemapService sitemapService,
                                       S3BookCoverService s3BookCoverService) {
        this.sitemapService = sitemapService;
        this.s3BookCoverService = s3BookCoverService;
    }

    /**
     * Runs hourly to touch sitemap queries (Postgres) and ensure representative covers are refreshed in S3.
     */
    @Scheduled(cron = "0 15 * * * *")
    public void runSitemapMaintenance() {
        logger.info("Sitemap maintenance scheduler started");

        // Warm core author pagination to keep frequently accessed data responsive.
        sitemapService.getAuthorsByLetter("A", 1);

        List<BookSitemapItem> books = sitemapService.getBooksForXmlPage(1);
        if (books.isEmpty()) {
            logger.info("No books available for sitemap maintenance run");
            return;
        }

        books.stream()
                .limit(COVER_SAMPLE_LIMIT)
                .forEach(item -> {
                    Book book = new Book();
                    book.setId(item.bookId());
                    book.setTitle(item.title());
                    s3BookCoverService.fetchCover(book)
                            .exceptionally(ex -> {
                                logger.warn("Cover fetch check failed for book {}: {}", item.bookId(), ex.getMessage());
                                return Optional.empty();
                            });
                });

        logger.info("Sitemap maintenance scheduler finished (sample size: {})", Math.min(COVER_SAMPLE_LIMIT, books.size()));
    }
}
