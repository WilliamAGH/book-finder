package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.config.SitemapProperties;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookSitemapService;
import com.williamcallahan.book_recommendation_engine.service.SitemapService;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.BookSitemapItem;
import com.williamcallahan.book_recommendation_engine.service.image.S3BookCoverService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Consolidated sitemap refresh job that warms Postgres queries, persists S3 artefacts, and hydrates external data.
 */
@Component
public class SitemapRefreshScheduler {

    private static final Logger logger = LoggerFactory.getLogger(SitemapRefreshScheduler.class);

    private final SitemapProperties sitemapProperties;
    private final BookSitemapService bookSitemapService;
    private final SitemapService sitemapService;
    private final ObjectProvider<S3BookCoverService> coverServiceProvider;

    public SitemapRefreshScheduler(SitemapProperties sitemapProperties,
                                   BookSitemapService bookSitemapService,
                                   SitemapService sitemapService,
                                   ObjectProvider<S3BookCoverService> coverServiceProvider) {
        this.sitemapProperties = sitemapProperties;
        this.bookSitemapService = bookSitemapService;
        this.sitemapService = sitemapService;
        this.coverServiceProvider = coverServiceProvider;
    }

    @Scheduled(cron = "${sitemap.scheduler-cron:0 15 * * * *}")
    public void refreshSitemapArtifacts() {
        if (!sitemapProperties.isSchedulerEnabled()) {
            logger.debug("Sitemap refresh scheduler skipped – disabled via configuration.");
            return;
        }

        Instant start = Instant.now();
        logger.info("Sitemap refresh scheduler started.");

        try {
            sitemapService.getOverview();
            sitemapService.getAuthorsByLetter("A", 1);
            sitemapService.getBooksByLetter("A", 1);
        } catch (Exception e) {
            logger.warn("Sitemap warmup queries encountered an error: {}", e.getMessage(), e);
        }

        BookSitemapService.SnapshotSyncResult snapshotResult = bookSitemapService.synchronizeSnapshot();
        List<BookSitemapItem> books = snapshotResult.snapshot().books();

        int coverSampleSize = Math.max(0, sitemapProperties.getSchedulerCoverSampleSize());
        int externalHydrationLimit = Math.max(0, sitemapProperties.getSchedulerExternalHydrationSize());

        BookSitemapService.ExternalHydrationSummary hydrationSummary =
                bookSitemapService.hydrateExternally(books, externalHydrationLimit);
        int coverWarmups = warmCoverAssets(books, coverSampleSize);

        Duration elapsed = Duration.between(start, Instant.now());
        logger.info("Sitemap refresh scheduler finished in {}s (books={}, s3Upload={}, hydration={{attempted:{}, success:{}}}, coverWarmups={}).",
                elapsed.toSeconds(),
                books.size(),
                snapshotResult.uploaded(),
                hydrationSummary.attempted(),
                hydrationSummary.succeeded(),
                coverWarmups);
    }

    private int warmCoverAssets(List<BookSitemapItem> candidates, int limit) {
        if (candidates == null || candidates.isEmpty() || limit <= 0) {
            return 0;
        }
        S3BookCoverService coverService = coverServiceProvider.getIfAvailable();
        if (coverService == null) {
            logger.debug("Skipping cover warmup – S3BookCoverService not available.");
            return 0;
        }

        List<BookSitemapItem> slice = candidates.stream().limit(limit).toList();
        int successes = 0;
        for (BookSitemapItem item : slice) {
            Book book = new Book();
            book.setId(item.bookId());
            book.setTitle(item.title());
            try {
                coverService.fetchCover(book).exceptionally(ex -> {
                    logger.debug("Cover warmup failed for {}: {}", item.bookId(), ex.getMessage());
                    return Optional.empty();
                }).join();
                successes++;
            } catch (Exception e) {
                logger.debug("Cover warmup encountered error for {}: {}", item.bookId(), e.getMessage());
            }
        }
        return successes;
    }
}
