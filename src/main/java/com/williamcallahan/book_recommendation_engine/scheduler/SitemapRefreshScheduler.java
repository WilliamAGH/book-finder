package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.config.SitemapProperties;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookSitemapService;
import com.williamcallahan.book_recommendation_engine.service.SitemapService;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.BookSitemapItem;
import com.williamcallahan.book_recommendation_engine.service.image.S3BookCoverService;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Consolidated sitemap refresh job that warms Postgres queries, persists S3 artefacts, and hydrates external data.
 */
@Component
@Slf4j
public class SitemapRefreshScheduler {

    
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
            log.debug("Sitemap refresh scheduler skipped – disabled via configuration.");
            return;
        }

        int maxJitterSeconds = sitemapProperties.getSchedulerJitterSeconds();
        long jitterSeconds = maxJitterSeconds > 0
                ? ThreadLocalRandom.current().nextLong(0, maxJitterSeconds + 1L)
                : 0L;
        if (jitterSeconds > 0) {
            try {
                Thread.sleep(jitterSeconds * 1000L);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }

        Instant start = Instant.now();
        log.info("Sitemap refresh scheduler started.");

        try {
            sitemapService.getOverview();
            sitemapService.getAuthorsByLetter("A", 1);
            sitemapService.getBooksByLetter("A", 1);
        } catch (Exception e) {
            LoggingUtils.warn(log, e, "Sitemap warmup queries encountered an error");
        }

        BookSitemapService.SnapshotSyncResult snapshotResult = bookSitemapService.synchronizeSnapshot();
        List<BookSitemapItem> books = snapshotResult.snapshot().books();

        boolean cachesCleared = sitemapService.refreshSitemapCachesIfDatasetChanged();
        if (cachesCleared) {
            sitemapService.getBooksXmlPageCount();
            sitemapService.getBookSitemapPageMetadata();
            sitemapService.getAuthorSitemapPageMetadata();
        }

        int coverSampleSize = PagingUtils.atLeast(sitemapProperties.getSchedulerCoverSampleSize(), 0);
        int externalHydrationLimit = PagingUtils.atLeast(sitemapProperties.getSchedulerExternalHydrationSize(), 0);

        BookSitemapService.ExternalHydrationSummary hydrationSummary =
                new BookSitemapService.ExternalHydrationSummary(externalHydrationLimit, 0, 0, 0);
        int coverWarmups = warmCoverAssets(books, coverSampleSize);

        Duration elapsed = Duration.between(start, Instant.now());
        log.info("Sitemap refresh scheduler finished in {}s (books={}, s3Upload={}, hydration={{attempted:{}, success:{}}}, coverWarmups={}, cachesRefreshed={}).",
                elapsed.toSeconds(),
                books.size(),
                snapshotResult.uploaded(),
                hydrationSummary.attempted(),
                hydrationSummary.succeeded(),
                coverWarmups,
                cachesCleared);
    }

    private int warmCoverAssets(List<BookSitemapItem> candidates, int limit) {
        if (candidates == null || candidates.isEmpty() || limit <= 0) {
            return 0;
        }
        S3BookCoverService coverService = coverServiceProvider.getIfAvailable();
        if (coverService == null) {
            log.debug("Skipping cover warmup – S3BookCoverService not available.");
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
                    log.debug("Cover warmup failed for {}: {}", item.bookId(), ex.getMessage());
                    return Optional.empty();
                }).join();
                successes++;
            } catch (Exception e) {
                log.debug("Cover warmup encountered error for {}: {}", item.bookId(), e.getMessage());
            }
        }
        return successes;
    }
}
