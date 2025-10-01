package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.config.SitemapProperties;
import com.williamcallahan.book_recommendation_engine.service.BookSitemapService;
import com.williamcallahan.book_recommendation_engine.service.BookSitemapService.ExternalHydrationSummary;
import com.williamcallahan.book_recommendation_engine.service.BookSitemapService.SitemapSnapshot;
import com.williamcallahan.book_recommendation_engine.service.BookSitemapService.SnapshotSyncResult;
import com.williamcallahan.book_recommendation_engine.service.SitemapService;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.BookSitemapItem;
import com.williamcallahan.book_recommendation_engine.service.image.S3BookCoverService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SitemapRefreshSchedulerTest {

    @Mock
    private BookSitemapService bookSitemapService;

    @Mock
    private SitemapService sitemapService;

    @Mock
    private ObjectProvider<S3BookCoverService> coverServiceProvider;

    @Mock
    private S3BookCoverService coverService;

    private SitemapProperties sitemapProperties;

    private SitemapRefreshScheduler scheduler;

    @BeforeEach
    void setUp() {
        sitemapProperties = new SitemapProperties();
        sitemapProperties.setSchedulerEnabled(true);
        sitemapProperties.setSchedulerCoverSampleSize(5);
        sitemapProperties.setSchedulerExternalHydrationSize(3);
        when(coverServiceProvider.getIfAvailable()).thenReturn(coverService);
        when(coverService.fetchCover(org.mockito.ArgumentMatchers.any()))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        scheduler = new SitemapRefreshScheduler(sitemapProperties, bookSitemapService, sitemapService, coverServiceProvider);
    }

    @Test
    void refreshSitemapArtifacts_runsSnapshotUploadAndWarmups() {
        List<BookSitemapItem> items = List.of(new BookSitemapItem("book-1", "slug-1", "Title", Instant.now()));
        SitemapSnapshot snapshot = new SitemapSnapshot(Instant.now(), items);
        when(bookSitemapService.synchronizeSnapshot()).thenReturn(new SnapshotSyncResult(snapshot, true, "sitemaps/books.json"));
        when(bookSitemapService.hydrateExternally(items, 3)).thenReturn(new ExternalHydrationSummary(3, 1, 1, 0));

        scheduler.refreshSitemapArtifacts();

        verify(bookSitemapService).synchronizeSnapshot();
        verify(bookSitemapService).hydrateExternally(items, 3);
        verify(coverService).fetchCover(org.mockito.ArgumentMatchers.any());
    }
}
