package com.williamcallahan.book_recommendation_engine.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.config.SitemapProperties;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.BookSitemapService;
import com.williamcallahan.book_recommendation_engine.service.S3StorageService;
import com.williamcallahan.book_recommendation_engine.service.SitemapService;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.AuthorSection;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.BookSitemapItem;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.PagedResult;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.SitemapOverview;
import com.williamcallahan.book_recommendation_engine.service.image.S3BookCoverService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.ObjectProvider;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SitemapRefreshSchedulerIntegrationTest {

    private SitemapProperties sitemapProperties;
    private SitemapService sitemapService;
    private BookDataOrchestrator bookDataOrchestrator;
    private S3StorageService s3StorageService;
    private S3BookCoverService coverService;

    @BeforeEach
    void setUp() {
        sitemapProperties = new SitemapProperties();
        sitemapProperties.setSchedulerEnabled(true);
        sitemapProperties.setSchedulerCoverSampleSize(1);
        sitemapProperties.setSchedulerExternalHydrationSize(1);
        sitemapProperties.setS3AccumulatedIdsKey("sitemaps/books.json");

        sitemapService = Mockito.mock(SitemapService.class);
        bookDataOrchestrator = Mockito.mock(BookDataOrchestrator.class);
        s3StorageService = Mockito.mock(S3StorageService.class);
        coverService = Mockito.mock(S3BookCoverService.class);
    }

    @Test
    void refreshSitemapArtifacts_executesSnapshotHydrationAndCoverWarmup() {
        when(sitemapService.getBooksXmlPageCount()).thenReturn(1);
        BookSitemapItem sitemapItem = new BookSitemapItem("book-1", "slug-1", "Title", Instant.parse("2024-01-01T00:00:00Z"));
        when(sitemapService.getBooksForXmlPage(1)).thenReturn(List.of(sitemapItem));
        when(sitemapService.getOverview()).thenReturn(new SitemapOverview(Map.of("A", 1), Map.of("A", 1)));
        when(sitemapService.getAuthorsByLetter("A", 1)).thenReturn(new PagedResult<>(List.of(new AuthorSection("author-1", "Author Name", Instant.now(), List.of(sitemapItem))), 1, 1, 1));
        when(sitemapService.getBooksByLetter("A", 1)).thenReturn(new PagedResult<>(List.of(sitemapItem), 1, 1, 1));

        when(s3StorageService.uploadGenericJsonAsync(eq("sitemaps/books.json"), any(), eq(true)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(bookDataOrchestrator.getBookByIdTiered("book-1"))
                .thenReturn(Mono.just(new Book()));
        when(coverService.fetchCover(any(Book.class)))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        BookSitemapService bookSitemapService = new BookSitemapService(
                sitemapService,
                sitemapProperties,
                new ObjectMapper(),
                bookDataOrchestrator,
                s3StorageService
        );

        ObjectProvider<S3BookCoverService> coverProvider = new ObjectProvider<>() {
            @Override
            public S3BookCoverService getObject(Object... args) {
                return coverService;
            }

            @Override
            public S3BookCoverService getIfAvailable() {
                return coverService;
            }

            @Override
            public S3BookCoverService getIfUnique() {
                return coverService;
            }

            @Override
            public S3BookCoverService getObject() {
                return coverService;
            }
        };

        SitemapRefreshScheduler scheduler = new SitemapRefreshScheduler(
                sitemapProperties,
                bookSitemapService,
                sitemapService,
                coverProvider
        );

        scheduler.refreshSitemapArtifacts();

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(s3StorageService).uploadGenericJsonAsync(eq("sitemaps/books.json"), payloadCaptor.capture(), eq(true));
        verify(bookDataOrchestrator).getBookByIdTiered("book-1");
        verify(coverService).fetchCover(any(Book.class));
        assertThat(payloadCaptor.getValue()).contains("\"slug\":\"slug-1\"");
    }
}
