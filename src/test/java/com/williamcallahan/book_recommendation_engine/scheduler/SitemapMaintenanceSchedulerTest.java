package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.SitemapService;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.BookSitemapItem;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.PagedResult;
import com.williamcallahan.book_recommendation_engine.service.image.S3BookCoverService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class SitemapMaintenanceSchedulerTest {

    @Mock
    private SitemapService sitemapService;

    @Mock
    private S3BookCoverService s3BookCoverService;

    @InjectMocks
    private SitemapMaintenanceScheduler scheduler;

    @BeforeEach
    void setUp() {
        when(sitemapService.getAuthorsByLetter(anyString(), anyInt()))
                .thenReturn(new PagedResult<>(List.of(), 1, 0, 0));
        when(s3BookCoverService.fetchCover(org.mockito.Mockito.any(Book.class)))
                .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    }

    @Test
    @DisplayName("Scheduler warms sitemap and triggers S3 cover checks")
    void runSitemapMaintenance_warmsCachesAndCovers() {
        List<BookSitemapItem> sample = List.of(
                new BookSitemapItem("book-1", "slug-1", "Title 1", Instant.now()),
                new BookSitemapItem("book-2", "slug-2", "Title 2", Instant.now())
        );
        when(sitemapService.getBooksForXmlPage(1)).thenReturn(sample);

        scheduler.runSitemapMaintenance();

        verify(sitemapService, times(1)).getAuthorsByLetter("A", 1);
        verify(sitemapService, times(1)).getBooksForXmlPage(1);

        ArgumentCaptor<Book> bookCaptor = ArgumentCaptor.forClass(Book.class);
        verify(s3BookCoverService, times(sample.size())).fetchCover(bookCaptor.capture());

        List<Book> captured = bookCaptor.getAllValues();
        assertThat(captured)
                .extracting(Book::getId)
                .containsExactly("book-1", "book-2");
    }

    @Test
    @DisplayName("Scheduler exits gracefully when no books present")
    void runSitemapMaintenance_noBooks() {
        when(sitemapService.getBooksForXmlPage(1)).thenReturn(List.of());

        scheduler.runSitemapMaintenance();

        verify(sitemapService, times(1)).getBooksForXmlPage(1);
        verify(s3BookCoverService, times(0)).fetchCover(org.mockito.Mockito.any(Book.class));
    }
}
