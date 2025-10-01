package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class RecentlyViewedServiceTest {

    private GoogleBooksService googleBooksService;
    private DuplicateBookService duplicateBookService;
    private BookDataOrchestrator bookDataOrchestrator;
    private RecentBookViewRepository recentBookViewRepository;
    private RecentlyViewedService recentlyViewedService;

    @BeforeEach
    void setUp() {
        googleBooksService = mock(GoogleBooksService.class);
        duplicateBookService = mock(DuplicateBookService.class);
        bookDataOrchestrator = mock(BookDataOrchestrator.class);
        recentBookViewRepository = mock(RecentBookViewRepository.class);

        when(recentBookViewRepository.isEnabled()).thenReturn(false);
        when(duplicateBookService.findPrimaryCanonicalBook(any())).thenReturn(Optional.empty());

        when(googleBooksService.searchBooksAsyncReactive(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of()));

        recentlyViewedService = createService(false);
    }

    private RecentlyViewedService createService(boolean googleFallbackEnabled) {
        return new RecentlyViewedService(googleBooksService, duplicateBookService, bookDataOrchestrator, recentBookViewRepository, googleFallbackEnabled);
    }

    @Test
    void fetchDefaultBooks_prefersPostgresResults() {
Book postgresBook = com.williamcallahan.book_recommendation_engine.testutil.BookTestData.aBook()
                .id("postgres-book").publishedDate(Date.from(Instant.parse("2022-01-01T00:00:00Z"))).s3ImagePath("https://cdn.example/postgres-book.jpg").build();
        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any(), anyBoolean()))
            .thenReturn(Mono.just(List.of(postgresBook)));

com.williamcallahan.book_recommendation_engine.testutil.ReactorAssertions.verifyListHasSingleId(recentlyViewedService.fetchDefaultBooksAsync(), "postgres-book");

        verify(bookDataOrchestrator).searchBooksTiered(anyString(), any(), anyInt(), any(), anyBoolean());
        verify(googleBooksService, never()).searchBooksAsyncReactive(anyString(), any(), anyInt(), any());
    }

    @Test
    void fetchDefaultBooks_fallsBackToGoogleWhenPostgresEmpty() {
        when(recentBookViewRepository.isEnabled()).thenReturn(false);

        recentlyViewedService = createService(true);

        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any(), anyBoolean()))
            .thenReturn(Mono.just(List.of()));

Book googleBook = com.williamcallahan.book_recommendation_engine.testutil.BookTestData.aBook()
                .id("google-book").publishedDate(Date.from(Instant.parse("2020-01-01T00:00:00Z"))).s3ImagePath("https://cdn.example/google-book.jpg").build();
        when(googleBooksService.searchBooksAsyncReactive(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of(googleBook)));

com.williamcallahan.book_recommendation_engine.testutil.ReactorAssertions.verifyListHasSingleId(recentlyViewedService.fetchDefaultBooksAsync(), "google-book");

        verify(bookDataOrchestrator).searchBooksTiered(anyString(), any(), anyInt(), any(), anyBoolean());
        verify(googleBooksService).searchBooksAsyncReactive(anyString(), any(), anyInt(), any());
    }

    @Test
    void fetchDefaultBooks_googleFallbackOnPostgresError() {
        when(recentBookViewRepository.isEnabled()).thenReturn(false);

        recentlyViewedService = createService(true);

        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any(), anyBoolean()))
            .thenReturn(Mono.error(new RuntimeException("boom")));

Book googleBook = com.williamcallahan.book_recommendation_engine.testutil.BookTestData.aBook()
                .id("google-book").publishedDate(Date.from(Instant.parse("2018-01-01T00:00:00Z"))).s3ImagePath("https://cdn.example/google-book.jpg").build();
        when(googleBooksService.searchBooksAsyncReactive(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of(googleBook)));

com.williamcallahan.book_recommendation_engine.testutil.ReactorAssertions.verifyListHasSingleId(recentlyViewedService.fetchDefaultBooksAsync(), "google-book");

        verify(bookDataOrchestrator).searchBooksTiered(anyString(), any(), anyInt(), any(), anyBoolean());
        verify(googleBooksService).searchBooksAsyncReactive(anyString(), any(), anyInt(), any());
    }

    @Test
    void fetchDefaultBooks_returnsEmptyWhenFallbackDisabled() {
        when(recentBookViewRepository.isEnabled()).thenReturn(false);

        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any(), anyBoolean()))
            .thenReturn(Mono.just(List.of()));

com.williamcallahan.book_recommendation_engine.testutil.ReactorAssertions.verifyEmptyList(recentlyViewedService.fetchDefaultBooksAsync());

        verify(bookDataOrchestrator).searchBooksTiered(anyString(), any(), anyInt(), any(), anyBoolean());
        verify(googleBooksService, never()).searchBooksAsyncReactive(anyString(), any(), anyInt(), any());
    }

    @Test
    void fetchDefaultBooks_usesRepositoryWhenAvailable() {
        Instant now = Instant.parse("2024-01-01T00:00:00Z");
        when(recentBookViewRepository.isEnabled()).thenReturn(true);
when(recentBookViewRepository.fetchMostRecentViews(anyInt()))
            .thenReturn(List.of(com.williamcallahan.book_recommendation_engine.testutil.RecentViewStatsTestData.viewStats("uuid-1", now, 3L, 10L, 15L)));

Book dbBook = com.williamcallahan.book_recommendation_engine.testutil.BookTestData.aBook()
                .id("uuid-1").publishedDate(Date.from(Instant.parse("2021-01-01T00:00:00Z"))).s3ImagePath("https://cdn.example/uuid-1.jpg").build();
        dbBook.setSlug("slug-uuid-1");
        when(bookDataOrchestrator.getBookFromDatabase("uuid-1"))
            .thenReturn(Optional.of(dbBook));

        StepVerifier.create(recentlyViewedService.fetchDefaultBooksAsync())
            .expectNextMatches(results -> {
                if (results.size() != 1) {
                    return false;
                }
                Book result = results.get(0);
                Object lastViewed = result.getQualifiers().get("recent.views.lastViewedAt");
                Object views7d = result.getQualifiers().get("recent.views.7d");
                return "uuid-1".equals(result.getId())
                        && lastViewed instanceof Instant && ((Instant) lastViewed).equals(now)
                        && views7d instanceof Long && ((Long) views7d) == 10L;
            })
            .verifyComplete();

        verify(recentBookViewRepository).fetchMostRecentViews(anyInt());
        verify(bookDataOrchestrator).getBookFromDatabase("uuid-1");
        verify(bookDataOrchestrator, never()).searchBooksTiered(anyString(), any(), anyInt(), any(), anyBoolean());
    }

    @Test
    void addToRecentlyViewed_recordsViewAndAppliesStats() {
        when(recentBookViewRepository.isEnabled()).thenReturn(true);
        Instant now = Instant.parse("2024-02-02T10:15:30Z");
        when(recentBookViewRepository.fetchStatsForBook("uuid-2"))
            .thenReturn(Optional.of(new RecentBookViewRepository.ViewStats("uuid-2", now, 5L, 12L, 20L)));

Book book = com.williamcallahan.book_recommendation_engine.testutil.BookTestData.aBook()
                .id("uuid-2").publishedDate(Date.from(Instant.parse("2020-01-01T00:00:00Z"))).s3ImagePath("https://cdn.example/uuid-2.jpg").build();
        book.setSlug("slug-uuid-2");

        recentlyViewedService.addToRecentlyViewed(book);

        verify(recentBookViewRepository).recordView(eq("uuid-2"), any(Instant.class), eq("web"));
        verify(recentBookViewRepository).fetchStatsForBook("uuid-2");

        assertNotNull(book.getQualifiers());
        assertEquals(12L, book.getQualifiers().get("recent.views.7d"));
        assertEquals(now, book.getQualifiers().get("recent.views.lastViewedAt"));
    }

}
