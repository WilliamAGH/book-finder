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
        Book postgresBook = buildBook("postgres-book", 2022);
        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of(postgresBook)));

        StepVerifier.create(recentlyViewedService.fetchDefaultBooksAsync())
            .expectNextMatches(results -> results.size() == 1 && "postgres-book".equals(results.get(0).getId()))
            .verifyComplete();

        verify(bookDataOrchestrator).searchBooksTiered(anyString(), any(), anyInt(), any());
        verify(googleBooksService, never()).searchBooksAsyncReactive(anyString(), any(), anyInt(), any());
    }

    @Test
    void fetchDefaultBooks_fallsBackToGoogleWhenPostgresEmpty() {
        when(recentBookViewRepository.isEnabled()).thenReturn(false);

        recentlyViewedService = createService(true);

        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of()));

        Book googleBook = buildBook("google-book", 2020);
        when(googleBooksService.searchBooksAsyncReactive(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of(googleBook)));

        StepVerifier.create(recentlyViewedService.fetchDefaultBooksAsync())
            .expectNextMatches(results -> results.size() == 1 && "google-book".equals(results.get(0).getId()))
            .verifyComplete();

        verify(bookDataOrchestrator).searchBooksTiered(anyString(), any(), anyInt(), any());
        verify(googleBooksService).searchBooksAsyncReactive(anyString(), any(), anyInt(), any());
    }

    @Test
    void fetchDefaultBooks_googleFallbackOnPostgresError() {
        when(recentBookViewRepository.isEnabled()).thenReturn(false);

        recentlyViewedService = createService(true);

        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.error(new RuntimeException("boom")));

        Book googleBook = buildBook("google-book", 2018);
        when(googleBooksService.searchBooksAsyncReactive(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of(googleBook)));

        StepVerifier.create(recentlyViewedService.fetchDefaultBooksAsync())
            .expectNextMatches(results -> results.size() == 1 && "google-book".equals(results.get(0).getId()))
            .verifyComplete();

        verify(bookDataOrchestrator).searchBooksTiered(anyString(), any(), anyInt(), any());
        verify(googleBooksService).searchBooksAsyncReactive(anyString(), any(), anyInt(), any());
    }

    @Test
    void fetchDefaultBooks_returnsEmptyWhenFallbackDisabled() {
        when(recentBookViewRepository.isEnabled()).thenReturn(false);

        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of()));

        StepVerifier.create(recentlyViewedService.fetchDefaultBooksAsync())
            .expectNext(List.of())
            .verifyComplete();

        verify(bookDataOrchestrator).searchBooksTiered(anyString(), any(), anyInt(), any());
        verify(googleBooksService, never()).searchBooksAsyncReactive(anyString(), any(), anyInt(), any());
    }

    @Test
    void fetchDefaultBooks_usesRepositoryWhenAvailable() {
        Instant now = Instant.parse("2024-01-01T00:00:00Z");
        when(recentBookViewRepository.isEnabled()).thenReturn(true);
        when(recentBookViewRepository.fetchMostRecentViews(anyInt()))
            .thenReturn(List.of(new RecentBookViewRepository.ViewStats("uuid-1", now, 3L, 10L, 15L)));

        Book dbBook = buildBook("uuid-1", 2021);
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
        verify(bookDataOrchestrator, never()).searchBooksTiered(anyString(), any(), anyInt(), any());
    }

    @Test
    void addToRecentlyViewed_recordsViewAndAppliesStats() {
        when(recentBookViewRepository.isEnabled()).thenReturn(true);
        Instant now = Instant.parse("2024-02-02T10:15:30Z");
        when(recentBookViewRepository.fetchStatsForBook("uuid-2"))
            .thenReturn(Optional.of(new RecentBookViewRepository.ViewStats("uuid-2", now, 5L, 12L, 20L)));

        Book book = buildBook("uuid-2", 2020);
        book.setSlug("slug-uuid-2");

        recentlyViewedService.addToRecentlyViewed(book);

        verify(recentBookViewRepository).recordView(eq("uuid-2"), any(Instant.class), eq("web"));
        verify(recentBookViewRepository).fetchStatsForBook("uuid-2");

        assertNotNull(book.getQualifiers());
        assertEquals(12L, book.getQualifiers().get("recent.views.7d"));
        assertEquals(now, book.getQualifiers().get("recent.views.lastViewedAt"));
    }

    private Book buildBook(String id, int year) {
        Book book = new Book();
        book.setId(id);
        book.setTitle("Title " + id);
        book.setS3ImagePath("https://cdn.example/" + id + ".jpg");
        book.setPublishedDate(Date.from(Instant.parse(year + "-01-01T00:00:00Z")));
        book.setSlug("slug-" + id);
        return book;
    }
}
