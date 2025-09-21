package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.Date;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class RecentlyViewedServiceTest {

    private GoogleBooksService googleBooksService;
    private DuplicateBookService duplicateBookService;
    private BookDataOrchestrator bookDataOrchestrator;
    private RecentlyViewedService recentlyViewedService;

    @BeforeEach
    void setUp() {
        googleBooksService = mock(GoogleBooksService.class);
        duplicateBookService = mock(DuplicateBookService.class);
        bookDataOrchestrator = mock(BookDataOrchestrator.class);

        when(googleBooksService.searchBooksAsyncReactive(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of()));

        recentlyViewedService = createService(false);
    }

    private RecentlyViewedService createService(boolean googleFallbackEnabled) {
        return new RecentlyViewedService(googleBooksService, duplicateBookService, bookDataOrchestrator, googleFallbackEnabled);
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
        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of()));

        StepVerifier.create(recentlyViewedService.fetchDefaultBooksAsync())
            .expectNext(List.of())
            .verifyComplete();

        verify(bookDataOrchestrator).searchBooksTiered(anyString(), any(), anyInt(), any());
        verify(googleBooksService, never()).searchBooksAsyncReactive(anyString(), any(), anyInt(), any());
    }

    private Book buildBook(String id, int year) {
        Book book = new Book();
        book.setId(id);
        book.setTitle("Title " + id);
        book.setS3ImagePath("https://cdn.example/" + id + ".jpg");
        book.setPublishedDate(Date.from(Instant.parse(year + "-01-01T00:00:00Z")));
        return book;
    }
}
