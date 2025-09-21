package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RecommendationServiceTest {

    private GoogleBooksService googleBooksService;
    private BookDataOrchestrator bookDataOrchestrator;
    private RecommendationService recommendationService;

    @BeforeEach
    void setUp() {
        googleBooksService = mock(GoogleBooksService.class);
        bookDataOrchestrator = mock(BookDataOrchestrator.class);
        recommendationService = new RecommendationService(googleBooksService, bookDataOrchestrator);

        when(googleBooksService.searchBooksAsyncReactive(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of()));
        when(googleBooksService.getBookById(anyString()))
            .thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    void getSimilarBooks_prefersPostgresSearchResults() {
        Book source = buildBook("source", "en", List.of("Author One"), List.of("Fiction"));
        Book postgresMatch = buildBook("postgres-match", "en", List.of("Author Two"), List.of("Fiction"));

        when(bookDataOrchestrator.getBookByIdTiered("source")).thenReturn(Mono.just(source));
        when(bookDataOrchestrator.getBookBySlugTiered("source")).thenReturn(Mono.empty());
        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of(postgresMatch)));

        StepVerifier.create(recommendationService.getSimilarBooks("source", 3))
            .expectNextMatches(results -> results.stream().anyMatch(book -> "postgres-match".equals(book.getId())))
            .verifyComplete();

        verify(bookDataOrchestrator, atLeastOnce()).searchBooksTiered(anyString(), any(), anyInt(), any());
        verify(googleBooksService, never()).searchBooksAsyncReactive(anyString(), any(), anyInt(), any());
    }

    @Test
    void getSimilarBooks_fallsBackToGoogleSearchWhenPostgresEmpty() {
        Book source = buildBook("source", "en", List.of("Author One"), List.of("Fiction"));
        Book googleMatch = buildBook("google-match", "en", List.of("Author Two"), List.of("Fiction"));

        when(bookDataOrchestrator.getBookByIdTiered("source")).thenReturn(Mono.just(source));
        when(bookDataOrchestrator.getBookBySlugTiered("source")).thenReturn(Mono.empty());
        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of()));
        when(googleBooksService.searchBooksAsyncReactive(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of(googleMatch)));

        StepVerifier.create(recommendationService.getSimilarBooks("source", 3))
            .expectNextMatches(results -> results.stream().anyMatch(book -> "google-match".equals(book.getId())))
            .verifyComplete();

        verify(bookDataOrchestrator, atLeastOnce()).searchBooksTiered(anyString(), any(), anyInt(), any());
        verify(googleBooksService, atLeastOnce()).searchBooksAsyncReactive(anyString(), any(), anyInt(), any());
    }

    @Test
    void getSimilarBooks_fetchesSourceBookFromGoogleWhenOrchestratorMisses() {
        Book source = buildBook("source", "en", List.of("Author One"), List.of("Fiction"));
        Book googleMatch = buildBook("google-match", "en", List.of("Author Two"), List.of("Fiction"));

        when(bookDataOrchestrator.getBookByIdTiered("source")).thenReturn(Mono.empty());
        when(bookDataOrchestrator.getBookBySlugTiered("source")).thenReturn(Mono.empty());
        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of()));
        when(googleBooksService.getBookById("source"))
            .thenReturn(CompletableFuture.completedFuture(source));
        when(googleBooksService.searchBooksAsyncReactive(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of(googleMatch)));

        StepVerifier.create(recommendationService.getSimilarBooks("source", 3))
            .expectNextMatches(results -> results.stream().anyMatch(book -> "google-match".equals(book.getId())))
            .verifyComplete();

        verify(googleBooksService).getBookById("source");
        verify(googleBooksService, atLeastOnce()).searchBooksAsyncReactive(anyString(), any(), anyInt(), any());
    }

    private Book buildBook(String id, String language, List<String> authors, List<String> categories) {
        Book book = new Book();
        book.setId(id);
        book.setTitle("Title " + id);
        book.setDescription("Description for " + id);
        book.setLanguage(language);
        book.setAuthors(authors);
        book.setCategories(categories);
        book.setS3ImagePath("https://cdn.example/" + id + ".jpg");
        book.setPublishedDate(Date.from(Instant.parse("2020-01-01T00:00:00Z")));
        return book;
    }
}
