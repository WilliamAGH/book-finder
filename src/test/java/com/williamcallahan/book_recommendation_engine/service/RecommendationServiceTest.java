package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.Date;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RecommendationServiceTest {

    private BookDataOrchestrator bookDataOrchestrator;
    private BookRecommendationPersistenceService recommendationPersistenceService;
    private RecommendationService recommendationService;

    @BeforeEach
    void setUp() {
        bookDataOrchestrator = mock(BookDataOrchestrator.class);
        recommendationPersistenceService = mock(BookRecommendationPersistenceService.class);
        when(recommendationPersistenceService.persistPipelineRecommendations(any(), any()))
            .thenReturn(Mono.empty());

        recommendationService = new RecommendationService(bookDataOrchestrator, recommendationPersistenceService, true);
    }

    @Test
    void getSimilarBooks_prefersTieredResults() {
        Book source = buildBook("source", "en", List.of("Author One"), List.of("Fiction"));
        Book postgresMatch = buildBook("postgres-match", "en", List.of("Author Two"), List.of("Fiction"));

        when(bookDataOrchestrator.fetchCanonicalBookReactive("source")).thenReturn(Mono.just(source));
        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of(postgresMatch)));

        StepVerifier.create(recommendationService.getSimilarBooks("source", 3))
            .expectNextMatches(results -> results.stream().anyMatch(book -> "postgres-match".equals(book.getId())))
            .verifyComplete();

        verify(bookDataOrchestrator, atLeastOnce()).searchBooksTiered(anyString(), any(), anyInt(), any());
        verify(recommendationPersistenceService, atLeastOnce()).persistPipelineRecommendations(any(), any());
    }

    @Test
    void getSimilarBooks_returnsTieredFallbackResultsWhenDbMisses() {
        Book source = buildBook("source", "en", List.of("Author One"), List.of("Fiction"));
        Book fallbackMatch = buildBook("fallback-match", "en", List.of("Author Two"), List.of("Fiction"));

        when(bookDataOrchestrator.fetchCanonicalBookReactive("source")).thenReturn(Mono.just(source));
        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of(fallbackMatch)));

        StepVerifier.create(recommendationService.getSimilarBooks("source", 3))
            .expectNextMatches(results -> results.stream().anyMatch(book -> "fallback-match".equals(book.getId())))
            .verifyComplete();

        verify(bookDataOrchestrator, atLeastOnce()).searchBooksTiered(anyString(), any(), anyInt(), any());
        verify(recommendationPersistenceService, atLeastOnce()).persistPipelineRecommendations(any(), any());
    }

    @Test
    void getSimilarBooks_returnsEmptyWhenFallbackDisabled() {
        recommendationService = new RecommendationService(bookDataOrchestrator, recommendationPersistenceService, false);
        Book source = buildBook("source", "en", List.of("Author One"), List.of("Fiction"));

        when(bookDataOrchestrator.fetchCanonicalBookReactive("source")).thenReturn(Mono.just(source));
        when(bookDataOrchestrator.searchBooksTiered(anyString(), any(), anyInt(), any()))
            .thenReturn(Mono.just(List.of()));

        StepVerifier.create(recommendationService.getSimilarBooks("source", 3))
            .expectNext(List.of())
            .verifyComplete();

        verify(bookDataOrchestrator, atLeastOnce()).searchBooksTiered(anyString(), any(), anyInt(), any());
        verify(recommendationPersistenceService, never()).persistPipelineRecommendations(any(), any());
    }

    @Test
    void getSimilarBooks_returnsEmptyWhenCanonicalNotFound() {
        when(bookDataOrchestrator.fetchCanonicalBookReactive("missing")).thenReturn(Mono.empty());

        StepVerifier.create(recommendationService.getSimilarBooks("missing", 3))
            .expectNext(List.of())
            .verifyComplete();

        verify(bookDataOrchestrator, never()).searchBooksTiered(anyString(), any(), anyInt(), any());
        verify(recommendationPersistenceService, never()).persistPipelineRecommendations(any(), any());
    }

    private Book buildBook(String id, String language, List<String> authors, List<String> categories) {
        return com.williamcallahan.book_recommendation_engine.testutil.BookTestData.aBook()
                .id(id)
                .title("Title " + id)
                .description("Description for " + id)
                .language(language)
                .authors(authors)
                .categories(categories)
                .s3ImagePath("https://cdn.example/" + id + ".jpg")
                .publishedDate(Date.from(Instant.parse("2020-01-01T00:00:00Z")))
                .build();
    }
}
