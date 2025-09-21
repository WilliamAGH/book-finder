package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookRecommendationPersistenceService.RecommendationRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BookRecommendationPersistenceServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private BookDataOrchestrator bookDataOrchestrator;

    private BookRecommendationPersistenceService persistenceService;

    @BeforeEach
    void setUp() {
        persistenceService = new BookRecommendationPersistenceService(jdbcTemplate, bookDataOrchestrator);
    }

    @Test
    void persistPipelineRecommendations_insertsNormalizedScores() {
        UUID sourceUuid = UUID.randomUUID();
        UUID recommendationUuid = UUID.randomUUID();

        Book source = buildBook(sourceUuid.toString());
        Book recommended = buildBook(recommendationUuid.toString());

        RecommendationRecord record = new RecommendationRecord(recommended, 6.0d, List.of("AUTHOR"));

        StepVerifier.create(persistenceService.persistPipelineRecommendations(source, List.of(record)))
            .verifyComplete();

        verify(jdbcTemplate).update("DELETE FROM book_recommendations WHERE source_book_id = ? AND source = ?", sourceUuid, "RECOMMENDATION_PIPELINE");

        ArgumentCaptor<Double> scoreCaptor = ArgumentCaptor.forClass(Double.class);
        verify(jdbcTemplate).update(startsWith("INSERT INTO book_recommendations"),
            any(),
            eq(sourceUuid),
            eq(recommendationUuid),
            eq("RECOMMENDATION_PIPELINE"),
            scoreCaptor.capture(),
            eq("AUTHOR"));

        assertThat(scoreCaptor.getValue()).isEqualTo(0.6d);
    }

    @Test
    void persistPipelineRecommendations_resolvesCanonicalViaOrchestrator() {
        UUID sourceUuid = UUID.randomUUID();
        UUID resolvedUuid = UUID.randomUUID();

        Book source = buildBook(sourceUuid.toString());
        Book unresolved = buildBook("google-volume-id");
        Book resolved = buildBook(resolvedUuid.toString());

        RecommendationRecord record = new RecommendationRecord(unresolved, 4.0d, List.of("AUTHOR"));

        when(bookDataOrchestrator.getBookByIdTiered("google-volume-id")).thenReturn(Mono.just(resolved));

        StepVerifier.create(persistenceService.persistPipelineRecommendations(source, List.of(record)))
            .verifyComplete();

        verify(jdbcTemplate).update("DELETE FROM book_recommendations WHERE source_book_id = ? AND source = ?", sourceUuid, "RECOMMENDATION_PIPELINE");
        verify(jdbcTemplate).update(startsWith("INSERT INTO book_recommendations"), any(), eq(sourceUuid), eq(resolvedUuid), eq("RECOMMENDATION_PIPELINE"), anyDouble(), any());
    }

    @Test
    void persistPipelineRecommendations_skipsWhenRecommendationCannotResolve() {
        UUID sourceUuid = UUID.randomUUID();
        Book source = buildBook(sourceUuid.toString());
        Book unresolved = buildBook("unresolvable-id");

        RecommendationRecord record = new RecommendationRecord(unresolved, 3.0d, List.of("AUTHOR"));

        when(bookDataOrchestrator.getBookByIdTiered("unresolvable-id")).thenReturn(Mono.empty());

        StepVerifier.create(persistenceService.persistPipelineRecommendations(source, List.of(record)))
            .verifyComplete();

        verifyNoInteractions(jdbcTemplate);
    }

    private Book buildBook(String id) {
        Book book = new Book();
        book.setId(id);
        book.setTitle("Title " + id);
        return book;
    }
}
