package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BookRecommendationPersistenceServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private BookLookupService bookLookupService;

    private BookRecommendationPersistenceService service;

    @BeforeEach
    void setUp() {
        service = new BookRecommendationPersistenceService(jdbcTemplate, bookLookupService);
        lenient().when(jdbcTemplate.update(anyString(), ArgumentMatchers.<Object>any()))
            .thenReturn(1);
        lenient().when(jdbcTemplate.update(anyString(), ArgumentMatchers.<Object>any(), ArgumentMatchers.<Object>any()))
            .thenReturn(1);
        lenient().when(jdbcTemplate.update(anyString(), ArgumentMatchers.<Object>any(), ArgumentMatchers.<Object>any(), ArgumentMatchers.<Object>any(), ArgumentMatchers.<Object>any(), ArgumentMatchers.<Object>any(), ArgumentMatchers.<Object>any()))
            .thenReturn(1);
    }

    @Test
    void persistPipelineRecommendations_writesRowsForResolvedBooks() {
        UUID sourceId = UUID.randomUUID();
        UUID recommendedId = UUID.randomUUID();

        Book source = new Book();
        source.setId(sourceId.toString());
        Book recommended = new Book();
        recommended.setId(recommendedId.toString());

        BookRecommendationPersistenceService.RecommendationRecord record =
            new BookRecommendationPersistenceService.RecommendationRecord(
                recommended,
                8.5,
                List.of("AUTHOR", "CATEGORY")
            );

        StepVerifier.create(service.persistPipelineRecommendations(source, List.of(record)))
            .verifyComplete();

        verify(jdbcTemplate).update(
            startsWith("DELETE FROM book_recommendations"),
            eq(sourceId),
            eq("RECOMMENDATION_PIPELINE")
        );

        verify(jdbcTemplate).update(
            startsWith("INSERT INTO book_recommendations"),
            any(), // generated UUID string for the row id
            eq(sourceId),
            eq(recommendedId),
            eq("RECOMMENDATION_PIPELINE"),
            argThat(score -> {
                assertThat(score).isInstanceOf(Double.class);
                double value = (Double) score;
                assertThat(value).isGreaterThan(0.0).isLessThanOrEqualTo(1.0);
                return true;
            }),
            eq("AUTHOR,CATEGORY")
        );
    }

    @Test
    void persistPipelineRecommendations_skipsWhenCanonicalIdsUnknown() {
        Book source = new Book();
        source.setId("external-123");
        Book recommended = new Book();
        recommended.setId("also-external");

        lenient().when(bookLookupService.findBookIdByIsbn13(anyString())).thenReturn(Optional.empty());
        lenient().when(bookLookupService.findBookIdByIsbn10(anyString())).thenReturn(Optional.empty());
        lenient().when(bookLookupService.findBookIdByExternalIdentifier(anyString())).thenReturn(Optional.empty());

        BookRecommendationPersistenceService.RecommendationRecord record =
            new BookRecommendationPersistenceService.RecommendationRecord(
                recommended,
                4.2,
                List.of("TEXT")
            );

        StepVerifier.create(service.persistPipelineRecommendations(source, List.of(record)))
            .verifyComplete();

        verifyNoInteractions(jdbcTemplate);
    }
}
