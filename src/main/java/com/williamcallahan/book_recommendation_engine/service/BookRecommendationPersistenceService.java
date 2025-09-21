package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Persists recommendation relationships to Postgres so downstream flows (e.g. similarity)
 * can reuse Postgres/S3 data before touching external APIs.
 */
@Service
public class BookRecommendationPersistenceService {

    private static final Logger logger = LoggerFactory.getLogger(BookRecommendationPersistenceService.class);

    private static final String PIPELINE_SOURCE = "RECOMMENDATION_PIPELINE";
    private static final double SCORE_NORMALIZER = 10.0d; // keeps stored scores within 0..1 range

    private final JdbcTemplate jdbcTemplate;
    private final BookDataOrchestrator bookDataOrchestrator;

    public BookRecommendationPersistenceService(JdbcTemplate jdbcTemplate,
                                                BookDataOrchestrator bookDataOrchestrator) {
        this.jdbcTemplate = jdbcTemplate;
        this.bookDataOrchestrator = bookDataOrchestrator;
    }

    public Mono<Void> persistPipelineRecommendations(Book sourceBook, List<RecommendationRecord> recommendations) {
        if (jdbcTemplate == null || sourceBook == null || recommendations == null || recommendations.isEmpty()) {
            return Mono.empty();
        }

        return resolveCanonicalUuid(sourceBook)
            .flatMap(sourceUuid -> Flux.fromIterable(recommendations)
                .flatMapSequential(record -> resolveCanonicalUuid(record.book())
                    .map(recommendedUuid -> new PersistableRecommendation(record, recommendedUuid))
                    .flux(), 4, 8)
                .collectList()
                .flatMap(resolved -> {
                    if (resolved.isEmpty()) {
                        logger.debug("No canonical recommendations resolved for source {}. Skipping persistence.", sourceUuid);
                        return Mono.empty();
                    }
                    return Mono.fromCallable(() -> {
                            deleteExistingPipelineRows(sourceUuid);
                            resolved.forEach(candidate -> upsertRecommendation(sourceUuid, candidate));
                            return true;
                        })
                        .subscribeOn(Schedulers.boundedElastic())
                        .doOnSuccess(ignored -> logger.info("Persisted {} recommendation(s) for source {} via Postgres pipeline.", resolved.size(), sourceUuid))
                        .then();
                }))
            .onErrorResume(ex -> {
                logger.warn("Failed to persist recommendations for book {}: {}", sourceBook.getId(), ex.getMessage(), ex);
                return Mono.empty();
            });
    }

    private void deleteExistingPipelineRows(UUID sourceUuid) {
        jdbcTemplate.update(
            "DELETE FROM book_recommendations WHERE source_book_id = ? AND source = ?",
            sourceUuid,
            PIPELINE_SOURCE
        );
    }

    private void upsertRecommendation(UUID sourceUuid, PersistableRecommendation candidate) {
        double normalisedScore = Math.max(0.0d, Math.min(1.0d, candidate.record().score() / SCORE_NORMALIZER));
        String reason = formatReasons(candidate.record().reasons());

        jdbcTemplate.update(
            "INSERT INTO book_recommendations (id, source_book_id, recommended_book_id, source, score, reason) " +
            "VALUES (?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (source_book_id, recommended_book_id, source) " +
            "DO UPDATE SET score = EXCLUDED.score, reason = EXCLUDED.reason, generated_at = NOW(), expires_at = NOW() + INTERVAL '30 days'",
            IdGenerator.generate(),
            sourceUuid,
            candidate.recommendedUuid(),
            PIPELINE_SOURCE,
            normalisedScore,
            reason
        );
    }

    private Mono<UUID> resolveCanonicalUuid(Book book) {
        if (book == null) {
            return Mono.empty();
        }
        String identifier = book.getId();
        if (identifier == null || identifier.isBlank()) {
            return Mono.empty();
        }

        try {
            return Mono.just(UUID.fromString(identifier));
        } catch (IllegalArgumentException ex) {
            if (bookDataOrchestrator == null) {
                return Mono.empty();
            }
            return bookDataOrchestrator.getBookByIdTiered(identifier)
                .flatMap(resolved -> {
                    if (resolved == null || resolved.getId() == null) {
                        return Mono.empty();
                    }
                    try {
                        return Mono.just(UUID.fromString(resolved.getId()));
                    } catch (IllegalArgumentException inner) {
                        logger.debug("Resolved book {} still lacks canonical UUID: {}", identifier, resolved.getId());
                        return Mono.empty();
                    }
                })
                .onErrorResume(err -> {
                    logger.debug("Error resolving canonical book for {}: {}", identifier, err.getMessage());
                    return Mono.empty();
                });
        }
    }

    private String formatReasons(List<String> reasons) {
        if (reasons == null || reasons.isEmpty()) {
            return null;
        }
        Set<String> ordered = new LinkedHashSet<>(reasons);
        return String.join(",", ordered);
    }

    public record RecommendationRecord(Book book, double score, List<String> reasons) {
        public RecommendationRecord {
            reasons = reasons == null ? List.of() : new ArrayList<>(reasons);
        }
    }

    private record PersistableRecommendation(RecommendationRecord record, UUID recommendedUuid) {
    }
}
