package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.util.IsbnUtils;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import com.williamcallahan.book_recommendation_engine.util.SearchQueryUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
public class BookSearchService {

    private static final int DEFAULT_LIMIT = 20;
    private static final int MAX_LIMIT = 200;

    private final JdbcTemplate jdbcTemplate;
    private final ExternalBookIdResolver externalBookIdResolver;
    private final BackfillCoordinator backfillCoordinator;
    
    @Value("${app.features.async-backfill.enabled:false}")
    private boolean asyncBackfillEnabled;

    public BookSearchService(
        JdbcTemplate jdbcTemplate,
        Optional<ExternalBookIdResolver> externalBookIdResolver,
        Optional<BackfillCoordinator> backfillCoordinator
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.externalBookIdResolver = externalBookIdResolver.orElse(null);
        this.backfillCoordinator = backfillCoordinator.orElse(null);
    }

    public List<SearchResult> searchBooks(String query, Integer limit) {
        if (jdbcTemplate == null) {
            return List.of();
        }
        String sanitizedQuery = SearchQueryUtils.normalize(query);
        int safeLimit = PagingUtils.safeLimit(limit != null ? limit : 0, DEFAULT_LIMIT, 1, MAX_LIMIT);
        try {
            List<SearchResult> results = jdbcTemplate.query(
                    "SELECT * FROM search_books(?, ?)",
                    ps -> {
                        ps.setString(1, sanitizedQuery);
                        ps.setInt(2, safeLimit);
                    },
                    (rs, rowNum) -> new SearchResult(
                            rs.getObject("book_id", UUID.class),
                            rs.getDouble("relevance_score"),
                            rs.getString("match_type"))
            ).stream()
             .filter(result -> result.bookId() != null)
             .toList();
            
            // Enqueue backfill for books that might need enrichment
            if (asyncBackfillEnabled && !results.isEmpty()) {
                enqueueBackfillForResults(results);
            }
            
            return results;
        } catch (DataAccessException ex) {
            log.debug("Postgres search failed for query '{}': {}", sanitizedQuery, ex.getMessage());
            return Collections.emptyList();
        }
    }

    public Optional<IsbnSearchResult> searchByIsbn(String isbnQuery) {
        if (jdbcTemplate == null) {
            return Optional.empty();
        }
        String sanitized = IsbnUtils.sanitize(isbnQuery);
        if (sanitized == null) {
            return Optional.empty();
        }
        try {
            return jdbcTemplate.query(
                    "SELECT * FROM search_by_isbn(?)",
                    ps -> ps.setString(1, sanitized),
                    rs -> rs.next()
                            ? Optional.of(new IsbnSearchResult(
                                    rs.getObject("book_id", UUID.class),
                                    rs.getString("title"),
                                    rs.getString("subtitle"),
                                    rs.getString("authors"),
                                    rs.getString("isbn13"),
                                    rs.getString("isbn10"),
                                    rs.getDate("published_date"),
                                    ValidationUtils.stripWrappingQuotes(rs.getString("publisher"))))
                            : Optional.empty()
            );
        } catch (DataAccessException ex) {
            log.debug("Postgres ISBN search failed for '{}': {}", sanitized, ex.getMessage());
            return Optional.empty();
        }
    }

    public List<AuthorResult> searchAuthors(String query, Integer limit) {
        if (jdbcTemplate == null) {
            return List.of();
        }
        String sanitizedQuery = SearchQueryUtils.normalize(query);
        int safeLimit = PagingUtils.safeLimit(limit != null ? limit : 0, DEFAULT_LIMIT, 1, MAX_LIMIT);
        try {
            return jdbcTemplate.query(
                    "SELECT * FROM search_authors(?, ?)",
                    ps -> {
                        ps.setString(1, sanitizedQuery);
                        ps.setInt(2, safeLimit);
                    },
                    (rs, rowNum) -> new AuthorResult(
                            rs.getString("author_id"),
                            rs.getString("author_name"),
                            rs.getLong("book_count"),
                            rs.getDouble("relevance_score"))
            );
        } catch (DataAccessException ex) {
            log.debug("Postgres author search failed for query '{}': {}", sanitizedQuery, ex.getMessage());
            return Collections.emptyList();
        }
    }

    public void refreshMaterializedView() {
        if (jdbcTemplate == null) {
            return;
        }
        try {
            jdbcTemplate.execute("SELECT refresh_book_search_view()");
        } catch (DataAccessException ex) {
            log.debug("Failed to refresh book_search_view: {}", ex.getMessage());
        }
    }
    
    /**
     * Enqueue backfill tasks for search results that might need data enrichment.
     * <p>
     * Strategy:
     * - For each book in search results, look up external IDs
     * - Enqueue high-priority backfill tasks (priority=3) for user-facing search
     * - Runs asynchronously, doesn't block search response
     * <p>
     * Only runs when async-backfill feature flag is enabled.
     */
    private void enqueueBackfillForResults(List<SearchResult> results) {
        if (externalBookIdResolver == null || backfillCoordinator == null) {
            log.debug("Backfill components not available, skipping enqueue");
            return;
        }
        
        try {
            for (SearchResult result : results) {
                // Look up external IDs for this book
                Map<String, String> externalIds = externalBookIdResolver.reverse(result.bookId());
                
                // Enqueue backfill for each external provider
                for (Map.Entry<String, String> entry : externalIds.entrySet()) {
                    String source = entry.getKey();
                    String externalId = entry.getValue();
                    
                    // Priority 3 = high priority (user just searched)
                    backfillCoordinator.enqueue(source, externalId, 3);
                }
            }
            
            log.debug("Enqueued backfill for {} search results", results.size());
        } catch (Exception e) {
            // Don't let backfill enqueue errors affect search results
            log.warn("Error enqueuing backfill for search results: {}", e.getMessage());
        }
    }

    public record SearchResult(UUID bookId, double relevanceScore, String matchType) {
        public SearchResult {
            Objects.requireNonNull(bookId, "bookId");
        }

        public String matchTypeNormalised() {
            return matchType == null ? "UNKNOWN" : matchType.toUpperCase(Locale.ROOT);
        }
    }

    public record AuthorResult(String authorId, String authorName, long bookCount, double relevanceScore) {
    }

    public record IsbnSearchResult(UUID bookId,
                                   String title,
                                   String subtitle,
                                   String authors,
                                   String isbn13,
                                   String isbn10,
                                   java.util.Date publishedDate,
                                   String publisher) {
    }
}
