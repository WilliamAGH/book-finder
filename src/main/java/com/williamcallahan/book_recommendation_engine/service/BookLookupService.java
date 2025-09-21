package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.util.JdbcUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;

import static com.williamcallahan.book_recommendation_engine.util.ApplicationConstants.Database.Queries.BOOK_BY_ID;
import static com.williamcallahan.book_recommendation_engine.util.ApplicationConstants.Database.Queries.BOOK_BY_ISBN10;
import static com.williamcallahan.book_recommendation_engine.util.ApplicationConstants.Database.Queries.BOOK_BY_ISBN13;

/**
 * Centralized service for book lookup operations to eliminate duplicate ISBN query patterns
 * across the codebase. Provides consistent book ID resolution by ISBN13, ISBN10, and external IDs.
 */
@Service
@Slf4j
public class BookLookupService {

    private final JdbcTemplate jdbcTemplate;

    public BookLookupService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Find a book ID by ISBN13, checking both the books table and external IDs table.
     *
     * @param isbn13 the ISBN13 to search for
     * @return Optional containing the book ID if found
     */
    public Optional<String> findBookIdByIsbn13(String isbn13) {
        if (ValidationUtils.isNullOrBlank(isbn13)) {
            return Optional.empty();
        }

        // First check books table
        Optional<String> bookId = JdbcUtils.optionalString(
            jdbcTemplate,
            BOOK_BY_ISBN13,
            ex -> log.debug("Query failed for ISBN13 in books table: {}", ex.getMessage()),
            isbn13
        );

        // Fallback to external IDs table
        if (bookId.isEmpty()) {
            bookId = JdbcUtils.optionalString(
                jdbcTemplate,
                "SELECT book_id FROM book_external_ids WHERE provider_isbn13 = ? LIMIT 1",
                ex -> log.debug("Query failed for ISBN13 in external IDs: {}", ex.getMessage()),
                isbn13
            );
        }

        return bookId;
    }

    /**
     * Find a book ID by ISBN10, checking both the books table and external IDs table.
     *
     * @param isbn10 the ISBN10 to search for
     * @return Optional containing the book ID if found
     */
    public Optional<String> findBookIdByIsbn10(String isbn10) {
        if (ValidationUtils.isNullOrBlank(isbn10)) {
            return Optional.empty();
        }

        // First check books table
        Optional<String> bookId = JdbcUtils.optionalString(
            jdbcTemplate,
            BOOK_BY_ISBN10,
            ex -> log.debug("Query failed for ISBN10 in books table: {}", ex.getMessage()),
            isbn10
        );

        // Fallback to external IDs table
        if (bookId.isEmpty()) {
            bookId = JdbcUtils.optionalString(
                jdbcTemplate,
                "SELECT book_id FROM book_external_ids WHERE provider_isbn10 = ? LIMIT 1",
                ex -> log.debug("Query failed for ISBN10 in external IDs: {}", ex.getMessage()),
                isbn10
            );
        }

        return bookId;
    }

    /**
     * Find a book ID by external provider ID.
     *
     * @param source the source system (e.g., "GOOGLE_BOOKS")
     * @param externalId the external ID from that source
     * @return Optional containing the book ID if found
     */
    public Optional<String> findBookIdByExternalId(String source, String externalId) {
        if (ValidationUtils.isNullOrBlank(source) || ValidationUtils.isNullOrBlank(externalId)) {
            return Optional.empty();
        }

        return JdbcUtils.optionalString(
            jdbcTemplate,
            "SELECT book_id FROM book_external_ids WHERE source = ? AND external_id = ? LIMIT 1",
            ex -> log.debug("Query failed for external ID: {}", ex.getMessage()),
            source,
            externalId
        );
    }

    /**
     * Resolve a canonical book ID by trying ISBN13 first, then ISBN10 as fallback.
     * This method replicates the common pattern used throughout the codebase.
     *
     * @param isbn13 the ISBN13 to search for (can be null)
     * @param isbn10 the ISBN10 to search for (can be null)
     * @return the book ID if found, null otherwise
     */
    public String resolveCanonicalBookId(String isbn13, String isbn10) {
        return findBookIdByIsbn13(isbn13)
            .or(() -> findBookIdByIsbn10(isbn10))
            .orElse(null);
    }

    /**
     * Check if a book exists by its ID.
     *
     * @param bookId the book ID to check
     * @return Optional containing the book ID if it exists
     */
    public Optional<String> findBookById(String bookId) {
        if (ValidationUtils.isNullOrBlank(bookId)) {
            return Optional.empty();
        }

        return JdbcUtils.optionalString(
            jdbcTemplate,
            BOOK_BY_ID,
            ex -> log.debug("Query failed for book ID: {}", ex.getMessage()),
            bookId
        );
    }

    /**
     * Helper method matching the queryForId pattern used in other services.
     * Provided for backward compatibility during migration.
     *
     * @param sql the SQL query to execute
     * @param params the query parameters
     * @return the result string or null
     */
    public String queryForId(String sql, Object... params) {
        return JdbcUtils.optionalString(
            jdbcTemplate,
            sql,
            ex -> log.debug("Query failed: {}", ex.getMessage()),
            params
        ).orElse(null);
    }
}
