package com.williamcallahan.book_recommendation_engine.repository;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Repository responsible for Postgres backed sitemap queries.
 *
 * Fetches letter bucket counts and paginated slices for books and authors so the
 * higher level service can build both HTML and XML sitemap payloads without any
 * S3 dependence.
 */
@Repository
public class SitemapRepository {

    private static final String LETTER_BUCKET_EXPRESSION =
            "CASE " +
            "WHEN substring(lower(trim(%s)), 1, 1) BETWEEN 'a' AND 'z' THEN substring(lower(trim(%s)), 1, 1) " +
            "ELSE '0-9' END";

    private static final RowMapper<BookRow> BOOK_ROW_MAPPER = (rs, rowNum) -> new BookRow(
            rs.getString("id"),
            rs.getString("slug"),
            rs.getString("title"),
            rs.getTimestamp("updated_at") != null ? rs.getTimestamp("updated_at").toInstant() : Instant.EPOCH
    );

    private final JdbcTemplate jdbcTemplate;

    public SitemapRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private boolean jdbcUnavailable() {
        return jdbcTemplate == null;
    }

    public int countAllBooks() {
        if (jdbcUnavailable()) {
            return 0;
        }
        String sql = "SELECT COUNT(*) FROM books WHERE slug IS NOT NULL";
        return Objects.requireNonNullElse(jdbcTemplate.queryForObject(sql, Integer.class), 0);
    }

    public Map<String, Integer> countBooksByBucket() {
        if (jdbcUnavailable()) {
            return Collections.emptyMap();
        }
        String expr = LETTER_BUCKET_EXPRESSION.formatted("title", "title");
        String sql = "SELECT " + expr + " AS bucket, COUNT(*) AS total FROM books " +
                     "WHERE slug IS NOT NULL GROUP BY bucket";
        try {
            return jdbcTemplate.query(sql, rs -> {
                Map<String, Integer> counts = new LinkedHashMap<>();
                while (rs.next()) {
                    counts.put(rs.getString("bucket").toUpperCase(), rs.getInt("total"));
                }
                return counts;
            });
        } catch (DataAccessException e) {
            return Collections.emptyMap();
        }
    }

    public int countBooksForBucket(String bucket) {
        if (jdbcUnavailable()) {
            return 0;
        }
        String expr = LETTER_BUCKET_EXPRESSION.formatted("title", "title");
        String sql = "SELECT COUNT(*) FROM books WHERE slug IS NOT NULL AND " + expr + " = ?";
        return Objects.requireNonNullElse(jdbcTemplate.queryForObject(sql, Integer.class, bucket.toLowerCase()), 0);
    }

    public List<BookRow> fetchBooksForBucket(String bucket, int limit, int offset) {
        if (jdbcUnavailable()) {
            return Collections.emptyList();
        }
        String expr = LETTER_BUCKET_EXPRESSION.formatted("title", "title");
        String sql = "SELECT id, slug, title, updated_at FROM books " +
                     "WHERE slug IS NOT NULL AND " + expr + " = ? " +
                     "ORDER BY lower(title), slug LIMIT ? OFFSET ?";
        try {
            return jdbcTemplate.query(sql, BOOK_ROW_MAPPER, bucket.toLowerCase(), limit, offset);
        } catch (DataAccessException e) {
            return Collections.emptyList();
        }
    }

    public List<BookRow> fetchBooksForXml(int limit, int offset) {
        if (jdbcUnavailable()) {
            return Collections.emptyList();
        }
        String sql = "SELECT id, slug, title, updated_at FROM books WHERE slug IS NOT NULL " +
                     "ORDER BY updated_at DESC, lower(title) ASC LIMIT ? OFFSET ?";
        try {
            return jdbcTemplate.query(sql, BOOK_ROW_MAPPER, limit, offset);
        } catch (DataAccessException e) {
            return Collections.emptyList();
        }
    }

    public Map<String, Integer> countAuthorsByBucket() {
        if (jdbcUnavailable()) {
            return Collections.emptyMap();
        }
        String expr = LETTER_BUCKET_EXPRESSION.formatted("COALESCE(normalized_name, name)", "COALESCE(normalized_name, name)");
        String sql = "SELECT " + expr + " AS bucket, COUNT(*) AS total FROM authors GROUP BY bucket";
        try {
            return jdbcTemplate.query(sql, rs -> {
                Map<String, Integer> counts = new LinkedHashMap<>();
                while (rs.next()) {
                    counts.put(rs.getString("bucket").toUpperCase(), rs.getInt("total"));
                }
                return counts;
            });
        } catch (DataAccessException e) {
            return Collections.emptyMap();
        }
    }

    public int countAuthorsForBucket(String bucket) {
        if (jdbcUnavailable()) {
            return 0;
        }
        String expr = LETTER_BUCKET_EXPRESSION.formatted("COALESCE(normalized_name, name)", "COALESCE(normalized_name, name)");
        String sql = "SELECT COUNT(*) FROM authors WHERE " + expr + " = ?";
        return Objects.requireNonNullElse(jdbcTemplate.queryForObject(sql, Integer.class, bucket.toLowerCase()), 0);
    }

    public List<AuthorRow> fetchAuthorsForBucket(String bucket, int limit, int offset) {
        if (jdbcUnavailable()) {
            return Collections.emptyList();
        }
        String expr = LETTER_BUCKET_EXPRESSION.formatted("COALESCE(normalized_name, name)", "COALESCE(normalized_name, name)");
        String sql = "SELECT id, name, updated_at FROM authors WHERE " + expr + " = ? " +
                     "ORDER BY lower(name), id LIMIT ? OFFSET ?";
        try {
            return jdbcTemplate.query(sql, (rs, rowNum) -> new AuthorRow(
                    rs.getString("id"),
                    rs.getString("name"),
                    rs.getTimestamp("updated_at") != null ? rs.getTimestamp("updated_at").toInstant() : Instant.EPOCH
            ), bucket.toLowerCase(), limit, offset);
        } catch (DataAccessException e) {
            return Collections.emptyList();
        }
    }

    public Map<String, List<BookRow>> fetchBooksForAuthors(Set<String> authorIds) {
        if (jdbcUnavailable() || authorIds == null || authorIds.isEmpty()) {
            return Collections.emptyMap();
        }
        String sql = "SELECT baj.author_id, b.id, b.slug, b.title, b.updated_at " +
                     "FROM book_authors_join baj " +
                     "JOIN books b ON b.id = baj.book_id " +
                     "WHERE baj.author_id IN (%s) AND b.slug IS NOT NULL " +
                     "ORDER BY baj.author_id, lower(b.title), b.slug";
        String placeholders = authorIds.stream().map(id -> "?").collect(Collectors.joining(","));
        sql = sql.formatted(placeholders);
        Object[] params = authorIds.toArray();
        try {
            return jdbcTemplate.query(sql, params, rs -> {
                Map<String, List<BookRow>> results = new LinkedHashMap<>();
                while (rs.next()) {
                    String authorId = rs.getString("author_id");
                    BookRow row = new BookRow(
                            rs.getString("id"),
                            rs.getString("slug"),
                            rs.getString("title"),
                            rs.getTimestamp("updated_at") != null ? rs.getTimestamp("updated_at").toInstant() : Instant.EPOCH
                    );
                    results.computeIfAbsent(authorId, k -> new ArrayList<>()).add(row);
                }
                return results;
            });
        } catch (DataAccessException e) {
            return Collections.emptyMap();
        }
    }

    public record BookRow(String bookId, String slug, String title, Instant updatedAt) {}

    public record AuthorRow(String id, String name, Instant updatedAt) {}
}
