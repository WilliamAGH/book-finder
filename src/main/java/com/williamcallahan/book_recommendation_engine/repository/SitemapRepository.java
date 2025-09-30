package com.williamcallahan.book_recommendation_engine.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Repository responsible for Postgres backed sitemap queries.
 */
@Repository
public class SitemapRepository {

    private static final String LETTER_BUCKET_EXPRESSION =
            "CASE " +
            "WHEN substring(lower(trim(%s)), 1, 1) BETWEEN 'a' AND 'z' THEN substring(lower(trim(%s)), 1, 1) " +
            "ELSE '0-9' END";

    private static final String BOOK_UPDATED_AT_ALIAS = "book_updated_at";

    private static final RowMapper<BookRow> BOOK_ROW_MAPPER = (rs, rowNum) -> new BookRow(
            rs.getString("id"),
            rs.getString("slug"),
            rs.getString("title"),
            rs.getTimestamp(BOOK_UPDATED_AT_ALIAS).toInstant()
    );

    private final JdbcTemplate jdbcTemplate;

    public SitemapRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public int countAllBooks() {
        String sql = "SELECT COUNT(*) FROM books WHERE slug IS NOT NULL";
        return Objects.requireNonNullElse(jdbcTemplate.queryForObject(sql, Integer.class), 0);
    }

    public Map<String, Integer> countBooksByBucket() {
        String expr = LETTER_BUCKET_EXPRESSION.formatted("title", "title");
        String sql = "SELECT " + expr + " AS bucket, COUNT(*) AS total FROM books " +
                     "WHERE slug IS NOT NULL GROUP BY bucket";
        return jdbcTemplate.query(sql, rs -> {
            Map<String, Integer> counts = new LinkedHashMap<>();
            while (rs.next()) {
                counts.put(rs.getString("bucket").toUpperCase(Locale.ROOT), rs.getInt("total"));
            }
            return counts;
        });
    }

    public int countBooksForBucket(String bucket) {
        String expr = LETTER_BUCKET_EXPRESSION.formatted("title", "title");
        String sql = "SELECT COUNT(*) FROM books WHERE slug IS NOT NULL AND " + expr + " = ?";
        return Objects.requireNonNullElse(jdbcTemplate.queryForObject(sql, Integer.class, bucket.toLowerCase(Locale.ROOT)), 0);
    }

    public List<BookRow> fetchBooksForBucket(String bucket, int limit, int offset) {
        String expr = LETTER_BUCKET_EXPRESSION.formatted("title", "title");
        String sql = "SELECT id, slug, title, COALESCE(updated_at, created_at, NOW()) AS " + BOOK_UPDATED_AT_ALIAS +
                     " FROM books WHERE slug IS NOT NULL AND " + expr + " = ? " +
                     "ORDER BY lower(title) NULLS LAST, slug NULLS LAST, id ASC LIMIT ? OFFSET ?";
        return jdbcTemplate.query(sql, BOOK_ROW_MAPPER, bucket.toLowerCase(Locale.ROOT), limit, offset);
    }

    public List<BookRow> fetchBooksForXml(int limit, int offset) {
        String sql = "SELECT id, slug, title, COALESCE(updated_at, created_at, NOW()) AS " + BOOK_UPDATED_AT_ALIAS +
                     " FROM books WHERE slug IS NOT NULL " +
                     "ORDER BY COALESCE(updated_at, created_at, NOW()) ASC NULLS LAST, " +
                     "         lower(title) ASC NULLS LAST, " +
                     "         slug ASC NULLS LAST, " +
                     "         id ASC " +
                     "LIMIT ? OFFSET ?";
        return jdbcTemplate.query(sql, BOOK_ROW_MAPPER, limit, offset);
    }

    public Map<String, Integer> countAuthorsByBucket() {
        String expr = LETTER_BUCKET_EXPRESSION.formatted("COALESCE(normalized_name, name)", "COALESCE(normalized_name, name)");
        String sql = "SELECT " + expr + " AS bucket, COUNT(*) AS total FROM authors GROUP BY bucket";
        return jdbcTemplate.query(sql, rs -> {
            Map<String, Integer> counts = new LinkedHashMap<>();
            while (rs.next()) {
                counts.put(rs.getString("bucket").toUpperCase(Locale.ROOT), rs.getInt("total"));
            }
            return counts;
        });
    }

    public int countAuthorsForBucket(String bucket) {
        String expr = LETTER_BUCKET_EXPRESSION.formatted("COALESCE(normalized_name, name)", "COALESCE(normalized_name, name)");
        String sql = "SELECT COUNT(*) FROM authors WHERE " + expr + " = ?";
        return Objects.requireNonNullElse(jdbcTemplate.queryForObject(sql, Integer.class, bucket.toLowerCase(Locale.ROOT)), 0);
    }

    public List<AuthorRow> fetchAuthorsForBucket(String bucket, int limit, int offset) {
        String expr = LETTER_BUCKET_EXPRESSION.formatted("COALESCE(normalized_name, name)", "COALESCE(normalized_name, name)");
        String sql = "SELECT id, name, COALESCE(updated_at, created_at, NOW()) AS author_updated_at " +
                     "FROM authors WHERE " + expr + " = ? " +
                     "ORDER BY lower(COALESCE(name, '')) NULLS LAST, id ASC LIMIT ? OFFSET ?";
        return jdbcTemplate.query(sql, (rs, rowNum) -> new AuthorRow(
                rs.getString("id"),
                rs.getString("name"),
                rs.getTimestamp("author_updated_at").toInstant()
        ), bucket.toLowerCase(Locale.ROOT), limit, offset);
    }

    public Map<String, List<BookRow>> fetchBooksForAuthors(Set<String> authorIds) {
        if (authorIds == null || authorIds.isEmpty()) {
            return Map.of();
        }
        String sql = "SELECT baj.author_id, b.id, b.slug, b.title, COALESCE(b.updated_at, b.created_at, NOW()) AS " + BOOK_UPDATED_AT_ALIAS +
                     " FROM book_authors_join baj " +
                     " JOIN books b ON b.id = baj.book_id " +
                     " WHERE baj.author_id IN (%s) AND b.slug IS NOT NULL " +
                     " ORDER BY baj.author_id, lower(b.title), b.slug";
        String placeholders = authorIds.stream().map(id -> "?").collect(Collectors.joining(","));
        String resolvedSql = sql.formatted(placeholders);
        Object[] params = authorIds.toArray();
        return jdbcTemplate.query(resolvedSql, rs -> {
            Map<String, List<BookRow>> results = new LinkedHashMap<>();
            while (rs.next()) {
                String authorId = rs.getString("author_id");
                BookRow row = new BookRow(
                        rs.getString("id"),
                        rs.getString("slug"),
                        rs.getString("title"),
                        rs.getTimestamp(BOOK_UPDATED_AT_ALIAS).toInstant()
                );
                results.computeIfAbsent(authorId, key -> new ArrayList<>()).add(row);
            }
            return results;
        }, params);
    }

    public List<PageMetadata> fetchBookPageMetadata(int pageSize) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("Page size must be positive, got: " + pageSize);
        }
        String sql = "WITH ordered AS (" +
                "    SELECT COALESCE(updated_at, created_at, NOW()) AS " + BOOK_UPDATED_AT_ALIAS + "," +
                "           row_number() OVER (ORDER BY COALESCE(updated_at, created_at, NOW()) ASC NULLS LAST, " +
                "                                       lower(title) ASC NULLS LAST, " +
                "                                       slug ASC NULLS LAST, " +
                "                                       id ASC) AS rn" +
                "    FROM books" +
                "    WHERE slug IS NOT NULL" +
                ") " +
                "SELECT CAST(FLOOR((rn - 1) / ?::numeric) AS bigint) + 1 AS page_number, " +
                "       MAX(" + BOOK_UPDATED_AT_ALIAS + ") AS last_modified " +
                "FROM ordered GROUP BY page_number ORDER BY page_number";
        return jdbcTemplate.query(sql, (rs, rowNum) -> new PageMetadata(
                rs.getInt("page_number"),
                rs.getTimestamp("last_modified").toInstant()
        ), pageSize);
    }

    public DatasetFingerprint fetchBookFingerprint() {
        String sql = "SELECT COUNT(*) AS total_records, " +
                "COALESCE(MAX(updated_at), MAX(created_at), TIMESTAMP 'epoch') AS last_modified " +
                "FROM books WHERE slug IS NOT NULL";
        return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> new DatasetFingerprint(
                rs.getInt("total_records"),
                rs.getTimestamp("last_modified").toInstant()
        ));
    }

    public DatasetFingerprint fetchAuthorFingerprint() {
        String sql = "SELECT COUNT(DISTINCT a.id) AS total_records, " +
                "GREATEST(" +
                "    COALESCE(MAX(a.updated_at), MAX(a.created_at), TIMESTAMP 'epoch')," +
                "    COALESCE(MAX(b.updated_at), MAX(b.created_at), TIMESTAMP 'epoch')" +
                ") AS last_modified " +
                "FROM authors a " +
                "LEFT JOIN book_authors_join baj ON baj.author_id = a.id " +
                "LEFT JOIN books b ON b.id = baj.book_id AND b.slug IS NOT NULL";
        return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> new DatasetFingerprint(
                rs.getInt("total_records"),
                rs.getTimestamp("last_modified").toInstant()
        ));
    }

    public record BookRow(String bookId, String slug, String title, Instant updatedAt) {}

    public record AuthorRow(String id, String name, Instant updatedAt) {}

    public record PageMetadata(int pageNumber, Instant lastModified) {}

    public record DatasetFingerprint(int totalRecords, Instant lastModified) {}
}
