package com.williamcallahan.book_recommendation_engine.repository;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.dto.BookCard;
import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.dto.BookListItem;
import com.williamcallahan.book_recommendation_engine.dto.EditionSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Repository that centralises optimized Postgres read queries for book views.
 * Replaces the hydration-heavy implementation with concise functions per DTO use case.
 */
@Repository
public class BookQueryRepository {

    private static final Logger log = LoggerFactory.getLogger(BookQueryRepository.class);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public BookQueryRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    // ==================== Book Cards ====================
    
    /**
     * Fetch minimal book data for card displays (homepage, search grid).
     * SINGLE QUERY replaces 5 hydration queries per book.
     * 
     * This is THE SINGLE SOURCE for card data - all card views must use this method.
     * 
     * @param bookIds List of book UUIDs to fetch
     * @return List of BookCard DTOs with all card-specific data
     */
    public List<BookCard> fetchBookCards(List<UUID> bookIds) {
        if (bookIds == null || bookIds.isEmpty()) {
            return List.of();
        }

        try {
            String sql = "SELECT * FROM get_book_cards(?::UUID[])";
            UUID[] idsArray = bookIds.toArray(new UUID[0]);
            
            return jdbcTemplate.query(sql, new BookCardRowMapper(), (Object) idsArray);
        } catch (DataAccessException ex) {
            log.error("Failed to fetch book cards for {} books: {}", bookIds.size(), ex.getMessage(), ex);
            return List.of();
        }
    }

    /**
     * Fetch book cards by collection (e.g., NYT bestsellers for homepage).
     * SINGLE QUERY with position ordering.
     * 
     * This is THE SINGLE SOURCE for collection-based card fetching.
     * 
     * @param collectionId Collection ID (NanoID text, not UUID)
     * @param limit Maximum number of books to return
     * @return List of BookCard DTOs ordered by collection position
     */
    public List<BookCard> fetchBookCardsByCollection(String collectionId, int limit) {
        if (collectionId == null || collectionId.isBlank() || limit <= 0) {
            return List.of();
        }

        try {
            // Note: collection_id is TEXT (NanoID), not UUID
            String sql = "SELECT * FROM get_book_cards_by_collection(?, ?)";
            
            return jdbcTemplate.query(sql, new BookCardRowMapper(), collectionId, limit);
        } catch (DataAccessException ex) {
            log.error("Failed to fetch book cards for collection {}: {}", collectionId, ex.getMessage(), ex);
            return List.of();
        }
    }

    /**
     * Fetch book cards by NYT bestseller list provider code.
     * THE SINGLE SOURCE for NYT bestseller fetching.
     * 
     * This method finds the latest collection for the given provider code,
     * then fetches the book cards. Replaces PostgresBookRepository.fetchLatestBestsellerBooks.
     * 
     * @param providerListCode NYT list code (e.g., "hardcover-fiction")
     * @param limit Maximum number of books to return
     * @return List of BookCard DTOs ordered by position
     */
    public List<BookCard> fetchBookCardsByProviderListCode(String providerListCode, int limit) {
        if (providerListCode == null || providerListCode.isBlank() || limit <= 0) {
            return List.of();
        }

        try {
            // First get the latest collection ID for this provider code
            String collectionSql = """
                SELECT id
                FROM book_collections
                WHERE collection_type = 'BESTSELLER_LIST'
                  AND source = 'NYT'
                  AND provider_list_code = ?
                ORDER BY published_date DESC, updated_at DESC
                LIMIT 1
                """;
            
            List<String> collectionIds = jdbcTemplate.query(
                collectionSql,
                (rs, rowNum) -> rs.getString("id"),
                providerListCode.trim()
            ).stream()
            .filter(Objects::nonNull)
            .filter(id -> !id.isBlank())
            .collect(Collectors.toList());
            
            if (collectionIds.isEmpty()) {
                log.debug("No collection found for provider list code: {}", providerListCode);
                return List.of();
            }
            
            // Then fetch the book cards for that collection
            return fetchBookCardsByCollection(collectionIds.get(0), limit);
        } catch (DataAccessException ex) {
            log.error("Failed to fetch book cards for provider list '{}': {}", providerListCode, ex.getMessage(), ex);
            return List.of();
        }
    }

    // ==================== Book List Items ====================
    
    /**
     * Fetch extended book data for search list view.
     * SINGLE QUERY replaces 6 hydration queries per book.
     * 
     * This is THE SINGLE SOURCE for list item data - all list views must use this method.
     * 
     * @param bookIds List of book UUIDs to fetch
     * @return List of BookListItem DTOs with description and categories
     */
    public List<BookListItem> fetchBookListItems(List<UUID> bookIds) {
        if (bookIds == null || bookIds.isEmpty()) {
            return List.of();
        }

        try {
            String sql = "SELECT * FROM get_book_list_items(?::UUID[])";
            UUID[] idsArray = bookIds.toArray(new UUID[0]);
            
            return jdbcTemplate.query(sql, new BookListItemRowMapper(), (Object) idsArray);
        } catch (DataAccessException ex) {
            log.error("Failed to fetch book list items for {} books: {}", bookIds.size(), ex.getMessage(), ex);
            return List.of();
        }
    }

    // ==================== Book Detail ====================
    
    /**
     * Fetch complete book detail for detail page by UUID.
     * SINGLE QUERY replaces 6-10 hydration queries per book.
     * 
     * This is THE SINGLE SOURCE for book detail data by ID.
     * 
     * @param bookId Book UUID
     * @return Optional BookDetail DTO, empty if not found
     */
    public Optional<BookDetail> fetchBookDetail(UUID bookId) {
        if (bookId == null) {
            return Optional.empty();
        }

        try {
            String sql = "SELECT * FROM get_book_detail(?::UUID)";
            
            List<BookDetail> results = jdbcTemplate.query(sql, new BookDetailRowMapper(), bookId);
            return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
        } catch (DataAccessException ex) {
            log.error("Failed to fetch book detail for {}: {}", bookId, ex.getMessage(), ex);
            return Optional.empty();
        }
    }

    /**
     * Fetch book detail by slug (URL-friendly identifier).
     * This is THE SINGLE SOURCE for book detail data by slug.
     * 
     * @param slug Book slug (e.g., "harry-potter-philosophers-stone")
     * @return Optional BookDetail DTO, empty if not found
     */
    public Optional<BookDetail> fetchBookDetailBySlug(String slug) {
        if (slug == null || slug.isBlank()) {
            return Optional.empty();
        }

        try {
            // First get the book ID by slug, then fetch detail
            String idSql = "SELECT id FROM books WHERE slug = ? LIMIT 1";
            List<UUID> ids = jdbcTemplate.query(idSql, (rs, rowNum) -> (UUID) rs.getObject("id"), slug.trim());
            
            if (ids.isEmpty()) {
                return Optional.empty();
            }
            
            return fetchBookDetail(ids.get(0));
        } catch (DataAccessException ex) {
            log.error("Failed to fetch book detail by slug '{}': {}", slug, ex.getMessage(), ex);
            return Optional.empty();
        }
    }

    // ==================== Book Editions ====================
    
    /**
     * Fetch other editions of a book (for Editions tab).
     * This is THE SINGLE SOURCE for editions data.
     * 
     * @param bookId Book UUID to find editions for
     * @return List of EditionSummary DTOs (other editions excluding the book itself)
     */
    public List<EditionSummary> fetchBookEditions(UUID bookId) {
        if (bookId == null) {
            return List.of();
        }

        try {
            String sql = "SELECT * FROM get_book_editions(?::UUID)";
            
            return jdbcTemplate.query(sql, new EditionSummaryRowMapper(), bookId);
        } catch (DataAccessException ex) {
            log.error("Failed to fetch editions for book {}: {}", bookId, ex.getMessage(), ex);
            return List.of();
        }
    }

    // ==================== Row Mappers (Internal) ====================
    
    /**
     * Maps database row to BookCard DTO - internal implementation detail.
     */
    private class BookCardRowMapper implements RowMapper<BookCard> {
        @Override
        public BookCard mapRow(@NonNull ResultSet rs, int rowNum) throws SQLException {
            List<String> authors = parseTextArray(rs.getArray("authors"));
            log.debug("BookCard row: id={}, title={}, authors={}, coverUrl={}", 
                rs.getString("id"), rs.getString("title"), authors, rs.getString("cover_url"));
            return new BookCard(
                rs.getString("id"),
                rs.getString("slug"),
                rs.getString("title"),
                authors,
                rs.getString("cover_url"),
                getDoubleOrNull(rs, "average_rating"),
                getIntOrNull(rs, "ratings_count"),
                parseJsonb(rs.getString("tags"))
            );
        }
    }

    /**
     * Maps database row to BookListItem DTO - internal implementation detail.
     */
    private class BookListItemRowMapper implements RowMapper<BookListItem> {
        @Override
        public BookListItem mapRow(@NonNull ResultSet rs, int rowNum) throws SQLException {
            return new BookListItem(
                rs.getString("id"),
                rs.getString("slug"),
                rs.getString("title"),
                rs.getString("description"),
                parseTextArray(rs.getArray("authors")),
                parseTextArray(rs.getArray("categories")),
                rs.getString("cover_url"),
                getDoubleOrNull(rs, "average_rating"),
                getIntOrNull(rs, "ratings_count"),
                parseJsonb(rs.getString("tags"))
            );
        }
    }

    /**
     * Maps database row to BookDetail DTO - internal implementation detail.
     */
    private class BookDetailRowMapper implements RowMapper<BookDetail> {
        @Override
        public BookDetail mapRow(@NonNull ResultSet rs, int rowNum) throws SQLException {
            return new BookDetail(
                rs.getString("id"),
                rs.getString("slug"),
                rs.getString("title"),
                rs.getString("description"),
                rs.getString("publisher"),
                getLocalDateOrNull(rs, "published_date"),
                rs.getString("language"),
                getIntOrNull(rs, "page_count"),
                parseTextArray(rs.getArray("authors")),
                parseTextArray(rs.getArray("categories")),
                rs.getString("cover_url"),
                rs.getString("thumbnail_url"),
                getDoubleOrNull(rs, "average_rating"),
                getIntOrNull(rs, "ratings_count"),
                rs.getString("isbn_10"),
                rs.getString("isbn_13"),
                rs.getString("preview_link"),
                rs.getString("info_link"),
                parseJsonb(rs.getString("tags")),
                List.of() // Editions fetched separately via fetchBookEditions()
            );
        }
    }

    /**
     * Maps database row to EditionSummary DTO - internal implementation detail.
     */
    private class EditionSummaryRowMapper implements RowMapper<EditionSummary> {
        @Override
        public EditionSummary mapRow(@NonNull ResultSet rs, int rowNum) throws SQLException {
            return new EditionSummary(
                rs.getString("id"),
                rs.getString("slug"),
                rs.getString("title"),
                getLocalDateOrNull(rs, "published_date"),
                rs.getString("publisher"),
                rs.getString("isbn_13"),
                rs.getString("cover_url"),
                rs.getString("language"),
                getIntOrNull(rs, "page_count")
            );
        }
    }

    // ==================== Helper Methods (Internal) ====================
    
    /**
     * Parse Postgres TEXT[] array to Java List<String>.
     */
    private List<String> parseTextArray(java.sql.Array sqlArray) throws SQLException {
        if (sqlArray == null) {
            return List.of();
        }
        
        String[] array = (String[]) sqlArray.getArray();
        return array == null ? List.of() : Arrays.asList(array);
    }

    /**
     * Parse Postgres JSONB to Java Map.
     */
    private Map<String, Object> parseJsonb(String jsonb) {
        if (jsonb == null || jsonb.isBlank() || jsonb.equals("{}")) {
            return Map.of();
        }
        
        try {
            return objectMapper.readValue(jsonb, MAP_TYPE);
        } catch (Exception ex) {
            log.warn("Failed to parse JSONB: {}", ex.getMessage());
            return Map.of();
        }
    }

    /**
     * Safely get nullable Double from ResultSet.
     */
    private Double getDoubleOrNull(ResultSet rs, String columnName) throws SQLException {
        double value = rs.getDouble(columnName);
        return rs.wasNull() ? null : value;
    }

    /**
     * Safely get nullable Integer from ResultSet.
     */
    private Integer getIntOrNull(ResultSet rs, String columnName) throws SQLException {
        int value = rs.getInt(columnName);
        return rs.wasNull() ? null : value;
    }

    /**
     * Safely get nullable LocalDate from ResultSet.
     */
    private LocalDate getLocalDateOrNull(ResultSet rs, String columnName) throws SQLException {
        Date date = rs.getDate(columnName);
        return date == null ? null : date.toLocalDate();
    }
}
