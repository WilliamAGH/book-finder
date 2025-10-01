package com.williamcallahan.book_recommendation_engine.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Single source of truth for mapping external provider IDs to internal book UUIDs.
 * <p>
 * This service provides bidirectional lookup:
 * - resolve(source, sourceId) → Optional&lt;UUID&gt; (external → internal)
 * - reverse(bookId) → Map&lt;String, String&gt; (internal → all externals)
 * <p>
 * Uses the book_external_ids table which maps:
 * - source: 'GOOGLE_BOOKS', 'OPEN_LIBRARY', 'AMAZON', etc.
 * - external_id: Provider's book identifier
 * - book_id: Our canonical UUID
 * <p>
 * Thread-safe and can be called concurrently from multiple services.
 * Optional caching can be added later using Caffeine if lookup volume becomes high.
 */
@Service
@Slf4j
public class ExternalBookIdResolver {
    
    private final JdbcTemplate jdbcTemplate;
    
    public ExternalBookIdResolver(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    /**
     * Resolves an external (source, sourceId) pair to our internal book UUID.
     * <p>
     * Returns empty if no mapping exists yet (book not backfilled).
     * <p>
     * Example:
     * <pre>
     * resolve("GOOGLE_BOOKS", "abc123") → Optional[UUID(…)]
     * </pre>
     *
     * @param source External provider name (e.g., 'GOOGLE_BOOKS')
     * @param sourceId Provider's book identifier
     * @return Optional containing book UUID if mapping exists, empty otherwise
     */
    public Optional<UUID> resolve(String source, String sourceId) {
        if (source == null || source.isBlank() || sourceId == null || sourceId.isBlank()) {
            log.debug("Invalid resolve parameters: source={}, sourceId={}", source, sourceId);
            return Optional.empty();
        }
        
        String sql = "SELECT book_id FROM book_external_ids WHERE source = ? AND external_id = ?";
        
        try {
            return jdbcTemplate.query(sql, rs -> {
                if (rs.next()) {
                    UUID bookId = (UUID) rs.getObject("book_id");
                    log.debug("Resolved {} {} → {}", source, sourceId, bookId);
                    return Optional.of(bookId);
                }
                log.debug("No mapping found for {} {}", source, sourceId);
                return Optional.empty();
            }, source, sourceId);
        } catch (Exception e) {
            log.error("Error resolving external ID: source={}, sourceId={}", source, sourceId, e);
            return Optional.empty();
        }
    }
    
    /**
     * Reverse lookup: finds all external IDs for a given book.
     * <p>
     * Returns a map where:
     * - Key: source name (e.g., 'GOOGLE_BOOKS', 'OPEN_LIBRARY')
     * - Value: external_id from that provider
     * <p>
     * Example:
     * <pre>
     * reverse(bookUuid) → {
     *   "GOOGLE_BOOKS": "abc123",
     *   "OPEN_LIBRARY": "OL12345M"
     * }
     * </pre>
     * <p>
     * Useful for:
     * - Enqueuing backfill tasks (need to know which providers have this book)
     * - Generating resolver URLs (/r/gbooks/{id})
     *
     * @param bookId Internal book UUID
     * @return Map of source → external_id, empty if no external IDs exist
     */
    public Map<String, String> reverse(UUID bookId) {
        if (bookId == null) {
            log.debug("Null bookId provided to reverse()");
            return Map.of();
        }
        
        String sql = "SELECT source, external_id FROM book_external_ids WHERE book_id = ?";
        
        try {
            return jdbcTemplate.query(sql, rs -> {
                Map<String, String> result = new HashMap<>();
                while (rs.next()) {
                    String source = rs.getString("source");
                    String externalId = rs.getString("external_id");
                    result.put(source, externalId);
                }
                log.debug("Found {} external IDs for book {}", result.size(), bookId);
                return result;
            }, bookId);
        } catch (Exception e) {
            log.error("Error in reverse lookup for bookId={}", bookId, e);
            return Map.of();
        }
    }
    
    /**
     * Check if a specific external ID exists (without fetching book UUID).
     * <p>
     * Useful for quick existence checks before enqueuing backfill tasks.
     *
     * @param source External provider name
     * @param sourceId Provider's book identifier
     * @return true if mapping exists, false otherwise
     */
    public boolean exists(String source, String sourceId) {
        if (source == null || source.isBlank() || sourceId == null || sourceId.isBlank()) {
            return false;
        }
        
        String sql = "SELECT COUNT(*) FROM book_external_ids WHERE source = ? AND external_id = ?";
        
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, source, sourceId);
            return count != null && count > 0;
        } catch (Exception e) {
            log.error("Error checking existence: source={}, sourceId={}", source, sourceId, e);
            return false;
        }
    }
    
    /**
     * Count how many external IDs exist for a given book.
     * <p>
     * Useful for monitoring data completeness.
     *
     * @param bookId Internal book UUID
     * @return Number of external ID mappings, 0 if none exist
     */
    public int countExternalIds(UUID bookId) {
        if (bookId == null) {
            return 0;
        }
        
        String sql = "SELECT COUNT(*) FROM book_external_ids WHERE book_id = ?";
        
        try {
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, bookId);
            return count != null ? count : 0;
        } catch (Exception e) {
            log.error("Error counting external IDs for bookId={}", bookId, e);
            return 0;
        }
    }
}
