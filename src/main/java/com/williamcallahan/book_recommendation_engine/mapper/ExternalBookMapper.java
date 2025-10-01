package com.williamcallahan.book_recommendation_engine.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;

/**
 * Contract for mapping external API responses to normalized BookAggregate.
 * <p>
 * Each external book provider (Google Books, Open Library, Amazon, etc.) has its own implementation
 * that knows how to parse that provider's JSON structure and extract book data.
 * <p>
 * Implementations should handle:
 * - Missing/null fields gracefully
 * - Invalid data formats (dates, ISBNs, etc.)
 * - Provider-specific quirks and edge cases
 * <p>
 * Example implementations:
 * - {@link GoogleBooksMapper} for Google Books API
 * - OpenLibraryMapper for Open Library API (future)
 */
public interface ExternalBookMapper {
    
    /**
     * Maps external API JSON response to normalized BookAggregate.
     * <p>
     * Returns null if the JSON is invalid, missing required fields, or cannot be parsed.
     * Implementations should log warnings for partial data but return best-effort results.
     *
     * @param externalJson JSON response from external API
     * @return BookAggregate with normalized data, or null if unparseable
     */
    BookAggregate map(JsonNode externalJson);
    
    /**
     * Returns the source name for this mapper.
     * <p>
     * Must match the value used in book_external_ids.source column.
     * Examples: 'GOOGLE_BOOKS', 'OPEN_LIBRARY', 'AMAZON'
     *
     * @return source identifier constant
     */
    String getSourceName();
}
