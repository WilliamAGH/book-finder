package com.williamcallahan.book_recommendation_engine.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

/**
 * Single source of truth for book card displays (homepage, search grid).
 * Contains ONLY fields actually rendered in card templates - no over-fetching.
 * 
 * This DTO is populated by optimized SQL queries that fetch exactly what's needed
 * in a single database round-trip, eliminating N+1 query problems.
 * 
 * Used by:
 * - Homepage bestsellers grid
 * - Search results grid view
 * - Recently viewed books section
 * 
 * @param id Book UUID as string
 * @param slug URL-friendly book identifier
 * @param title Book title
 * @param authors List of author names in order
 * @param coverUrl Primary cover image URL (S3 or external)
 * @param averageRating Average rating (0.0-5.0)
 * @param ratingsCount Total number of ratings
 * @param tags Qualifier tags as key-value pairs (e.g., {"nyt_bestseller": {"list": "hardcover-fiction"}})
 */
public record BookCard(
    String id,
    String slug,
    String title,
    
    @JsonProperty("authors")
    List<String> authors,
    
    @JsonProperty("cover_url")
    String coverUrl,
    
    @JsonProperty("average_rating")
    Double averageRating,
    
    @JsonProperty("ratings_count")
    Integer ratingsCount,
    
    /**
     * Tags/qualifiers for rendering badges like "NYT Bestseller", "Award Winner", etc.
     * Key is tag type (e.g., "nyt_bestseller"), value is metadata object
     */
    Map<String, Object> tags
) {
    /**
     * Compact constructor ensuring defensive copies for immutability
     */
    public BookCard {
        authors = authors == null ? List.of() : List.copyOf(authors);
        tags = tags == null ? Map.of() : Map.copyOf(tags);
    }
    
    /**
     * Check if book has a specific qualifier tag
     */
    public boolean hasTag(String tagKey) {
        return tags != null && tags.containsKey(tagKey);
    }
}