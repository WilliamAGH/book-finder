package com.williamcallahan.book_recommendation_engine.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

/**
 * Single source of truth for book list view in search results.
 * Extends card data with description and categories for richer display.
 * 
 * This DTO is populated by optimized SQL queries that fetch exactly what's needed
 * in a single database round-trip.
 * 
 * Used by:
 * - Search results list view
 * - Category browse list view
 * 
 * @param id Book UUID as string
 * @param slug URL-friendly book identifier
 * @param title Book title
 * @param description Book description/summary
 * @param authors List of author names in order
 * @param categories List of category names
 * @param coverUrl Primary cover image URL
 * @param averageRating Average rating (0.0-5.0)
 * @param ratingsCount Total number of ratings
 * @param tags Qualifier tags as key-value pairs
 */
public record BookListItem(
    String id,
    String slug,
    String title,
    String description,
    
    @JsonProperty("authors")
    List<String> authors,
    
    @JsonProperty("categories")
    List<String> categories,
    
    @JsonProperty("cover_url")
    String coverUrl,
    
    @JsonProperty("average_rating")
    Double averageRating,
    
    @JsonProperty("ratings_count")
    Integer ratingsCount,
    
    Map<String, Object> tags
) {
    /**
     * Compact constructor ensuring defensive copies for immutability
     */
    public BookListItem {
        authors = authors == null ? List.of() : List.copyOf(authors);
        categories = categories == null ? List.of() : List.copyOf(categories);
        tags = tags == null ? Map.of() : Map.copyOf(tags);
    }
    
    /**
     * Get truncated description for list view
     */
    public String getTruncatedDescription(int maxLength) {
        if (description == null || description.length() <= maxLength) {
            return description;
        }
        return description.substring(0, maxLength) + "...";
    }
}