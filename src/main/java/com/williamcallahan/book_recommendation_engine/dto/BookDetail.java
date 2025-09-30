package com.williamcallahan.book_recommendation_engine.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * Single source of truth for complete book details (book detail page).
 * Contains all fields actually rendered in book.html template - nothing more, nothing less.
 * 
 * This DTO is populated by optimized SQL queries that fetch exactly what's needed
 * in 1-2 database queries total (book detail + editions if needed).
 * 
 * DOES NOT INCLUDE fields never rendered:
 * - dimensions (height, width, thickness, weight)
 * - rawPayload/rawJsonResponse
 * - unused provider metadata
 * 
 * Used by:
 * - Book detail page (/book/{id})
 * - Book preview modals
 * 
 * @param id Book UUID as string
 * @param slug URL-friendly book identifier
 * @param title Book title
 * @param description Full book description
 * @param publisher Publisher name
 * @param publishedDate Publication date
 * @param language Language code (e.g., "en", "es")
 * @param pageCount Number of pages
 * @param authors List of author names in order
 * @param categories List of category/genre names
 * @param coverUrl Large cover image URL for detail page
 * @param thumbnailUrl Thumbnail cover URL for smaller displays
 * @param averageRating Average rating (0.0-5.0)
 * @param ratingsCount Total number of ratings
 * @param isbn10 ISBN-10 identifier
 * @param isbn13 ISBN-13 identifier
 * @param previewLink Preview/read link (Google Books, etc.)
 * @param infoLink More info link
 * @param tags Qualifier tags as key-value pairs
 * @param editions List of other editions (loaded separately or empty)
 */
public record BookDetail(
    String id,
    String slug,
    String title,
    String description,
    String publisher,
    
    @JsonProperty("published_date")
    LocalDate publishedDate,
    
    String language,
    
    @JsonProperty("page_count")
    Integer pageCount,
    
    @JsonProperty("authors")
    List<String> authors,
    
    @JsonProperty("categories")
    List<String> categories,
    
    @JsonProperty("cover_url")
    String coverUrl,
    
    @JsonProperty("thumbnail_url")
    String thumbnailUrl,
    
    @JsonProperty("average_rating")
    Double averageRating,
    
    @JsonProperty("ratings_count")
    Integer ratingsCount,
    
    @JsonProperty("isbn_10")
    String isbn10,
    
    @JsonProperty("isbn_13")
    String isbn13,
    
    @JsonProperty("preview_link")
    String previewLink,
    
    @JsonProperty("info_link")
    String infoLink,
    
    Map<String, Object> tags,
    
    /**
     * Other editions - can be loaded separately (lazy) or eagerly depending on use case
     */
    List<EditionSummary> editions
) {
    /**
     * Compact constructor ensuring defensive copies for immutability
     */
    public BookDetail {
        authors = authors == null ? List.of() : List.copyOf(authors);
        categories = categories == null ? List.of() : List.copyOf(categories);
        tags = tags == null ? Map.of() : Map.copyOf(tags);
        editions = editions == null ? List.of() : List.copyOf(editions);
    }
    
    /**
     * Create a copy with editions populated
     */
    public BookDetail withEditions(List<EditionSummary> newEditions) {
        return new BookDetail(
            id, slug, title, description, publisher, publishedDate, language, pageCount,
            authors, categories, coverUrl, thumbnailUrl, averageRating, ratingsCount,
            isbn10, isbn13, previewLink, infoLink, tags, newEditions
        );
    }
    
    /**
     * Check if book has editions to display
     */
    public boolean hasEditions() {
        return editions != null && !editions.isEmpty();
    }
}