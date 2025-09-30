package com.williamcallahan.book_recommendation_engine.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDate;

/**
 * Single source of truth for edition summary display (Editions tab on detail page).
 * Contains minimal information needed to show other editions of a book.
 * 
 * Used by:
 * - Book detail page Editions tab
 * - Edition comparison views
 * 
 * @param id Edition UUID as string
 * @param slug URL-friendly edition identifier
 * @param title Edition title (may vary slightly from canonical)
 * @param publishedDate Edition publication date
 * @param publisher Edition publisher
 * @param isbn13 Edition ISBN-13
 * @param coverUrl Edition cover image
 * @param language Edition language
 * @param pageCount Edition page count
 */
public record EditionSummary(
    String id,
    String slug,
    String title,
    
    @JsonProperty("published_date")
    LocalDate publishedDate,
    
    String publisher,
    
    @JsonProperty("isbn_13")
    String isbn13,
    
    @JsonProperty("cover_url")
    String coverUrl,
    
    String language,
    
    @JsonProperty("page_count")
    Integer pageCount
) {
}