package com.williamcallahan.book_recommendation_engine.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Data Transfer Object for Book entity.
 * Used for API responses and inter-service communication.
 *
 * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.controller.dto.BookDto}
 * as the canonical API contract.
 */
@Deprecated(since = "2025-10-01", forRemoval = true)
// Lombok annotations retained for backwards compatibility during the sunset period.
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BookDto {
    private String id;
    private String isbn13;
    private String isbn10;
    private String title;
    private String subtitle;
    private List<String> authors;
    private String publisher;
    private String publishDate;
    private String description;
    private Integer pageCount;
    private List<String> categories;
    private String coverUrl;
    private String language;
    private String slug;
}
