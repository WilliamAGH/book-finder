package com.williamcallahan.book_recommendation_engine.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Data Transfer Object for Book entity.
 * Used for API responses and inter-service communication.
 */
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