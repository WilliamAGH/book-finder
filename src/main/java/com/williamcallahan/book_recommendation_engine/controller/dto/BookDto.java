package com.williamcallahan.book_recommendation_engine.controller.dto;

import java.util.List;
import java.util.Map;

/**
 * Canonical API representation of a book assembled from Postgres-backed data.
 */
public record BookDto(String id,
                      String slug,
                      String title,
                      String description,
                      PublicationDto publication,
                      List<AuthorDto> authors,
                      List<String> categories,
                      List<CollectionDto> collections,
                      List<TagDto> tags,
                      CoverDto cover,
                      List<EditionDto> editions,
                      List<String> recommendationIds,
                      Map<String, Object> extras) {
}
