package com.williamcallahan.book_recommendation_engine.controller.dto;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.util.SlugGenerator;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Shared mapper that transforms domain {@link Book} objects into API-facing DTOs.
 * Centralising this logic lets controller layers adopt Postgres-first data
 * without rewriting mapping code in multiple places.
 */
public final class BookDtoMapper {

    private BookDtoMapper() {
    }

    public static BookDto toDto(Book book) {
        if (book == null) {
            return null;
        }

        PublicationDto publication = new PublicationDto(
                safeCopy(book.getPublishedDate()),
                book.getLanguage(),
                book.getPageCount(),
                book.getPublisher()
        );

        CoverDto cover = buildCover(book);

        List<AuthorDto> authors = mapAuthors(book);
        List<String> categories = book.getCategories() == null ? List.of() : List.copyOf(book.getCategories());
        List<TagDto> tags = mapTags(book);
        List<EditionDto> editions = mapEditions(book);
        List<String> recommendationIds = book.getCachedRecommendationIds() == null
                ? List.of()
                : List.copyOf(book.getCachedRecommendationIds());

        String slug = resolveSlug(book);

        return new BookDto(
                book.getId(),
                slug,
                book.getTitle(),
                book.getDescription(),
                publication,
                authors,
                categories,
                List.of(), // Collections/normalized lists will be populated once Postgres-first data is fully wired
                tags,
                cover,
                editions,
                recommendationIds,
                book.getQualifiers() == null ? Map.of() : Map.copyOf(book.getQualifiers())
        );
    }

    private static String resolveSlug(Book book) {
        if (book.getSlug() != null && !book.getSlug().isBlank()) {
            return book.getSlug();
        }
        return SlugGenerator.generateBookSlug(book.getTitle(), book.getAuthors());
    }

    private static CoverDto buildCover(Book book) {
        CoverImages coverImages = book.getCoverImages();
        return new CoverDto(
                book.getS3ImagePath(),
                book.getExternalImageUrl(),
                book.getCoverImageWidth(),
                book.getCoverImageHeight(),
                book.getIsCoverHighResolution(),
                coverImages != null ? coverImages.getPreferredUrl() : null,
                coverImages != null ? coverImages.getFallbackUrl() : null,
                coverImages != null && coverImages.getSource() != null ? coverImages.getSource().name() : null
        );
    }

    private static List<AuthorDto> mapAuthors(Book book) {
        if (book.getAuthors() == null || book.getAuthors().isEmpty()) {
            return List.of();
        }
        return book.getAuthors().stream()
                .filter(Objects::nonNull)
                .map(name -> new AuthorDto(null, name))
                .toList();
    }

    private static List<TagDto> mapTags(Book book) {
        if (book.getQualifiers() == null || book.getQualifiers().isEmpty()) {
            return List.of();
        }
        return book.getQualifiers().entrySet().stream()
                .map(entry -> new TagDto(entry.getKey(), toAttributeMap(entry.getValue())))
                .toList();
    }

    private static Map<String, Object> toAttributeMap(Object value) {
        if (value instanceof Map<?, ?> mapValue) {
            return mapValue.entrySet().stream()
                    .filter(e -> e.getKey() != null)
                    .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
        }
        return Map.of("value", value);
    }

    private static List<EditionDto> mapEditions(Book book) {
        if (book.getOtherEditions() == null || book.getOtherEditions().isEmpty()) {
            return List.of();
        }
        return book.getOtherEditions().stream()
                .map(edition -> new EditionDto(
                        edition.getGoogleBooksId(),
                        edition.getType(),
                        edition.getIdentifier(),
                        edition.getEditionIsbn10(),
                        edition.getEditionIsbn13(),
                        safeCopy(edition.getPublishedDate()),
                        edition.getCoverImageUrl()
                ))
                .toList();
    }

    private static Date safeCopy(Date date) {
        return date == null ? null : new Date(date.getTime());
    }
}
