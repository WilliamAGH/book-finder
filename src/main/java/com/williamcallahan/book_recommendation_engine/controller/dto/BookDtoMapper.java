package com.williamcallahan.book_recommendation_engine.controller.dto;

import com.williamcallahan.book_recommendation_engine.dto.BookCard;
import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.dto.BookListItem;
import com.williamcallahan.book_recommendation_engine.dto.EditionSummary;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.util.SlugGenerator;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;

import java.time.LocalDate;
import java.time.ZoneId;
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
        List<CollectionDto> collections = mapCollections(book);
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
                collections,
                tags,
                cover,
                editions,
                recommendationIds,
                book.getQualifiers() == null ? Map.of() : Map.copyOf(book.getQualifiers())
        );
    }

    public static BookDto fromDetail(BookDetail detail) {
        return fromDetail(detail, Map.of());
    }

    public static BookDto fromDetail(BookDetail detail, Map<String, Object> extras) {
        if (detail == null) {
            return null;
        }

        PublicationDto publication = new PublicationDto(
            toDate(detail.publishedDate()),
            detail.language(),
            detail.pageCount(),
            detail.publisher()
        );

        String detailCoverUrl = detail.coverUrl();
        String thumbnailUrl = detail.thumbnailUrl();
        String s3ImagePath = resolveS3Candidate(detailCoverUrl);
        String externalImageUrl = (s3ImagePath == null) ? detailCoverUrl : null;
        String preferredUrl = ValidationUtils.hasText(thumbnailUrl) ? thumbnailUrl : firstNonBlank(detailCoverUrl);
        String fallbackUrl = firstNonBlank(detailCoverUrl, thumbnailUrl);

        CoverDto cover = new CoverDto(
            s3ImagePath,
            externalImageUrl,
            detail.coverWidth(),
            detail.coverHeight(),
            detail.coverHighResolution(),
            preferredUrl,
            fallbackUrl,
            detail.dataSource()
        );

        List<AuthorDto> authors = toAuthorDtos(detail.authors());
        List<CollectionDto> collections = List.of();
        List<TagDto> tags = toTagDtos(detail.tags());
        List<EditionDto> editions = detail.editions().stream()
            .map(BookDtoMapper::toEditionDto)
            .toList();

        return new BookDto(
            detail.id(),
            detail.slug(),
            detail.title(),
            detail.description(),
            publication,
            authors,
            detail.categories(),
            collections,
            tags,
            cover,
            editions,
            List.of(),
            extras == null ? Map.of() : Map.copyOf(extras)
        );
    }

    public static BookDto fromCard(BookCard card) {
        return fromCard(card, Map.of());
    }

    public static BookDto fromCard(BookCard card, Map<String, Object> extras) {
        if (card == null) {
            return null;
        }

        String cardCoverUrl = card.coverUrl();
        String cardS3ImagePath = resolveS3Candidate(cardCoverUrl);
        String cardExternalUrl = (cardS3ImagePath == null) ? cardCoverUrl : null;
        String resolvedCoverUrl = firstNonBlank(cardS3ImagePath, cardExternalUrl);

        CoverDto cover = new CoverDto(
            cardS3ImagePath,
            cardExternalUrl,
            null,
            null,
            null,
            resolvedCoverUrl,
            resolvedCoverUrl,
            null
        );

        PublicationDto publication = new PublicationDto(null, null, null, null);

        return new BookDto(
            card.id(),
            card.slug(),
            card.title(),
            null,
            publication,
            toAuthorDtos(card.authors()),
            List.of(),
            List.of(),
            toTagDtos(card.tags()),
            cover,
            List.of(),
            List.of(),
            extras == null ? Map.of() : Map.copyOf(extras)
        );
    }

    public static BookDto fromListItem(BookListItem item) {
        return fromListItem(item, Map.of());
    }

    public static BookDto fromListItem(BookListItem item, Map<String, Object> extras) {
        if (item == null) {
            return null;
        }

        PublicationDto publication = new PublicationDto(null, null, null, null);

        CoverDto cover = new CoverDto(
            item.coverUrl(),
            item.coverUrl(),
            null,
            null,
            null,
            item.coverUrl(),
            item.coverUrl(),
            null
        );

        return new BookDto(
            item.id(),
            item.slug(),
            item.title(),
            item.description(),
            publication,
            toAuthorDtos(item.authors()),
            item.categories(),
            List.of(),
            toTagDtos(item.tags()),
            cover,
            List.of(),
            List.of(),
            extras == null ? Map.of() : Map.copyOf(extras)
        );
    }

    private static List<AuthorDto> toAuthorDtos(List<String> authors) {
        if (authors == null || authors.isEmpty()) {
            return List.of();
        }
        return authors.stream()
            .filter(Objects::nonNull)
            .map(name -> new AuthorDto(null, name))
            .toList();
    }

    private static List<TagDto> toTagDtos(Map<String, Object> tags) {
        if (tags == null || tags.isEmpty()) {
            return List.of();
        }
        return tags.entrySet().stream()
            .map(entry -> new TagDto(entry.getKey(), toAttributeMap(entry.getValue())))
            .toList();
    }

    private static EditionDto toEditionDto(EditionSummary summary) {
        if (summary == null) {
            return null;
        }
        Date published = toDate(summary.publishedDate());
        return new EditionDto(
            summary.id(),
            summary.slug(),
            summary.title(),
            null,
            summary.isbn13(),
            published,
            summary.coverUrl()
        );
    }

    private static Date toDate(LocalDate date) {
        return date == null ? null : Date.from(date.atStartOfDay(ZoneId.systemDefault()).toInstant());
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

    private static String resolveS3Candidate(String candidate) {
        if (!ValidationUtils.hasText(candidate)) {
            return null;
        }
        String value = candidate.trim();
        if (value.startsWith("s3://")) {
            return value;
        }
        if (!value.contains("://")) {
            return value;
        }
        return null;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (ValidationUtils.hasText(value)) {
                return value;
            }
        }
        return null;
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

    private static List<CollectionDto> mapCollections(Book book) {
        List<Book.CollectionAssignment> assignments = book.getCollections();
        if (assignments == null || assignments.isEmpty()) {
            return List.of();
        }
        return assignments.stream()
                .map(assignment -> new CollectionDto(
                        assignment.getCollectionId(),
                        assignment.getName(),
                        assignment.getCollectionType(),
                        assignment.getRank(),
                        assignment.getSource()
                ))
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
