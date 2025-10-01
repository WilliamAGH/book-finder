package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.dto.BookCard;
import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.dto.BookListItem;
import com.williamcallahan.book_recommendation_engine.dto.EditionSummary;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.Book.EditionInfo;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Converts projection DTOs into legacy {@link Book} instances while the
 * application transitions toward DTO-first controllers. This centralises
 * the remaining bridge logic so deprecated mappers can be retired without
 * duplicating conversion code across controllers and schedulers.
 */
public final class BookDomainMapper {

    private BookDomainMapper() {
    }

    public static Book fromDetail(BookDetail detail) {
        if (detail == null) {
            return null;
        }

        Book book = base(detail.id(), detail.slug(), detail.title(), detail.authors(), detail.coverUrl());
        book.setDescription(detail.description());
        book.setPublisher(detail.publisher());
        book.setPublishedDate(toDate(detail.publishedDate()));
        book.setLanguage(detail.language());
        book.setPageCount(detail.pageCount());
        book.setCategories(detail.categories());
        book.setAverageRating(detail.averageRating());
        book.setRatingsCount(detail.ratingsCount());
        book.setIsbn10(detail.isbn10());
        book.setIsbn13(detail.isbn13());
        book.setPreviewLink(detail.previewLink());
        book.setInfoLink(detail.infoLink());
        book.setQualifiers(copyMap(detail.tags()));
        book.setOtherEditions(toEditionInfo(detail.editions()));
        setCoverImages(book, detail.coverUrl(), detail.thumbnailUrl());
        book.setCoverImageWidth(detail.coverWidth());
        book.setCoverImageHeight(detail.coverHeight());
        book.setIsCoverHighResolution(detail.coverHighResolution());
        book.setDataSource(detail.dataSource());
        book.setRetrievedFrom("POSTGRES");
        book.setInPostgres(true);
        return book;
    }

    public static Book fromCard(BookCard card) {
        if (card == null) {
            return null;
        }
        Book book = base(card.id(), card.slug(), card.title(), card.authors(), card.coverUrl());
        book.setAverageRating(card.averageRating());
        book.setRatingsCount(card.ratingsCount());
        book.setQualifiers(copyMap(card.tags()));
        setCoverImages(book, card.coverUrl(), card.coverUrl());
        return book;
    }

    public static List<Book> fromCards(List<BookCard> cards) {
        if (cards == null || cards.isEmpty()) {
            return List.of();
        }
        return cards.stream()
            .map(BookDomainMapper::fromCard)
            .filter(Objects::nonNull)
            .toList();
    }

    public static Book fromListItem(BookListItem item) {
        if (item == null) {
            return null;
        }
        Book book = base(item.id(), item.slug(), item.title(), item.authors(), item.coverUrl());
        book.setDescription(item.description());
        book.setCategories(item.categories());
        book.setAverageRating(item.averageRating());
        book.setRatingsCount(item.ratingsCount());
        book.setQualifiers(copyMap(item.tags()));
        setCoverImages(book, item.coverUrl(), item.coverUrl());
        return book;
    }

    public static List<Book> fromListItems(List<BookListItem> items) {
        if (items == null || items.isEmpty()) {
            return List.of();
        }
        return items.stream()
            .map(BookDomainMapper::fromListItem)
            .filter(Objects::nonNull)
            .toList();
    }

    public static Book fromAggregate(BookAggregate aggregate) {
        if (aggregate == null) {
            return null;
        }

        BookAggregate.ExternalIdentifiers identifiers = aggregate.getIdentifiers();
        SelectedImages images = resolvePreferredImages(identifiers);

        Book book = base(
            identifiers != null ? identifiers.getExternalId() : null,
            aggregate.getSlugBase(),
            aggregate.getTitle(),
            aggregate.getAuthors(),
            images.preferred()
        );

        book.setDescription(aggregate.getDescription());
        book.setPublisher(aggregate.getPublisher());
        book.setPublishedDate(toDate(aggregate.getPublishedDate()));
        book.setLanguage(aggregate.getLanguage());
        book.setPageCount(aggregate.getPageCount());
        book.setCategories(aggregate.getCategories());
        book.setIsbn10(aggregate.getIsbn10());
        book.setIsbn13(aggregate.getIsbn13());

        if (identifiers != null) {
            book.setAverageRating(identifiers.getAverageRating());
            book.setRatingsCount(identifiers.getRatingsCount());
            book.setPreviewLink(identifiers.getPreviewLink());
            book.setInfoLink(identifiers.getInfoLink());
            book.setPurchaseLink(identifiers.getPurchaseLink());
            book.setWebReaderLink(identifiers.getWebReaderLink());
            book.setListPrice(identifiers.getListPrice());
            book.setCurrencyCode(identifiers.getCurrencyCode());
            book.setPdfAvailable(identifiers.getPdfAvailable());
            book.setEpubAvailable(identifiers.getEpubAvailable());
        }

        setCoverImages(book, images.preferred(), images.fallback());

        book.setEditionNumber(aggregate.getEditionNumber());
        book.setEditionGroupKey(aggregate.getEditionGroupKey());
        return book;
    }

    private static Book base(String id,
                             String slug,
                             String title,
                             List<String> authors,
                             String coverUrl) {
        Book book = new Book();
        book.setId(id);
        book.setSlug(ValidationUtils.hasText(slug) ? slug : id);
        book.setTitle(title);
        book.setAuthors(authors);
        setCoverImages(book, coverUrl, coverUrl);
        return book;
    }

    private static void setCoverImages(Book book, String preferredUrl, String fallbackUrl) {
        if (!ValidationUtils.hasText(preferredUrl) && !ValidationUtils.hasText(fallbackUrl)) {
            return;
        }
        String effectivePreferred = ValidationUtils.hasText(preferredUrl) ? preferredUrl : fallbackUrl;
        book.setExternalImageUrl(effectivePreferred);

        Optional<String> storageLocation = UrlSourceDetector.detectStorageLocation(effectivePreferred);
        if (storageLocation.filter(ImageDetails.STORAGE_S3::equals).isPresent()) {
            book.setS3ImagePath(effectivePreferred);
        }

        CoverImages images = new CoverImages(
            effectivePreferred,
            ValidationUtils.hasText(fallbackUrl) ? fallbackUrl : effectivePreferred,
            UrlSourceDetector.detectSource(effectivePreferred)
        );
        book.setCoverImages(images);
    }

    private static Date toDate(LocalDate value) {
        return value == null ? null : Date.from(value.atStartOfDay(ZoneId.systemDefault()).toInstant());
    }

    private static Map<String, Object> copyMap(Map<String, Object> source) {
        return (source == null || source.isEmpty()) ? Map.of() : Map.copyOf(source);
    }

    private static List<EditionInfo> toEditionInfo(List<EditionSummary> summaries) {
        if (summaries == null || summaries.isEmpty()) {
            return List.of();
        }
        List<EditionInfo> editions = new ArrayList<>(summaries.size());
        for (EditionSummary summary : summaries) {
            if (summary == null) {
                continue;
            }
            EditionInfo info = new EditionInfo();
            info.setGoogleBooksId(summary.id());
            info.setIdentifier(summary.slug());
            info.setEditionIsbn13(summary.isbn13());
            info.setPublishedDate(toDate(summary.publishedDate()));
            info.setCoverImageUrl(summary.coverUrl());
            editions.add(info);
        }
        return editions;
    }

    private static SelectedImages resolvePreferredImages(BookAggregate.ExternalIdentifiers identifiers) {
        if (identifiers == null || identifiers.getImageLinks() == null || identifiers.getImageLinks().isEmpty()) {
            return new SelectedImages(null, null);
        }

        Map<String, String> links = identifiers.getImageLinks();
        List<String> priorityOrder = List.of("extraLarge", "large", "medium", "small", "thumbnail", "smallThumbnail");

        String primary = null;
        String secondary = null;
        for (String key : priorityOrder) {
            String candidate = links.get(key);
            if (!ValidationUtils.hasText(candidate)) {
                continue;
            }
            if (primary == null) {
                primary = candidate;
            } else if (secondary == null) {
                secondary = candidate;
            }
        }

        if (!ValidationUtils.hasText(secondary)) {
            secondary = primary;
        }

        return new SelectedImages(primary, secondary);
    }

    private record SelectedImages(String preferred, String fallback) {
    }
}
