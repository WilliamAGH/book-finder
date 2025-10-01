package com.williamcallahan.book_recommendation_engine.dto;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;

import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * TEMPORARY BRIDGE: Maps DTOs back to Book entities for backward compatibility.
 * 
 * This class exists ONLY during the migration period to allow existing code
 * that expects {@link Book} objects to work with the new optimized DTOs.
 * 
 * <p><b>DEPRECATION PLAN:</b></p>
 * <ul>
 *   <li>Phase 4-5: Use this mapper to gradually migrate controllers/services</li>
 *   <li>Phase 6: Once all consumers updated to use DTOs directly, mark as @Deprecated</li>
 *   <li>Phase 7: Remove this class entirely</li>
 * </ul>
 * 
 * <p><b>WHY THIS EXISTS:</b></p>
 * The old {@link Book} entity contains many fields not in DTOs (dimensions, rawPayload, etc.).
 * This mapper creates "good enough" Book objects for services that still need them.
 * 
 * @see BookCard Source of truth for card data
 * @see BookDetail Source of truth for detail data
 */
public class DtoToBookMapper {

    /**
     * Map BookCard DTO to Book entity (temporary bridge).
     * Creates a Book with only card-relevant fields populated.
     */
    public static Book toBook(BookCard card) {
        if (card == null) {
            return null;
        }

        Book book = new Book();
        book.setId(card.id());
        book.setSlug(card.slug());
        book.setTitle(card.title());
        book.setAuthors(card.authors());
        
        // Cover URL handling
        if (card.coverUrl() != null) {
            book.setExternalImageUrl(card.coverUrl());
            // Create CoverImages for compatibility
            CoverImages coverImages = new CoverImages(
                card.coverUrl(), 
                card.coverUrl(), 
                CoverImageSource.GOOGLE_BOOKS
            );
            book.setCoverImages(coverImages);
        }
        
        // Ratings
        book.setAverageRating(card.averageRating());
        book.setRatingsCount(card.ratingsCount());
        
        // Tags/qualifiers
        if (card.tags() != null && !card.tags().isEmpty()) {
            book.setQualifiers(card.tags());
        }
        
        return book;
    }

    /**
     * Map multiple BookCards to Books (temporary bridge).
     */
    public static List<Book> toBooks(List<BookCard> cards) {
        if (cards == null) {
            return List.of();
        }
        return cards.stream()
            .map(DtoToBookMapper::toBook)
            .collect(Collectors.toList());
    }

    /**
     * Map BookDetail DTO to Book entity (temporary bridge).
     * Creates a more complete Book with detail fields populated.
     */
    public static Book toBook(BookDetail detail) {
        if (detail == null) {
            return null;
        }

        Book book = new Book();
        book.setId(detail.id());
        book.setSlug(detail.slug());
        book.setTitle(detail.title());
        book.setDescription(detail.description());
        book.setPublisher(detail.publisher());
        
        // Date conversion
        if (detail.publishedDate() != null) {
            book.setPublishedDate(Date.from(
                detail.publishedDate().atStartOfDay(ZoneId.systemDefault()).toInstant()
            ));
        }
        
        book.setLanguage(detail.language());
        book.setPageCount(detail.pageCount());
        book.setAuthors(detail.authors());
        book.setCategories(detail.categories());
        
        // Cover URLs
        if (detail.coverUrl() != null) {
            book.setExternalImageUrl(detail.coverUrl());
            CoverImages coverImages = new CoverImages(
                detail.coverUrl(),
                detail.thumbnailUrl() != null ? detail.thumbnailUrl() : detail.coverUrl(),
                CoverImageSource.GOOGLE_BOOKS
            );
            book.setCoverImages(coverImages);
        }
        
        // Ratings
        book.setAverageRating(detail.averageRating());
        book.setRatingsCount(detail.ratingsCount());
        
        // ISBNs
        book.setIsbn10(detail.isbn10());
        book.setIsbn13(detail.isbn13());
        
        // Links
        book.setPreviewLink(detail.previewLink());
        book.setInfoLink(detail.infoLink());
        
        // Tags/qualifiers
        if (detail.tags() != null && !detail.tags().isEmpty()) {
            book.setQualifiers(detail.tags());
        }
        
        return book;
    }

    /**
     * Map BookListItem DTO to Book entity (temporary bridge).
     */
    public static Book toBook(BookListItem item) {
        if (item == null) {
            return null;
        }

        Book book = new Book();
        book.setId(item.id());
        book.setSlug(item.slug());
        book.setTitle(item.title());
        book.setDescription(item.description());
        book.setAuthors(item.authors());
        book.setCategories(item.categories());
        
        if (item.coverUrl() != null) {
            book.setExternalImageUrl(item.coverUrl());
            CoverImages coverImages = new CoverImages(
                item.coverUrl(),
                item.coverUrl(),
                CoverImageSource.GOOGLE_BOOKS
            );
            book.setCoverImages(coverImages);
        }
        
        book.setAverageRating(item.averageRating());
        book.setRatingsCount(item.ratingsCount());
        
        if (item.tags() != null && !item.tags().isEmpty()) {
            book.setQualifiers(item.tags());
        }
        
        return book;
    }

    /**
     * Map multiple BookListItems to Books (temporary bridge).
     */
    public static List<Book> toBooksFromListItems(List<BookListItem> items) {
        if (items == null) {
            return List.of();
        }
        return items.stream()
            .map(DtoToBookMapper::toBook)
            .collect(Collectors.toList());
    }
}