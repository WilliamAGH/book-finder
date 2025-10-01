/**
 * Converts BookAggregate DTO to Book model.
 * <p>
 * This allows both legacy and new code paths to use GoogleBooksMapper as SSOT,
 * then convert the normalized BookAggregate to the Book model used throughout the app.
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.model.Book;

import java.util.Date;

/**
 * Utility for converting BookAggregate (normalized external data) to Book (app model).
 * <p>
 * This enables the consolidation strategy where:
 * 1. GoogleBooksMapper is SSOT for parsing Google Books JSON → BookAggregate
 * 2. Both old path (BookJsonParser → Book) and new path (BookUpsertService) use GoogleBooksMapper
 * 3. This converter bridges BookAggregate → Book for legacy code paths
 */
public class BookAggregateToBookConverter {
    
    /**
     * Convert BookAggregate to Book model.
     * <p>
     * Maps all fields from normalized BookAggregate to legacy Book model structure.
     * Does NOT set rawJsonResponse (caller should set this separately if needed).
     * 
     * @param aggregate Normalized book data from external source
     * @return Book model populated with aggregate data
     */
    public static Book convert(BookAggregate aggregate) {
        if (aggregate == null) {
            return null;
        }
        
        Book book = new Book();
        
        // Core fields
        book.setTitle(aggregate.getTitle());
        // Note: Book model doesn't have subtitle field
        book.setDescription(aggregate.getDescription());
        book.setIsbn13(aggregate.getIsbn13());
        book.setIsbn10(aggregate.getIsbn10());
        book.setPublisher(aggregate.getPublisher());
        book.setLanguage(aggregate.getLanguage());
        book.setPageCount(aggregate.getPageCount());
        
        // Convert LocalDate to Date
        if (aggregate.getPublishedDate() != null) {
            book.setPublishedDate(java.sql.Date.valueOf(aggregate.getPublishedDate()));
        }
        
        // Authors
        if (aggregate.getAuthors() != null) {
            book.setAuthors(aggregate.getAuthors());
        }
        
        // Categories
        if (aggregate.getCategories() != null) {
            book.setCategories(aggregate.getCategories());
        }
        
        // Extract external identifiers
        if (aggregate.getIdentifiers() != null) {
            BookAggregate.ExternalIdentifiers ids = aggregate.getIdentifiers();
            
            // Set ID from external ID
            book.setId(ids.getExternalId());
            
            // Links
            if (ids.getInfoLink() != null) {
                book.setInfoLink(ids.getInfoLink());
            }
            if (ids.getPreviewLink() != null) {
                book.setPreviewLink(ids.getPreviewLink());
            }
            if (ids.getWebReaderLink() != null) {
                book.setWebReaderLink(ids.getWebReaderLink());
            }
            // Note: Book model doesn't have canonicalVolumeLink field
            
            // Ratings
            if (ids.getAverageRating() != null) {
                book.setAverageRating(ids.getAverageRating());
            }
            if (ids.getRatingsCount() != null) {
                book.setRatingsCount(ids.getRatingsCount());
            }
            
            // Availability
            if (ids.getPdfAvailable() != null) {
                book.setPdfAvailable(ids.getPdfAvailable());
            }
            if (ids.getEpubAvailable() != null) {
                book.setEpubAvailable(ids.getEpubAvailable());
            }
            
            // Price info
            if (ids.getListPrice() != null) {
                book.setListPrice(ids.getListPrice());
            }
            if (ids.getCurrencyCode() != null) {
                book.setCurrencyCode(ids.getCurrencyCode());
            }
            
            // Image URLs - prefer largest available
            if (ids.getImageLinks() != null && !ids.getImageLinks().isEmpty()) {
                String imageUrl = selectBestImageUrl(ids.getImageLinks());
                if (imageUrl != null) {
                    book.setExternalImageUrl(imageUrl);
                }
            }
        }
        
        // Slug (if provided in slugBase)
        if (aggregate.getSlugBase() != null) {
            book.setSlug(aggregate.getSlugBase());
        }
        
        return book;
    }
    
    /**
     * Select best quality image URL from available image links.
     * Priority: extraLarge > large > medium > small > thumbnail > smallThumbnail
     */
    private static String selectBestImageUrl(java.util.Map<String, String> imageLinks) {
        String[] priorities = {"extraLarge", "large", "medium", "small", "thumbnail", "smallThumbnail"};
        
        for (String key : priorities) {
            String url = imageLinks.get(key);
            if (url != null && !url.isBlank()) {
                return url;
            }
        }
        
        return null;
    }
}
