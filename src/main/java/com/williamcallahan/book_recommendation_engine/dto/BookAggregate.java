package com.williamcallahan.book_recommendation_engine.dto;

import lombok.Builder;
import lombok.Value;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * Normalized book data from external sources, ready for UPSERT to database.
 * <p>
 * This DTO represents a book with all necessary data to persist to the following tables:
 * - books
 * - authors
 * - book_authors_join
 * - book_external_ids
 * - book_collections_join (categories)
 * <p>
 * Mappers (e.g., GoogleBooksMapper) transform provider-specific JSON into this normalized format.
 */
@Value
@Builder
public class BookAggregate {
    // Core book data
    String title;
    String subtitle;
    String description;
    String isbn13;
    String isbn10;
    LocalDate publishedDate;
    String language;
    String publisher;
    Integer pageCount;
    
    // Authors (will be deduplicated during persistence)
    List<String> authors;
    
    // Categories/genres
    List<String> categories;
    
    // External identifiers and provider-specific metadata
    ExternalIdentifiers identifiers;
    
    // Slug base for URL generation (title + first author)
    String slugBase;
    
    // Physical dimensions (from Google Books API volumeInfo.dimensions)
    Dimensions dimensions;
    
    // Edition information (derived from various sources)
    Integer editionNumber;
    String editionGroupKey;  // For clustering different editions of same work
    
    /**
     * External provider identifiers and metadata.
     * Maps to book_external_ids table.
     */
    @Value
    @Builder
    public static class ExternalIdentifiers {
        // Primary identifiers
        String source;              // 'GOOGLE_BOOKS', 'OPEN_LIBRARY', 'AMAZON', etc.
        String externalId;          // Provider's primary ID
        
        // Provider ISBNs (may differ from canonical)
        String providerIsbn10;
        String providerIsbn13;
        
        // Links
        String infoLink;
        String previewLink;
        String webReaderLink;
        String purchaseLink;
        String canonicalVolumeLink;
        
        // Ratings and reviews
        Double averageRating;
        Integer ratingsCount;
        Integer reviewCount;
        
        // Availability
        Boolean isEbook;
        Boolean pdfAvailable;
        Boolean epubAvailable;
        Boolean embeddable;
        Boolean publicDomain;
        String viewability;         // 'FULL', 'PARTIAL', 'NO_PAGES', 'ALL_PAGES'
        Boolean textReadable;
        Boolean imageReadable;
        
        // Content metadata
        String printType;           // 'BOOK', 'MAGAZINE'
        String maturityRating;      // 'NOT_MATURE', 'MATURE'
        String contentVersion;
        String textToSpeechPermission;
        
        // Sale information
        String saleability;         // 'FOR_SALE', 'NOT_FOR_SALE', 'FREE'
        String countryCode;
        Boolean isEbookForSale;
        Double listPrice;
        Double retailPrice;
        String currencyCode;
        
        // Work identifiers (for clustering editions)
        String oclcWorkId;
        String openlibraryWorkId;
        String goodreadsWorkId;
        String googleCanonicalId;
        
        // Image URLs
        Map<String, String> imageLinks; // Key: imageType, Value: URL
    }
    
    /**
     * Physical dimensions of the book.
     * Maps to book_dimensions table.
     */
    @Value
    @Builder
    public static class Dimensions {
        String height;      // e.g., "24.00 cm" from Google Books
        String width;       // e.g., "16.00 cm"
        String thickness;   // e.g., "2.50 cm"
    }
}
