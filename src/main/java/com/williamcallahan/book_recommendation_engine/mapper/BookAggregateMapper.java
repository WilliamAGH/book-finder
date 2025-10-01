package com.williamcallahan.book_recommendation_engine.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.util.SlugGenerator;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps Book model to BookAggregate DTO.
 * <p>
 * This is a transitional mapper used during Phase 3 integration.
 * It bridges the existing Book model with the new BookAggregate structure.
 * <p>
 * Key mappings:
 * - Book fields → BookAggregate core fields
 * - Book metadata → BookAggregate.ExternalIdentifiers
 * - JsonNode sourceJson → extract external IDs and provider metadata
 * <p>
 * Thread-safe, stateless utility class.
 *
 * @deprecated Parse provider payloads through
 * {@link com.williamcallahan.book_recommendation_engine.mapper.GoogleBooksMapper}
 * and convert straight into Postgres persistence via
 * {@link com.williamcallahan.book_recommendation_engine.service.BookUpsertService}.
 */
@Deprecated(since = "2025-10-01", forRemoval = true)
@Slf4j
public final class BookAggregateMapper {
    
    private BookAggregateMapper() {
        // Utility class
    }
    
    /**
     * Map Book model to BookAggregate DTO.
     * <p>
     * Extracts external identifiers from sourceJson if available.
     * Falls back to Book model fields if sourceJson is null.
     *
     * @param book Book model (existing domain object)
     * @param sourceJson Optional JSON from external API (for external IDs)
     * @return BookAggregate ready for BookUpsertService
     */
    public static BookAggregate fromBook(Book book, JsonNode sourceJson) {
        if (book == null) {
            throw new IllegalArgumentException("Book cannot be null");
        }
        
        if (book.getTitle() == null || book.getTitle().isBlank()) {
            throw new IllegalArgumentException("Book title is required");
        }
        
        // Convert published date
        LocalDate publishedDate = null;
        if (book.getPublishedDate() != null) {
            publishedDate = book.getPublishedDate()
                .toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
        }
        
        // Generate slug base
        String slugBase = SlugGenerator.generateBookSlug(book.getTitle(), book.getAuthors());
        
        // Build external identifiers
        BookAggregate.ExternalIdentifiers identifiers = buildExternalIdentifiers(book, sourceJson);
        
        return BookAggregate.builder()
            .title(book.getTitle())
            .subtitle(null) // Book model doesn't have subtitle field
            .description(book.getDescription())
            .isbn13(book.getIsbn13())
            .isbn10(book.getIsbn10())
            .publishedDate(publishedDate)
            .language(book.getLanguage())
            .publisher(book.getPublisher())
            .pageCount(book.getPageCount())
            .authors(book.getAuthors() != null ? book.getAuthors() : List.of())
            .categories(book.getCategories() != null ? book.getCategories() : List.of())
            .identifiers(identifiers)
            .slugBase(slugBase)
            .build();
    }
    
    /**
     * Build external identifiers from Book model and optional sourceJson.
     */
    private static BookAggregate.ExternalIdentifiers buildExternalIdentifiers(Book book, JsonNode sourceJson) {
        // Determine source - check if sourceJson has Google Books structure
        String source = determineSource(sourceJson);
        String externalId = extractExternalId(book, sourceJson);
        
        // Extract image links
        Map<String, String> imageLinks = new HashMap<>();
        if (book.getExternalImageUrl() != null) {
            imageLinks.put("external", book.getExternalImageUrl());
        }
        if (book.getS3ImagePath() != null) {
            imageLinks.put("s3", book.getS3ImagePath());
        }
        
        // Build identifiers
        return BookAggregate.ExternalIdentifiers.builder()
            .source(source)
            .externalId(externalId)
            .providerIsbn10(book.getIsbn10())
            .providerIsbn13(book.getIsbn13())
            .infoLink(book.getInfoLink())
            .previewLink(book.getPreviewLink())
            .purchaseLink(book.getPurchaseLink())
            .webReaderLink(book.getWebReaderLink())
            .averageRating(book.getAverageRating())
            .ratingsCount(book.getRatingsCount())
            .pdfAvailable(book.getPdfAvailable())
            .epubAvailable(book.getEpubAvailable())
            .listPrice(book.getListPrice())
            .currencyCode(book.getCurrencyCode())
            .imageLinks(imageLinks)
            .build();
    }
    
    /**
     * Determine source from JSON or fall back to generic.
     */
    private static String determineSource(JsonNode sourceJson) {
        if (sourceJson == null) {
            return "UNKNOWN";
        }
        
        // Google Books has "volumeInfo" field
        if (sourceJson.has("volumeInfo")) {
            return "GOOGLE_BOOKS";
        }
        
        // OpenLibrary has "key" field with /works/ or /books/
        if (sourceJson.has("key")) {
            String key = sourceJson.get("key").asText();
            if (key.contains("/works/")) {
                return "OPEN_LIBRARY";
            }
        }
        
        return "UNKNOWN";
    }
    
    /**
     * Extract external ID from JSON or Book model.
     */
    private static String extractExternalId(Book book, JsonNode sourceJson) {
        // Try to extract from sourceJson first
        if (sourceJson != null) {
            // Google Books: id field at root
            if (sourceJson.has("id")) {
                return sourceJson.get("id").asText();
            }
            
            // OpenLibrary: key field
            if (sourceJson.has("key")) {
                String key = sourceJson.get("key").asText();
                // Extract ID from /works/OL123W or /books/OL123M
                int lastSlash = key.lastIndexOf('/');
                if (lastSlash != -1) {
                    return key.substring(lastSlash + 1);
                }
                return key;
            }
        }
        
        // Fall back to book.id if it doesn't look like a UUID
        if (book.getId() != null) {
            String id = book.getId();
            // If it's not a UUID, it's probably an external ID
            if (!id.matches("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")) {
                return id;
            }
        }
        
        // Last resort: use ISBN-13 as external ID
        return book.getIsbn13();
    }
}
