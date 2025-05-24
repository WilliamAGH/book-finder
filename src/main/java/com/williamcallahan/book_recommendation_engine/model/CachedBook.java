/**
 * Data model for storing book data in Redis cache
 *
 * @author William Callahan
 *
 * Features:
 * - Extends book model with caching metadata
 * - Tracks access patterns and embedding vectors for similarity search
 * - Contains conversion methods between Book and CachedBook
 * - Used for reducing API calls and improving performance
 * - Stored as JSON objects in Redis
 */

package com.williamcallahan.book_recommendation_engine.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.util.UuidUtil; // Added import
import lombok.Data;
import lombok.NoArgsConstructor;
import com.williamcallahan.book_recommendation_engine.types.RedisVector;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

@Data
@NoArgsConstructor
public class CachedBook {

    private String id;
    private String googleBooksId;
    private String title;
    private List<String> authors;
    private String description;
    private String coverImageUrl;
    private String isbn10;
    private String isbn13;
    private LocalDateTime publishedDate;
    private List<String> categories;
    private BigDecimal averageRating;
    private Integer ratingsCount;
    private Integer pageCount;
    private String language;
    private String publisher;
    private String infoLink;
    private String previewLink;
    private String purchaseLink;
    private JsonNode rawData;

    /**
     * Vector embedding for semantic similarity searches
     *
     * Features:
     * - Custom RedisVector type wraps float arrays for the embedding
     * - Stored in Redis as JSON
     * - Used with Redis vector similarity functions for recommendations
     */
    private RedisVector embedding;

    private LocalDateTime createdAt;
    private LocalDateTime lastAccessed;
    private Integer accessCount;
    private List<String> cachedRecommendationIds;
    private Map<String, Object> qualifiers;
    private List<Book.EditionInfo> otherEditions;
    private String s3Key;
    private Instant lastUpdated;
    private String slug; // For SEO-friendly URLs

    /**
     * Convert from Book to CachedBook entity
     *
     * @param book Book object to convert
     * @param rawData Raw JSON data from external API
     * @param embedding Vector embedding for similarity search
     * @return New CachedBook entity ready for persistence
     */
    public static CachedBook fromBook(Book book, JsonNode rawData, RedisVector embedding) {
        CachedBook cachedBook = new CachedBook();

        if (book.getId() != null) {
            if (UuidUtil.isUuid(book.getId())) {
                cachedBook.setId(book.getId()); // Preserve if Book.id is already a UUIDv7
                // Attempt to get GoogleBooksID from qualifiers if Book.id was a UUID
                if (book.getQualifiers() != null && book.getQualifiers().containsKey("googleBooksId")) {
                    Object gbIdObj = book.getQualifiers().get("googleBooksId");
                    if (gbIdObj instanceof String) {
                        cachedBook.setGoogleBooksId((String) gbIdObj);
                    }
                }
            } else {
                // Book.id is an external ID (e.g., Google Books ID)
                // CachedBook.id will be set by RedisCachedBookRepository.save() if null/non-UUID here.
                cachedBook.setGoogleBooksId(book.getId());
            }
        }

        cachedBook.setTitle(book.getTitle());
        cachedBook.setAuthors(book.getAuthors() != null ? new ArrayList<>(book.getAuthors()) : new ArrayList<>());
        cachedBook.setDescription(book.getDescription());
        cachedBook.setCoverImageUrl(book.getCoverImageUrl());
        cachedBook.setIsbn10(book.getIsbn10());
        cachedBook.setIsbn13(book.getIsbn13());
        cachedBook.setCategories(book.getCategories() != null ? new ArrayList<>(book.getCategories()) : null);
        cachedBook.setAverageRating(book.getAverageRating() != null ? BigDecimal.valueOf(book.getAverageRating()) : null);
        cachedBook.setRatingsCount(book.getRatingsCount());
        cachedBook.setPageCount(book.getPageCount());
        cachedBook.setLanguage(book.getLanguage());
        cachedBook.setPublisher(book.getPublisher());
        cachedBook.setInfoLink(book.getInfoLink());
        cachedBook.setPreviewLink(book.getPreviewLink());
        cachedBook.setPurchaseLink(book.getPurchaseLink());
        cachedBook.setRawData(rawData);
        cachedBook.setEmbedding(embedding);
        
        LocalDateTime now = LocalDateTime.now();
        cachedBook.setCreatedAt(now);
        cachedBook.setLastAccessed(now);
        cachedBook.setAccessCount(1);

        // Populate new fields
        cachedBook.setCachedRecommendationIds(book.getCachedRecommendationIds() != null ? new ArrayList<>(book.getCachedRecommendationIds()) : new ArrayList<>());
        cachedBook.setQualifiers(book.getQualifiers() != null ? new HashMap<>(book.getQualifiers()) : new HashMap<>());
        cachedBook.setOtherEditions(book.getOtherEditions() != null ? new ArrayList<>(book.getOtherEditions()) : new ArrayList<>());
        
        return cachedBook;
    }

    /**
     * Convert CachedBook entity to Book object
     *
     * @return Book object with data from this cached entity
     */
    public Book toBook() {
        Book book = new Book();
        book.setId(this.id); // Book.id should be the canonical UUIDv7 ID from CachedBook
        book.setTitle(this.title);
        book.setAuthors(this.authors != null ? new ArrayList<>(this.authors) : new ArrayList<>());
        book.setDescription(this.description);
        book.setCoverImageUrl(this.coverImageUrl);
        book.setIsbn10(this.isbn10);
        book.setIsbn13(this.isbn13);
        book.setCategories(this.categories);
        book.setAverageRating(this.averageRating != null ? this.averageRating.doubleValue() : null);
        book.setRatingsCount(this.ratingsCount);
        book.setPageCount(this.pageCount);
        book.setLanguage(this.language);
        book.setPublisher(this.publisher);
        book.setInfoLink(this.infoLink);
        book.setPreviewLink(this.previewLink);
        book.setPurchaseLink(this.purchaseLink);

        // Populate new fields in Book from CachedBook
        book.setCachedRecommendationIds(this.cachedRecommendationIds != null ? new ArrayList<>(this.cachedRecommendationIds) : new ArrayList<>());
        
        Map<String, Object> bookQualifiers = this.qualifiers != null ? new HashMap<>(this.qualifiers) : new HashMap<>();
        if (this.googleBooksId != null && !UuidUtil.isUuid(this.googleBooksId)) { // Store original GBID if it's not a UUID itself
            bookQualifiers.put("googleBooksId", this.googleBooksId);
        }
        book.setQualifiers(bookQualifiers);
        
        book.setOtherEditions(this.otherEditions != null ? new ArrayList<>(this.otherEditions) : new ArrayList<>());
        
        // It's good practice to also set the rawJsonResponse if it's available in CachedBook,
        // though Book.rawJsonResponse is transient, it can be useful if the Book object is immediately processed.
        if (this.rawData != null) {
            book.setRawJsonResponse(this.rawData.toString());
        }
        
        return book;
    }
}
