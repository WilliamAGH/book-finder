/**
 * Persistent entity for storing book data in database
 *
 * @author William Callahan
 *
 * Features:
 * - Extends book model with caching metadata
 * - Tracks access patterns and embedding vectors for similarity search
 * - Contains conversion methods between Book and CachedBook
 * - Used for reducing API calls and improving performance
 */

package com.williamcallahan.book_recommendation_engine.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.williamcallahan.book_recommendation_engine.types.PgVector;
import com.williamcallahan.book_recommendation_engine.types.PgVectorConverter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.hibernate.annotations.CreationTimestamp;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;
import java.util.Map; // For qualifiers
import java.util.HashMap; // For qualifiers initialization
@Entity
@Table(name = "cached_books")
@Data
@NoArgsConstructor
public class CachedBook {

    @Id
    private String id;

    @Column(name = "google_books_id", nullable = false)
    private String googleBooksId;

    @Column(nullable = false, length = 1000)
    private String title;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private List<String> authors;

    @Column(columnDefinition = "text")
    private String description;

    @Column(name = "cover_image_url")
    private String coverImageUrl;

    @Column(length = 20)
    private String isbn10;

    @Column(length = 20)
    private String isbn13;

    @Column(name = "published_date")
    private LocalDateTime publishedDate;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "cached_book_categories", joinColumns = @JoinColumn(name = "book_id"))
    @Column(name = "category", length = 255) // plain TEXT is fine; columnDefinition not needed
    private List<String> categories;

    @Column(name = "average_rating")
    private BigDecimal averageRating;

    @Column(name = "ratings_count")
    private Integer ratingsCount;

    @Column(name = "page_count")
    private Integer pageCount;

    @Column(length = 10)
    private String language;

    @Column(length = 255)
    private String publisher;

    @Column(name = "info_link")
    private String infoLink;

    @Column(name = "preview_link")
    private String previewLink;

    @Column(name = "purchase_link")
    private String purchaseLink;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private JsonNode rawData;

    /**
     * Vector embedding for semantic similarity searches
     *
     * Features:
     * - Custom PgVector type wraps float arrays for the embedding
     * - Stored in PostgreSQL vector column in production
     * - Stored as TEXT in H2 for testing compatibility
     * - Used with vector_similarity functions for recommendations
     */
    @Convert(converter = PgVectorConverter.class)
    private PgVector embedding;

    @CreationTimestamp
    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "last_accessed", nullable = false)
    private LocalDateTime lastAccessed;

    @Column(name = "access_count", nullable = false)
    private Integer accessCount;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb", name = "cached_recommendation_ids")
    private List<String> cachedRecommendationIds;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private Map<String, Object> qualifiers;

    // For simplicity, storing EditionInfo as JSONB. Requires EditionInfo to be Jackson-compatible.
    // If complex queries on editions are needed, a separate @OneToMany entity might be better.
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "other_editions", columnDefinition = "jsonb")
    private List<Book.EditionInfo> otherEditions;


    /**
     * Convert from Book to CachedBook entity
     *
     * @param book Book object to convert
     * @param rawData Raw JSON data from external API
     * @param embedding Vector embedding for similarity search
     * @return New CachedBook entity ready for persistence
     */
    public static CachedBook fromBook(Book book, JsonNode rawData, PgVector embedding) {
        CachedBook cachedBook = new CachedBook();
        cachedBook.setId(book.getId());
        cachedBook.setGoogleBooksId(book.getId());
        cachedBook.setTitle(book.getTitle());
        // Ensure we have a non-null authors list, even if the book's authors are null
        cachedBook.setAuthors(book.getAuthors() != null ? book.getAuthors() : new ArrayList<>());
        cachedBook.setDescription(book.getDescription());
        cachedBook.setCoverImageUrl(book.getCoverImageUrl());
        cachedBook.setIsbn10(book.getIsbn10());
        cachedBook.setIsbn13(book.getIsbn13());
        cachedBook.setCategories(book.getCategories());
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
        book.setId(this.googleBooksId);
        book.setTitle(this.title);
        book.setAuthors(this.authors != null ? this.authors : new ArrayList<>());
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
        book.setQualifiers(this.qualifiers != null ? new HashMap<>(this.qualifiers) : new HashMap<>());
        book.setOtherEditions(this.otherEditions != null ? new ArrayList<>(this.otherEditions) : new ArrayList<>());
        
        // It's good practice to also set the rawJsonResponse if it's available in CachedBook,
        // though Book.rawJsonResponse is transient, it can be useful if the Book object is immediately processed.
        if (this.rawData != null) {
            book.setRawJsonResponse(this.rawData.toString());
        }
        
        return book;
    }
}
