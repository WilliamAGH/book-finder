/**
 * Persistent entity for storing book data in database
 * - Extends book model with caching metadata
 * - Tracks access patterns and embedding vectors for similarity search
 * - Contains conversion methods between Book and CachedBook
 * - Used for reducing API calls and improving performance
 * 
 * @author William Callahan
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
    @Column(name = "category", columnDefinition = "text") // Keep for explicit text array if needed, or remove if default is fine
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

    /**
     * Convert from Book to CachedBook entity
     * - Creates a persistent entity from transient book object
     * - Adds metadata for caching like timestamps and access count
     * - Stores vector embedding for similarity search
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
        cachedBook.setAuthors(book.getAuthors());
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
        
        return cachedBook;
    }

    /**
     * Convert CachedBook entity to Book object
     * - Creates a transient Book object from persistent entity
     * - Transfers all book metadata to Book model
     * - Used when returning cached books to application layer
     * 
     * @return Book object with data from this cached entity
     */
    public Book toBook() {
        Book book = new Book();
        book.setId(this.googleBooksId);
        book.setTitle(this.title);
        book.setAuthors(this.authors);
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
        
        return book;
    }
}
