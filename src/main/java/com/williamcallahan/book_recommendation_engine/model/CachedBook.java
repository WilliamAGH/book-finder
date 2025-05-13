package com.williamcallahan.book_recommendation_engine.model;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

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

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
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

    @Column(columnDefinition = "vector(384)")
    private float[] embedding;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "raw_data", columnDefinition = "jsonb", nullable = false)
    private JsonNode rawData;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "last_accessed", nullable = false)
    private LocalDateTime lastAccessed;

    @Column(name = "access_count", nullable = false)
    private Integer accessCount;

    // Convert from Book to CachedBook
    public static CachedBook fromBook(Book book, JsonNode rawData, float[] embedding) {
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

    // Convert to Book
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
