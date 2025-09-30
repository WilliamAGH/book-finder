package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for BookAggregateToBookConverter.
 * Ensures proper conversion from BookAggregate (normalized DTO) to Book (app model).
 */
class BookAggregateToBookConverterTest {
    
    @Test
    void shouldConvertCompleteAggregate() {
        BookAggregate aggregate = createCompleteAggregate();
        
        Book book = BookAggregateToBookConverter.convert(aggregate);
        
        assertNotNull(book);
        assertEquals("Test Book Title", book.getTitle());
        assertEquals("Test description", book.getDescription());
        assertEquals("9781234567890", book.getIsbn13());
        assertEquals("1234567890", book.getIsbn10());
        assertEquals("Test Publisher", book.getPublisher());
        assertEquals("en", book.getLanguage());
        assertEquals(300, book.getPageCount());
    }
    
    @Test
    void shouldConvertAuthors() {
        BookAggregate aggregate = createCompleteAggregate();
        
        Book book = BookAggregateToBookConverter.convert(aggregate);
        
        assertNotNull(book.getAuthors());
        assertEquals(2, book.getAuthors().size());
        assertTrue(book.getAuthors().contains("Author One"));
        assertTrue(book.getAuthors().contains("Author Two"));
    }
    
    @Test
    void shouldConvertCategories() {
        BookAggregate aggregate = createCompleteAggregate();
        
        Book book = BookAggregateToBookConverter.convert(aggregate);
        
        assertNotNull(book.getCategories());
        assertEquals(2, book.getCategories().size());
        assertTrue(book.getCategories().contains("Fiction"));
        assertTrue(book.getCategories().contains("Mystery"));
    }
    
    @Test
    void shouldConvertPublishedDate() {
        BookAggregate aggregate = createCompleteAggregate();
        
        Book book = BookAggregateToBookConverter.convert(aggregate);
        
        assertNotNull(book.getPublishedDate());
        // Date conversion from LocalDate to java.sql.Date
    }
    
    @Test
    void shouldConvertExternalIdentifiers() {
        BookAggregate aggregate = createCompleteAggregate();
        
        Book book = BookAggregateToBookConverter.convert(aggregate);
        
        assertEquals("test123", book.getId());
        assertNotNull(book.getInfoLink());
        assertNotNull(book.getPreviewLink());
        assertNotNull(book.getWebReaderLink());
        assertEquals(4.5, book.getAverageRating());
        assertEquals(100, book.getRatingsCount());
        assertTrue(book.getPdfAvailable());
        assertTrue(book.getEpubAvailable());
    }
    
    @Test
    void shouldSelectBestImageUrl() {
        BookAggregate.ExternalIdentifiers ids = BookAggregate.ExternalIdentifiers.builder()
            .source("GOOGLE_BOOKS")
            .externalId("test123")
            .imageLinks(Map.of(
                "thumbnail", "https://example.com/thumb.jpg",
                "small", "https://example.com/small.jpg",
                "large", "https://example.com/large.jpg"
            ))
            .build();
        
        BookAggregate aggregate = BookAggregate.builder()
            .title("Test")
            .identifiers(ids)
            .build();
        
        Book book = BookAggregateToBookConverter.convert(aggregate);
        
        assertNotNull(book.getExternalImageUrl());
        // Should select "large" over "small" and "thumbnail"
        assertEquals("https://example.com/large.jpg", book.getExternalImageUrl());
    }
    
    @Test
    void shouldHandleNullAggregate() {
        Book book = BookAggregateToBookConverter.convert(null);
        
        assertNull(book);
    }
    
    @Test
    void shouldHandleMinimalAggregate() {
        BookAggregate aggregate = BookAggregate.builder()
            .title("Minimal Book")
            .build();
        
        Book book = BookAggregateToBookConverter.convert(aggregate);
        
        assertNotNull(book);
        assertEquals("Minimal Book", book.getTitle());
        assertNull(book.getIsbn13());
        assertNull(book.getPublisher());
    }
    
    @Test
    void shouldHandleNullFields() {
        BookAggregate aggregate = BookAggregate.builder()
            .title("Test")
            .subtitle(null)
            .description(null)
            .authors(null)
            .categories(null)
            .identifiers(null)
            .build();
        
        Book book = BookAggregateToBookConverter.convert(aggregate);
        
        assertNotNull(book);
        assertEquals("Test", book.getTitle());
        assertNull(book.getDescription());
    }
    
    private BookAggregate createCompleteAggregate() {
        BookAggregate.ExternalIdentifiers ids = BookAggregate.ExternalIdentifiers.builder()
            .source("GOOGLE_BOOKS")
            .externalId("test123")
            .infoLink("https://example.com/info")
            .previewLink("https://example.com/preview")
            .webReaderLink("https://example.com/reader")
            .averageRating(4.5)
            .ratingsCount(100)
            .pdfAvailable(true)
            .epubAvailable(true)
            .listPrice(19.99)
            .currencyCode("USD")
            .imageLinks(Map.of(
                "thumbnail", "https://example.com/thumb.jpg",
                "large", "https://example.com/large.jpg"
            ))
            .build();
        
        return BookAggregate.builder()
            .title("Test Book Title")
            .subtitle("Test Subtitle")
            .description("Test description")
            .isbn13("9781234567890")
            .isbn10("1234567890")
            .publishedDate(LocalDate.of(2023, 1, 15))
            .language("en")
            .publisher("Test Publisher")
            .pageCount(300)
            .authors(List.of("Author One", "Author Two"))
            .categories(List.of("Fiction", "Mystery"))
            .identifiers(ids)
            .slugBase("test-book-title-author-one")
            .build();
    }
}
