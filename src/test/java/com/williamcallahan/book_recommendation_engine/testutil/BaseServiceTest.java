package com.williamcallahan.book_recommendation_engine.testutil;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.UUID;

/**
 * Base class for service tests with common setup and utilities.
 * Reduces duplication across service test classes.
 */
@ExtendWith(MockitoExtension.class)
public abstract class BaseServiceTest {

    protected static final UUID TEST_BOOK_ID = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    protected static final String TEST_ISBN13 = "9780136083252";
    protected static final String TEST_ISBN10 = "0136083250";
    protected static final String TEST_TITLE = "Test Book Title";

    /**
     * Create a basic test book with common fields.
     */
    protected Book createTestBook() {
        Book book = new Book();
        book.setId(TEST_BOOK_ID.toString());
        book.setIsbn13(TEST_ISBN13);
        book.setIsbn10(TEST_ISBN10);
        book.setTitle(TEST_TITLE);
        book.setPublisher("Test Publisher");
        book.setPublishedDate(new java.util.Date());
        return book;
    }

    /**
     * Create a test book with a specific ID.
     */
    protected Book createTestBook(String id) {
        Book book = createTestBook();
        book.setId(id);
        return book;
    }

    /**
     * Create a test book with specific ISBN values.
     */
    protected Book createTestBook(String isbn13, String isbn10) {
        Book book = createTestBook();
        book.setIsbn13(isbn13);
        book.setIsbn10(isbn10);
        return book;
    }

    /**
     * Create a test book with a specific title.
     */
    protected Book createTestBookWithTitle(String title) {
        Book book = createTestBook();
        book.setTitle(title);
        return book;
    }
}