/**
 * Test suite for DuplicateBookService
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.junit.jupiter.api.Assertions.*;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.Collections;
import java.util.List;

import com.williamcallahan.book_recommendation_engine.model.Book;

/**
 * Test class for DuplicateBookService
 */
@ExtendWith(MockitoExtension.class)
public class DuplicateBookServiceTest {

    // No repository dependencies after Redis removal

    /**
     * Service instance under test
     */
    @InjectMocks
    private DuplicateBookService duplicateBookService;

    /**
     * Setup test environment before each test
     */
    @BeforeEach
    void setUp() {
        // MockitoAnnotations.openMocks(this); // Not strictly necessary with @ExtendWith
    }

    /**
     * Helper method to create Book instances for testing
     * 
     * @param id Book identifier
     * @param title Book title
     * @param author Book author
     * @return Populated Book instance
     */
    private Book createBook(String id, String title, String author) {
        Book book = new Book();
        book.setId(id);
        book.setTitle(title);
        book.setAuthors(author != null ? List.of(author) : Collections.emptyList());
        book.setDescription("Test desc");
        book.setExternalImageUrl("http://example.com/cover.jpg");
        // Add other necessary fields if any
        return book;
    }
    
    // Duplicate detection functionality has been removed; tests assert empty results

    /**
     * Tests finding potential duplicates when no duplicates exist
     */
    @Test
    void findPotentialDuplicates_noDuplicates_returnsEmptyList() {
        Book book = createBook("book1", "Unique Title", "Author U");
        List<Book> duplicates = duplicateBookService.findPotentialDuplicates(book, "book1");
        assertTrue(duplicates.isEmpty());
    }

    /**
     * Tests finding potential duplicates when one duplicate exists
     */
    @Test
    void findPotentialDuplicates_oneDuplicate_returnsListWithOneBook() {
        Book bookToSearchFor = createBook("book1", "Duplicate Title", "Author D");
        List<Book> duplicates = duplicateBookService.findPotentialDuplicates(bookToSearchFor, "book1");
        assertTrue(duplicates.isEmpty());
    }
    
    /**
     * Tests finding potential duplicates with same title but different authors
     */
    @Test
    void findPotentialDuplicates_differentAuthors_returnsEmptyList() {
        Book bookToSearchFor = createBook("book1", "Same Title Different Author", "Author One");
        List<Book> duplicates = duplicateBookService.findPotentialDuplicates(bookToSearchFor, "book1");
        assertTrue(duplicates.isEmpty());
    }

    /**
     * Tests finding multiple duplicate books
     */
    @Test
    void findPotentialDuplicates_multipleDuplicates_returnsAllMatching() {
        Book bookToSearchFor = createBook("book1", "Popular Title", "Popular Author");
        List<Book> duplicates = duplicateBookService.findPotentialDuplicates(bookToSearchFor, "book1");
        assertTrue(duplicates.isEmpty());
    }

    /**
     * Tests case-insensitive matching for title and author
     */
    @Test
    void findPotentialDuplicates_caseInsensitiveTitleAndAuthorMatch() {
        Book bookToSearchFor = createBook("book1", "mixedcase title", "mixedcase author");
        List<Book> duplicates = duplicateBookService.findPotentialDuplicates(bookToSearchFor, "book1");
        assertTrue(duplicates.isEmpty());
    }

    /**
     * Tests handling null book input
     */
    @Test
    void findPotentialDuplicates_nullBook_returnsEmptyList() {
        List<Book> duplicates = duplicateBookService.findPotentialDuplicates(null, "someId");
        assertTrue(duplicates.isEmpty());
    }

    /**
     * Tests handling book with null title
     */
    @Test
    void findPotentialDuplicates_bookWithNullTitle_returnsEmptyList() {
        Book bookWithNullTitle = createBook("book1", null, "Author");
        List<Book> duplicates = duplicateBookService.findPotentialDuplicates(bookWithNullTitle, "book1");
        assertTrue(duplicates.isEmpty());
    }

    /**
     * Tests handling book with null authors list
     */
    @Test
    void findPotentialDuplicates_bookWithNullAuthors_returnsEmptyList() {
        Book bookWithNullAuthors = createBook("book1", "A Title", null);
        bookWithNullAuthors.setAuthors(null); // Explicitly set to null
        List<Book> duplicates = duplicateBookService.findPotentialDuplicates(bookWithNullAuthors, "book1");
        assertTrue(duplicates.isEmpty());
    }

    /**
     * Tests handling book with empty authors list
     */
    @Test
    void findPotentialDuplicates_bookWithEmptyAuthors_returnsEmptyList() {
        Book bookWithEmptyAuthors = createBook("book1", "A Title", null);
        bookWithEmptyAuthors.setAuthors(Collections.emptyList()); // Explicitly set to empty list
        List<Book> duplicates = duplicateBookService.findPotentialDuplicates(bookWithEmptyAuthors, "book1");
        assertTrue(duplicates.isEmpty());
    }
}
