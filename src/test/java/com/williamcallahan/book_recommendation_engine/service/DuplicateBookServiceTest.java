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
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.eq; // Added import for eq
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook; // Added import
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository; // Changed from BookRepository

/**
 * Test class for DuplicateBookService
 */
@ExtendWith(MockitoExtension.class)
public class DuplicateBookServiceTest {

    /**
     * Mock repository for database operations
     */
    @Mock
    private CachedBookRepository cachedBookRepository; // Changed from BookRepository

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
        book.setCoverImageUrl("http://example.com/cover.jpg");
        // Add other necessary fields if any
        return book;
    }
    
    /**
     * Helper method to create CachedBook instances for testing
     * 
     * @param id Database ID for CachedBook
     * @param title Book title
     * @param author Book author
     * @param googleBooksId Google Books API identifier
     * @return Populated CachedBook instance
     */
    private CachedBook createCachedBook(String id, String title, String author, String googleBooksId) {
        CachedBook cachedBook = new CachedBook();
        cachedBook.setId(id); // Internal DB ID
        cachedBook.setGoogleBooksId(googleBooksId);
        cachedBook.setTitle(title);
        cachedBook.setAuthors(author != null ? List.of(author) : Collections.emptyList());
        // Add other necessary fields
        return cachedBook;
    }

    /**
     * Tests finding potential duplicates when no duplicates exist
     */
    @Test
    void findPotentialDuplicates_noDuplicates_returnsEmptyList() {
        Book book = createBook("book1", "Unique Title", "Author U");
        when(cachedBookRepository.findByTitleIgnoreCaseAndIdNot(eq("Unique Title"), eq("book1"))).thenReturn(Collections.emptyList());

        List<CachedBook> duplicates = duplicateBookService.findPotentialDuplicates(book, "book1");
        assertTrue(duplicates.isEmpty());
    }

    /**
     * Tests finding potential duplicates when one duplicate exists
     */
    @Test
    void findPotentialDuplicates_oneDuplicate_returnsListWithOneBook() {
        Book bookToSearchFor = createBook("book1", "Duplicate Title", "Author D");
        CachedBook duplicateInDb = createCachedBook("dbId2", "Duplicate Title", "Author D", "googleId2");

        when(cachedBookRepository.findByTitleIgnoreCaseAndIdNot(eq("Duplicate Title"), eq("book1")))
            .thenReturn(List.of(duplicateInDb));

        List<CachedBook> duplicates = duplicateBookService.findPotentialDuplicates(bookToSearchFor, "book1");
        assertEquals(1, duplicates.size());
        assertEquals("dbId2", duplicates.get(0).getId());
    }
    
    /**
     * Tests finding potential duplicates with same title but different authors
     */
    @Test
    void findPotentialDuplicates_differentAuthors_returnsEmptyList() {
        Book bookToSearchFor = createBook("book1", "Same Title Different Author", "Author One");
        CachedBook potentialDuplicateInDb = createCachedBook("dbId2", "Same Title Different Author", "Author Two", "googleId2");

        when(cachedBookRepository.findByTitleIgnoreCaseAndIdNot(eq("Same Title Different Author"), eq("book1")))
            .thenReturn(List.of(potentialDuplicateInDb));

        List<CachedBook> duplicates = duplicateBookService.findPotentialDuplicates(bookToSearchFor, "book1");
        assertTrue(duplicates.isEmpty());
    }

    /**
     * Tests finding multiple duplicate books
     */
    @Test
    void findPotentialDuplicates_multipleDuplicates_returnsAllMatching() {
        Book bookToSearchFor = createBook("book1", "Popular Title", "Popular Author");
        CachedBook dup1InDb = createCachedBook("dbId2", "Popular Title", "Popular Author", "googleId2");
        CachedBook dup2InDb = createCachedBook("dbId3", "Popular Title", "Popular Author", "googleId3");
        
        when(cachedBookRepository.findByTitleIgnoreCaseAndIdNot(eq("Popular Title"), eq("book1")))
            .thenReturn(Arrays.asList(dup1InDb, dup2InDb));

        List<CachedBook> duplicates = duplicateBookService.findPotentialDuplicates(bookToSearchFor, "book1");
        assertEquals(2, duplicates.size());
    }

    /**
     * Tests case-insensitive matching for title and author
     */
    @Test
    void findPotentialDuplicates_caseInsensitiveTitleAndAuthorMatch() {
        Book bookToSearchFor = createBook("book1", "mixedcase title", "mixedcase author");
        CachedBook duplicateInDb = createCachedBook("dbId2", "MIXEDCASE TITLE", "MIXEDCASE AUTHOR", "googleId2");
        duplicateInDb.setAuthors(List.of("MIXEDCASE AUTHOR")); // Ensure authors list matches for comparison logic

        when(cachedBookRepository.findByTitleIgnoreCaseAndIdNot(eq("mixedcase title"), eq("book1")))
            .thenReturn(List.of(duplicateInDb));
        
        List<CachedBook> duplicates = duplicateBookService.findPotentialDuplicates(bookToSearchFor, "book1");
        assertEquals(1, duplicates.size());
        assertEquals("dbId2", duplicates.get(0).getId());
    }

    /**
     * Tests handling null book input
     */
    @Test
    void findPotentialDuplicates_nullBook_returnsEmptyList() {
        List<CachedBook> duplicates = duplicateBookService.findPotentialDuplicates(null, "someId");
        assertTrue(duplicates.isEmpty());
    }

    /**
     * Tests handling book with null title
     */
    @Test
    void findPotentialDuplicates_bookWithNullTitle_returnsEmptyList() {
        Book bookWithNullTitle = createBook("book1", null, "Author");
        List<CachedBook> duplicates = duplicateBookService.findPotentialDuplicates(bookWithNullTitle, "book1");
        assertTrue(duplicates.isEmpty());
    }

    /**
     * Tests handling book with null authors list
     */
    @Test
    void findPotentialDuplicates_bookWithNullAuthors_returnsEmptyList() {
        Book bookWithNullAuthors = createBook("book1", "A Title", null);
        bookWithNullAuthors.setAuthors(null); // Explicitly set to null
        List<CachedBook> duplicates = duplicateBookService.findPotentialDuplicates(bookWithNullAuthors, "book1");
        assertTrue(duplicates.isEmpty());
    }

    /**
     * Tests handling book with empty authors list
     */
    @Test
    void findPotentialDuplicates_bookWithEmptyAuthors_returnsEmptyList() {
        Book bookWithEmptyAuthors = createBook("book1", "A Title", null);
        bookWithEmptyAuthors.setAuthors(Collections.emptyList()); // Explicitly set to empty list
        List<CachedBook> duplicates = duplicateBookService.findPotentialDuplicates(bookWithEmptyAuthors, "book1");
        assertTrue(duplicates.isEmpty());
    }
}
