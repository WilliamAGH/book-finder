/**
 * Test suite for BookCacheService functionality
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.Answers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.springframework.web.reactive.function.client.WebClient;
// import com.williamcallahan.book_recommendation_engine.service.BookCacheService; // Already imported by @InjectMocks
import java.util.List; // For List.of() in helper

@ExtendWith(MockitoExtension.class)
class BookCacheServiceTest {

    @Mock
    private Cache cache;

    @Mock
    private CacheManager cacheManager;

    @Mock
    private GoogleBooksService googleBooksService;

    @Mock
    private ObjectMapper objectMapper;

    @Mock(answer = Answers.RETURNS_SELF)
    private WebClient.Builder webClientBuilder;

    @Mock
    private WebClient webClient; // Mock for the actual WebClient

    @Mock
    private CachedBookRepository cachedBookRepository;

    @Mock
    private DuplicateBookService duplicateBookService;

    @InjectMocks
    private BookCacheService bookCacheService;

    @BeforeEach
    void setUp() {
        // Configure the WebClient.Builder mock
        lenient().when(webClientBuilder.build()).thenReturn(webClient);
    }

    /**
     * Tests that getCachedBook returns book when found in cache
     */
    @Test
    @DisplayName("getCachedBook returns book when present in cache")
    void getCachedBook_ShouldReturnBook_WhenPresent() {
        // Given
        String bookId = "123";
        Book expected = createTestBook(bookId, "Effective Java", "Joshua Bloch");
        when(cacheManager.getCache("books")).thenReturn(cache);
        when(cache.get(bookId, Book.class)).thenReturn(expected);

        // When
        Optional<Book> actual = bookCacheService.getCachedBook(bookId);

        // Then
        assertTrue(actual.isPresent(), "Expected Optional to be present");
        assertEquals(expected, actual.get(), "Expected returned Book to match cached value");
        verify(cache).get(bookId, Book.class);
    }

    /**
     * Tests that getCachedBook returns empty Optional when book not in cache
     */
    @Test
    @DisplayName("getCachedBook returns empty Optional when cache miss")
    void getCachedBook_ShouldReturnEmpty_WhenAbsent() {
        // Given
        String bookId = "999";
        when(cacheManager.getCache("books")).thenReturn(cache);
        when(cache.get(bookId, Book.class)).thenReturn(null);

        // When
        Optional<Book> actual = bookCacheService.getCachedBook(bookId);

        // Then
        assertFalse(actual.isPresent(), "Expected Optional to be empty on cache miss");
        verify(cache).get(bookId, Book.class);
    }

    /**
     * Tests that getCachedBook throws exception when ID is null
     */
    @Test
    @DisplayName("getCachedBook throws IllegalArgumentException for null id")
    void getCachedBook_ShouldThrowException_WhenIdIsNull() {
        assertThrows(IllegalArgumentException.class, () -> bookCacheService.getCachedBook(null));
    }

    /**
     * Tests that getCachedBook throws exception when ID is empty
     */
    @Test
    @DisplayName("getCachedBook throws IllegalArgumentException for empty id")
    void getCachedBook_ShouldThrowException_WhenIdIsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> bookCacheService.getCachedBook(""));
    }

    /**
     * Tests that cacheBook properly adds book to cache
     */
    @Test
    @DisplayName("cacheBook puts the Book into cache")
    void cacheBook_ShouldPutBookInCache() {
        // Given
        Book book = createTestBook("456", "Clean Code", "Robert C. Martin");
        when(cacheManager.getCache("books")).thenReturn(cache);

        // When
        bookCacheService.cacheBook(book);

        // Then
        verify(cache).put(book.getId(), book);
    }

    /**
     * Tests that cacheBook throws exception when book is null
     */
    @Test
    @DisplayName("cacheBook throws IllegalArgumentException for null Book")
    void cacheBook_ShouldThrowException_WhenBookIsNull() {
        assertThrows(IllegalArgumentException.class, () -> bookCacheService.cacheBook(null));
    }

    /**
     * Tests that evictBook properly removes book from cache
     */
    @Test
    @DisplayName("evictBook evicts the Book from cache")
    void evictBook_ShouldEvictBook_WhenIdProvided() {
        // Given
        String bookId = "456";
        when(cacheManager.getCache("books")).thenReturn(cache);

        // When
        bookCacheService.evictBook(bookId);

        // Then
        verify(cache).evictIfPresent(bookId); // Changed from evict to evictIfPresent
    }

    /**
     * Tests that evictBook throws exception when ID is null
     */
    @Test
    @DisplayName("evictBook throws IllegalArgumentException for null id")
    void evictBook_ShouldThrowException_WhenIdIsNull() {
        assertThrows(IllegalArgumentException.class, () -> bookCacheService.evictBook(null));
    }

    /**
     * Tests that evictBook throws exception when ID is empty
     */
    @Test
    @DisplayName("evictBook throws IllegalArgumentException for empty id")
    void evictBook_ShouldThrowException_WhenIdIsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> bookCacheService.evictBook(""));
    }

    /**
     * Tests that clearAll properly clears the entire cache
     */
    @Test
    @DisplayName("clearAll clears the entire cache")
    void clearAll_ShouldClearCache() {
        when(cacheManager.getCache("books")).thenReturn(cache);
        // When
        bookCacheService.clearAll();

        // Then
        verify(cache).clear();
    }

    /**
     * Helper method to create test Book instances
     * 
     * @param id Book identifier
     * @param title Book title 
     * @param author Book author
     * @return Populated Book instance for testing
     */
    private Book createTestBook(String id, String title, String author) {
        Book book = new Book();
        book.setId(id);
        book.setTitle(title);
        if (author != null) {
            book.setAuthors(List.of(author));
        } else {
            book.setAuthors(List.of("Unknown Author"));
        }
        book.setDescription("Test description for " + title);
        book.setCoverImageUrl("http://example.com/cover/" + (id != null ? id : "new") + ".jpg");
        book.setImageUrl("http://example.com/image/" + (id != null ? id : "new") + ".jpg");
        return book;
    }
}
