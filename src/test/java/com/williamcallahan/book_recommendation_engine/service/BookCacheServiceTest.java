package com.williamcallahan.book_recommendation_engine.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookCacheService;

@ExtendWith(MockitoExtension.class)
class BookCacheServiceTest {

    @Mock
    private Cache cache;

    @Mock
    private CacheManager cacheManager;

    @InjectMocks
    private BookCacheService bookCacheService;

    @BeforeEach
    void setUp() {
        when(cacheManager.getCache("books")).thenReturn(cache);
    }

    @Test
    @DisplayName("getCachedBook returns book when present in cache")
    void getCachedBook_ShouldReturnBook_WhenPresent() {
        // Given
        String bookId = "123";
        Book expected = new Book(bookId, "Effective Java", "Joshua Bloch");
        when(cache.get(bookId, Book.class)).thenReturn(expected);

        // When
        Optional<Book> actual = bookCacheService.getCachedBook(bookId);

        // Then
        assertTrue(actual.isPresent(), "Expected Optional to be present");
        assertEquals(expected, actual.get(), "Expected returned Book to match cached value");
        verify(cache).get(bookId, Book.class);
    }

    @Test
    @DisplayName("getCachedBook returns empty Optional when cache miss")
    void getCachedBook_ShouldReturnEmpty_WhenAbsent() {
        // Given
        String bookId = "999";
        when(cache.get(bookId, Book.class)).thenReturn(null);

        // When
        Optional<Book> actual = bookCacheService.getCachedBook(bookId);

        // Then
        assertFalse(actual.isPresent(), "Expected Optional to be empty on cache miss");
        verify(cache).get(bookId, Book.class);
    }

    @Test
    @DisplayName("getCachedBook throws IllegalArgumentException for null id")
    void getCachedBook_ShouldThrowException_WhenIdIsNull() {
        assertThrows(IllegalArgumentException.class, () -> bookCacheService.getCachedBook(null));
    }

    @Test
    @DisplayName("getCachedBook throws IllegalArgumentException for empty id")
    void getCachedBook_ShouldThrowException_WhenIdIsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> bookCacheService.getCachedBook(""));
    }

    @Test
    @DisplayName("cacheBook puts the Book into cache")
    void cacheBook_ShouldPutBookInCache() {
        // Given
        Book book = new Book("456", "Clean Code", "Robert C. Martin");

        // When
        bookCacheService.cacheBook(book);

        // Then
        verify(cache).put(book.getId(), book);
    }

    @Test
    @DisplayName("cacheBook throws IllegalArgumentException for null Book")
    void cacheBook_ShouldThrowException_WhenBookIsNull() {
        assertThrows(IllegalArgumentException.class, () -> bookCacheService.cacheBook(null));
    }

    @Test
    @DisplayName("evictBook evicts the Book from cache")
    void evictBook_ShouldEvictBook_WhenIdProvided() {
        // Given
        String bookId = "456";

        // When
        bookCacheService.evictBook(bookId);

        // Then
        verify(cache).evict(bookId);
    }

    @Test
    @DisplayName("evictBook throws IllegalArgumentException for null id")
    void evictBook_ShouldThrowException_WhenIdIsNull() {
        assertThrows(IllegalArgumentException.class, () -> bookCacheService.evictBook(null));
    }

    @Test
    @DisplayName("evictBook throws IllegalArgumentException for empty id")
    void evictBook_ShouldThrowException_WhenIdIsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> bookCacheService.evictBook(""));
    }

    @Test
    @DisplayName("clearAll clears the entire cache")
    void clearAll_ShouldClearCache() {
        // When
        bookCacheService.clearAll();

        // Then
        verify(cache).clear();
    }
}