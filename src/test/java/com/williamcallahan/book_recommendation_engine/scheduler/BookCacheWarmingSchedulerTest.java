/**
 * Unit tests for BookCacheWarmingScheduler functionality
 *
 * @author William Callahan
 *
 * Features:
 * - Tests cache warming operations for popular books
 * - Validates interaction with recently viewed service
 * - Verifies proper completion of async cache warming tasks
 * - Mocks all external dependencies for isolated testing
 */

package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.service.BookCacheFacadeService;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BookCacheWarmingSchedulerTest {

    private RecentlyViewedService recentlyViewedService;
    private BookCacheFacadeService bookCacheFacadeService;
    private BookCacheWarmingScheduler scheduler;

    @BeforeEach
    void setUp() {
        recentlyViewedService = Mockito.mock(RecentlyViewedService.class);
        bookCacheFacadeService = Mockito.mock(BookCacheFacadeService.class);
        scheduler = new BookCacheWarmingScheduler(bookCacheFacadeService, recentlyViewedService);
    }

    @Test
    void warmPopularBookCaches_emptyListCompletesSuccessfully() {
        // Given
        ReflectionTestUtils.setField(scheduler, "cacheWarmingEnabled", true); // Ensure scheduler is enabled
        // Configure rate limit and max books for test iterations
        ReflectionTestUtils.setField(scheduler, "rateLimit", 10);
        ReflectionTestUtils.setField(scheduler, "maxBooksPerRun", 10);
        when(recentlyViewedService.getRecentlyViewedBooksAsync())
            .thenReturn(CompletableFuture.completedFuture(List.of()));

        // When & Then
        assertDoesNotThrow(() -> scheduler.warmPopularBookCaches(), 
            "Cache warming should complete without exception");
        verify(recentlyViewedService, times(1)).getRecentlyViewedBooksAsync();
        verifyNoInteractions(bookCacheFacadeService);
    }

    @Test
    void warmPopularBookCaches_withBooksCallsCacheService() {
        // Given
        ReflectionTestUtils.setField(scheduler, "cacheWarmingEnabled", true); // Ensure scheduler is enabled
        // Configure rate limit and max books for test iterations
        ReflectionTestUtils.setField(scheduler, "rateLimit", 10);
        ReflectionTestUtils.setField(scheduler, "maxBooksPerRun", 10);
        Book book1 = new Book(); book1.setId("book1");
        Book book2 = new Book(); book2.setId("book2");
        Book book3 = new Book(); book3.setId("book3");
        List<Book> books = List.of(book1, book2, book3);

        when(recentlyViewedService.getRecentlyViewedBooksAsync())
            .thenReturn(CompletableFuture.completedFuture(books));
        
        // Mock the facade to return a book successfully
        Book mockBook = new Book();
        mockBook.setTitle("Mock Book Title");
        when(bookCacheFacadeService.getBookById(anyString())).thenReturn(CompletableFuture.completedFuture(mockBook));

        // When
        assertDoesNotThrow(() -> scheduler.warmPopularBookCaches());

        // Then
        // Wait a bit for async operations within the scheduler to potentially complete
        try {
            Thread.sleep(1000); // Increased sleep duration
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        verify(recentlyViewedService, times(1)).getRecentlyViewedBooksAsync();
        verify(bookCacheFacadeService, times(1)).getBookById("book1");
        verify(bookCacheFacadeService, times(1)).getBookById("book2");
        verify(bookCacheFacadeService, times(1)).getBookById("book3");
    }

    @Test
    void warmPopularBookCaches_handlesServiceErrors() {
        // Given
        ReflectionTestUtils.setField(scheduler, "cacheWarmingEnabled", true); // Ensure scheduler is enabled
        // Simulate error from recentlyViewedService
        when(recentlyViewedService.getRecentlyViewedBooksAsync())
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Service error from recentlyViewedService")));

        // When & Then
        assertDoesNotThrow(() -> scheduler.warmPopularBookCaches(),
            "Cache warming should handle errors from recentlyViewedService gracefully");
        
        // Wait a bit for async operations
        try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } // Increased sleep duration

        verify(recentlyViewedService, times(1)).getRecentlyViewedBooksAsync();
        verifyNoInteractions(bookCacheFacadeService); 

        // Reset mocks for next scenario
        Mockito.reset(recentlyViewedService, bookCacheFacadeService);
        ReflectionTestUtils.setField(scheduler, "cacheWarmingEnabled", true); // Re-enable for the next part of the test
        ReflectionTestUtils.setField(scheduler, "rateLimit", 10); // Re-set configured fields
        ReflectionTestUtils.setField(scheduler, "maxBooksPerRun", 10); // Re-set configured fields


        // Given
        // Simulate error from bookCacheFacadeService.getBookById
        Book book1 = new Book(); book1.setId("book1");
        List<Book> books = List.of(book1);
        when(recentlyViewedService.getRecentlyViewedBooksAsync())
            .thenReturn(CompletableFuture.completedFuture(books));
        when(bookCacheFacadeService.getBookById(eq("book1")))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Service error from bookCacheFacadeService")));
        
        // When & Then
        assertDoesNotThrow(() -> scheduler.warmPopularBookCaches(),
            "Cache warming should handle errors from bookCacheFacadeService gracefully");

        // Wait a bit for async operations
        try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } // Increased sleep duration

        verify(recentlyViewedService, times(1)).getRecentlyViewedBooksAsync();
        verify(bookCacheFacadeService, times(1)).getBookById("book1");
    }
}
