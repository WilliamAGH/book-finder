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
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BookCacheWarmingSchedulerTest {

    private RecentlyViewedService recentlyViewedService;
    private BookCacheFacadeService bookCacheFacadeService;
    private GoogleBooksService googleBooksService;
    private BookCacheWarmingScheduler scheduler;

    @BeforeEach
    void setUp() {
        recentlyViewedService = Mockito.mock(RecentlyViewedService.class);
        bookCacheFacadeService = Mockito.mock(BookCacheFacadeService.class);
        googleBooksService = Mockito.mock(GoogleBooksService.class);
        scheduler = new BookCacheWarmingScheduler(bookCacheFacadeService, googleBooksService, recentlyViewedService);
    }

    @Test
    void warmPopularBookCaches_emptyListCompletesSuccessfully() {
        // Given
        when(recentlyViewedService.getRecentlyViewedBooksAsync())
            .thenReturn(CompletableFuture.completedFuture(List.of()));

        // When & Then
        assertDoesNotThrow(() -> scheduler.warmPopularBookCaches(), 
            "Cache warming should complete without exception");
        verify(recentlyViewedService, times(1)).getRecentlyViewedBooksAsync();
        verifyNoInteractions(bookCacheFacadeService);
        verifyNoInteractions(googleBooksService);
    }

}
