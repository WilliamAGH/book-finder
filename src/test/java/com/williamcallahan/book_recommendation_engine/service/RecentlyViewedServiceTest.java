package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

class RecentlyViewedServiceTest {

    private BookSearchService bookSearchService;
    private DuplicateBookService duplicateBookService;
    private BookDataOrchestrator bookDataOrchestrator;
    private BookQueryRepository bookQueryRepository;
    private RecentBookViewRepository recentBookViewRepository;
    private RecentlyViewedService recentlyViewedService;

    @BeforeEach
    void setUp() {
        bookSearchService = mock(BookSearchService.class);
        duplicateBookService = mock(DuplicateBookService.class);
        bookDataOrchestrator = mock(BookDataOrchestrator.class);
        bookQueryRepository = mock(BookQueryRepository.class);
        recentBookViewRepository = mock(RecentBookViewRepository.class);

        when(recentBookViewRepository.isEnabled()).thenReturn(false);
        when(duplicateBookService.findPrimaryCanonicalBook(any())).thenReturn(Optional.empty());

        recentlyViewedService = createService();
    }

    private RecentlyViewedService createService() {
        return new RecentlyViewedService(bookSearchService, duplicateBookService, bookDataOrchestrator, bookQueryRepository, recentBookViewRepository);
    }

    @Test
    void getRecentlyViewedBookIds_returnsRepositoryIdsWhenAvailable() {
        Instant now = Instant.parse("2024-01-01T00:00:00Z");
        when(recentBookViewRepository.isEnabled()).thenReturn(true);
        when(recentBookViewRepository.fetchMostRecentViews(3))
            .thenReturn(List.of(
                new RecentBookViewRepository.ViewStats("uuid-1", now, 5L, 9L, 12L),
                new RecentBookViewRepository.ViewStats("uuid-2", now.minusSeconds(60), 4L, 7L, 10L)
            ));

        List<String> ids = recentlyViewedService.getRecentlyViewedBookIds(3);

        assertEquals(List.of("uuid-1", "uuid-2"), ids);
        verify(recentBookViewRepository).fetchMostRecentViews(3);
    }

    @Test
    void getRecentlyViewedBookIds_fallsBackToCacheWhenRepositoryDisabled() {
        when(recentBookViewRepository.isEnabled()).thenReturn(false);

        Book cached = com.williamcallahan.book_recommendation_engine.testutil.BookTestData.aBook()
            .id("uuid-cache")
            .s3ImagePath("https://cdn.example/uuid-cache.jpg")
            .build();
        cached.setSlug("uuid-cache");

        recentlyViewedService.addToRecentlyViewed(cached);

        List<String> ids = recentlyViewedService.getRecentlyViewedBookIds(5);

        assertEquals(List.of("uuid-cache"), ids);
    }

    @Test
    void getRecentlyViewedBookIds_handlesRepositoryErrorsGracefully() {
        when(recentBookViewRepository.isEnabled()).thenReturn(true);
        when(recentBookViewRepository.fetchMostRecentViews(anyInt())).thenThrow(new RuntimeException("boom"));

        List<String> ids = recentlyViewedService.getRecentlyViewedBookIds(4);

        assertTrue(ids.isEmpty());
        verify(recentBookViewRepository).fetchMostRecentViews(4);
    }

    @Test
    void addToRecentlyViewed_recordsViewAndAppliesStats() {
        when(recentBookViewRepository.isEnabled()).thenReturn(true);
        Instant now = Instant.parse("2024-02-02T10:15:30Z");
        when(recentBookViewRepository.fetchStatsForBook("uuid-2"))
            .thenReturn(Optional.of(new RecentBookViewRepository.ViewStats("uuid-2", now, 5L, 12L, 20L)));

Book book = com.williamcallahan.book_recommendation_engine.testutil.BookTestData.aBook()
                .id("uuid-2").publishedDate(Date.from(Instant.parse("2020-01-01T00:00:00Z"))).s3ImagePath("https://cdn.example/uuid-2.jpg").build();
        book.setSlug("slug-uuid-2");

        recentlyViewedService.addToRecentlyViewed(book);

        verify(recentBookViewRepository).recordView(eq("uuid-2"), any(Instant.class), eq("web"));
        verify(recentBookViewRepository).fetchStatsForBook("uuid-2");

        assertNotNull(book.getQualifiers());
        assertEquals(12L, book.getQualifiers().get("recent.views.7d"));
        assertEquals(now, book.getQualifiers().get("recent.views.lastViewedAt"));
    }

}
