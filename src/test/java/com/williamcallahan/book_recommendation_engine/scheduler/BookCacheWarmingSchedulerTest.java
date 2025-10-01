package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.service.ApiRequestMonitor;
import com.williamcallahan.book_recommendation_engine.service.BookIdentifierResolver;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class BookCacheWarmingSchedulerTest {

    private RecentlyViewedService recentlyViewedService;
    private ApplicationContext applicationContext;
    private BookQueryRepository bookQueryRepository;
    private BookIdentifierResolver bookIdentifierResolver;
    private BookCacheWarmingScheduler scheduler;

    @BeforeEach
    void setUp() {
        recentlyViewedService = mock(RecentlyViewedService.class);
        applicationContext = mock(ApplicationContext.class);
        bookQueryRepository = mock(BookQueryRepository.class);
        bookIdentifierResolver = mock(BookIdentifierResolver.class);

        scheduler = new BookCacheWarmingScheduler(recentlyViewedService, applicationContext, bookQueryRepository, bookIdentifierResolver);

        setField("cacheWarmingEnabled", true);
        setField("rateLimit", 60_000); // ~1 request per millisecond
        setField("maxBooksPerRun", 5);

        ApiRequestMonitor apiRequestMonitor = mock(ApiRequestMonitor.class);
        when(apiRequestMonitor.getCurrentHourlyRequests()).thenReturn(0);
        when(applicationContext.getBean(ApiRequestMonitor.class)).thenReturn(apiRequestMonitor);
    }

    @Test
    void warmPopularBookCaches_skipsWhenDisabled() {
        setField("cacheWarmingEnabled", false);

        scheduler.warmPopularBookCaches();

        verifyNoInteractions(recentlyViewedService);
    }

    @Test
    void warmPopularBookCaches_resolvesBooksFromRepository() {
        setField("cacheWarmingEnabled", true);
        setField("maxBooksPerRun", 1);
        setField("rateLimit", 60_000);

        BookDetail detail = new BookDetail(
            "uuid-123",
            "slug-123",
            "Test Title",
            "Test Description",
            "Test Publisher",
            LocalDate.now(),
            "en",
            320,
            List.of("Author"),
            List.of("Fiction"),
            "https://example.test/cover.jpg",
            "https://example.test/thumb.jpg",
            600,
            900,
            Boolean.TRUE,
            "POSTGRES",
            4.5,
            120,
            "1234567890",
            "1234567890123",
            "https://example.test/preview",
            "https://example.test/info",
            java.util.Collections.<String, Object>emptyMap(),
            java.util.Collections.<com.williamcallahan.book_recommendation_engine.dto.EditionSummary>emptyList()
        );

        when(recentlyViewedService.getRecentlyViewedBookIds(anyInt())).thenReturn(List.of("slug-123"));
        when(bookQueryRepository.fetchBookDetailBySlug("slug-123")).thenReturn(Optional.of(detail));

        scheduler.warmPopularBookCaches();

        verify(bookQueryRepository).fetchBookDetailBySlug("slug-123");
    }

    private void setField(String name, Object value) {
        try {
            Field field = BookCacheWarmingScheduler.class.getDeclaredField(name);
            field.setAccessible(true);
            field.set(scheduler, value);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to set field " + name, e);
        }
    }
}
