package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.ArgumentMatchers.anyInt;

import java.lang.reflect.Field;

class BookCacheWarmingSchedulerTest {

    private GoogleBooksService googleBooksService;
    private BookDataOrchestrator bookDataOrchestrator;
    private RecentlyViewedService recentlyViewedService;
    private ApplicationContext applicationContext;
    private BookCacheWarmingScheduler scheduler;

    @BeforeEach
    void setUp() {
        googleBooksService = mock(GoogleBooksService.class);
        bookDataOrchestrator = mock(BookDataOrchestrator.class);
        recentlyViewedService = mock(RecentlyViewedService.class);
        applicationContext = mock(ApplicationContext.class);

        scheduler = new BookCacheWarmingScheduler(googleBooksService, recentlyViewedService, applicationContext, bookDataOrchestrator);

        setField("cacheWarmingEnabled", true);
        setField("rateLimit", 60_000); // effectively immediate scheduling
        setField("maxBooksPerRun", 5);
    }

    @Test
    void warmPopularBookCaches_prefersOrchestratorDataBeforeGoogle() {
        Book orchestrated = new Book();
        orchestrated.setId("pg-tier");

        Book fallback = new Book();
        fallback.setId("google-tier");

        given(recentlyViewedService.getRecentlyViewedBookIds(anyInt())).willReturn(List.of("pg-tier"));
        given(bookDataOrchestrator.getBookByIdTiered("pg-tier")).willReturn(Mono.just(orchestrated));
        given(googleBooksService.getBookById("pg-tier")).willReturn(CompletableFuture.completedFuture(fallback));

        scheduler.warmPopularBookCaches();

        verify(bookDataOrchestrator).getBookByIdTiered("pg-tier");
        verify(googleBooksService, never()).getBookById("pg-tier");
    }

    @Test
    void warmPopularBookCaches_fallsBackToGoogleWhenOrchestratorEmpty() {
        Book fallback = new Book();
        fallback.setId("google-tier");

        given(recentlyViewedService.getRecentlyViewedBookIds(anyInt())).willReturn(List.of("google-tier"));
        given(bookDataOrchestrator.getBookByIdTiered("google-tier")).willReturn(Mono.empty());
        given(googleBooksService.getBookById("google-tier")).willReturn(CompletableFuture.completedFuture(fallback));

        scheduler.warmPopularBookCaches();

        verify(bookDataOrchestrator).getBookByIdTiered("google-tier");
        verify(googleBooksService).getBookById("google-tier");
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
