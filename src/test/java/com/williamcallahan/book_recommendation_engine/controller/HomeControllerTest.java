/**
 * Test suite for HomeController web endpoints
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService;
import com.williamcallahan.book_recommendation_engine.service.image.BookCoverManagementService;
import com.williamcallahan.book_recommendation_engine.service.image.LocalDiskCoverCacheService;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.mockito.Mockito;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.http.MediaType;
import java.util.List;
import java.util.ArrayList;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyString; // For mocking getSimilarBooks
import static org.mockito.ArgumentMatchers.anyInt; // For mocking getSimilarBooks
import static org.mockito.ArgumentMatchers.any; // For mocking any objects
import static org.mockito.ArgumentMatchers.eq; // For mocking specific values
import static org.mockito.ArgumentMatchers.isNull; // For mocking null argument
import static org.mockito.ArgumentMatchers.argThat; // For custom argument matcher
import reactor.core.publisher.Mono; // For mocking reactive service
import com.williamcallahan.book_recommendation_engine.service.NewYorkTimesService;
@WebFluxTest(value = HomeController.class,
    excludeAutoConfiguration = org.springframework.boot.autoconfigure.security.reactive.ReactiveSecurityAutoConfiguration.class)
class HomeControllerTest {
    /**
     * WebTestClient for controller integration testing
     */
    @Autowired
    private WebTestClient webTestClient;
    /**
     * Mock for RecommendationService dependency
     */
    @org.springframework.beans.factory.annotation.Autowired
    @SuppressWarnings("unused")
    private RecommendationService recommendationService;
    
    @org.springframework.beans.factory.annotation.Autowired
    @SuppressWarnings("unused")
    private BookDataOrchestrator bookDataOrchestrator;

    @org.springframework.beans.factory.annotation.Autowired
    private GoogleBooksService googleBooksService;
    
    @org.springframework.beans.factory.annotation.Autowired
    private RecentlyViewedService recentlyViewedService;
    
    @org.springframework.beans.factory.annotation.Autowired
    @SuppressWarnings("unused")
    private BookImageOrchestrationService bookImageOrchestrationService;
    
    @org.springframework.beans.factory.annotation.Autowired
    private BookCoverManagementService bookCoverManagementService;

    @org.springframework.beans.factory.annotation.Autowired
    private LocalDiskCoverCacheService localDiskCoverCacheService;
    
    @org.springframework.beans.factory.annotation.Autowired
    @SuppressWarnings("unused")
    private com.williamcallahan.book_recommendation_engine.service.EnvironmentService environmentService;
    
    @org.springframework.beans.factory.annotation.Autowired
    @SuppressWarnings("unused")
    private com.williamcallahan.book_recommendation_engine.service.DuplicateBookService duplicateBookService;

    @org.springframework.beans.factory.annotation.Autowired
    private NewYorkTimesService newYorkTimesService;

    @TestConfiguration
    static class MocksConfig {
        @Bean RecommendationService recommendationService() { return Mockito.mock(RecommendationService.class); }
        @Bean BookDataOrchestrator bookDataOrchestrator() { return Mockito.mock(BookDataOrchestrator.class); }
        @Bean GoogleBooksService googleBooksService() { return Mockito.mock(GoogleBooksService.class); }
        @Bean RecentlyViewedService recentlyViewedService() { return Mockito.mock(RecentlyViewedService.class); }
        @Bean BookImageOrchestrationService bookImageOrchestrationService() { return Mockito.mock(BookImageOrchestrationService.class); }
        @Bean BookCoverManagementService bookCoverManagementService() { return Mockito.mock(BookCoverManagementService.class); }
        @Bean LocalDiskCoverCacheService localDiskCoverCacheService() { return Mockito.mock(LocalDiskCoverCacheService.class); }
        @Bean com.williamcallahan.book_recommendation_engine.service.EnvironmentService environmentService() { return Mockito.mock(com.williamcallahan.book_recommendation_engine.service.EnvironmentService.class); }
        @Bean com.williamcallahan.book_recommendation_engine.service.DuplicateBookService duplicateBookService() { return Mockito.mock(com.williamcallahan.book_recommendation_engine.service.DuplicateBookService.class); }
        @Bean NewYorkTimesService newYorkTimesService() { return Mockito.mock(NewYorkTimesService.class); }
    }
    
    /**
     * Sets up common test fixtures
     * Configures mock services with default behaviors
     */
    @BeforeEach
    void setUp() {
        // Configure GoogleBooksService to return empty results by default
        when(googleBooksService.searchBooksAsyncReactive(anyString(), isNull(), anyInt(), isNull()))
            .thenReturn(Mono.just(java.util.Collections.emptyList()));

        // Configure NewYorkTimesService to return empty list by default
        when(newYorkTimesService.getCurrentBestSellers(anyString(), anyInt()))
            .thenReturn(Mono.just(java.util.Collections.emptyList()));

        // Configure BookCoverManagementService with mock cover generation
        when(bookCoverManagementService.getInitialCoverUrlAndTriggerBackgroundUpdate(any(Book.class)))
            .thenAnswer(invocation -> {
                Book book = invocation.getArgument(0);
                String mockCoverUrl = "http://example.com/mockcover/" + book.getId() + ".jpg";
                CoverImages coverImages = new CoverImages(
                    mockCoverUrl,
                    mockCoverUrl,
                    CoverImageSource.S3_CACHE
                );
                book.setCoverImages(coverImages);
                book.setCoverImageUrl(mockCoverUrl);
                return Mono.just(coverImages);
            });

        when(localDiskCoverCacheService.getLocalPlaceholderPath()).thenReturn("/images/placeholder-book-cover.svg");
    
        // Configure RecentlyViewedService with empty view history
        when(recentlyViewedService.getRecentlyViewedBooks()).thenReturn(new ArrayList<>());
    }
    /**
     * Helper method to create test Book instances
     * 
     * @param id Book identifier
     * @param title Book title
     * @param author Book authorer
     * @return Populated Book instance for testing
     */
    private Book createTestBook(String id, String title, String author) {
        Book book = new Book();
        book.setId(id);
        book.setTitle(title);
        book.setAuthors(List.of(author));
        book.setDescription("Test description for " + title);
        String coverUrl = "http://example.com/cover/" + (id != null ? id : "new") + ".jpg";
        book.setCoverImageUrl(coverUrl);
        book.setImageUrl("http://example.com/image/" + (id != null ? id : "new") + ".jpg");
        
        CoverImages coverImages = new CoverImages(
            coverUrl,
            coverUrl,
            CoverImageSource.S3_CACHE 
        );
        book.setCoverImages(coverImages);
        return book;
    }
    /**
     * Tests home page with successful recommendations
     */
    @Test
    void shouldReturnHomeViewWithBestsellersAndRecentBooks() {
        // Arrange
        Book bestsellerBook = createTestBook("bestseller1", "NYT Bestseller", "Author A");
        List<Book> bestsellers = List.of(bestsellerBook);
        Book recentBook = createTestBook("recent1", "Recent Read", "Author B");
        List<Book> additionalRecentBooks = List.of(recentBook);
        // Mock for bestsellers from NYT service
        when(newYorkTimesService.getCurrentBestSellers(eq("hardcover-fiction"), eq(8)))
            .thenReturn(Mono.just(bestsellers));
        when(recentlyViewedService.getRecentlyViewedBooks()).thenReturn(new ArrayList<>());
        // Mock for the "additional books" call (triggered because recentlyViewed is empty and needs 8 books) - updated signature
        // Make this mock more specific to avoid clashing with the bestsellers mock.
        // It should match any string EXCEPT "hardcover-fiction" for the query,
        // as "hardcover-fiction" is now used for the NYT service call.
        when(googleBooksService.searchBooksAsyncReactive(
                argThat((String query) -> query != null && !query.equals("hardcover-fiction")),
                isNull(String.class),
                eq(8),
                isNull(String.class)))
            .thenReturn(Mono.just(additionalRecentBooks));
        // Act & Assert
        webTestClient.get().uri("/")
            .accept(MediaType.TEXT_HTML)
            .exchange()
            .expectStatus().isOk()
            .expectBody(String.class)
            .value(body -> {
                try {
                    assertTrue(body.contains("NYT Bestseller"), "Response body did not contain 'NYT Bestseller'.\nBody:\n" + body);
                    assertTrue(body.contains("Recent Read"), "Response body did not contain 'Recent Read'.\nBody:\n" + body);
                } catch (AssertionError e) {
                    System.out.println("\n\n==== DEBUG: Response Body ====");
                    System.out.println(body);
                    System.out.println("==== END RESPONSE BODY ====");
                    throw e;
                }
            });
    }
    /**
     * Tests home page with empty recommendations
     */
    @Test
    void shouldShowEmptyHomePageWhenServicesReturnEmptyLists() {
        // Default setUp mocks already return empty lists

        // Act & Assert
        webTestClient.get().uri("/")
            .accept(MediaType.TEXT_HTML)
            .exchange()
            .expectStatus().isOk()
            .expectBody(String.class)
            .value(body -> {
                assertFalse(body.contains("NYT Bestseller"));
                assertFalse(body.contains("Recent Read"));
            });
    }
    /**
     * Tests home page when recommendation service fails
     */
    @Test
    void shouldShowEmptyHomePageWhenServiceThrowsException() {
        // Arrange - updated signature
        when(newYorkTimesService.getCurrentBestSellers(eq("hardcover-fiction"), eq(8)))
            .thenReturn(Mono.error(new RuntimeException("simulated bestseller fetch failure")));
        // Act & Assert
        webTestClient.get().uri("/")
            .accept(MediaType.TEXT_HTML)
            .exchange()
            .expectStatus().isOk()
            .expectBody(String.class)
            .value(body -> {
                assertFalse(body.contains("NYT Bestseller"));
                assertFalse(body.contains("Recent Read"));
            });
    }
}
