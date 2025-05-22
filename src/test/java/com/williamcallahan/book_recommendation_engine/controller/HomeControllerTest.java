/**
 * Test suite for HomeController web endpoints
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookCacheFacadeService;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService;
import com.williamcallahan.book_recommendation_engine.service.image.BookCoverManagementService;
import com.williamcallahan.book_recommendation_engine.service.image.LocalDiskCoverCacheService;
import com.williamcallahan.book_recommendation_engine.types.CoverImages;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
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
import com.williamcallahan.book_recommendation_engine.service.AffiliateLinkService;
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
    @MockitoBean
    private RecommendationService recommendationService;
    
    @MockitoBean
    private BookCacheFacadeService bookCacheFacadeService;
    
    @MockitoBean
    private RecentlyViewedService recentlyViewedService;
    
    @MockitoBean
    private BookImageOrchestrationService bookImageOrchestrationService;
    
    @MockitoBean
    private BookCoverManagementService bookCoverManagementService;

    @MockitoBean
    private LocalDiskCoverCacheService localDiskCoverCacheService;
    
    @MockitoBean
    private com.williamcallahan.book_recommendation_engine.service.EnvironmentService environmentService;
    
    @MockitoBean
    private com.williamcallahan.book_recommendation_engine.service.DuplicateBookService duplicateBookService;

    @MockitoBean
    private NewYorkTimesService newYorkTimesService;
    
    @MockitoBean
    private AffiliateLinkService affiliateLinkService;
    /**
     * Sets up common test fixtures
     * Configures mock services with default behaviors
     */
    @BeforeEach
    void setUp() {
        // Configure BookCacheFacadeService to return empty results by default
        when(bookCacheFacadeService.searchBooksReactive(anyString(), anyInt(), anyInt(), isNull(), isNull(), isNull()))
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
                    com.williamcallahan.book_recommendation_engine.types.CoverImageSource.S3_CACHE
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
            com.williamcallahan.book_recommendation_engine.types.CoverImageSource.S3_CACHE 
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
        // It should match any string EXCEPT "new york times bestsellers" for the query.
        when(bookCacheFacadeService.searchBooksReactive(
                argThat((String query) -> query != null && !query.equals("new york times bestsellers")), 
                eq(0), 
                eq(8), 
                isNull(Integer.class), 
                isNull(String.class), 
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
