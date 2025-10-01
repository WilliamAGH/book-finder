/**
 * Test suite for HomeController web endpoints
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.dto.BookCard;
import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.BookIdentifierResolver;
import com.williamcallahan.book_recommendation_engine.service.BookSearchService;
import com.williamcallahan.book_recommendation_engine.service.BookViewService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
// Use fully-qualified names for image services to avoid import resolution issues in test slice
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
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import java.time.LocalDate;
import java.util.List;
import java.util.Collections;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyString; // For mocking getSimilarBooks
import static org.mockito.ArgumentMatchers.anyInt; // For mocking getSimilarBooks
import static org.mockito.ArgumentMatchers.any; // For mocking any objects
import static org.mockito.ArgumentMatchers.eq; // For mocking specific values
import reactor.core.publisher.Mono; // For mocking reactive service
import com.williamcallahan.book_recommendation_engine.service.NewYorkTimesService;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
@WebFluxTest(value = HomeController.class,
    excludeAutoConfiguration = org.springframework.boot.autoconfigure.security.reactive.ReactiveSecurityAutoConfiguration.class)
class HomeControllerTest {
    /**
     * WebTestClient for controller integration testing
     */
    @Autowired
    private WebTestClient webTestClient;
    @org.springframework.beans.factory.annotation.Autowired
    private BookDataOrchestrator bookDataOrchestrator;

    @org.springframework.beans.factory.annotation.Autowired
    private RecentlyViewedService recentlyViewedService;
    
    @org.springframework.beans.factory.annotation.Autowired
    private com.williamcallahan.book_recommendation_engine.service.image.BookCoverManagementService bookCoverManagementService;

    @org.springframework.beans.factory.annotation.Autowired
    private com.williamcallahan.book_recommendation_engine.service.image.LocalDiskCoverCacheService localDiskCoverCacheService;
    
    @org.springframework.beans.factory.annotation.Autowired
    private NewYorkTimesService newYorkTimesService;
    
    @org.springframework.beans.factory.annotation.Autowired
    private BookQueryRepository bookQueryRepository;

    @org.springframework.beans.factory.annotation.Autowired
    private BookIdentifierResolver bookIdentifierResolver;

    @TestConfiguration
    static class MocksConfig {
        @Bean BookDataOrchestrator bookDataOrchestrator() { return Mockito.mock(BookDataOrchestrator.class); }
        @Bean RecentlyViewedService recentlyViewedService() { return Mockito.mock(RecentlyViewedService.class); }
        @Bean com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService bookImageOrchestrationService() { return Mockito.mock(com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService.class); }
        @Bean com.williamcallahan.book_recommendation_engine.service.image.BookCoverManagementService bookCoverManagementService() { return Mockito.mock(com.williamcallahan.book_recommendation_engine.service.image.BookCoverManagementService.class); }
        @Bean com.williamcallahan.book_recommendation_engine.service.image.LocalDiskCoverCacheService localDiskCoverCacheService() { return Mockito.mock(com.williamcallahan.book_recommendation_engine.service.image.LocalDiskCoverCacheService.class); }
        @Bean com.williamcallahan.book_recommendation_engine.service.EnvironmentService environmentService() { return Mockito.mock(com.williamcallahan.book_recommendation_engine.service.EnvironmentService.class); }
        @Bean com.williamcallahan.book_recommendation_engine.service.DuplicateBookService duplicateBookService() { return Mockito.mock(com.williamcallahan.book_recommendation_engine.service.DuplicateBookService.class); }
        @Bean com.williamcallahan.book_recommendation_engine.service.AffiliateLinkService affiliateLinkService() { return Mockito.mock(com.williamcallahan.book_recommendation_engine.service.AffiliateLinkService.class); }
        @Bean NewYorkTimesService newYorkTimesService() { return Mockito.mock(NewYorkTimesService.class); }
        @Bean BookQueryRepository bookQueryRepository() { return Mockito.mock(BookQueryRepository.class); }
        @Bean BookViewService bookViewService() { return Mockito.mock(BookViewService.class); }
        @Bean BookSearchService bookSearchService() { return Mockito.mock(BookSearchService.class); }
        @Bean BookIdentifierResolver bookIdentifierResolver() { return Mockito.mock(BookIdentifierResolver.class); }
    }
    
    /**
     * Sets up common test fixtures
     * Configures mock services with default behaviors
     */
    @BeforeEach
    void setUp() {
        // Configure NewYorkTimesService to return empty BookCard list by default (NEW OPTIMIZED METHOD)
        when(newYorkTimesService.getCurrentBestSellersCards(anyString(), anyInt()))
            .thenReturn(Mono.just(java.util.Collections.emptyList()));
        // Configure BookQueryRepository to return empty BookCard list by default
        when(bookQueryRepository.fetchBookCards(org.mockito.ArgumentMatchers.anyList()))
            .thenReturn(java.util.Collections.emptyList());
        when(bookQueryRepository.fetchBookDetailBySlug(anyString())).thenReturn(Optional.empty());
        when(bookQueryRepository.fetchBookDetail(org.mockito.ArgumentMatchers.any(UUID.class))).thenReturn(Optional.empty());

        when(bookIdentifierResolver.resolveToUuid(anyString())).thenReturn(Optional.empty());

        // Configure BookCoverManagementService with mock cover generation
        when(bookCoverManagementService.getInitialCoverUrlAndTriggerBackgroundUpdate(any(Book.class)))
            .thenAnswer(invocation -> {
                Book book = invocation.getArgument(0);
                String mockCoverUrl = "http://example.com/mockcover/" + book.getId() + ".jpg";
                CoverImages coverImages = new CoverImages(
                    mockCoverUrl,
                    mockCoverUrl,
                    CoverImageSource.UNDEFINED
                );
                book.setCoverImages(coverImages);
                book.setExternalImageUrl(mockCoverUrl);
                return Mono.just(coverImages);
            });

        when(localDiskCoverCacheService.getLocalPlaceholderPath()).thenReturn(ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH);
    
        // Configure RecentlyViewedService to return empty BookCard IDs by default
        when(recentlyViewedService.getRecentlyViewedBookIds(anyInt()))
            .thenReturn(java.util.Collections.emptyList());

        when(bookDataOrchestrator.fetchCanonicalBookReactive(anyString())).thenReturn(Mono.empty());
    }

    @Test
    void shouldRedirectIsbnToCanonicalSlugWhenFoundInPostgres() {
        String isbn = "978-0590353427";
        String sanitizedIsbn = "9780590353427";

        Book canonicalBook = createTestBook("123e4567-e89b-12d3-a456-426614174000", "Test Title", "Author A");
        canonicalBook.setSlug("test-title");

        UUID canonicalUuid = UUID.fromString(canonicalBook.getId());
        BookDetail detail = new BookDetail(
            canonicalBook.getId(),
            canonicalBook.getSlug(),
            canonicalBook.getTitle(),
            canonicalBook.getDescription(),
            "Test Publisher",
            LocalDate.of(2024, 1, 1),
            "en",
            canonicalBook.getPageCount(),
            canonicalBook.getAuthors(),
            List.of(),
            canonicalBook.getExternalImageUrl(),
            canonicalBook.getExternalImageUrl(),
            600,
            900,
            Boolean.TRUE,
            "POSTGRES",
            4.2,
            128,
            canonicalBook.getIsbn10(),
            sanitizedIsbn,
            "https://preview",
            "https://info",
            Collections.<String, Object>emptyMap(),
            Collections.<com.williamcallahan.book_recommendation_engine.dto.EditionSummary>emptyList()
        );

        when(bookIdentifierResolver.resolveToUuid(eq(sanitizedIsbn))).thenReturn(Optional.of(canonicalUuid));
        when(bookQueryRepository.fetchBookDetail(eq(canonicalUuid))).thenReturn(Optional.of(detail));

        webTestClient.get().uri("/book/isbn/" + isbn)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.SEE_OTHER)
            .expectHeader().valueEquals("Location", "/book/" + canonicalBook.getSlug());
    }

    @Test
    void shouldRedirectToNotFoundWhenIsbnMissingFromPostgresAndApis() {
        String rawIsbn = "978-0307465351";
        String sanitizedIsbn = "9780307465351";

        webTestClient.get().uri("/book/isbn/" + rawIsbn)
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.SEE_OTHER)
            .expectHeader().valueEquals("Location", "/?info=bookNotFound&isbn=" + sanitizedIsbn);

    }

    @Test
    void exploreRedirectsToSearchWithEncodedQuery() {
        webTestClient.get().uri("/explore")
            .exchange()
            .expectStatus().isEqualTo(HttpStatus.SEE_OTHER)
            .expectHeader().valueMatches("Location", "/search\\?query=.*&source=explore");
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
        book.setExternalImageUrl(coverUrl);
        
        CoverImages coverImages = new CoverImages(
            coverUrl,
            coverUrl,
            CoverImageSource.UNDEFINED 
        );
        book.setCoverImages(coverImages);
        return book;
    }
    /**
     * Tests home page with successful recommendations
     */
    @Test
    void shouldReturnHomeViewWithBestsellersAndRecentBooks() {
        // Arrange - use BookCard DTOs (THE NEW WAY)
        BookCard bestsellerCard = new BookCard(
            "bestseller1",
            "nyt-bestseller-slug",
            "NYT Bestseller",
            List.of("Author A"),
            "http://example.com/cover/bestseller1.jpg",
            4.5,
            100,
            java.util.Map.of()
        );
        List<BookCard> bestsellerCards = List.of(bestsellerCard);
        
        // Mock for bestsellers from NYT service (NEW OPTIMIZED METHOD)
        when(newYorkTimesService.getCurrentBestSellersCards(eq("hardcover-fiction"), eq(8)))
            .thenReturn(Mono.just(bestsellerCards));
        // Mock recently viewed book IDs (NEW OPTIMIZED WAY) - use proper UUIDs
        String recentBookUuid = "550e8400-e29b-41d4-a716-446655440001";
        when(recentlyViewedService.getRecentlyViewedBookIds(anyInt()))
            .thenReturn(List.of(recentBookUuid));
        
        // Create recent card with same UUID
        BookCard recentCardWithUuid = new BookCard(
            recentBookUuid,
            "recent-read-slug",
            "Recent Read",
            List.of("Author B"),
            "http://example.com/cover/recent1.jpg",
            4.0,
            50,
            java.util.Map.of()
        );
        
        // Mock BookQueryRepository to return recent cards
        when(bookQueryRepository.fetchBookCards(org.mockito.ArgumentMatchers.anyList()))
            .thenReturn(List.of(recentCardWithUuid));

        // Act & Assert
        webTestClient.get().uri("/")
            .accept(MediaType.TEXT_HTML)
            .exchange()
            .expectStatus().isOk()
            .expectBody(String.class)
            .value(body -> {
                try {
                    // Verify sections are populated (no empty alerts), not specific titles
                    assertTrue(body.contains("NYT Bestsellers"), "Response body did not contain 'NYT Bestsellers' header.\nBody:\n" + body);
                    assertFalse(body.contains("No current bestsellers to display."), "Bestsellers section unexpectedly empty.\nBody:\n" + body);
                    assertFalse(body.contains("No recent books to display."), "Recent section unexpectedly empty.\nBody:\n" + body);
                    assertTrue(body.contains("class=\"card h-100\""), "Expected at least one rendered book card.\nBody:\n" + body);
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
                assertTrue(body.contains("No current bestsellers to display."));
                assertTrue(body.contains("No recent books to display."));
            });
    }
    /**
     * Tests home page when recommendation service fails
     */
    @Test
    void shouldShowEmptyHomePageWhenServiceThrowsException() {
        // Arrange - NEW OPTIMIZED METHOD
        when(newYorkTimesService.getCurrentBestSellersCards(eq("hardcover-fiction"), eq(8)))
            .thenReturn(Mono.error(new RuntimeException("simulated bestseller fetch failure")));
        when(recentlyViewedService.getRecentlyViewedBookIds(anyInt()))
            .thenReturn(java.util.Collections.emptyList());
        // Act & Assert
        webTestClient.get().uri("/")
            .accept(MediaType.TEXT_HTML)
            .exchange()
            .expectStatus().isOk()
            .expectBody(String.class)
            .value(body -> {
                assertTrue(body.contains("No current bestsellers to display."));
                assertTrue(body.contains("No recent books to display."));
            });
    }
}
