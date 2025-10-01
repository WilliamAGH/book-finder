package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.dto.BookCard;
import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.dto.BookListItem;
import com.williamcallahan.book_recommendation_engine.dto.RecommendationCard;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.BookIdentifierResolver;
import com.williamcallahan.book_recommendation_engine.service.BookSearchService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class BookControllerTest {

    @Mock
    private BookSearchService bookSearchService;

    @Mock
    private BookQueryRepository bookQueryRepository;

    @Mock
    private BookIdentifierResolver bookIdentifierResolver;

    @Mock
    private BookDataOrchestrator bookDataOrchestrator;

    @Mock
    private BookImageOrchestrationService bookImageOrchestrationService;

    private MockMvc mockMvc;

    private Book fixtureBook;

    @BeforeEach
    void setUp() {
        BookController bookController = new BookController(bookSearchService, bookQueryRepository, bookIdentifierResolver);
        BookCoverController bookCoverController = new BookCoverController(
            bookImageOrchestrationService,
            bookQueryRepository,
            bookIdentifierResolver,
            bookDataOrchestrator
        );

        mockMvc = MockMvcBuilders.standaloneSetup(bookController, bookCoverController).build();
        fixtureBook = buildBook("11111111-1111-4111-8111-111111111111", "fixture-book-of-secrets");
    }

    @Test
    @DisplayName("GET /api/books/search returns DTO results")
    void searchBooks_returnsDtos() throws Exception {
        UUID bookUuid = UUID.fromString(fixtureBook.getId());
        List<BookSearchService.SearchResult> searchResults = List.of(
            new BookSearchService.SearchResult(bookUuid, 0.92, "POSTGRES")
        );
        Map<String, Object> tags = Map.of("nytBestseller", Map.of("rank", 1));
        BookListItem listItem = new BookListItem(
            fixtureBook.getId(),
            fixtureBook.getSlug(),
            fixtureBook.getTitle(),
            fixtureBook.getDescription(),
            fixtureBook.getAuthors(),
            fixtureBook.getCategories(),
            fixtureBook.getCoverImages().getPreferredUrl(),
            4.8,
            123,
            tags
        );

        when(bookSearchService.searchBooks(eq("Fixture"), anyInt())).thenReturn(searchResults);
        when(bookQueryRepository.fetchBookListItems(eq(List.of(bookUuid)))).thenReturn(List.of(listItem));

        performAsync(get("/api/books/search")
            .param("query", "Fixture")
            .param("maxResults", "5"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.query", equalTo("Fixture")))
            .andExpect(jsonPath("$.results", hasSize(1)))
            .andExpect(jsonPath("$.results[0].id", equalTo(fixtureBook.getId())))
            .andExpect(jsonPath("$.results[0].slug", equalTo(fixtureBook.getSlug())))
            .andExpect(jsonPath("$.results[0].cover.preferredUrl", containsString("preferred")))
            .andExpect(jsonPath("$.results[0].tags[0].key", equalTo("nytBestseller")))
            .andExpect(jsonPath("$.results[0].tags[0].attributes.rank", equalTo(1)))
            .andExpect(jsonPath("$.results[0].matchType", equalTo("POSTGRES")));
    }

    @Test
    @DisplayName("GET /api/books/{id} returns mapped DTO")
    void getBookByIdentifier_returnsDto() throws Exception {
        BookDetail detail = buildDetailFromBook(fixtureBook);
        when(bookQueryRepository.fetchBookDetailBySlug(fixtureBook.getSlug()))
            .thenReturn(Optional.of(detail));

        performAsync(get("/api/books/" + fixtureBook.getSlug()))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", equalTo(fixtureBook.getId())))
            .andExpect(jsonPath("$.slug", equalTo(fixtureBook.getSlug())))
            .andExpect(jsonPath("$.authors[0].name", equalTo("Fixture Author")))
            .andExpect(jsonPath("$.cover.s3ImagePath", equalTo(fixtureBook.getS3ImagePath())))
            .andExpect(jsonPath("$.tags[0].key", equalTo("nytBestseller")));
    }

    @Test
    @DisplayName("GET /api/books/{identifier} falls back to canonical UUID when slug missing")
    void getBookByIdentifier_fallsBackToCanonicalId() throws Exception {
        when(bookQueryRepository.fetchBookDetailBySlug(fixtureBook.getSlug()))
            .thenReturn(Optional.empty());
        when(bookIdentifierResolver.resolveCanonicalId(fixtureBook.getSlug()))
            .thenReturn(Optional.of(fixtureBook.getId()));

        BookDetail detail = buildDetailFromBook(fixtureBook);
        when(bookQueryRepository.fetchBookDetail(UUID.fromString(fixtureBook.getId())))
            .thenReturn(Optional.of(detail));

        performAsync(get("/api/books/" + fixtureBook.getSlug()))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.id", equalTo(fixtureBook.getId())))
            .andExpect(jsonPath("$.slug", equalTo(fixtureBook.getSlug())));
    }

    @Test
    @DisplayName("GET /api/books/{identifier} returns 404 when not found")
    void getBook_notFound() throws Exception {
        when(bookQueryRepository.fetchBookDetailBySlug("missing")).thenReturn(Optional.empty());
        when(bookIdentifierResolver.resolveCanonicalId("missing")).thenReturn(Optional.empty());

        performAsync(get("/api/books/missing"))
            .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("GET /api/books/{id}/similar returns cached DTOs")
    void getBookSimilar_returnsDtos() throws Exception {
        UUID bookUuid = UUID.fromString(fixtureBook.getId());
        when(bookIdentifierResolver.resolveToUuid(fixtureBook.getSlug()))
            .thenReturn(Optional.of(bookUuid));

        BookCard card = new BookCard(
            fixtureBook.getId(),
            fixtureBook.getSlug(),
            fixtureBook.getTitle(),
            fixtureBook.getAuthors(),
            fixtureBook.getCoverImages().getPreferredUrl(),
            4.7,
            321,
            Map.of("reason", Map.of("type", "AUTHOR"))
        );
        List<RecommendationCard> cards = List.of(new RecommendationCard(card, 0.9, "AUTHOR"));
        when(bookQueryRepository.fetchRecommendationCards(bookUuid, 3)).thenReturn(cards);

        performAsync(get("/api/books/" + fixtureBook.getSlug() + "/similar")
            .param("limit", "3"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.length()").value(1))
            .andExpect(jsonPath("$[0].id", equalTo(fixtureBook.getId())));
    }

    @Test
    @DisplayName("GET /api/books/{id}/similar returns 404 when canonical lookup fails")
    void getBookSimilar_returnsEmptyWhenMissing() throws Exception {
        when(bookIdentifierResolver.resolveToUuid("unknown"))
            .thenReturn(Optional.empty());

        performAsync(get("/api/books/unknown/similar"))
            .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("GET /api/covers/{id} uses orchestrator fallback when repository misses")
    void getBookCover_fallsBackToOrchestrator() throws Exception {
        stubRepositoryMiss("orchestrator-id");
        Book fallback = buildBook(UUID.randomUUID().toString(), "fallback-book");
        when(bookDataOrchestrator.fetchCanonicalBookReactive("orchestrator-id"))
            .thenReturn(Mono.just(fallback));
        stubCoverPipeline();

        String expectedCoverUrl = fallback.getS3ImagePath();
        String expectedPreferred = fallback.getCoverImages().getPreferredUrl();

        performAsync(get("/api/covers/orchestrator-id"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.preferredUrl", equalTo(expectedPreferred)))
            .andExpect(jsonPath("$.coverUrl", equalTo(expectedCoverUrl)))
            .andExpect(jsonPath("$.requestedSourcePreference", equalTo("ANY")));
    }

    @Test
    @DisplayName("GET /api/covers/{id} resolves via repository detail first")
    void getBookCover_usesRepositoryFirst() throws Exception {
        BookDetail detail = buildDetailFromBook(fixtureBook);
        when(bookQueryRepository.fetchBookDetailBySlug(fixtureBook.getSlug()))
            .thenReturn(Optional.of(detail));
        when(bookDataOrchestrator.fetchCanonicalBookReactive(fixtureBook.getSlug()))
            .thenReturn(Mono.empty());
        stubCoverPipeline();

        performAsync(get("/api/covers/" + fixtureBook.getSlug()))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.coverUrl", containsString(fixtureBook.getId())));
    }

    @Test
    @DisplayName("GET /api/books/authors/search returns author results")
    void searchAuthors_returnsResults() throws Exception {
        List<BookSearchService.AuthorResult> results = List.of(
            new BookSearchService.AuthorResult("author-1", "Fixture Author", 12, 0.98)
        );
        when(bookSearchService.searchAuthors(eq("Fixture"), anyInt())).thenReturn(results);

        performAsync(get("/api/books/authors/search")
            .param("query", "Fixture")
            .param("limit", "5"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.query", equalTo("Fixture")))
            .andExpect(jsonPath("$.results", hasSize(1)))
            .andExpect(jsonPath("$.results[0].id", containsString("author-1")));
    }

    private ResultActions performAsync(MockHttpServletRequestBuilder builder) throws Exception {
        MvcResult result = mockMvc.perform(builder)
            .andExpect(request().asyncStarted())
            .andReturn();
        return mockMvc.perform(asyncDispatch(result));
    }

    private Book buildBook(String id, String slug) {
        Book book = new Book();
        book.setId(id);
        book.setTitle("Fixture Title");
        book.setSlug(slug);
        book.setDescription("Fixture Description");
        book.setAuthors(List.of("Fixture Author"));
        book.setCategories(List.of("NYT Fiction"));
        book.setLanguage("en");
        book.setPageCount(320);
        book.setPublisher("Fixture Publisher");
        book.setS3ImagePath("s3://covers/" + id + ".jpg");
        book.setExternalImageUrl("https://example.test/cover/" + id + ".jpg");
        book.setCoverImageWidth(600);
        book.setCoverImageHeight(900);
        book.setIsCoverHighResolution(Boolean.TRUE);
        book.setCoverImages(new CoverImages(
            "https://cdn.test/preferred/" + id + ".jpg",
            "https://cdn.test/fallback/" + id + ".jpg",
            CoverImageSource.GOOGLE_BOOKS));
        book.setQualifiers(Map.of("nytBestseller", Map.of("rank", 1)));
        book.setCachedRecommendationIds(List.of("rec-1", "rec-2"));
        book.setPublishedDate(Date.from(Instant.parse("2020-01-01T00:00:00Z")));
        book.setDataSource("POSTGRES");
        return book;
    }

    private BookDetail buildDetailFromBook(Book book) {
        return new BookDetail(
            book.getId(),
            book.getSlug(),
            book.getTitle(),
            book.getDescription(),
            book.getPublisher(),
            LocalDate.of(2024, 1, 1),
            book.getLanguage(),
            book.getPageCount(),
            book.getAuthors(),
            book.getCategories(),
            book.getS3ImagePath(),
            book.getExternalImageUrl(),
            book.getCoverImageWidth(),
            book.getCoverImageHeight(),
            book.getIsCoverHighResolution(),
            book.getDataSource(),
            4.6,
            87,
            "1234567890",
            "1234567890123",
            "https://preview",
            "https://info",
            Map.of("nytBestseller", Map.of("rank", 1)),
            List.of()
        );
    }

    private void stubRepositoryMiss(String identifier) {
        when(bookQueryRepository.fetchBookDetailBySlug(identifier)).thenReturn(Optional.empty());
        when(bookIdentifierResolver.resolveCanonicalId(identifier)).thenReturn(Optional.empty());
        when(bookIdentifierResolver.resolveToUuid(identifier)).thenReturn(Optional.empty());
    }

    private void stubCoverPipeline() {
        when(bookImageOrchestrationService.getBestCoverUrlAsync(any(Book.class), any(CoverImageSource.class)))
            .thenAnswer(invocation -> {
                Book book = invocation.getArgument(0);
                CoverImageSource source = invocation.getArgument(1);
                CoverImages images = book.getCoverImages();
                if (images == null) {
                    images = new CoverImages(
                        "https://cdn.example/preferred/" + book.getId() + ".jpg",
                        book.getExternalImageUrl(),
                        source
                    );
                    book.setCoverImages(images);
                }
                if (book.getS3ImagePath() == null) {
                    book.setS3ImagePath(images.getPreferredUrl());
                }
                return CompletableFuture.completedFuture(book);
            });
    }
}
