package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
import com.williamcallahan.book_recommendation_engine.service.BookSearchService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest({BookController.class, BookCoverController.class})
@AutoConfigureMockMvc(addFilters = false)
class BookControllerTest {

    @Autowired
    private MockMvc mockMvc;


    @MockitoBean
    private BookDataOrchestrator bookDataOrchestrator;

    @MockitoBean
    private RecommendationService recommendationService;

    @MockitoBean
    private GoogleBooksService googleBooksService;

    @MockitoBean
    private BookImageOrchestrationService bookImageOrchestrationService;

    private Book fixtureBook;

    @BeforeEach
    void setUp() {
        fixtureBook = buildBook("11111111-1111-4111-8111-111111111111", "Fixture Title");
        when(bookDataOrchestrator.fetchCanonicalBookReactive(anyString())).thenReturn(Mono.empty());

        when(googleBooksService.getBookById(anyString())).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));
        when(bookImageOrchestrationService.getBestCoverUrlAsync(any(Book.class), any(CoverImageSource.class)))
            .thenAnswer(invocation -> java.util.concurrent.CompletableFuture.completedFuture(invocation.getArgument(0)));
    }

    @Test
    @DisplayName("GET /api/books/search returns DTO results")
    void searchBooks_returnsDtos() throws Exception {
        when(bookDataOrchestrator.searchBooksTiered(eq("Fixture"), eq(null), eq(5), eq("newest")))
                .thenReturn(Mono.just(List.of(fixtureBook)));

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
                .andExpect(jsonPath("$.results[0].collections", hasSize(1)))
                .andExpect(jsonPath("$.results[0].collections[0].name", equalTo("NYT Hardcover Fiction")));
    }

    @Test
    @DisplayName("GET /api/books/{id} returns mapped DTO")
    void getBookByIdentifier_returnsDto() throws Exception {
        when(bookDataOrchestrator.fetchCanonicalBookReactive(fixtureBook.getId())).thenReturn(Mono.just(fixtureBook));

        performAsync(get("/api/books/" + fixtureBook.getId()))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.id", equalTo(fixtureBook.getId())))
                .andExpect(jsonPath("$.slug", equalTo(fixtureBook.getSlug())))
                .andExpect(jsonPath("$.authors[0].name", equalTo("Fixture Author")))
                .andExpect(jsonPath("$.cover.s3ImagePath", equalTo(fixtureBook.getS3ImagePath())))
                .andExpect(jsonPath("$.collections[0].type", equalTo("BESTSELLER_LIST")));
    }

    @Test
    @DisplayName("GET /api/books/{slug} falls back to slug lookup")
    void getBookBySlug_fallsBackToSlugLookup() throws Exception {
        when(bookDataOrchestrator.fetchCanonicalBookReactive("fixture-book-of-secrets")).thenReturn(Mono.just(fixtureBook));

        performAsync(get("/api/books/fixture-book-of-secrets"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", equalTo(fixtureBook.getId())))
                .andExpect(jsonPath("$.slug", equalTo(fixtureBook.getSlug())))
                .andExpect(jsonPath("$.collections", hasSize(1)));
    }

    @Test
    @DisplayName("GET /api/books/{id} returns 404 when not found")
    void getBook_notFound() throws Exception {
        when(bookDataOrchestrator.fetchCanonicalBookReactive("missing")).thenReturn(Mono.empty());

        performAsync(get("/api/books/missing"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("GET /api/books/{id}/similar returns mapped DTOs")
    void getSimilarBooks_returnsDtos() throws Exception {
        Book similar = buildBook("22222222-2222-4222-8222-222222222222", "Sibling Title");
        when(bookDataOrchestrator.fetchCanonicalBookReactive(fixtureBook.getId())).thenReturn(Mono.just(fixtureBook));
        when(recommendationService.getSimilarBooks(eq(fixtureBook.getId()), anyInt()))
                .thenReturn(Mono.just(List.of(similar)));

        performAsync(get("/api/books/" + fixtureBook.getId() + "/similar").param("limit", "3"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].id", equalTo(similar.getId())));
    }

    @Test
    @DisplayName("GET /api/covers/{id} hydrates via orchestrator before Google fallback")
    void getBookCover_usesOrchestratorFirst() throws Exception {
        Book hydrated = buildBook("orchestrator-id", "Hydrated Title");
        when(bookDataOrchestrator.getBookByIdTiered("orchestrator-id")).thenReturn(Mono.just(hydrated));
        when(bookImageOrchestrationService.getBestCoverUrlAsync(eq(hydrated), eq(CoverImageSource.ANY)))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(hydrated));

        performAsync(get("/api/covers/orchestrator-id"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bookId", equalTo("orchestrator-id")))
                .andExpect(jsonPath("$.coverUrl", equalTo(hydrated.getS3ImagePath())));

        verify(googleBooksService, never()).getBookById("orchestrator-id");
    }

    @Test
    @DisplayName("GET /api/covers/{id} falls back to Google when orchestrator returns empty")
    void getBookCover_fallsBackToGoogle() throws Exception {
        Book fallback = buildBook("fallback-id", "Fallback Title");
        when(bookDataOrchestrator.getBookByIdTiered("fallback-id")).thenReturn(Mono.empty());
        when(googleBooksService.getBookById("fallback-id"))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(fallback));
        when(bookImageOrchestrationService.getBestCoverUrlAsync(eq(fallback), eq(CoverImageSource.ANY)))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(fallback));

        performAsync(get("/api/covers/fallback-id"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bookId", equalTo("fallback-id")))
                .andExpect(jsonPath("$.coverUrl", equalTo(fallback.getS3ImagePath())));

        verify(googleBooksService).getBookById("fallback-id");
    }

    @Test
    @DisplayName("GET /api/books/authors/search returns fallback IDs when missing from Postgres")
    void searchAuthors_returnsFallbackIds() throws Exception {
        List<BookSearchService.AuthorResult> authorResults = List.of(
            new BookSearchService.AuthorResult("db-1", "Known Author", 5, 0.92),
            new BookSearchService.AuthorResult(null, "Mystery Author", 1, 0.35)
        );

        when(bookDataOrchestrator.searchAuthors(eq("Mystery"), anyInt())).thenReturn(Mono.just(authorResults));

        performAsync(get("/api/books/authors/search")
                .param("query", "Mystery"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.results", hasSize(2)))
            .andExpect(jsonPath("$.results[0].authorId", equalTo("db-1")))
            .andExpect(jsonPath("$.results[1].authorId", containsString("external-author-")))
            .andExpect(jsonPath("$.results[1].authorName", equalTo("Mystery Author")));
    }

    private ResultActions performAsync(MockHttpServletRequestBuilder builder) throws Exception {
        MvcResult result = mockMvc.perform(builder).andReturn();
        return mockMvc.perform(asyncDispatch(result));
    }

    private Book buildBook(String id, String title) {
        Book book = new Book();
        book.setId(id);
        book.setTitle(title);
        book.setSlug("fixture-title");
        book.setDescription("Fixture Description");
        book.setAuthors(List.of("Fixture Author"));
        book.setCategories(List.of("NYT Fiction"));
        book.setS3ImagePath("s3://covers/" + id + ".jpg");
        book.setExternalImageUrl("https://example.test/cover/" + id + ".jpg");
        book.setCoverImageWidth(640);
        book.setCoverImageHeight(960);
        book.setIsCoverHighResolution(true);
        book.setQualifiers(Map.of("nytBestseller", Map.of("rank", 1)));
        book.setCachedRecommendationIds(List.of("rec-1", "rec-2"));

        CoverImages coverImages = new CoverImages("https://cdn.test/preferred/" + id + ".jpg",
                "https://cdn.test/fallback/" + id + ".jpg",
                CoverImageSource.GOOGLE_BOOKS);
        book.setCoverImages(coverImages);
        Book.CollectionAssignment assignment = new Book.CollectionAssignment("nyt-hardcover-fiction", "NYT Hardcover Fiction", "BESTSELLER_LIST", 1, "NYT");
        book.setCollections(List.of(assignment));
        return book;
    }
}
