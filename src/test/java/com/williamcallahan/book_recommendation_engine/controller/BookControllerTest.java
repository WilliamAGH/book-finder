package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SuppressWarnings({"removal"})
@WebMvcTest(BookController.class)
@AutoConfigureMockMvc(addFilters = false)
class BookControllerTest {

    @Autowired
    private MockMvc mockMvc;


    @MockBean
    private BookDataOrchestrator bookDataOrchestrator;

    @MockBean
    private RecommendationService recommendationService;

    private Book fixtureBook;

    @BeforeEach
    void setUp() {
        fixtureBook = buildBook("11111111-1111-4111-8111-111111111111", "Fixture Title");
        when(bookDataOrchestrator.getBookBySlugTiered(anyString())).thenReturn(Mono.empty());
    }

    @Test
    @DisplayName("GET /api/books/search returns DTO results")
    void searchBooks_returnsDtos() throws Exception {
        when(bookDataOrchestrator.searchBooksTiered(eq("Fixture"), eq(null), eq(5), eq(null)))
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
                .andExpect(jsonPath("$.results[0].tags[0].key", equalTo("nytBestseller")));
    }

    @Test
    @DisplayName("GET /api/books/{id} returns mapped DTO")
    void getBookByIdentifier_returnsDto() throws Exception {
        when(bookDataOrchestrator.getBookByIdTiered(fixtureBook.getId())).thenReturn(Mono.just(fixtureBook));

        performAsync(get("/api/books/" + fixtureBook.getId()))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.id", equalTo(fixtureBook.getId())))
                .andExpect(jsonPath("$.slug", equalTo(fixtureBook.getSlug())))
                .andExpect(jsonPath("$.authors[0].name", equalTo("Fixture Author")))
                .andExpect(jsonPath("$.cover.s3ImagePath", equalTo(fixtureBook.getS3ImagePath())));
    }

    @Test
    @DisplayName("GET /api/books/{slug} falls back to slug lookup")
    void getBookBySlug_fallsBackToSlugLookup() throws Exception {
        when(bookDataOrchestrator.getBookByIdTiered("fixture-book-of-secrets")).thenReturn(Mono.empty());
        when(bookDataOrchestrator.getBookBySlugTiered("fixture-book-of-secrets")).thenReturn(Mono.just(fixtureBook));

        performAsync(get("/api/books/fixture-book-of-secrets"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", equalTo(fixtureBook.getId())))
                .andExpect(jsonPath("$.slug", equalTo(fixtureBook.getSlug())));
    }

    @Test
    @DisplayName("GET /api/books/{id} returns 404 when not found")
    void getBook_notFound() throws Exception {
        when(bookDataOrchestrator.getBookByIdTiered("missing")).thenReturn(Mono.empty());
        when(bookDataOrchestrator.getBookBySlugTiered("missing")).thenReturn(Mono.empty());

        performAsync(get("/api/books/missing"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("GET /api/books/{id}/similar returns mapped DTOs")
    void getSimilarBooks_returnsDtos() throws Exception {
        Book similar = buildBook("22222222-2222-4222-8222-222222222222", "Sibling Title");
        when(bookDataOrchestrator.getBookByIdTiered(fixtureBook.getId())).thenReturn(Mono.just(fixtureBook));
        when(recommendationService.getSimilarBooks(eq(fixtureBook.getId()), anyInt()))
                .thenReturn(Mono.just(List.of(similar)));

        performAsync(get("/api/books/" + fixtureBook.getId() + "/similar").param("limit", "3"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].id", equalTo(similar.getId())));
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
        return book;
    }
}
