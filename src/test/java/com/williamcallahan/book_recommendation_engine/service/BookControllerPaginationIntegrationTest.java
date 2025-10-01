package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.controller.BookController;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(BookController.class)
@AutoConfigureMockMvc(addFilters = false)
@Import(SearchPaginationService.class)
class BookControllerPaginationIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private TieredBookSearchService tieredBookSearchService;

    @MockBean
    private BookDataOrchestrator bookDataOrchestrator;

    @MockBean
    private BookSearchService bookSearchService;

    @MockBean
    private BookQueryRepository bookQueryRepository;

    @MockBean
    private BookIdentifierResolver bookIdentifierResolver;

    private final List<Book> dataset = new ArrayList<>();

    @BeforeEach
    void setUp() {
        dataset.clear();
        IntStream.range(0, 14)
            .forEach(i -> dataset.add(buildBook("pg-%02d".formatted(i), "Postgres %02d".formatted(i), true)));
        IntStream.range(0, 15)
            .forEach(i -> dataset.add(buildBook("ext-%02d".formatted(i), "External %02d".formatted(i), false)));

        when(tieredBookSearchService.streamSearch(anyString(), isNull(), anyInt(), anyString(), eq(false)))
            .thenAnswer(invocation -> Flux.fromIterable(dataset));
        doNothing().when(bookDataOrchestrator).persistBooksAsync(any(), anyString());
    }

    @Test
    @DisplayName("Page 1 honours Postgres-first ordering and exposes prefetch metadata")
    void firstPageMaintainsOrderingAndPrefetch() throws Exception {
        MvcResult result = performAsync(get("/api/books/search")
                .param("query", "multi")
                .param("maxResults", "12"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.results", org.hamcrest.Matchers.hasSize(12)))
            .andExpect(jsonPath("$.results[0].id", org.hamcrest.Matchers.equalTo("pg-00")))
            .andExpect(jsonPath("$.results[11].id", org.hamcrest.Matchers.equalTo("pg-11")))
            .andExpect(jsonPath("$.hasMore", org.hamcrest.Matchers.equalTo(true)))
            .andExpect(jsonPath("$.nextStartIndex", org.hamcrest.Matchers.equalTo(12)))
            .andExpect(jsonPath("$.prefetchedCount", org.hamcrest.Matchers.equalTo(17)))
            .andReturn();

        List<String> ids = extractIds(result);
        assertThat(new HashSet<>(ids)).hasSize(ids.size());
    }

    @Test
    @DisplayName("Page 2 advances cursor without duplicating page 1 results")
    void secondPageAdvancesCursorWithoutDupes() throws Exception {
        MvcResult firstPage = performAsync(get("/api/books/search")
                .param("query", "multi")
                .param("maxResults", "12"))
            .andExpect(status().isOk())
            .andReturn();

        MvcResult secondPage = performAsync(get("/api/books/search")
                .param("query", "multi")
                .param("startIndex", "12")
                .param("maxResults", "12"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON))
            .andExpect(jsonPath("$.results", org.hamcrest.Matchers.hasSize(12)))
            .andExpect(jsonPath("$.results[0].id", org.hamcrest.Matchers.equalTo("pg-12")))
            .andExpect(jsonPath("$.results[1].id", org.hamcrest.Matchers.equalTo("pg-13")))
            .andExpect(jsonPath("$.results[2].id", org.hamcrest.Matchers.equalTo("ext-00")))
            .andExpect(jsonPath("$.hasMore", org.hamcrest.Matchers.equalTo(true)))
            .andExpect(jsonPath("$.nextStartIndex", org.hamcrest.Matchers.equalTo(24)))
            .andExpect(jsonPath("$.prefetchedCount", org.hamcrest.Matchers.equalTo(5)))
            .andReturn();

        List<String> firstIds = extractIds(firstPage);
        List<String> secondIds = extractIds(secondPage);

        assertThat(secondIds).doesNotContainAnyElementsOf(firstIds);
        assertThat(new HashSet<>(secondIds)).hasSize(secondIds.size());
    }

    private List<String> extractIds(MvcResult result) throws Exception {
        JsonNode root = objectMapper.readTree(result.getResponse().getContentAsString());
        List<String> ids = new ArrayList<>();
        root.path("results").forEach(node -> ids.add(node.path("id").asText()));
        return ids;
    }

    private ResultActions performAsync(MockHttpServletRequestBuilder builder) throws Exception {
        builder.accept(MediaType.APPLICATION_JSON);
        ResultActions initial = mockMvc.perform(builder);
        MvcResult mvcResult = initial.andReturn();
        if (mvcResult.getRequest().isAsyncStarted()) {
            return mockMvc.perform(asyncDispatch(mvcResult));
        }
        return initial;
    }

    private Book buildBook(String id, String title, boolean inPostgres) {
        Book book = new Book();
        book.setId(id);
        book.setTitle(title);
        book.setInPostgres(inPostgres);
        book.setRetrievedFrom(inPostgres ? "POSTGRES" : "EXTERNAL_API");
        Map<String, Object> qualifiers = new HashMap<>();
        qualifiers.put("nytBestseller", Map.of("rank", 1));
        book.setQualifiers(qualifiers);
        return book;
    }
}
