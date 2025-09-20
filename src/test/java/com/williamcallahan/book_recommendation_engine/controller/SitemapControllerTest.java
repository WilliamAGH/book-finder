package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.config.SitemapProperties;
import com.williamcallahan.book_recommendation_engine.service.SitemapService;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.AuthorListingXmlItem;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.AuthorSection;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.BookSitemapItem;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.PagedResult;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.SitemapOverview;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.model;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

@WebMvcTest(SitemapController.class)
@AutoConfigureMockMvc(addFilters = false)
class SitemapControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SitemapService sitemapService;

    @MockBean
    private SitemapProperties sitemapProperties;

    @BeforeEach
    void setUp() {
        when(sitemapProperties.getBaseUrl()).thenReturn("https://findmybook.net");
        when(sitemapProperties.getHtmlPageSize()).thenReturn(100);
        when(sitemapProperties.getXmlPageSize()).thenReturn(5000);

        Map<String, Integer> defaultCounts = SitemapService.LETTER_BUCKETS.stream()
                .collect(Collectors.toMap(letter -> letter, letter -> 0));
        when(sitemapService.getOverview()).thenReturn(new SitemapOverview(defaultCounts, defaultCounts));
        when(sitemapService.normalizeBucket(Mockito.any())).thenAnswer(invocation -> {
            String arg = invocation.getArgument(0);
            return arg == null ? "A" : arg.toUpperCase();
        });
    }

    @Test
    @DisplayName("GET /sitemap/books/A/1 renders the Thymeleaf sitemap view")
    void sitemapDynamicBooksRendersView() throws Exception {
        List<BookSitemapItem> books = List.of(new BookSitemapItem("book-id", "book-slug", "Demo Book", Instant.parse("2024-01-01T00:00:00Z")));
        when(sitemapService.getBooksByLetter("A", 1)).thenReturn(new PagedResult<>(books, 1, 1, 1));

        mockMvc.perform(get("/sitemap/books/A/1"))
                .andExpect(status().isOk())
                .andExpect(view().name("sitemap"))
                .andExpect(model().attribute("canonicalUrl", "https://findmybook.net/sitemap/books/A/1"));
    }

    @Test
    @DisplayName("GET /sitemap/authors/A/2 renders author view and canonical url")
    void sitemapDynamicAuthorsRendersView() throws Exception {
        List<AuthorSection> authors = List.of(new AuthorSection("author-id", "Demo Author", Instant.parse("2024-01-01T00:00:00Z"), List.of()));
        when(sitemapService.getAuthorsByLetter("A", 2)).thenReturn(new PagedResult<>(authors, 2, 3, 10));

        mockMvc.perform(get("/sitemap/authors/A/2"))
                .andExpect(status().isOk())
                .andExpect(view().name("sitemap"))
                .andExpect(model().attribute("canonicalUrl", "https://findmybook.net/sitemap/authors/A/2"));
    }

    @Test
    @DisplayName("GET /sitemap with parameters redirects to canonical dynamic route")
    void sitemapLandingRedirectsToDynamicRoute() throws Exception {
        mockMvc.perform(get("/sitemap")
                        .param("view", "Books")
                        .param("letter", "a")
                        .param("page", "0"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("/sitemap/books/A/1"));
    }

    @Test
    @DisplayName("GET /sitemap.xml returns sitemap index")
    void sitemapIndexReturnsXml() throws Exception {
        when(sitemapService.getBooksXmlPageCount()).thenReturn(2);
        when(sitemapService.getAuthorXmlPageCount()).thenReturn(1);

        mockMvc.perform(get("/sitemap.xml"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_XML))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("sitemap-xml/books/1.xml")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("sitemap-xml/authors/1.xml")))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("sitemap-static"))));
    }

    @Test
    @DisplayName("GET /sitemap-xml/books/1.xml returns book urlset")
    void booksSitemapReturnsXml() throws Exception {
        when(sitemapService.getBooksXmlPageCount()).thenReturn(1);
        when(sitemapService.getBooksForXmlPage(1)).thenReturn(List.of(
                new BookSitemapItem("book-id", "book-slug", "Demo Book", Instant.parse("2024-01-01T00:00:00Z"))
        ));

        mockMvc.perform(get("/sitemap-xml/books/1.xml"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_XML))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("/book/book-slug")));
    }

    @Test
    @DisplayName("GET /sitemap-xml/books/5.xml returns 404 when page exceeds total")
    void booksSitemapOutOfRangeReturns404() throws Exception {
        when(sitemapService.getBooksXmlPageCount()).thenReturn(2);

        mockMvc.perform(get("/sitemap-xml/books/5.xml"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("GET /sitemap-xml/authors/1.xml returns author listing urlset")
    void authorsSitemapReturnsXml() throws Exception {
        when(sitemapService.getAuthorXmlPageCount()).thenReturn(1);
        when(sitemapService.getAuthorListingsForXmlPage(1)).thenReturn(List.of(
                new AuthorListingXmlItem("A", 1, Instant.parse("2024-01-01T00:00:00Z"))
        ));

        mockMvc.perform(get("/sitemap-xml/authors/1.xml"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_XML))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("/sitemap/authors/A/1")));
    }

    @Test
    @DisplayName("GET /sitemap-xml/authors/4.xml returns 404 when page exceeds total")
    void authorsSitemapOutOfRangeReturns404() throws Exception {
        when(sitemapService.getAuthorXmlPageCount()).thenReturn(2);

        mockMvc.perform(get("/sitemap-xml/authors/4.xml"))
                .andExpect(status().isNotFound());
    }
}
