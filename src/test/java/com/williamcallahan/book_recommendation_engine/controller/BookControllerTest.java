/**
 * Test suite for BookController REST API endpoints
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.controller;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.test.context.support.WithAnonymousUser;
import org.springframework.test.context.TestPropertySource;
import com.williamcallahan.book_recommendation_engine.config.SecurityConfig;
import com.williamcallahan.book_recommendation_engine.config.CustomBasicAuthenticationEntryPoint;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookCacheFacadeService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
import com.williamcallahan.book_recommendation_engine.service.S3RetryService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService; // Corrected import path
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.mockito.Mockito;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.hamcrest.Matchers.*;

@WebMvcTest(com.williamcallahan.book_recommendation_engine.controller.BookController.class)
@Import({SecurityConfig.class, BookControllerTest.BookControllerTestConfiguration.class}) // Import SecurityConfig and explicit test config
@WithAnonymousUser // Apply anonymous user for now, will refine for POST/PUT/DELETE
@TestPropertySource(properties = {
    "app.security.admin.password=testadminpass",
    "app.security.user.password=testuserpass",
    "app.clicky.enabled=false" // Disable clicky for these tests if not relevant
})
class BookControllerTest {

    @TestConfiguration
    static class BookControllerTestConfiguration {

        @Bean
        public BookCacheFacadeService bookCacheFacadeService() {
            return Mockito.mock(BookCacheFacadeService.class);
        }

        @Bean
        public RecommendationService recommendationService() {
            return Mockito.mock(RecommendationService.class);
        }

        @Bean
        public RecentlyViewedService recentlyViewedService() {
            return Mockito.mock(RecentlyViewedService.class);
        }

        @Bean
        public BookImageOrchestrationService bookImageOrchestrationService() {
            return Mockito.mock(BookImageOrchestrationService.class);
        }

        @Bean
        public WebClient.Builder webClientBuilder() {
            WebClient.Builder builderMock = Mockito.mock(WebClient.Builder.class);
            WebClient clientMock = Mockito.mock(WebClient.class);
            Mockito.when(builderMock.baseUrl(anyString())).thenReturn(builderMock);
            Mockito.when(builderMock.build()).thenReturn(clientMock);
            return builderMock;
        }

        @Bean
        public S3RetryService s3RetryService() {
            return Mockito.mock(S3RetryService.class);
        }

        @Bean
        public boolean isYearFilteringEnabled() {
            return false;
        }

        @Bean
        public CustomBasicAuthenticationEntryPoint customBasicAuthenticationEntryPoint() {
            return Mockito.mock(CustomBasicAuthenticationEntryPoint.class);
        }
    }

  @Autowired
  private MockMvc mockMvc;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private BookCacheFacadeService bookCacheFacadeService;

  @Autowired
  private RecommendationService recommendationService;

  @Autowired
  private S3RetryService s3RetryService;
  
  @Autowired
  private BookImageOrchestrationService bookImageOrchestrationService; 

  @AfterEach
  void tearDown() {
    reset(bookCacheFacadeService, recommendationService, s3RetryService, bookImageOrchestrationService); 
  }

  @BeforeEach
  void commonMockSetup() {
    // Default mocks for BookCacheFacadeService, can be overridden by specific tests
    when(bookCacheFacadeService.getBookByIdReactive(anyString())).thenReturn(Mono.empty());
    when(bookCacheFacadeService.getBooksByIsbnReactive(anyString())).thenReturn(Mono.just(Collections.emptyList()));
    // Default mock for removeBook, can be overridden by specific tests
    when(bookCacheFacadeService.removeBook(anyString())).thenReturn(CompletableFuture.completedFuture(null));
    // Default mock for S3RetryService
    when(s3RetryService.updateBookJsonWithRetry(any(Book.class))).thenReturn(CompletableFuture.completedFuture(null)); // Returns CompletableFuture<Void>
    when(bookImageOrchestrationService.getBestCoverUrlAsync(any(Book.class), any(CoverImageSource.class), any(ImageResolutionPreference.class)))
        .thenAnswer(invocation -> {
            Book book = invocation.getArgument(0);
            return CompletableFuture.completedFuture(book.getCoverImageUrl());
        });
  }

  /**
   * Creates a test book with basic properties for testing
   *
   * @param id The Google Books ID to assign
   * @param title Book title
   * @param author Book author name
   * @return Configured book instance
   */
  private Book createTestBook(String id, String title, String author) {
      Book book = new Book();
      book.setId(id);
      book.setTitle(title);
      book.setAuthors(List.of(author));
      book.setDescription("Test description for " + title);
      book.setCoverImageUrl("http://example.com/cover/" + (id != null ? id : "new") + ".jpg");
      book.setImageUrl("http://example.com/image/" + (id != null ? id : "new") + ".jpg");
      return book;
  }

  @Test
  @DisplayName("GET /api/books/search - empty list returns 200 and [] in results")
  void searchBooks_emptyList_returnsEmptyArrayInResults() throws Exception {
    when(bookCacheFacadeService.searchBooksReactive(eq("*"), eq(0), anyInt(), eq(null), eq(null), eq(null))).thenReturn(Mono.just(Collections.emptyList()));
    
    org.springframework.test.web.servlet.MvcResult mvcResult = mockMvc.perform(get("/api/books/search").param("query", ""))
      .andExpect(request().asyncStarted())
      .andReturn();

    mockMvc.perform(asyncDispatch(mvcResult))
      .andExpect(status().isOk())
      .andExpect(content().contentType(MediaType.APPLICATION_JSON))
      .andExpect(jsonPath("$.results", hasSize(0)));
  }

  @Test
  @DisplayName("GET /api/books/search - non-empty list returns 200 and array of books in results")
  void searchBooks_nonEmptyList_returnsArrayInResults() throws Exception {
    Book book = createTestBook("1", "Effective Java", "Joshua Bloch");
    when(bookCacheFacadeService.searchBooksReactive(eq("*"), eq(0), anyInt(), eq(null), eq(null), eq(null))).thenReturn(Mono.just(List.of(book)));
    
    org.springframework.test.web.servlet.MvcResult mvcResult = mockMvc.perform(get("/api/books/search").param("query", ""))
      .andExpect(request().asyncStarted())
      .andReturn();

    mockMvc.perform(asyncDispatch(mvcResult))
      .andExpect(status().isOk())
      .andExpect(content().contentType(MediaType.APPLICATION_JSON))
      .andExpect(jsonPath("$.results", hasSize(1)))
      .andExpect(jsonPath("$.results[0].id").value("1"))
      .andExpect(jsonPath("$.results[0].title").value("Effective Java"))
      .andExpect(jsonPath("$.results[0].authors[0]").value("Joshua Bloch"));
  }

  @Test
  @DisplayName("GET /api/books/{id} - existing id returns 200 and book JSON")
  void getBookById_found_returnsBook() throws Exception {
    Book book = createTestBook("1", "Domain-Driven Design", "Eric Evans");
    when(bookCacheFacadeService.getBookByIdReactive("1")).thenReturn(Mono.just(book));

    org.springframework.test.web.servlet.MvcResult mvcResult = mockMvc.perform(get("/api/books/1"))
      .andExpect(request().asyncStarted())
      .andReturn();

    mockMvc.perform(asyncDispatch(mvcResult))
      .andExpect(status().isOk())
      .andExpect(content().contentType(MediaType.APPLICATION_JSON))
      .andExpect(jsonPath("$.id").value("1"))
      .andExpect(jsonPath("$.title").value("Domain-Driven Design"))
      .andExpect(jsonPath("$.authors[0]").value("Eric Evans"));
  }

  @Test
  @DisplayName("GET /api/books/{id} - non-existent id returns 404")
  void getBookById_notFound_returns404() throws Exception {
    when(bookCacheFacadeService.getBookByIdReactive("99")).thenReturn(Mono.empty());
    
    org.springframework.test.web.servlet.MvcResult mvcResult = mockMvc.perform(get("/api/books/99"))
      .andExpect(request().asyncStarted())
      .andReturn();

    mockMvc.perform(asyncDispatch(mvcResult))
      .andExpect(status().isNotFound());
  }

  @Test
  @DisplayName("POST /api/books - valid input returns 201 and created book")
  void createBook_validInput_returnsCreated() throws Exception {
    Book input = createTestBook(null, "Clean Code", "Robert C. Martin"); 

    doAnswer(invocation -> {
        Book bookArg = invocation.getArgument(0);
        bookArg.setId("1"); 
        return null; 
    }).when(bookCacheFacadeService).cacheBook(any(Book.class));

    org.springframework.test.web.servlet.MvcResult mvcResult = mockMvc.perform(post("/api/books").with(csrf())
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(input)))
        .andExpect(request().asyncStarted())
        .andReturn();

    mockMvc.perform(asyncDispatch(mvcResult))
        .andExpect(status().isCreated()) 
        .andExpect(header().string("Location", containsString("/api/books/1"))) 
        .andExpect(jsonPath("$.id").value("1")) 
        .andExpect(jsonPath("$.title").value("Clean Code"))
        .andExpect(jsonPath("$.authors[0]").value("Robert C. Martin"));
  }

  @Test
  @DisplayName("POST /api/books - invalid input returns 400")
  void createBook_invalidInput_returnsBadRequest() throws Exception {
    Book invalid = createTestBook(null, "", "Author"); 
    Mockito.doThrow(new IllegalArgumentException("Title cannot be empty"))
      .when(bookCacheFacadeService).cacheBook(any(Book.class));
      
    org.springframework.test.web.servlet.MvcResult mvcResult = mockMvc.perform(post("/api/books").with(csrf())
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(invalid)))
        .andExpect(request().asyncStarted())
        .andReturn();
        
    mockMvc.perform(asyncDispatch(mvcResult))
      .andExpect(status().isBadRequest());
  }

  @Test
  @DisplayName("PUT /api/books/{id} - existing id returns 200 and updated book")
  void updateBook_found_returnsUpdated() throws Exception {
    Book existingBook = createTestBook("1", "Old Title", "Old Author");
    Book updatePayload = createTestBook("1", "Refactoring", "Martin Fowler"); 

    when(bookCacheFacadeService.getBookByIdReactive("1")).thenReturn(Mono.just(existingBook));
    when(bookCacheFacadeService.updateBook(any(Book.class))).thenReturn(CompletableFuture.completedFuture(null));

    org.springframework.test.web.servlet.MvcResult mvcResult = mockMvc.perform(put("/api/books/1").with(csrf()) 
        .contentType(MediaType.APPLICATION_JSON)
        .content(objectMapper.writeValueAsString(updatePayload)))
      .andExpect(request().asyncStarted())
      .andReturn();

    mockMvc.perform(asyncDispatch(mvcResult))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.id", is("1")))
      .andExpect(jsonPath("$.title", is("Refactoring")));
  }

  @Test
  @DisplayName("PUT /api/books/{id} - non-existent id returns 200 (upsert)")
  void updateBook_notFound_returns200() throws Exception {
    Book updatePayload = createTestBook("99", "Non Existent", "Author"); // ID should be set for upsert

    when(bookCacheFacadeService.getBookByIdReactive("99")).thenReturn(Mono.empty()); // Simulate not found
    when(bookCacheFacadeService.updateBook(any(Book.class))).thenReturn(CompletableFuture.completedFuture(null));

    org.springframework.test.web.servlet.MvcResult mvcResult = mockMvc.perform(put("/api/books/99").with(csrf()) 
        .contentType(MediaType.APPLICATION_JSON)
        .content(objectMapper.writeValueAsString(updatePayload)))
      .andExpect(request().asyncStarted())
      .andReturn();

    mockMvc.perform(asyncDispatch(mvcResult))
      .andExpect(status().isOk()) // Controller has upsert logic, so 200 OK
      .andExpect(jsonPath("$.id", is("99")))
      .andExpect(jsonPath("$.title", is("Non Existent")));
  }

  @Test
  @DisplayName("DELETE /api/books/{id} - existing id returns 200")
  void deleteBook_found_returnsOk() throws Exception {
    when(bookCacheFacadeService.removeBook("1")).thenReturn(CompletableFuture.completedFuture(null));

    mockMvc.perform(delete("/api/books/1").with(csrf())) 
      .andExpect(status().isOk());
  }

  @Test
  @DisplayName("DELETE /api/books/{id} - non-existent id returns 200")
  void deleteBook_notFound_returnsOk() throws Exception {
    // Controller always returns 200 OK for delete, even if book not found by removeBook.
    when(bookCacheFacadeService.removeBook("99")).thenReturn(CompletableFuture.completedFuture(null));

    mockMvc.perform(delete("/api/books/99").with(csrf())) 
      .andExpect(status().isOk());
  }
}
