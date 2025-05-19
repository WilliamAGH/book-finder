/**
 * Test suite for BookController REST API endpoints
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.controller;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookCacheService;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.hamcrest.Matchers.*;
import java.util.Collections;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import com.williamcallahan.book_recommendation_engine.service.S3RetryService;
import org.mockito.Mockito;
import static org.mockito.ArgumentMatchers.anyString;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import java.util.concurrent.CompletableFuture;

@WebMvcTest(value = BookController.class,
    excludeAutoConfiguration = {
        org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration.class,
        org.springframework.boot.autoconfigure.security.servlet.SecurityFilterAutoConfiguration.class
    })
class BookControllerTest {

    @TestConfiguration
    static class BookControllerTestConfiguration {

        @Bean
        public BookCacheService bookCacheService() {
            return Mockito.mock(BookCacheService.class);
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
        public S3RetryService s3RetryService() {
            return Mockito.mock(S3RetryService.class);
        }
        
        @Bean
        public boolean isYearFilteringEnabled() {
            // Default value for tests
            return false;
        }

        @Bean
        public WebClient.Builder webClientBuilder() {
            WebClient.Builder builderMock = Mockito.mock(WebClient.Builder.class);
            WebClient clientMock = Mockito.mock(WebClient.class);
            Mockito.when(builderMock.baseUrl(anyString())).thenReturn(builderMock);
            Mockito.when(builderMock.build()).thenReturn(clientMock);
            return builderMock;
        }
    }

  @Autowired
  private MockMvc mockMvc;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private BookCacheService bookCacheService;

  @Autowired
  private RecommendationService recommendationService;
  
  @Autowired
  private BookImageOrchestrationService bookImageOrchestrationService; 

  @AfterEach
  void tearDown() {
    reset(bookCacheService, recommendationService, bookImageOrchestrationService); 
  }

  @BeforeEach
  void commonMockSetup() {
    when(bookImageOrchestrationService.getBestCoverUrlAsync(any(Book.class), any(CoverImageSource.class), any(ImageResolutionPreference.class)))
        .thenAnswer(invocation -> {
            // Book book = invocation.getArgument(0); // This line is not strictly necessary if we only return a URL
            // if (book != null) {
            //     // Mutating the book object here might not be seen by the caller if only the Future<String> is used.
            //     // If the intention is to ensure the book object passed in gets these values set,
            //     // this approach is fine, but the return type of the mock must match the method signature.
            //     book.setCoverImageUrl("http://example.com/fake-cover.jpg");
            //     book.setCoverImages(new CoverImages(
            //         "http://example.com/fake-cover.jpg", 
            //         "http://example.com/fake-cover-fallback.jpg",
            //         CoverImageSource.LOCAL_CACHE));
            // }
            // Return the expected URL string, not the Book instance
            return CompletableFuture.completedFuture("http://example.com/fake-cover.jpg");
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
    when(bookCacheService.searchBooksReactive(eq("*"), eq(0), anyInt(), eq(null), eq(null), eq(null))).thenReturn(Mono.just(Collections.emptyList()));
    
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
    when(bookCacheService.searchBooksReactive(eq("*"), eq(0), anyInt(), eq(null), eq(null), eq(null))).thenReturn(Mono.just(List.of(createTestBook("1", "Effective Java", "Joshua Bloch"))));
    
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
    when(bookCacheService.getBookByIdReactive("1")).thenReturn(Mono.just(createTestBook("1", "Domain-Driven Design", "Eric Evans")));

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
    // Mock to correctly simulate a book not being found
    when(bookCacheService.getBookByIdReactive("99")).thenReturn(Mono.empty());
    
    org.springframework.test.web.servlet.MvcResult mvcResult = mockMvc.perform(get("/api/books/99"))
      .andExpect(request().asyncStarted())
      .andReturn();

    mockMvc.perform(asyncDispatch(mvcResult))
      .andExpect(status().isNotFound())
      .andExpect(jsonPath("$.error").value("Not Found"))
      .andExpect(jsonPath("$.message").value("Book not found with ID: 99"));
  }

  @Test
  @DisplayName("POST /api/books - valid input returns 201 and created book")
  void createBook_validInput_returnsCreated() throws Exception {
    Book input = createTestBook(null, "Clean Code", "Robert C. Martin"); 

    doAnswer(invocation -> {
        Book bookArg = invocation.getArgument(0);
        bookArg.setId("1"); 
        return null; 
    }).when(bookCacheService).cacheBook(any(Book.class));

    org.springframework.test.web.servlet.MvcResult mvcResult = mockMvc.perform(post("/api/books")
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
      .when(bookCacheService).cacheBook(any(Book.class));
      
    org.springframework.test.web.servlet.MvcResult mvcResult = mockMvc.perform(post("/api/books")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(invalid)))
        .andExpect(request().asyncStarted())
        .andReturn();
        
    mockMvc.perform(asyncDispatch(mvcResult))
      .andExpect(status().isBadRequest());
  }

  @Test
  @DisplayName("PUT /api/books/{id} - returns 200 OK")
  void updateBook_returns200() throws Exception {
    Book updatePayload = createTestBook(null, "Refactoring", "Martin Fowler");
    Book existingBook = createTestBook("1", "Clean Code", "Robert C. Martin");
    
    // Mock to avoid NullPointerException
    when(bookCacheService.getBookByIdReactive(eq("1"))).thenReturn(Mono.just(existingBook));
    
    mockMvc.perform(put("/api/books/1") 
        .contentType(MediaType.APPLICATION_JSON)
        .content(objectMapper.writeValueAsString(updatePayload)))
      .andExpect(status().isOk());
  }

  @Test
  @DisplayName("PUT /api/books/{id} - different id returns 200 OK")
  void updateBook_differentId_returns200() throws Exception {
    Book updatePayload = createTestBook(null, "Non Existent", "Author");
    
    // Mock to avoid NullPointerException and return empty for ID 99
    when(bookCacheService.getBookByIdReactive(eq("99"))).thenReturn(Mono.empty());
    
    mockMvc.perform(put("/api/books/99") 
        .contentType(MediaType.APPLICATION_JSON)
        .content(objectMapper.writeValueAsString(updatePayload)))
      .andExpect(status().isOk());
  }

  @Test
  @DisplayName("DELETE /api/books/{id} - returns 200 OK")
  void deleteBook_returns200() throws Exception {
    Book existingBook = createTestBook("1", "Clean Code", "Robert C. Martin");
    
    // Mock to avoid NullPointerException
    when(bookCacheService.getBookByIdReactive(eq("1"))).thenReturn(Mono.just(existingBook));
    
    mockMvc.perform(delete("/api/books/1")) 
      .andExpect(status().isOk());
  }

  @Test
  @DisplayName("DELETE /api/books/{id} - different id returns 200 OK")
  void deleteBook_differentId_returns200() throws Exception {
    // Mock to avoid NullPointerException
    when(bookCacheService.getBookByIdReactive(eq("99"))).thenReturn(Mono.empty());
    
    mockMvc.perform(delete("/api/books/99")) 
      .andExpect(status().isOk());
  }
}
