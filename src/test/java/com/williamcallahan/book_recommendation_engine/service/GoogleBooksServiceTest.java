/**
 * Test suite for GoogleBooksService
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.image.ExternalCoverFetchHelper;
import com.williamcallahan.book_recommendation_engine.service.image.GoogleCoverUrlEvaluator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.CompletionStage;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atLeastOnce;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GoogleBooksServiceTest {

    @Mock
    private ApiRequestMonitor apiRequestMonitorMock;

    @Mock
    private GoogleApiFetcher googleApiFetcherMock;

    @Mock // Added mock for BookDataOrchestrator
    private BookDataOrchestrator bookDataOrchestratorMock;

    @Mock // New dependency required by GoogleBooksService constructor
    private ExternalCoverFetchHelper externalCoverFetchHelperMock;

    @Mock // Additional dependency required by newer constructor
    private GoogleCoverUrlEvaluator googleCoverUrlEvaluatorMock;


    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    private GoogleBooksService googleBooksService;

    /**
     * Sets up the test environment before each test
     */
    @BeforeEach
    void setUp() {
        // Manually instantiate GoogleBooksService with new constructor
        // Old constructor: new GoogleBooksService(webClientBuilderMock, s3RetryServiceMock, objectMapper, apiRequestMonitorMock);
        googleBooksService = new GoogleBooksService(
                objectMapper,
                apiRequestMonitorMock,
                googleApiFetcherMock,
                bookDataOrchestratorMock,
                externalCoverFetchHelperMock,
                googleCoverUrlEvaluatorMock
        );

        // @Value fields are no longer in GoogleBooksService, they are in GoogleApiFetcher
        // ReflectionTestUtils.setField(googleBooksService, "googleBooksApiUrl", "http://fakeapi.com");
        // ReflectionTestUtils.setField(googleBooksService, "googleBooksApiKey", "fakeKey");

        // WebClient mock setup is no longer needed here as GoogleBooksService delegates to GoogleApiFetcher
        // doReturn(requestHeadersUriSpecMock).when(webClientMock).get();
        // doReturn(requestHeadersSpecMock).when(requestHeadersUriSpecMock).uri(anyString());
        // when(requestHeadersSpecMock.retrieve()).thenReturn(responseSpecMock);
    }

    /**
     * Creates a mock Google Books API JSON response for a book
     *
     * @param id Book identifier
     * @param title Book title
     * @param author Book author
     * @return JsonNode representing a Google Books volume
     */
    private JsonNode createMockVolumeJson(String id, String title, String author) {
return com.williamcallahan.book_recommendation_engine.testutil.GoogleBooksStubs.volume(objectMapper, id, title, author);
    }

    /**
     * Verifies searchBooksAsyncReactive returns a list of books when API returns results
     */
    @Test
    void searchBooksAsyncReactive_returnsListOfBooks_whenApiReturnsItems() {
        ObjectNode mockApiResponse = objectMapper.createObjectNode();
        ArrayNode items = objectMapper.createArrayNode();
        items.add(createMockVolumeJson("id1", "Title 1", "Author 1"));
        mockApiResponse.set("items", items);

        // Mock the call to googleApiFetcherMock
com.williamcallahan.book_recommendation_engine.testutil.GoogleBooksStubs.stubSearchReturns(
                googleApiFetcherMock, "test query", "relevance", "en", mockApiResponse);

        Mono<List<Book>> result = googleBooksService.searchBooksAsyncReactive("test query", "en", 10, "relevance");

        StepVerifier.create(result)
                .expectNextMatches(books -> {
                    assertFalse(books.isEmpty());
                    assertEquals(1, books.size());
                    Book book = books.get(0);
                    assertEquals("id1", book.getId());
                    assertEquals("Title 1", book.getTitle());
                    assertTrue(book.getAuthors().contains("Author 1"));
                    return true;
                })
                .verifyComplete();
        // API monitoring is now handled by GoogleApiFetcher, not GoogleBooksService
    }

    /**
     * Verifies searchBooksAsyncReactive returns empty list when API returns no results
     */
    @Test
    void searchBooksAsyncReactive_returnsEmptyList_whenApiReturnsNoItems() {
        ObjectNode mockApiResponse = objectMapper.createObjectNode();
        mockApiResponse.set("items", objectMapper.createArrayNode()); // Empty items array

        // Mock the call to googleApiFetcherMock
com.williamcallahan.book_recommendation_engine.testutil.GoogleBooksStubs.stubSearchReturns(
                googleApiFetcherMock, "test query", "relevance", "en", mockApiResponse);
        
        Mono<List<Book>> result = googleBooksService.searchBooksAsyncReactive("test query", "en", 10, "relevance");

        StepVerifier.create(result)
                .expectNextMatches(List::isEmpty)
                .verifyComplete();
        // API monitoring is now handled by GoogleApiFetcher, not GoogleBooksService
    }
    
    /**
     * Verifies searchBooksAsyncReactive handles API errors gracefully
     */
    @Test
    void searchBooksAsyncReactive_handlesApiErrorGracefully() {
        // Mock the call to googleApiFetcherMock to return an error
com.williamcallahan.book_recommendation_engine.testutil.GoogleBooksStubs.stubSearchError(
                googleApiFetcherMock, "error query", "relevance", "en", new RuntimeException("API error"));

        Mono<List<Book>> result = googleBooksService.searchBooksAsyncReactive("error query", "en", 10, "relevance");
        
        StepVerifier.create(result)
            .expectNextMatches(books -> books.isEmpty()) // Explicitly check that the list is empty
            .verifyComplete();
        // Verify that the fallback method recorded the failure
        verify(apiRequestMonitorMock, atLeastOnce()).recordFailedRequest(anyString(), anyString());
    }

    /**
     * Verifies getBookById fetches from API (via GoogleApiFetcher).
     * S3 logic is now in BookDataOrchestrator, so this test focuses on the direct API call path.
     *
     * @throws Exception if test fails
     */
    @Test
    void getBookById_returnsBookFromApiFetcher() throws Exception {
        String bookId = "apiId1";
        JsonNode mockVolumeNode = createMockVolumeJson(bookId, "API Book", "API Author");

        // Mock GoogleApiFetcher to return the mock volume
com.williamcallahan.book_recommendation_engine.testutil.GoogleBooksStubs.stubFetchVolumeReturns(
                googleApiFetcherMock, bookId, mockVolumeNode);

        StepVerifier.create(Mono.fromCompletionStage(googleBooksService.getBookById(bookId)))
                .assertNext(book -> {
                    assertNotNull(book);
                    assertEquals(bookId, book.getId());
                    assertEquals("API Book", book.getTitle());
                    assertTrue(book.getAuthors().contains("API Author"));
                })
                .verifyComplete();
    }
    
    /**
     * Verifies the service handles books with null or empty authors gracefully
     */
    @Test
    void searchBooksAsyncReactive_handlesNullOrEmptyAuthors() {
        ObjectNode mockApiResponse = objectMapper.createObjectNode();
        ArrayNode items = objectMapper.createArrayNode();
        
        // Create a book with null authors field
        ObjectNode volumeWithNullAuthors = objectMapper.createObjectNode();
        volumeWithNullAuthors.put("id", "id1");
        ObjectNode volumeInfoNull = objectMapper.createObjectNode();
        volumeInfoNull.put("title", "Book with Null Authors");
        // Intentionally not setting authors
        volumeWithNullAuthors.set("volumeInfo", volumeInfoNull);
        items.add(volumeWithNullAuthors);
        
        // Create a book with empty authors array
        ObjectNode volumeWithEmptyAuthors = objectMapper.createObjectNode();
        volumeWithEmptyAuthors.put("id", "id2");
        ObjectNode volumeInfoEmpty = objectMapper.createObjectNode();
        volumeInfoEmpty.put("title", "Book with Empty Authors");
        volumeInfoEmpty.set("authors", objectMapper.createArrayNode()); // Empty array
        volumeWithEmptyAuthors.set("volumeInfo", volumeInfoEmpty);
        items.add(volumeWithEmptyAuthors);
        
        mockApiResponse.set("items", items);

        // Mock the call to googleApiFetcherMock
        when(googleApiFetcherMock.searchVolumesAuthenticated(eq("test query"), anyInt(), eq("relevance"), eq("en")))
                .thenReturn(Mono.just(mockApiResponse));

        Mono<List<Book>> result = googleBooksService.searchBooksAsyncReactive("test query", "en", 10, "relevance");

        StepVerifier.create(result)
                .expectNextMatches(books -> {
                    assertEquals(2, books.size());
                    
                    // First book should have null authors transformed to empty list
                    Book book1 = books.get(0);
                    assertEquals("id1", book1.getId());
                    assertEquals("Book with Null Authors", book1.getTitle());
                    assertNotNull(book1.getAuthors());
                    assertTrue(book1.getAuthors().isEmpty());
                    
                    // Second book should also have empty authors list
                    Book book2 = books.get(1);
                    assertEquals("id2", book2.getId());
                    assertEquals("Book with Empty Authors", book2.getTitle());
                    assertNotNull(book2.getAuthors());
                    assertTrue(book2.getAuthors().isEmpty());
                    
                    return true;
                })
                .verifyComplete();
    }
    
    /**
     * Verifies getBookById handles API errors from GoogleApiFetcher gracefully
     */
    @Test
    void getBookById_handlesApiErrorFromFetcher() {
        String bookId = "errorId";
        // Mock GoogleApiFetcher to return an error
com.williamcallahan.book_recommendation_engine.testutil.GoogleBooksStubs.stubFetchVolumeError(
                googleApiFetcherMock, bookId, new RuntimeException("Fetcher API error"));

        CompletionStage<Book> resultStage = googleBooksService.getBookById(bookId);
        
        StepVerifier.create(Mono.fromCompletionStage(resultStage))
            .verifyComplete(); // no item emitted – just completion
         // Verify that the fallback method recorded the failure
        verify(apiRequestMonitorMock).recordFailedRequest(anyString(), anyString());
    }
}
