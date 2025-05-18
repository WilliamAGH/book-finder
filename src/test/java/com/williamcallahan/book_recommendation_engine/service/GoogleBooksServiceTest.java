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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.mockito.ArgumentMatchers.anyString;
import com.williamcallahan.book_recommendation_engine.types.S3FetchResult;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doReturn;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class GoogleBooksServiceTest {

    @Mock(answer = Answers.RETURNS_SELF)
    private WebClient.Builder webClientBuilderMock;

    @Mock
    private WebClient webClientMock;
    @Mock
    private WebClient.RequestHeadersUriSpec<?> requestHeadersUriSpecMock;
    @Mock
    private WebClient.RequestHeadersSpec<?> requestHeadersSpecMock;
    @Mock
    private WebClient.ResponseSpec responseSpecMock;

    @Mock
    private S3RetryService s3RetryServiceMock;

    @Mock
    private ApiRequestMonitor apiRequestMonitorMock;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    // Remove @InjectMocks, will be manually initialized
    private GoogleBooksService googleBooksService;

    /**
     * Sets up the test environment before each test
     */
    @BeforeEach
    void setUp() {
        // Configure the WebClient.Builder to return the WebClient mock
        // This MUST be done before GoogleBooksService is instantiated
        lenient().when(webClientBuilderMock.build()).thenReturn(webClientMock);

        // Manually instantiate GoogleBooksService with mocks
        googleBooksService = new GoogleBooksService(
                webClientBuilderMock,
                s3RetryServiceMock,
                objectMapper, // Use the @Spy instance
                apiRequestMonitorMock
        );

        // Set @Value fields on the manually instantiated service
        ReflectionTestUtils.setField(googleBooksService, "googleBooksApiUrl", "http://fakeapi.com");
        ReflectionTestUtils.setField(googleBooksService, "googleBooksApiKey", "fakeKey");

        // Configure the WebClient mock (returned by webClientBuilderMock.build())
        doReturn(requestHeadersUriSpecMock).when(webClientMock).get();
        doReturn(requestHeadersSpecMock).when(requestHeadersUriSpecMock).uri(anyString());
        when(requestHeadersSpecMock.retrieve()).thenReturn(responseSpecMock);
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
        ObjectNode volume = objectMapper.createObjectNode();
        volume.put("id", id);
        ObjectNode volumeInfo = objectMapper.createObjectNode();
        volumeInfo.put("title", title);
        ArrayNode authors = objectMapper.createArrayNode();
        authors.add(author);
        volumeInfo.set("authors", authors);
        ObjectNode imageLinks = objectMapper.createObjectNode();
        imageLinks.put("thumbnail", "http://example.com/thumbnail.jpg");
        volumeInfo.set("imageLinks", imageLinks);
        volume.set("volumeInfo", volumeInfo);
        return volume;
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

        when(responseSpecMock.bodyToMono(eq(JsonNode.class))).thenReturn(Mono.just(mockApiResponse));

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
        verify(apiRequestMonitorMock).recordSuccessfulRequest(anyString());
    }

    /**
     * Verifies searchBooksAsyncReactive returns empty list when API returns no results
     */
    @Test
    void searchBooksAsyncReactive_returnsEmptyList_whenApiReturnsNoItems() {
        ObjectNode mockApiResponse = objectMapper.createObjectNode();
        mockApiResponse.set("items", objectMapper.createArrayNode()); // Empty items array

        when(responseSpecMock.bodyToMono(eq(JsonNode.class))).thenReturn(Mono.just(mockApiResponse));

        Mono<List<Book>> result = googleBooksService.searchBooksAsyncReactive("test query", "en", 10, "relevance");

        StepVerifier.create(result)
                .expectNextMatches(List::isEmpty)
                .verifyComplete();
        verify(apiRequestMonitorMock).recordSuccessfulRequest(anyString());
    }
    
    /**
     * Verifies searchBooksAsyncReactive handles API errors gracefully
     */
    @Test
    void searchBooksAsyncReactive_handlesApiErrorGracefully() {
        when(responseSpecMock.bodyToMono(eq(JsonNode.class))).thenReturn(Mono.error(new RuntimeException("API error")));

        Mono<List<Book>> result = googleBooksService.searchBooksAsyncReactive("error query", "en", 10, "relevance");
        
        StepVerifier.create(result)
            .expectNextMatches(List::isEmpty)
            .verifyComplete();
        verify(apiRequestMonitorMock).recordFailedRequest(anyString(), anyString());
    }

    /**
     * Verifies getBookById fetches from API when book not in S3 cache
     *
     * @throws Exception if test fails
     */
    @Test
    void getBookById_returnsBookFromApi_whenNotInS3Cache() throws Exception {
        String bookId = "apiId1";
        JsonNode mockVolumeNode = createMockVolumeJson(bookId, "API Book", "API Author");

        when(s3RetryServiceMock.fetchJsonWithRetry(bookId)).thenReturn(CompletableFuture.completedFuture(S3FetchResult.notFound()));
        when(responseSpecMock.bodyToMono(eq(JsonNode.class))).thenReturn(Mono.just(mockVolumeNode));
        lenient().when(s3RetryServiceMock.uploadJsonWithRetry(eq(bookId), anyString())).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Book> resultFuture = googleBooksService.getBookById(bookId).toCompletableFuture();

        Book book = resultFuture.get();

        assertNotNull(book);
        assertEquals(bookId, book.getId());
        assertEquals("API Book", book.getTitle());
        assertTrue(book.getAuthors().contains("API Author"));
        verify(s3RetryServiceMock).uploadJsonWithRetry(eq(bookId), anyString());
    }

    /**
     * Verifies getBookById returns book from S3 cache when present
     *
     * @throws Exception if test fails
     */
    @Test
    void getBookById_returnsBookFromS3Cache_whenPresent() throws Exception {
        String bookId = "s3Id1";
        JsonNode cachedVolumeNode = createMockVolumeJson(bookId, "S3 Book", "S3 Author");
        String cachedJsonString = objectMapper.writeValueAsString(cachedVolumeNode);

        when(s3RetryServiceMock.fetchJsonWithRetry(bookId)).thenReturn(CompletableFuture.completedFuture(S3FetchResult.success(cachedJsonString)));

        CompletableFuture<Book> resultFuture = googleBooksService.getBookById(bookId).toCompletableFuture();
        Book book = resultFuture.get();

        assertNotNull(book);
        assertEquals(bookId, book.getId());
        assertEquals("S3 Book", book.getTitle());
        assertTrue(book.getAuthors().contains("S3 Author"));
    }
    
    /**
     * Verifies getBookById handles API errors gracefully after S3 cache miss
     */
    @Test
    void getBookById_handlesApiErrorAfterS3Miss() {
        String bookId = "errorId";
        when(s3RetryServiceMock.fetchJsonWithRetry(bookId)).thenReturn(CompletableFuture.completedFuture(S3FetchResult.notFound()));
        when(responseSpecMock.bodyToMono(eq(JsonNode.class))).thenReturn(Mono.error(new RuntimeException("API error")));

        CompletionStage<Book> resultStage = googleBooksService.getBookById(bookId);
        
        StepVerifier.create(Mono.fromCompletionStage(resultStage))
            .expectErrorMatches(throwable -> throwable instanceof RuntimeException && "API error".equals(throwable.getMessage()))
            .verify();
    }

    /**
     * Verifies getBookById handles S3 JSON parsing errors gracefully
     */
    @Test
    void getBookById_handlesS3JsonParsingError() {
        String bookId = "parseErrorId";
        String wellFormedJsonFromS3 = "{ \"id\": \"testId\", \"volumeInfo\": { \"title\": \"S3 Test Title\", \"authors\": [\"S3 Test Author\"] } }";

        when(s3RetryServiceMock.fetchJsonWithRetry(bookId)).thenReturn(CompletableFuture.completedFuture(S3FetchResult.success(wellFormedJsonFromS3)));
        JsonNode mockApiVolumeNode = createMockVolumeJson(bookId, "API Fallback Book", "API Fallback Author");
        when(responseSpecMock.bodyToMono(eq(JsonNode.class))).thenReturn(Mono.just(mockApiVolumeNode));
        lenient().when(s3RetryServiceMock.uploadJsonWithRetry(eq(bookId), anyString())).thenReturn(CompletableFuture.completedFuture(null));

        CompletableFuture<Book> resultFuture = googleBooksService.getBookById(bookId).toCompletableFuture();

        StepVerifier.create(Mono.fromFuture(resultFuture))
            .expectNextMatches(book -> {
                assertNotNull(book);
                assertEquals("testId", book.getId());
                assertEquals("S3 Test Title", book.getTitle());
                assertTrue(book.getAuthors().contains("S3 Test Author"));
                return true;
            })
            .verifyComplete();
    }
}
