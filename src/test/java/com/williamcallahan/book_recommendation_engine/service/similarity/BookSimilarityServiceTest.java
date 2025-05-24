/**
 * Unit tests for the BookSimilarityService
 *
 * @author William Callahan
 *
 * Tests the following functionality:
 * - Embedding generation for books with valid and invalid data
 * - Placeholder embedding creation when embedding service is disabled
 * - External embedding service integration when enabled
 * - Error handling and fallback mechanisms
 * - Deterministic placeholder embedding generation
 * - Edge cases including null and empty book fields
 */
package com.williamcallahan.book_recommendation_engine.service.similarity;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.cache.BookReactiveCacheService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BookSimilarityServiceTest {

    @Mock
    private CachedBookRepository cachedBookRepository;

    @Mock
    private GoogleBooksService googleBooksService;

    @Mock
    private BookReactiveCacheService bookReactiveCacheService;

    @Mock
    private WebClient.Builder webClientBuilder;

    @Mock
    private WebClient webClient;

    @Mock
    private WebClient.RequestBodyUriSpec requestBodyUriSpec;

    @Mock
    private WebClient.RequestHeadersSpec<?> requestHeadersSpec;

    @Mock
    private WebClient.ResponseSpec responseSpec;

    @Mock
    private AsyncTaskExecutor mvcTaskExecutor;

    private BookSimilarityService bookSimilarityService;

    @BeforeEach
    void setUp() {
        when(webClientBuilder.baseUrl(any(String.class))).thenReturn(webClientBuilder);
        when(webClientBuilder.build()).thenReturn(webClient);
        
        // Test with embedding service disabled by default
        bookSimilarityService = new BookSimilarityService(
            cachedBookRepository,
            googleBooksService,
            bookReactiveCacheService,
            null, // embeddingServiceUrl
            webClientBuilder,
            false, // embeddingServiceEnabled
            mvcTaskExecutor
        );
    }

    @Test
    void generateEmbeddingReactive_withEmptyBookFields_returnsZeroFilledArray() {
        // Given
        Book book = new Book();
        book.setId("test-id");
        // All other fields are null/empty

        // When & Then
        StepVerifier.create(bookSimilarityService.generateEmbeddingReactive(book))
            .assertNext(embedding -> {
                assertThat(embedding).hasSize(1536);
                assertThat(embedding).containsOnly(0.0f);
            })
            .verifyComplete();
    }

    @Test
    void generateEmbeddingReactive_withNullBookFields_returnsZeroFilledArray() {
        // Given
        Book book = new Book();
        book.setId("test-id");
        book.setTitle(null);
        book.setAuthors(null);
        book.setDescription(null);
        book.setCategories(null);

        // When & Then
        StepVerifier.create(bookSimilarityService.generateEmbeddingReactive(book))
            .assertNext(embedding -> {
                assertThat(embedding).hasSize(1536);
                assertThat(embedding).containsOnly(0.0f);
            })
            .verifyComplete();
    }

    @Test
    void generateEmbeddingReactive_withValidBookData_serviceDisabled_returnsPlaceholderEmbedding() {
        // Given
        Book book = createTestBook();

        // When & Then
        StepVerifier.create(bookSimilarityService.generateEmbeddingReactive(book))
            .assertNext(embedding -> {
                assertThat(embedding).hasSize(1536);
                // Should not be all zeros since we have text content
                assertThat(embedding).isNotEqualTo(new float[1536]);
                // Verify it's deterministic
                float[] expected = bookSimilarityService.createPlaceholderEmbedding("Test Title Test Author Test Description Fiction");
                assertThat(embedding).isEqualTo(expected);
            })
            .verifyComplete();
    }

    @Test
    void generateEmbeddingReactive_withValidBookData_serviceEnabled_successfulCall() {
        // Given
        Book book = createTestBook();
        float[] mockEmbedding = new float[1536];
        Arrays.fill(mockEmbedding, 0.5f);

        // Create service with embedding enabled
        bookSimilarityService = new BookSimilarityService(
            cachedBookRepository,
            googleBooksService,
            bookReactiveCacheService,
            "http://localhost:8080/api/embedding",
            webClientBuilder,
            true, // embeddingServiceEnabled
            mvcTaskExecutor
        );

        // Mock WebClient chain
        when(webClient.post()).thenReturn(requestBodyUriSpec);
        doReturn(requestHeadersSpec).when(requestBodyUriSpec).bodyValue(any());
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.bodyToMono(float[].class)).thenReturn(Mono.just(mockEmbedding));

        // When & Then
        StepVerifier.create(bookSimilarityService.generateEmbeddingReactive(book))
            .assertNext(embedding -> {
                assertThat(embedding).hasSize(1536);
                assertThat(embedding).containsOnly(0.5f);
            })
            .verifyComplete();
    }

    @Test
    void generateEmbeddingReactive_withValidBookData_serviceEnabled_errorFallback() {
        // Given
        Book book = createTestBook();

        // Create service with embedding enabled
        bookSimilarityService = new BookSimilarityService(
            cachedBookRepository,
            googleBooksService,
            bookReactiveCacheService,
            "http://localhost:8080/api/embedding",
            webClientBuilder,
            true, // embeddingServiceEnabled
            mvcTaskExecutor
        );

        // Mock WebClient chain to throw error
        when(webClient.post()).thenReturn(requestBodyUriSpec);
        doReturn(requestHeadersSpec).when(requestBodyUriSpec).bodyValue(any());
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.bodyToMono(float[].class)).thenReturn(Mono.error(new RuntimeException("Service error")));

        // When & Then
        StepVerifier.create(bookSimilarityService.generateEmbeddingReactive(book))
            .assertNext(embedding -> {
                assertThat(embedding).hasSize(1536);
                // Should fallback to placeholder embedding
                float[] expected = bookSimilarityService.createPlaceholderEmbedding("Test Title Test Author Test Description Fiction");
                assertThat(embedding).isEqualTo(expected);
            })
            .verifyComplete();
    }

    @Test
    void generateEmbeddingReactive_unexpectedException_returnsZeroFilledArray() {
        // Given - Create a book that will cause an exception during text building
        Book book = new Book() {
            @Override
            public String getTitle() {
                throw new RuntimeException("Unexpected error");
            }
        };
        book.setId("test-id");

        // When & Then
        StepVerifier.create(bookSimilarityService.generateEmbeddingReactive(book))
            .assertNext(embedding -> {
                assertThat(embedding).hasSize(1536);
                assertThat(embedding).containsOnly(0.0f);
            })
            .verifyComplete();
    }

    @Test
    void createPlaceholderEmbedding_withNullText_returnsZeroArray() {
        // When
        float[] result = bookSimilarityService.createPlaceholderEmbedding(null);

        // Then
        assertThat(result).hasSize(1536);
        assertThat(result).containsOnly(0.0f);
    }

    @Test
    void createPlaceholderEmbedding_withEmptyText_returnsZeroArray() {
        // When
        float[] result = bookSimilarityService.createPlaceholderEmbedding("");

        // Then
        assertThat(result).hasSize(1536);
        assertThat(result).containsOnly(0.0f);
    }

    @Test
    void createPlaceholderEmbedding_withValidText_returnsDeterministicValues() {
        // Given
        String text = "Test book content";

        // When
        float[] result1 = bookSimilarityService.createPlaceholderEmbedding(text);
        float[] result2 = bookSimilarityService.createPlaceholderEmbedding(text);

        // Then
        assertThat(result1).hasSize(1536);
        assertThat(result1).isEqualTo(result2); // Should be deterministic
        assertThat(result1).isNotEqualTo(new float[1536]); // Should not be all zeros

        // Verify sine-based calculation
        int hash = text.hashCode();
        float expectedFirst = (float) Math.sin(hash * 1 / 100.0);
        assertThat(result1[0]).isEqualTo(expectedFirst);
    }

    @Test
    void createPlaceholderEmbedding_withDifferentTexts_returnsDifferentValues() {
        // Given
        String text1 = "First book";
        String text2 = "Second book";

        // When
        float[] result1 = bookSimilarityService.createPlaceholderEmbedding(text1);
        float[] result2 = bookSimilarityService.createPlaceholderEmbedding(text2);

        // Then
        assertThat(result1).isNotEqualTo(result2);
    }

    private Book createTestBook() {
        Book book = new Book();
        book.setId("test-id");
        book.setTitle("Test Title");
        book.setAuthors(List.of("Test Author"));
        book.setDescription("Test Description");
        book.setCategories(List.of("Fiction"));
        return book;
    }
}
