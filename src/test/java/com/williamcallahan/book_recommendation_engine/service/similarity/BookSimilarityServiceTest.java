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
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BookSimilarityServiceTest {

    private static final String TEST_EMBEDDING_SERVICE_URL = "http://localhost:8095/api/embedding";

    @Mock
    private GoogleBooksService googleBooksService;

    @Mock
    private BookDataOrchestrator bookDataOrchestrator;

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

    private BookSimilarityService bookSimilarityService;

    @BeforeEach
    void setUp() {
        when(webClientBuilder.baseUrl(any(String.class))).thenReturn(webClientBuilder);
        when(webClientBuilder.build()).thenReturn(webClient);
        
        // Test with embedding service disabled by default
        bookSimilarityService = new BookSimilarityService(
            googleBooksService,
            bookDataOrchestrator,
            null, // embeddingServiceUrl
            webClientBuilder,
            false // embeddingServiceEnabled
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
                assertThat(embedding).hasSize(384);
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
                assertThat(embedding).hasSize(384);
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
                assertThat(embedding).hasSize(384);
                // Should not be all zeros since we have text content
                assertThat(embedding).isNotEqualTo(new float[384]);
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
        float[] mockEmbedding = new float[384];
        Arrays.fill(mockEmbedding, 0.5f);

        // Create service with embedding enabled
        bookSimilarityService = new BookSimilarityService(
            googleBooksService,
            bookDataOrchestrator,
            TEST_EMBEDDING_SERVICE_URL,
            webClientBuilder,
            true // embeddingServiceEnabled
        );

        // Mock WebClient chain
        when(webClient.post()).thenReturn(requestBodyUriSpec);
        doReturn(requestHeadersSpec).when(requestBodyUriSpec).bodyValue(any());
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.bodyToMono(float[].class)).thenReturn(Mono.just(mockEmbedding));

        // When & Then
        StepVerifier.create(bookSimilarityService.generateEmbeddingReactive(book))
            .assertNext(embedding -> {
                assertThat(embedding).hasSize(384);
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
            googleBooksService,
            bookDataOrchestrator,
            TEST_EMBEDDING_SERVICE_URL,
            webClientBuilder,
            true // embeddingServiceEnabled
        );

        // Mock WebClient chain to throw error
        when(webClient.post()).thenReturn(requestBodyUriSpec);
        doReturn(requestHeadersSpec).when(requestBodyUriSpec).bodyValue(any());
        when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.bodyToMono(float[].class)).thenReturn(Mono.error(new RuntimeException("Service error")));

        // When & Then
        StepVerifier.create(bookSimilarityService.generateEmbeddingReactive(book))
            .assertNext(embedding -> {
                assertThat(embedding).hasSize(384);
                // Should fallback to placeholder embedding
                float[] expected = bookSimilarityService.createPlaceholderEmbedding("Test Title Test Author Test Description Fiction");
                assertThat(embedding).isEqualTo(expected);
            })
            .verifyComplete();
    }

    @Test
    void getSimilarBooks_returnsCachedRecommendationsBeforeGoogle() {
        Book source = createTestBook();
        source.setId("source-id");
        source.setCachedRecommendationIds(List.of("rec-1", "rec-2", "rec-3"));

        Book rec1 = new Book();
        rec1.setId("rec-1");
        rec1.setTitle("Recommendation 1");

        Book rec2 = new Book();
        rec2.setId("rec-2");
        rec2.setTitle("Recommendation 2");

        when(bookDataOrchestrator.getBookByIdTiered(eq("source-id"))).thenReturn(Mono.just(source));
        when(bookDataOrchestrator.getBookByIdTiered(eq("rec-1"))).thenReturn(Mono.just(rec1));
        when(bookDataOrchestrator.getBookByIdTiered(eq("rec-2"))).thenReturn(Mono.just(rec2));

        List<Book> result = bookSimilarityService.getSimilarBooks("source-id", 2);

        assertThat(result)
            .extracting(Book::getId)
            .containsExactly("rec-1", "rec-2");

        verify(googleBooksService, never()).getSimilarBooks(any(Book.class));
    }

    @Test
    void getSimilarBooks_withoutCachedRecommendationsFallsBackToGoogle() {
        Book source = createTestBook();
        source.setId("source-id");
        source.setCachedRecommendationIds(List.of());

        Book googleRec = new Book();
        googleRec.setId("google-1");
        googleRec.setTitle("Google Rec");

        when(bookDataOrchestrator.getBookByIdTiered(eq("source-id"))).thenReturn(Mono.just(source));
        when(googleBooksService.getSimilarBooks(same(source))).thenReturn(Mono.just(List.of(googleRec)));

        List<Book> result = bookSimilarityService.getSimilarBooks("source-id", 3);

        assertThat(result)
            .extracting(Book::getId)
            .containsExactly("google-1");

        verify(googleBooksService).getSimilarBooks(same(source));
    }

    @Test
    void getSimilarBooksReactive_returnsEmptyWhenBookMissing() {
        when(bookDataOrchestrator.getBookByIdTiered(eq("missing"))).thenReturn(Mono.empty());

        StepVerifier.create(bookSimilarityService.getSimilarBooksReactive("missing", 3))
            .expectNext(List.of())
            .verifyComplete();

        verifyNoInteractions(googleBooksService);
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
                assertThat(embedding).hasSize(384);
                assertThat(embedding).containsOnly(0.0f);
            })
            .verifyComplete();
    }

    @Test
    void createPlaceholderEmbedding_withNullText_returnsZeroArray() {
        // When
        float[] result = bookSimilarityService.createPlaceholderEmbedding(null);

        // Then
        assertThat(result).hasSize(384);
        assertThat(result).containsOnly(0.0f);
    }

    @Test
    void createPlaceholderEmbedding_withEmptyText_returnsZeroArray() {
        // When
        float[] result = bookSimilarityService.createPlaceholderEmbedding("");

        // Then
        assertThat(result).hasSize(384);
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
        assertThat(result1).hasSize(384);
        assertThat(result1).isEqualTo(result2); // Should be deterministic
        assertThat(result1).isNotEqualTo(new float[384]); // Should not be all zeros

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
