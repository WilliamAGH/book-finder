/**
 * Service for handling book similarity operations and recommendations
 * This service provides functionality to find and suggest books similar to a given book
 * It handles:
 * - Generating similarity scores between books using various algorithms
 * - Finding similar books based on content, metadata, and user behavior patterns
 * - Creating and managing book embeddings for similarity calculations
 * - Supporting both synchronous and reactive programming models
 * - Integrating with external services for enhanced similarity matching
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service.similarity;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator; // To get source book
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class BookSimilarityService {

    private static final Logger logger = LoggerFactory.getLogger(BookSimilarityService.class);

    private static final int DEFAULT_SIMILAR_COUNT = 6;
    private static final int MAX_SIMILAR_COUNT = 20;

    private final GoogleBooksService googleBooksService; // For fallback
    private final BookDataOrchestrator bookDataOrchestrator; // To fetch source book for fallback
    private final WebClient embeddingClient;
    private final boolean embeddingServiceEnabled;

    @Value("${app.cache.enabled:true}") // May influence DB vector search
    private boolean cacheEnabled;

    public BookSimilarityService(
            GoogleBooksService googleBooksService,
            BookDataOrchestrator bookDataOrchestrator,
            @Value("${app.embedding.service.url:#{null}}") String embeddingServiceUrl,
            WebClient.Builder webClientBuilder,
            @Value("${app.feature.embedding-service.enabled:false}") boolean embeddingServiceEnabled) {
        this.googleBooksService = googleBooksService;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.embeddingServiceEnabled = embeddingServiceEnabled;
        String baseUrl = embeddingServiceUrl != null ? embeddingServiceUrl : "http://localhost:8095/api/embedding";
        this.embeddingClient = webClientBuilder.baseUrl(baseUrl).build();
        
        // Vector similarity search functionality is disabled
        logger.info("BookSimilarityService initialized (vector similarity search disabled)");
    }

    public List<Book> getSimilarBooks(String bookId, int count) {
        int safeCount = normaliseCount(count);
        Book sourceBook = bookDataOrchestrator.getBookByIdTiered(bookId).block(Duration.ofSeconds(5));

        if (sourceBook == null) {
            logger.warn("Source book for similar search not found (ID: {}), returning empty list.", bookId);
            return Collections.emptyList();
        }

        try {
            List<Book> postgresRecommendations = fetchCachedSimilarBooks(sourceBook, safeCount)
                .block(Duration.ofSeconds(5));

            if (postgresRecommendations != null && !postgresRecommendations.isEmpty()) {
                logger.info("Returning {} cached recommendations for similar search on book {}.", postgresRecommendations.size(), bookId);
                return postgresRecommendations;
            }
        } catch (Exception ex) {
            logger.warn("Postgres-backed cached similarity failed for {}: {}. Falling back to Google.", bookId, ex.getMessage());
        }

        return fallbackToGoogleSimilarBooks(sourceBook, safeCount)
            .blockOptional()
            .orElse(Collections.emptyList());
    }

    public Mono<List<Book>> getSimilarBooksReactive(String bookId, int count) {
        int safeCount = normaliseCount(count);

        return bookDataOrchestrator.getBookByIdTiered(bookId)
            .flatMap(sourceBook -> fetchCachedSimilarBooks(sourceBook, safeCount)
                .flatMap(cached -> {
                    if (cached != null && !cached.isEmpty()) {
                        logger.info("Returning {} cached recommendations for similar search on book {} (reactive).", cached.size(), bookId);
                        return Mono.just(cached);
                    }
                    return fallbackToGoogleSimilarBooks(sourceBook, safeCount);
                }))
            .switchIfEmpty(Mono.just(Collections.emptyList()))
            .onErrorResume(ex -> {
                logger.error("Error during reactive similar book lookup for {}: {}", bookId, ex.getMessage(), ex);
                return Mono.just(Collections.emptyList());
            });
    }

    private Mono<List<Book>> fetchCachedSimilarBooks(Book sourceBook, int limit) {
        List<String> cachedIds = sourceBook.getCachedRecommendationIds();
        if (cachedIds == null || cachedIds.isEmpty()) {
            return Mono.just(Collections.emptyList());
        }

        return Flux.fromIterable(cachedIds)
            .flatMapSequential(bookDataOrchestrator::getBookByIdTiered, 4, 8)
            .filter(Objects::nonNull)
            .filter(recommended -> sourceBook.getId() == null || !sourceBook.getId().equals(recommended.getId()))
            .distinct(Book::getId)
            .take(limit)
            .collectList();
    }

    private Mono<List<Book>> fallbackToGoogleSimilarBooks(Book sourceBook, int count) {
        if (sourceBook == null) {
            return Mono.just(Collections.emptyList());
        }

        logger.info("Falling back to GoogleBooksService for similar books for ID: {}", sourceBook.getId());
        return this.googleBooksService.getSimilarBooks(sourceBook)
            .map(list -> list.stream()
                .filter(similarBook -> !Objects.equals(similarBook.getId(), sourceBook.getId()))
                .limit(count)
                .collect(Collectors.toList()))
            .switchIfEmpty(Mono.just(Collections.emptyList()))
            .onErrorResume(ex -> {
                logger.warn("Google fallback failed for {}: {}", sourceBook.getId(), ex.getMessage());
                return Mono.just(Collections.emptyList());
            });
    }

    private int normaliseCount(int requested) {
        if (requested <= 0) {
            return DEFAULT_SIMILAR_COUNT;
        }
        return Math.min(requested, MAX_SIMILAR_COUNT);
    }

    public Mono<float[]> generateEmbeddingReactive(Book book) {
        try {
            StringBuilder textBuilder = new StringBuilder();
            textBuilder.append(book.getTitle() != null ? book.getTitle() : "").append(" ");

            if (book.getAuthors() != null && !book.getAuthors().isEmpty()) {
                textBuilder.append(String.join(" ", book.getAuthors())).append(" ");
            }
            if (book.getDescription() != null) {
                textBuilder.append(book.getDescription()).append(" ");
            }
            if (book.getCategories() != null && !book.getCategories().isEmpty()) {
                textBuilder.append(String.join(" ", book.getCategories()));
            }
            String text = textBuilder.toString().trim();

            if (text.isEmpty()) {
                logger.warn("Cannot generate embedding for book ID {} as constructed text is empty.", book.getId());
                return Mono.just(new float[384]); // Placeholder dimension
            }

            if (this.embeddingServiceEnabled) {
                return embeddingClient.post()
                    .bodyValue(Collections.singletonMap("text", text))
                    .retrieve()
                    .bodyToMono(float[].class)
                    .onErrorResume(e -> {
                        logger.warn("Error generating embedding from service for book ID {}: {}. Falling back to placeholder.", book.getId(), e.getMessage());
                        return Mono.just(createPlaceholderEmbedding(text));
                    });
            }
            logger.debug("Embedding service is disabled or URL not configured. Using placeholder embedding for book ID {}.", book.getId());
            return Mono.just(createPlaceholderEmbedding(text));
        } catch (Exception e) {
            logger.error("Unexpected error in generateEmbeddingReactive for book ID {}: {}", book.getId(), e.getMessage(), e);
            return Mono.just(new float[384]); // Placeholder dimension
        }
    }

    public float[] createPlaceholderEmbedding(String text) { // Made public for utility usage
        float[] placeholder = new float[384]; // Ensure consistent dimension
        if (text == null || text.isEmpty()) return placeholder;
        int hash = text.hashCode();
        for (int i = 0; i < placeholder.length; i++) {
            placeholder[i] = (float) Math.sin(hash * (i + 1) / 100.0);
        }
        return placeholder;
    }
}
