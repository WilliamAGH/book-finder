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

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class BookSimilarityService {

    private static final Logger logger = LoggerFactory.getLogger(BookSimilarityService.class);

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
        
        // Vector search functionality removed with Redis
        logger.info("BookSimilarityService initialized (vector similarity search disabled after Redis removal)");
    }

    public List<Book> getSimilarBooks(String bookId, int count) {
        // Note: Vector similarity search has been disabled as CachedBook model was removed
        logger.debug("Using GoogleBooksService for similarity matching (vector search removed with Redis)");

        logger.info("No vector similarity data for book ID: {}, using GoogleBooksService category/author matching", bookId);
        // Fetch source book reactively and block. This might be an area for improvement if strict sync is needed without block.
        Book sourceBook = bookDataOrchestrator.getBookByIdTiered(bookId).block(Duration.ofSeconds(5));

        if (sourceBook == null) {
            logger.warn("Source book for similar search not found (ID: {}), returning empty list.", bookId);
            return Collections.emptyList();
        }
        // googleBooksService.getSimilarBooks itself returns a Mono, so block here.
        return googleBooksService.getSimilarBooks(sourceBook).blockOptional().orElse(Collections.emptyList());
    }

    public Mono<List<Book>> getSimilarBooksReactive(String bookId, int count) {
        // Note: Vector similarity search has been disabled as CachedBook model was removed
        logger.debug("Using GoogleBooksService for similarity matching (vector search removed with Redis)");
        return fallbackToGoogleSimilarBooks(bookId, count);
    }

    private Mono<List<Book>> fallbackToGoogleSimilarBooks(String bookId, int count) {
        logger.info("Falling back to GoogleBooksService for similar books for ID: {}", bookId);
        return bookDataOrchestrator.getBookByIdTiered(bookId) // Use injected data orchestrator
            .flatMap(sourceBook -> {
                if (sourceBook == null) {
                    logger.warn("Source book for similar search (Google fallback) not found (ID: {}), returning empty list.", bookId);
                    return Mono.just(Collections.<Book>emptyList());
                }
                return this.googleBooksService.getSimilarBooks(sourceBook)
                    .map(list -> list.stream().limit(count).collect(Collectors.toList()));
            })
            .switchIfEmpty(Mono.fromSupplier(() -> {
                 logger.warn("getBookByIdTiered returned empty for ID {} during similar books fallback.", bookId);
                 return Collections.<Book>emptyList();
            }));
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
