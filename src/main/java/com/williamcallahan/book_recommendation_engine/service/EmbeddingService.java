/**
 * Service for generating embeddings for books to enable semantic search and similarity matching.
 * Supports multiple embedding providers including OpenAI and local models (Ollama).
 * 
 * Features:
 * - Generates vector embeddings from book metadata (title, authors, description, categories)
 * - Supports OpenAI text-embedding-3-small model via API
 * - Supports local embedding models through Ollama API
 * - Provides fallback placeholder embeddings when service is unavailable
 * - Caches existing embeddings to avoid redundant API calls
 * - Configurable embedding dimensions and service endpoints
 * - Reactive programming model using {@link Mono} for non-blocking operations
 * 
 * The service integrates with {@link RedisVector} for storing embeddings in Redis
 * and supports both {@link Book} and {@link CachedBook} entities.
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.types.RedisVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Map;

@Service
public class EmbeddingService {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddingService.class);
    private static final int EMBEDDING_DIMENSION = 1536;

    private final WebClient embeddingClient;
    private final boolean embeddingServiceEnabled;
    private final String embeddingServiceUrl;
    private final String embeddingProvider;

    @Value("${app.openai.api.key:}")
    private String openAiApiKey;

    public EmbeddingService(
            WebClient.Builder webClientBuilder,
            @Value("${app.embedding.service.enabled:false}") boolean embeddingServiceEnabled,
            @Value("${app.embedding.service.url:}") String embeddingServiceUrl,
            @Value("${app.embedding.provider:local}") String embeddingProvider) {
        
        this.embeddingServiceEnabled = embeddingServiceEnabled;
        this.embeddingServiceUrl = embeddingServiceUrl != null && !embeddingServiceUrl.trim().isEmpty() ? 
                                   embeddingServiceUrl.trim() : "http://localhost:11434";
        this.embeddingProvider = embeddingProvider != null ? embeddingProvider : "local";
        
        WebClient client;
        try {
            client = webClientBuilder
                .baseUrl(this.embeddingServiceUrl)
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)) // 10MB buffer
                .defaultHeader("User-Agent", "BookRecommendationEngine/1.0")
                .build();
            logger.info("EmbeddingService initialized. Enabled: {}, Provider: {}, URL: {}", 
                       embeddingServiceEnabled, this.embeddingProvider, this.embeddingServiceUrl);
        } catch (Exception e) {
            logger.warn("Failed to initialize embedding client, will use placeholders: {}", e.getMessage());
            client = null;
        }
        this.embeddingClient = client;
    }

    public Mono<RedisVector> generateEmbeddingForBook(Book book) {
        // Generate embedding for any Book type

        String embeddingText = buildEmbeddingText(book);
        if (embeddingText.isEmpty()) {
            logger.warn("Cannot generate embedding for book ID {} - no text content", book.getId());
            return Mono.just(new RedisVector(createPlaceholderEmbedding(book.getId())));
        }

        return generateEmbeddingFromText(embeddingText, book.getId())
                .map(RedisVector::new)
                .doOnSuccess(vector -> logger.debug("Generated embedding for book ID: {}", book.getId()))
                .doOnError(error -> logger.error("Failed to generate embedding for book ID {}: {}", book.getId(), error.getMessage()));
    }

    // Overloaded method specifically for CachedBook to check existing embeddings
    public Mono<RedisVector> generateEmbeddingForBook(CachedBook cachedBook) {
        // Check if embedding already exists
        if (cachedBook.getEmbedding() != null && 
            cachedBook.getEmbedding().getValues() != null && 
            cachedBook.getEmbedding().getValues().length == EMBEDDING_DIMENSION) {
            logger.debug("Embedding already exists for book ID: {}", cachedBook.getId());
            return Mono.just(cachedBook.getEmbedding());
        }
        
        // Use the Book version for actual generation
        return generateEmbeddingForBook(cachedBook.toBook());
    }

    public Mono<float[]> generateEmbeddingFromText(String text, String bookId) {
        if (!embeddingServiceEnabled || embeddingClient == null) {
            logger.debug("Embedding service disabled or unavailable, using placeholder for book ID: {}", bookId);
            return Mono.just(createPlaceholderEmbedding(text));
        }

        return switch (embeddingProvider.toLowerCase()) {
            case "openai" -> generateOpenAIEmbedding(text, bookId);
            case "local", "ollama" -> generateLocalEmbedding(text, bookId);
            default -> {
                logger.warn("Unknown embedding provider: {}, using placeholder", embeddingProvider);
                yield Mono.just(createPlaceholderEmbedding(text));
            }
        };
    }

    private Mono<float[]> generateOpenAIEmbedding(String text, String bookId) {
        Map<String, Object> request = Map.of(
            "input", text,
            "model", "text-embedding-3-small"
        );

        return embeddingClient.post()
                .uri("/v1/embeddings")
                .header("Authorization", "Bearer " + openAiApiKey)
                .bodyValue(request)
                .retrieve()
                .bodyToMono(Map.class)
                .map(response -> {
                    @SuppressWarnings("unchecked")
                    var data = (java.util.List<Map<String, Object>>) response.get("data");
                    if (data != null && !data.isEmpty()) {
                        @SuppressWarnings("unchecked")
                        var embedding = (java.util.List<Double>) data.get(0).get("embedding");
                        return embedding.stream().mapToDouble(Double::doubleValue).toArray();
                    }
                    logger.debug("Invalid OpenAI response format for book ID: {}", bookId);
                    return new double[EMBEDDING_DIMENSION]; // Return empty array instead of throwing
                })
                .map(doubles -> {
                    float[] floats = new float[doubles.length];
                    for (int i = 0; i < doubles.length; i++) {
                        floats[i] = (float) doubles[i];
                    }
                    return floats;
                })
                .timeout(java.time.Duration.ofSeconds(30))
                .onErrorResume(error -> {
                    logger.debug("OpenAI embedding failed for book ID {}: {}, using placeholder", bookId, getErrorMessage(error));
                    return Mono.just(createPlaceholderEmbedding(text));
                });
    }

    private Mono<float[]> generateLocalEmbedding(String text, String bookId) {
        Map<String, Object> request = Map.of(
            "model", "nomic-embed-text",  // or your preferred embedding model
            "prompt", text
        );

        return embeddingClient.post()
                .uri("/api/embeddings")
                .bodyValue(request)
                .retrieve()
                .bodyToMono(Map.class)
                .map(response -> {
                    @SuppressWarnings("unchecked")
                    var embedding = (java.util.List<Double>) response.get("embedding");
                    if (embedding != null) {
                        return embedding.stream().mapToDouble(Double::doubleValue).toArray();
                    }
                    logger.debug("Invalid local embedding response format for book ID: {}", bookId);
                    return new double[EMBEDDING_DIMENSION]; // Return empty array instead of throwing
                })
                .map(doubles -> {
                    float[] floats = new float[Math.min(doubles.length, EMBEDDING_DIMENSION)];
                    for (int i = 0; i < floats.length; i++) {
                        floats[i] = (float) doubles[i];
                    }
                    // Pad with zeros if needed
                    if (floats.length < EMBEDDING_DIMENSION) {
                        float[] padded = new float[EMBEDDING_DIMENSION];
                        System.arraycopy(floats, 0, padded, 0, floats.length);
                        return padded;
                    }
                    return floats;
                })
                .timeout(java.time.Duration.ofSeconds(30))
                .onErrorResume(error -> {
                    logger.debug("Local embedding failed for book ID {}: {}, using placeholder", bookId, getErrorMessage(error));
                    return Mono.just(createPlaceholderEmbedding(text));
                });
    }

    private String buildEmbeddingText(Book book) {
        StringBuilder textBuilder = new StringBuilder();
        
        if (book.getTitle() != null) {
            textBuilder.append(book.getTitle()).append(" ");
        }
        if (book.getAuthors() != null && !book.getAuthors().isEmpty()) {
            textBuilder.append(String.join(" ", book.getAuthors())).append(" ");
        }
        if (book.getDescription() != null) {
            textBuilder.append(book.getDescription()).append(" ");
        }
        if (book.getCategories() != null && !book.getCategories().isEmpty()) {
            textBuilder.append(String.join(" ", book.getCategories()));
        }
        
        return textBuilder.toString().trim();
    }

    public float[] createPlaceholderEmbedding(String text) {
        float[] placeholder = new float[EMBEDDING_DIMENSION];
        if (text == null || text.isEmpty()) return placeholder;
        
        int hash = text.hashCode();
        for (int i = 0; i < placeholder.length; i++) {
            placeholder[i] = (float) Math.sin(hash * (i + 1) / 100.0);
        }
        return placeholder;
    }

    public boolean shouldGenerateEmbedding(CachedBook book) {
        return book.getEmbedding() == null || 
               book.getEmbedding().getValues() == null || 
               book.getEmbedding().getValues().length != EMBEDDING_DIMENSION;
    }

    private String getErrorMessage(Throwable error) {
        if (error instanceof java.net.ConnectException) {
            return "Connection refused - service unavailable";
        } else if (error instanceof java.util.concurrent.TimeoutException) {
            return "Request timeout";
        } else if (error instanceof org.springframework.web.reactive.function.client.WebClientResponseException) {
            return "HTTP " + ((org.springframework.web.reactive.function.client.WebClientResponseException) error).getStatusCode();
        } else {
            return error.getClass().getSimpleName();
        }
    }
}
