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
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.cache.BookReactiveCacheService; 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class BookSimilarityService {

    private static final Logger logger = LoggerFactory.getLogger(BookSimilarityService.class);

    private final CachedBookRepository cachedBookRepository; 
    private final GoogleBooksService googleBooksService; 
    private final BookReactiveCacheService bookReactiveCacheService; 
    private final WebClient embeddingClient;
    private final boolean embeddingServiceEnabled;
    private final String embeddingServiceUrl;
    private final AsyncTaskExecutor mvcTaskExecutor;


    @Value("${app.cache.enabled:true}") 
    private boolean cacheEnabled;

    public BookSimilarityService(
            @Autowired(required = false) CachedBookRepository cachedBookRepository,
            GoogleBooksService googleBooksService,
            BookReactiveCacheService bookReactiveCacheService,
            @Value("${app.embedding.service.url:#{null}}") String embeddingServiceUrl,
            WebClient.Builder webClientBuilder,
            @Value("${app.feature.embedding-service.enabled:false}") boolean embeddingServiceEnabled,
            @Qualifier("mvcTaskExecutor") AsyncTaskExecutor mvcTaskExecutor) {
        this.cachedBookRepository = cachedBookRepository;
        this.googleBooksService = googleBooksService;
        this.bookReactiveCacheService = bookReactiveCacheService;
        this.embeddingServiceEnabled = embeddingServiceEnabled;
        this.embeddingServiceUrl = embeddingServiceUrl;
        String baseUrl = embeddingServiceUrl != null ? embeddingServiceUrl : "http://localhost:8080/api/embedding";
        this.embeddingClient = webClientBuilder.baseUrl(baseUrl).build();
        this.mvcTaskExecutor = mvcTaskExecutor;
        
        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false; 
            logger.info("BookSimilarityService: Database cache (CachedBookRepository) is not available. Vector similarity search disabled.");
        }
    }

    /**
     * @deprecated Use {@link #getSimilarBooksReactive(String, int)} for reactive or {@link #getSimilarBooksAsync(String, int)} for async behavior.
     */
    @Deprecated
    public List<Book> getSimilarBooks(String bookId, int count) {
        logger.warn("Deprecated synchronous getSimilarBooks called. Consider using getSimilarBooksAsync() or getSimilarBooksReactive().");
        try {
            return getSimilarBooksAsync(bookId, count).join(); // Blocking for deprecated method
        } catch (Exception e) {
            logger.error("Error in synchronous getSimilarBooks for bookId {}: {}", bookId, e.getMessage(), e);
            Thread.currentThread().interrupt();
            return Collections.emptyList();
        }
    }

    @Async("mvcTaskExecutor")
    public CompletableFuture<List<Book>> getSimilarBooksAsync(String bookId, int count) {
        if (cacheEnabled && cachedBookRepository != null) {
            return CompletableFuture.supplyAsync(() -> cachedBookRepository.findByGoogleBooksId(bookId), mvcTaskExecutor)
                .thenComposeAsync(sourceCachedBookOpt -> {
                    if (sourceCachedBookOpt.isPresent()) {
                        return CompletableFuture.supplyAsync(() -> 
                                cachedBookRepository.findSimilarBooksById(sourceCachedBookOpt.get().getId(), count), mvcTaskExecutor)
                            .thenComposeAsync(similarCachedBooks -> {
                                if (!similarCachedBooks.isEmpty()) {
                                    logger.info("Found {} similar books using vector similarity for book ID: {}",
                                            similarCachedBooks.size(), bookId);
                                    List<Book> books = similarCachedBooks.stream()
                                            .map(CachedBook::toBook)
                                            .collect(Collectors.toList());
                                    return CompletableFuture.completedFuture(books);
                                }
                                // If DB vector search yields no results, fall back to Google.
                                return fallbackToGoogleSimilarBooksAsync(bookId, count);
                            }, mvcTaskExecutor);
                    }
                    // If source book not in DB, fall back to Google.
                    return fallbackToGoogleSimilarBooksAsync(bookId, count);
                }, mvcTaskExecutor)
                .exceptionally(e -> {
                    logger.warn("Error retrieving similar books from database for book ID {}: {}. Falling back to Google.", bookId, e.getMessage());
                    // Ensure fallbackToGoogleSimilarBooksAsync is also called here in case of exception
                    return fallbackToGoogleSimilarBooksAsync(bookId, count).join(); // .join() here as exceptionally expects a List<Book> not CF<List<Book>>
                                                                                // This is okay if the fallback itself is robust.
                                                                                // A better approach might be to ensure fallbackToGoogleSimilarBooksAsync is chained properly
                                                                                // or to rethrow and handle at a higher level if that's preferred.
                                                                                // For now, matching the reactive version's onErrorResume behavior.
                });
        }
        return fallbackToGoogleSimilarBooksAsync(bookId, count);
    }
    
    private CompletableFuture<List<Book>> fallbackToGoogleSimilarBooksAsync(String bookId, int count) {
        logger.info("Falling back to GoogleBooksService for similar books for ID: {}", bookId);
        return bookReactiveCacheService.getBookByIdReactive(bookId)
            .toFuture()
            .thenComposeAsync(sourceBook -> {
                if (sourceBook == null) {
                    logger.warn("Source book for similar search (Google fallback) not found (ID: {}), returning empty list.", bookId);
                    return CompletableFuture.completedFuture(Collections.<Book>emptyList());
                }
                return googleBooksService.getSimilarBooks(sourceBook)
                    .defaultIfEmpty(Collections.emptyList()) // Ensure we don't get null from Mono
                    .map(list -> list.stream().limit(count).collect(Collectors.toList()))
                    .toFuture();
            }, mvcTaskExecutor)
            .exceptionally(ex -> {
                logger.warn("Exception in fallbackToGoogleSimilarBooksAsync for bookId {}: {}", bookId, ex.getMessage());
                return Collections.emptyList();
            });
    }

    public Mono<List<Book>> getSimilarBooksReactive(String bookId, int count) {
        if (cacheEnabled && cachedBookRepository != null) {
            return Mono.fromCallable(() -> cachedBookRepository.findByGoogleBooksId(bookId))
                .subscribeOn(Schedulers.fromExecutor(mvcTaskExecutor)) // Use injected executor
                .flatMap(sourceCachedBookOpt -> {
                    if (sourceCachedBookOpt.isPresent()) {
                        return Mono.fromCallable(() -> cachedBookRepository.findSimilarBooksById(sourceCachedBookOpt.get().getId(), count))
                            .subscribeOn(Schedulers.fromExecutor(mvcTaskExecutor)) // Use injected executor
                            .flatMap(similarCachedBooks -> {
                                if (!similarCachedBooks.isEmpty()) {
                                    logger.info("Found {} similar books using vector similarity for book ID: {}", similarCachedBooks.size(), bookId);
                                    return Mono.just(similarCachedBooks.stream().map(CachedBook::toBook).collect(Collectors.toList()));
                                }
                                return Mono.<List<Book>>empty(); 
                            })
                            .switchIfEmpty(fallbackToGoogleSimilarBooksReactive(bookId, count)); 
                    }
                    return fallbackToGoogleSimilarBooksReactive(bookId, count);
                })
                .onErrorResume(e -> {
                    logger.warn("Error retrieving similar books from database for book ID {}: {}. Falling back to Google.", bookId, e.getMessage());
                    return fallbackToGoogleSimilarBooksReactive(bookId, count);
                });
        }
        return fallbackToGoogleSimilarBooksReactive(bookId, count);
    }

    private Mono<List<Book>> fallbackToGoogleSimilarBooksReactive(String bookId, int count) {
        logger.info("Falling back to GoogleBooksService for similar books for ID: {}", bookId);
        return bookReactiveCacheService.getBookByIdReactive(bookId) 
            .flatMap(sourceBook -> {
                if (sourceBook == null) {
                    logger.warn("Source book for similar search (Google fallback) not found (ID: {}), returning empty list.", bookId);
                    return Mono.just(Collections.<Book>emptyList());
                }
                return this.googleBooksService.getSimilarBooks(sourceBook)
                    .map(list -> list.stream().limit(count).collect(Collectors.toList()));
            })
            .switchIfEmpty(Mono.fromSupplier(() -> {
                 logger.warn("getBookByIdReactive returned empty for ID {} during similar books fallback.", bookId);
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
                return Mono.just(new float[1536]); 
            }

            if (this.embeddingServiceEnabled && this.embeddingServiceUrl != null) {
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
            return Mono.just(new float[1536]); 
        }
    }

    public float[] createPlaceholderEmbedding(String text) { 
        float[] placeholder = new float[1536]; 
        if (text == null || text.isEmpty()) return placeholder;
        int hash = text.hashCode();
        for (int i = 0; i < placeholder.length; i++) {
            placeholder[i] = (float) Math.sin(hash * (i + 1) / 100.0);
        }
        return placeholder;
    }
}
