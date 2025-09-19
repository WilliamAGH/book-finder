/**
 * Implementation of the Longitood service for retrieving high-quality book cover images
 * 
 * @author William Callahan
 * 
 * Features:
 * - Integrates with external Longitood API to fetch book cover images
 * - Implements circuit breaker, rate limiting, and timeout protections
 * - Handles image resolution preferences and source attribution
 * - Provides fallback mechanisms when service is unavailable
 * - Supports ISBN-based lookups for precise image matching
 */
package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
// LongitoodService is in the same package; explicit import not required

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Service
public class LongitoodServiceImpl implements LongitoodService { 

    private static final Logger logger = LoggerFactory.getLogger(LongitoodServiceImpl.class);
    private static final String LONGITOOD_SOURCE_NAME = "Longitood";

    private final WebClient webClient;

    /**
     * Constructs a new LongitoodServiceImpl with the specified WebClient
     *
     * @param webClient The WebClient for making HTTP requests to the Longitood API
     */
    public LongitoodServiceImpl(WebClient webClient) {
        this.webClient = webClient;
    }

    /**
     * Fetches book cover image from Longitood API
     *
     * @param book The book to fetch a cover for
     * @return A CompletableFuture emitting Optional<ImageDetails> if a cover is found, or an empty Optional if not available
     */
    @Override
    @CircuitBreaker(name = "longitoodService", fallbackMethod = "fetchCoverFallback")
    @TimeLimiter(name = "longitoodService")
    @RateLimiter(name = "longitoodServiceRateLimiter", fallbackMethod = "fetchCoverRateLimitFallback")
    public CompletableFuture<Optional<ImageDetails>> fetchCover(Book book) {
        String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();
        if (isbn == null || isbn.trim().isEmpty()) {
            logger.warn("No ISBN found for book ID: {}, cannot fetch cover from Longitood", book.getId());
            return CompletableFuture.completedFuture(Optional.empty());
        }

        String apiUrl = "https://bookcover.longitood.com/bookcover/" + isbn;
        final String finalIsbn = isbn; 

        Mono<ImageDetails> imageDetailsMono = webClient.get()
            .uri(apiUrl)
            .retrieve()
            .bodyToMono(new org.springframework.core.ParameterizedTypeReference<Map<String, String>>() {})
            .flatMap(response -> {
                if (response != null && response.containsKey("url")) {
                    String coverUrl = response.get("url");
                    if (coverUrl != null && !coverUrl.isEmpty()) {
                        logger.debug("Found cover URL from Longitood API for book {}: {}", book.getId(), coverUrl);
                        String sourceSystemId = String.format("%s-%s", LONGITOOD_SOURCE_NAME, finalIsbn);
                        ImageDetails imageDetails = new ImageDetails(
                            coverUrl,
                            LONGITOOD_SOURCE_NAME,
                            sourceSystemId,
                            CoverImageSource.LONGITOOD,
                            ImageResolutionPreference.ORIGINAL 
                        );
                        return Mono.just(imageDetails);
                    }
                }
                logger.debug("No valid cover URL found from Longitood API for book {}", book.getId());
                return Mono.empty();
            })
            .onErrorResume(WebClientResponseException.class, e -> {
                logger.warn("Longitood API error for book {}: {}", book.getId(), e.getMessage());
                return Mono.empty(); 
            })
            .onErrorResume(Exception.class, e -> {
                 logger.error("Unexpected error during Longitood API call for book {}: {}", book.getId(), e.getMessage(), e);
                 return Mono.empty(); 
            });
        
        // Convert Mono<ImageDetails> to CompletableFuture<Optional<ImageDetails>>
        return imageDetailsMono
            .map(Optional::of) // Map ImageDetails to Optional<ImageDetails>
            .defaultIfEmpty(Optional.empty()) // If Mono is empty, provide Optional.empty()
            .toFuture(); // Convert to CompletableFuture
    }

    /**
     * Fallback method for fetchCover when circuit breaker is triggered
     * - Handles when the Longitood service is degraded or unavailable
     * - Returns empty result to prevent cascade failures
     *
     * @param book The book that triggered the circuit breaker
     * @param t The throwable from the circuit breaker
     * @return Empty Optional wrapped in CompletableFuture
     */
    public CompletableFuture<Optional<ImageDetails>> fetchCoverFallback(Book book, Throwable t) {
        String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();
        logger.warn("LongitoodService.fetchCover circuit breaker opened for book ID: {}, ISBN: {}. Error: {}", 
            book.getId(), isbn, t.getMessage());
        return CompletableFuture.completedFuture(Optional.empty()); // Return empty Optional in CompletableFuture
    }
    
    /**
     * Fallback method for fetchCover when rate limit is exceeded
     * - Handles when too many Longitood API requests are made in the time period
     * - Logs the rate limiting event for monitoring
     * - Returns empty result to prevent excessive API calls
     *
     * @param book The book that triggered the rate limiter
     * @param t The throwable from the rate limiter
     * @return Empty Optional wrapped in CompletableFuture
     */
    public CompletableFuture<Optional<ImageDetails>> fetchCoverRateLimitFallback(Book book, Throwable t) {
        String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();
        logger.warn("LongitoodService.fetchCover rate limit exceeded for book ID: {}, ISBN: {}. Error: {}", 
            book.getId(), isbn, t.getMessage());
        return CompletableFuture.completedFuture(Optional.empty());
    }
}
