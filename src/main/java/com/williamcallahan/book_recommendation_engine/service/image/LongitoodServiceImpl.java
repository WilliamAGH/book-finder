package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.types.LongitoodService;
import com.williamcallahan.book_recommendation_engine.types.ImageDetails;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of the Longitood book cover service
 *
 * @author William Callahan
 *
 * Features:
 * - Fetches high-quality book cover images from the Longitood API
 * - Uses reactive WebClient for non-blocking HTTP requests
 * - Provides detailed error handling with appropriate logging
 * - Supports ISBN-based lookups for consistent cover retrieval
 * - Returns standardized ImageDetails with source attribution
 */
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
    @Autowired
    public LongitoodServiceImpl(WebClient webClient) {
        this.webClient = webClient;
    }

    /**
     * Fetches book cover image from Longitood API
     *
     * @param book The book to fetch a cover for
     * @return A Mono emitting Optional<ImageDetails> if a cover is found, or an empty Mono if not available
     */
    @Override
    @CircuitBreaker(name = "longitoodService", fallbackMethod = "fetchCoverFallback")
    @TimeLimiter(name = "longitoodService") // Using default config from application.properties or a specific one if defined
    public CompletableFuture<Optional<ImageDetails>> fetchCover(Book book) {
        String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();
        if (isbn == null || isbn.trim().isEmpty()) {
            logger.warn("No ISBN found for book ID: {}, cannot fetch cover from Longitood", book.getId());
            return CompletableFuture.completedFuture(Optional.empty());
        }

        String apiUrl = "https://bookcover.longitood.com/bookcover/" + isbn;
        final String finalIsbn = isbn;

        return webClient.get()
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
                        return Mono.just(Optional.of(imageDetails));
                    }
                }
                logger.debug("No valid cover URL found from Longitood API for book {}", book.getId());
                return Mono.just(Optional.<ImageDetails>empty());
            })
            .onErrorResume(WebClientResponseException.class, e -> {
                logger.warn("Longitood API error for book {}: {}", book.getId(), e.getMessage());
                return Mono.just(Optional.<ImageDetails>empty());
            })
            .onErrorResume(Exception.class, e -> {
                 logger.error("Unexpected error during Longitood API call for book {}: {}", book.getId(), e.getMessage(), e);
                 return Mono.just(Optional.<ImageDetails>empty());
            })
            .defaultIfEmpty(Optional.<ImageDetails>empty())
            .toFuture();
    }

    // Fallback method for fetchCover
    public CompletableFuture<Optional<ImageDetails>> fetchCoverFallback(Book book, Throwable t) {
        String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();
        logger.warn("LongitoodService.fetchCover circuit breaker opened for book ID: {}, ISBN: {}. Error: {}", book.getId(), isbn, t.getMessage());
        return CompletableFuture.completedFuture(Optional.empty());
    }
}
