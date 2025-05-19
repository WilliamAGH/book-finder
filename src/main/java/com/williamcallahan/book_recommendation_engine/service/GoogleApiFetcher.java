/**
 * Service for fetching book data from Google Books API
 *
 * @author William Callahan
 * 
 * Features:
 * - Provides authenticated and unauthenticated API access
 * - Offers volume details fetching by ID
 * - Supports search with pagination and filtering
 * - Includes retry logic for resilient API communication
 * - Monitors API call success/failure rates
 * - Handles proper error logging and response transformation
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;

/**
 * Fetches book data from Google Books API with resilient error handling
 * 
 * @author William Callahan
 */
@Service
public class GoogleApiFetcher {

    private static final Logger logger = LoggerFactory.getLogger(GoogleApiFetcher.class);

    private final WebClient webClient;
    private final ApiRequestMonitor apiRequestMonitor;

    @Value("${googlebooks.api.url}")
    private String googleBooksApiUrl;

    @Value("${googlebooks.api.key:#{null}}") // Allow API key to be optional
    private String googleBooksApiKey;

    /**
     * Creates a new GoogleApiFetcher with required dependencies
     * 
     * @param webClientBuilder Builder for creating WebClient instances
     * @param apiRequestMonitor Monitor for tracking API request metrics
     */
    @Autowired
    public GoogleApiFetcher(WebClient.Builder webClientBuilder, ApiRequestMonitor apiRequestMonitor) {
        this.webClient = webClientBuilder.build();
        this.apiRequestMonitor = apiRequestMonitor;
    }

    /**
     * Fetches a single volume by ID using an authenticated Google Books API call
     *
     * @param bookId The Google Books ID of the volume
     * @return A Mono emitting the raw JsonNode response from the API
     */
    public Mono<JsonNode> fetchVolumeByIdAuthenticated(String bookId) {
        if (googleBooksApiKey == null || googleBooksApiKey.isEmpty()) {
            logger.warn("Authenticated API call attempted for bookId {} without an API key. This call will likely fail or be rate-limited.", bookId);
            // Depending on strictness, could return Mono.error or proceed without key
        }
        String url = UriComponentsBuilder.fromUriString(googleBooksApiUrl)
                .pathSegment("v1", "volumes", bookId)
                .queryParamIfPresent("key", Optional.ofNullable(googleBooksApiKey).filter(key -> !key.isEmpty()))
                .build(true)
                .toUriString();
        
        String endpoint = "volumes/get/" + bookId + "/authenticated";
        logger.debug("Making Authenticated Google Books API GET call for book ID: {}, endpoint: {}", bookId, endpoint);

        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .doOnSubscribe(s -> logger.debug("Fetching book {} from Google API (Authenticated).", bookId))
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .filter(throwable -> throwable instanceof IOException || throwable instanceof WebClientRequestException)
                        .doBeforeRetry(retrySignal ->
                                logger.warn("Retrying Authenticated API call for book {}. Attempt #{}. Error: {}",
                                        bookId, retrySignal.totalRetries() + 1, retrySignal.failure().getMessage()))
                        .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                            logger.error("All retries failed for Authenticated API call for book {}. Final error: {}", bookId, retrySignal.failure().getMessage());
                            apiRequestMonitor.recordFailedRequest(endpoint, "All retries failed: " + retrySignal.failure().getMessage());
                            return retrySignal.failure();
                        }))
                .doOnSuccess(response -> apiRequestMonitor.recordSuccessfulRequest(endpoint))
                .onErrorResume(e -> {
                    logger.error("Error fetching book {} from Google API (Authenticated) after retries: {}", bookId, e.getMessage());
                    apiRequestMonitor.recordFailedRequest(endpoint, e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Fetches a single volume by ID using an unauthenticated Google Books API call
     *
     * @param bookId The Google Books ID of the volume
     * @return A Mono emitting the raw JsonNode response from the API
     */
    public Mono<JsonNode> fetchVolumeByIdUnauthenticated(String bookId) {
        String url = UriComponentsBuilder.fromUriString(googleBooksApiUrl)
                .pathSegment("v1", "volumes", bookId)
                // No API key for unauthenticated calls
                .build(true)
                .toUriString();

        String endpoint = "volumes/get/" + bookId + "/unauthenticated";
        logger.debug("Making Unauthenticated Google Books API GET call for book ID: {}, endpoint: {}", bookId, endpoint);
        
        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .doOnSubscribe(s -> logger.debug("Fetching book {} from Google API (Unauthenticated).", bookId))
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .filter(throwable -> throwable instanceof IOException || throwable instanceof WebClientRequestException)
                        .doBeforeRetry(retrySignal ->
                                logger.warn("Retrying Unauthenticated API call for book {}. Attempt #{}. Error: {}",
                                        bookId, retrySignal.totalRetries() + 1, retrySignal.failure().getMessage()))
                        .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                            logger.error("All retries failed for Unauthenticated API call for book {}. Final error: {}", bookId, retrySignal.failure().getMessage());
                            apiRequestMonitor.recordFailedRequest(endpoint, "All retries failed: " + retrySignal.failure().getMessage());
                            return retrySignal.failure();
                        }))
                .doOnSuccess(response -> apiRequestMonitor.recordSuccessfulRequest(endpoint))
                .onErrorResume(e -> {
                    logger.error("Error fetching book {} from Google API (Unauthenticated) after retries: {}", bookId, e.getMessage());
                    apiRequestMonitor.recordFailedRequest(endpoint, e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Searches volumes using an authenticated Google Books API call
     *
     * @param query The search query
     * @param startIndex The starting index for pagination
     * @param orderBy The order for results (e.g., "relevance", "newest")
     * @param langCode Optional language code to restrict results (e.g., "en")
     * @return A Mono emitting the raw JsonNode response for one page of search results
     */
    public Mono<JsonNode> searchVolumesAuthenticated(String query, int startIndex, String orderBy, String langCode) {
        if (googleBooksApiKey == null || googleBooksApiKey.isEmpty()) {
            logger.warn("Authenticated search API call attempted for query '{}' without an API key.", query);
        }
        return searchVolumesInternal(query, startIndex, orderBy, langCode, true);
    }

    /**
     * Searches volumes using an unauthenticated Google Books API call
     *
     * @param query The search query
     * @param startIndex The starting index for pagination
     * @param orderBy The order for results (e.g., "relevance", "newest")
     * @param langCode Optional language code to restrict results (e.g., "en")
     * @return A Mono emitting the raw JsonNode response for one page of search results
     */
    public Mono<JsonNode> searchVolumesUnauthenticated(String query, int startIndex, String orderBy, String langCode) {
        return searchVolumesInternal(query, startIndex, orderBy, langCode, false);
    }

    /**
     * Internal implementation for searching volumes with shared logic
     * 
     * @param query The search query
     * @param startIndex The starting index for pagination
     * @param orderBy The order for results
     * @param langCode Optional language code for filtering
     * @param authenticated Whether to use API key for authentication
     * @return A Mono emitting the raw JsonNode response
     */
    private Mono<JsonNode> searchVolumesInternal(String query, int startIndex, String orderBy, String langCode, boolean authenticated) {
        String encodedQuery;
        try {
            encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8.name());
        } catch (java.io.UnsupportedEncodingException e) {
            logger.error("Failed to URL encode query: {}", query, e);
            return Mono.error(e);
        }

        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(googleBooksApiUrl)
                .pathSegment("v1", "volumes")
                .queryParam("q", encodedQuery)
                .queryParam("startIndex", startIndex)
                .queryParam("maxResults", 40); // Standard maxResults

        if (authenticated && googleBooksApiKey != null && !googleBooksApiKey.isEmpty()) {
            builder.queryParam("key", googleBooksApiKey);
        }
        if (orderBy != null && !orderBy.isEmpty()) {
            builder.queryParam("orderBy", orderBy);
        }
        if (langCode != null && !langCode.isEmpty()) {
            builder.queryParam("langRestrict", langCode);
        }

        String url = builder.build(true).toUriString();
        String authStatus = authenticated ? "authenticated" : "unauthenticated";
        String endpoint = "volumes/search/" + getQueryTypeForMonitoring(query) + "/" + authStatus;

        logger.debug("Making Google Books API search call ({}) for query: {}, startIndex: {}, endpoint: {}",
                authStatus, query, startIndex, endpoint);

        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .doOnSubscribe(s -> logger.debug("Making Google Books API search call ({}) for query: {}, startIndex: {}", authStatus, query, startIndex))
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                        .filter(throwable -> throwable instanceof IOException || throwable instanceof WebClientRequestException)
                        .doBeforeRetry(retrySignal ->
                                logger.warn("Retrying API search call ({}) for query '{}', startIndex {}. Attempt #{}. Error: {}",
                                        authStatus, query, startIndex, retrySignal.totalRetries() + 1, retrySignal.failure().getMessage()))
                        .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                            logger.error("All retries failed for API search call ({}) for query '{}', startIndex {}. Final error: {}",
                                    authStatus, query, startIndex, retrySignal.failure().getMessage());
                            apiRequestMonitor.recordFailedRequest(endpoint, "All retries failed: " + retrySignal.failure().getMessage());
                            return retrySignal.failure();
                        }))
                .doOnSuccess(response -> apiRequestMonitor.recordSuccessfulRequest(endpoint))
                .onErrorResume(e -> {
                    logger.error("Error fetching page for API search call ({}) for query '{}' at startIndex {} after retries: {}",
                            authStatus, query, startIndex, e.getMessage());
                    apiRequestMonitor.recordFailedRequest(endpoint, e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Determines query type for monitoring metrics
     * 
     * @param query The search query to analyze
     * @return String representing query type category
     */
    private String getQueryTypeForMonitoring(String query) {
        if (query.contains("intitle:")) return "title";
        if (query.contains("inauthor:")) return "author";
        if (query.contains("isbn:")) return "isbn";
        return "general";
    }
}
