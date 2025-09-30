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
import lombok.extern.slf4j.Slf4j;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.ExternalApiLogger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.netty.http.client.PrematureCloseException;

import java.io.IOException;
import java.util.Optional;
import java.time.Duration;

@Service
@Slf4j
public class GoogleApiFetcher {

    
    private final WebClient webClient;
    private final ApiRequestMonitor apiRequestMonitor;
    private final ApiCircuitBreakerService circuitBreakerService;

    @Value("${google.books.api.base-url}")
    private String googleBooksApiUrl;

    @Value("${google.books.api.key:#{null}}") // Allow API key to be optional
    private String googleBooksApiKey;

    @Value("${app.features.external-fallback.enabled:${app.features.google-fallback.enabled:true}}")
    private boolean googleFallbackEnabled;

    /**
     * Constructs GoogleApiFetcher with required dependencies
     * 
     * @param webClientBuilder WebClient builder 
     * @param apiRequestMonitor API request tracking service
     * @param circuitBreakerService Circuit breaker for API rate limiting
     */
    public GoogleApiFetcher(WebClient.Builder webClientBuilder, 
                           ApiRequestMonitor apiRequestMonitor,
                           ApiCircuitBreakerService circuitBreakerService) {
        this.webClient = webClientBuilder.build();
        this.apiRequestMonitor = apiRequestMonitor;
        this.circuitBreakerService = circuitBreakerService;
    }

    /**
     * Fetches a volume using authenticated API call
     *
     * @param bookId Google Books ID
     * @return JsonNode response from API
     */
    public Mono<JsonNode> fetchVolumeByIdAuthenticated(String bookId) {
        return fetchVolumeByIdInternal(bookId, true);
    }

    /**
     * Fetches a volume using unauthenticated API call
     *
     * @param bookId Google Books ID
     * @return JsonNode response from API
     */
    public Mono<JsonNode> fetchVolumeByIdUnauthenticated(String bookId) {
        return fetchVolumeByIdInternal(bookId, false);
    }

    private Mono<JsonNode> fetchVolumeByIdInternal(String bookId, boolean authenticated) {
        if (!googleFallbackEnabled) {
            log.debug("Google fallback disabled - skipping {} fetch for {}", authenticated ? "authenticated" : "unauthenticated", bookId);
            return Mono.empty();
        }
        if (authenticated) {
            // Check circuit breaker first
            if (!circuitBreakerService.isApiCallAllowed()) {
                log.info("Circuit breaker is OPEN - skipping authenticated fetch for book ID: {}. Caller should try unauthenticated fallback.", bookId);
                ExternalApiLogger.logCircuitBreakerBlocked(log, "GoogleBooks", bookId);
                return Mono.empty();
            }
            if (googleBooksApiKey == null || googleBooksApiKey.isEmpty()) {
                log.debug("No API key configured - skipping authenticated fetch for bookId {}", bookId);
                return Mono.empty();
            }
        }

        String url = buildVolumeUrl(bookId, authenticated);
        String endpoint = "volumes/get/" + bookId + "/" + (authenticated ? "authenticated" : "unauthenticated");
        log.debug("Making {} Google Books API GET call for book ID: {}, endpoint: {}", authenticated ? "Authenticated" : "Unauthenticated", bookId, endpoint);
        ExternalApiLogger.logHttpRequest(log, "GET", url, authenticated);
        return performGetJson(url, endpoint, authenticated);
    }

    private String buildVolumeUrl(String bookId, boolean authenticated) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(googleBooksApiUrl)
                .pathSegment("volumes", bookId);
        if (authenticated) {
            builder.queryParamIfPresent("key", Optional.ofNullable(googleBooksApiKey).filter(key -> !key.isEmpty()));
        }
        return builder.build(false).toUriString();
    }

    private Mono<JsonNode> performGetJson(String url, String endpoint, boolean authenticated) {
        ExternalApiLogger.logHttpRequest(log, "GET", url, authenticated);

        return webClient.get()
                .uri(url)
                .retrieve()
                .toEntity(JsonNode.class)
                .doOnSubscribe(s -> log.debug("Fetching from Google API: {}", url))
                .timeout(Duration.ofSeconds(5))
                .retryWhen(Retry.backoff(1, Duration.ofSeconds(1))
                        .filter(throwable -> {
                            if (throwable instanceof WebClientResponseException wcre) {
                                // Don't retry on 429 (rate limit) - fail fast instead
                                return wcre.getStatusCode().is5xxServerError();
                            }
                            return throwable instanceof IOException || throwable instanceof WebClientRequestException || throwable instanceof PrematureCloseException;
                        })
                        .doBeforeRetry(retrySignal -> {
                            if (retrySignal.failure() instanceof WebClientResponseException wcre) {
                                LoggingUtils.warn(log, wcre,
                                        "Retrying API call to {} after status {}. Attempt #{}",
                                        url, wcre.getStatusCode(), retrySignal.totalRetries() + 1);
                            } else {
                                LoggingUtils.warn(log, retrySignal.failure(),
                                        "Retrying API call to {} after error. Attempt #{}",
                                        url, retrySignal.totalRetries() + 1);
                            }
                        })
                        .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                            LoggingUtils.error(log, retrySignal.failure(), "All retries failed for API call {}", url);
                            apiRequestMonitor.recordFailedRequest(endpoint, "All retries failed: " + retrySignal.failure().getMessage());
                            return retrySignal.failure();
                        }))
                .doOnSuccess(responseEntity -> {
                    if (responseEntity != null) {
                        JsonNode body = responseEntity.getBody();
                        int responseSize = body == null ? 0 : body.toString().length();
                        ExternalApiLogger.logHttpResponse(log,
                            responseEntity.getStatusCode().value(),
                            url,
                            responseSize);
                        apiRequestMonitor.recordSuccessfulRequest(endpoint);
                    }
                    if (authenticated) {
                        circuitBreakerService.recordSuccess();
                    }
                })
                .map(responseEntity -> responseEntity != null ? responseEntity.getBody() : null)
                .onErrorResume(e -> {
                    if (e instanceof PrematureCloseException) {
                        // Treat premature close as transient/cancellation; do not trip the circuit breaker
                        LoggingUtils.warn(log, e, "Connection prematurely closed during Google API call: {}", url);
                        apiRequestMonitor.recordFailedRequest(endpoint, "Premature close: " + e.getMessage());
                        return Mono.empty();
                    }
                    if (e instanceof WebClientResponseException wcre) {
                        LoggingUtils.error(log, wcre,
                                "Error fetching from Google API after retries: HTTP Status {}, Body: {}",
                                wcre.getStatusCode(), wcre.getResponseBodyAsString());
                        if (authenticated) {
                            if (wcre.getStatusCode().value() == 429) {
                                circuitBreakerService.recordRateLimitFailure();
                            } else {
                                circuitBreakerService.recordGeneralFailure();
                            }
                        }
                    } else {
                        LoggingUtils.error(log, e, "Error fetching from Google API after retries");
                        if (authenticated) {
                            circuitBreakerService.recordGeneralFailure();
                        }
                    }
                    ExternalApiLogger.logApiCallFailure(log, "GoogleBooks", "FETCH_VOLUME", url, e.getMessage());
                    apiRequestMonitor.recordFailedRequest(endpoint, e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Searches volumes with authenticated API call
     *
     * @param query Search terms
     * @param startIndex Pagination start index
     * @param orderBy Sort order ("relevance", "newest")
     * @param langCode Language restriction code
     * @return JsonNode containing search results
     */
    public Mono<JsonNode> searchVolumesAuthenticated(String query, int startIndex, String orderBy, String langCode) {
        if (!googleFallbackEnabled) {
            log.debug("Google fallback disabled - skipping authenticated search for query '{}'", query);
            return Mono.empty();
        }
        // Check circuit breaker first
        if (!circuitBreakerService.isApiCallAllowed()) {
            log.info("Circuit breaker is OPEN - skipping authenticated search for query '{}'. Caller should try unauthenticated fallback.", query);
            ExternalApiLogger.logCircuitBreakerBlocked(log, "GoogleBooks", query);
            return Mono.empty();
        }
        
        if (googleBooksApiKey == null || googleBooksApiKey.isEmpty()) {
            log.debug("No API key configured - skipping authenticated search for query '{}'", query);
            return Mono.empty();
        }
        return searchVolumesInternal(query, startIndex, orderBy, langCode, true);
    }

    /**
     * Searches volumes with unauthenticated API call
     *
     * @param query Search terms
     * @param startIndex Pagination start index
     * @param orderBy Sort order ("relevance", "newest")
     * @param langCode Language restriction code
     * @return JsonNode containing search results
     */
    public Mono<JsonNode> searchVolumesUnauthenticated(String query, int startIndex, String orderBy, String langCode) {
        if (!googleFallbackEnabled) {
            log.debug("Google fallback disabled - skipping unauthenticated search for query '{}'", query);
            return Mono.empty();
        }
        return searchVolumesInternal(query, startIndex, orderBy, langCode, false);
    }

    /**
     * Streams individual search result items across the requested page span using either the
     * authenticated or unauthenticated Google Books API. Callers can focus on item-level handling
     * without re-implementing the paging mechanics.
     *
     * @param query Search terms to execute
     * @param maxResultsToFetch Maximum number of results to retrieve (<= 0 treated as 40)
     * @param orderBy Sort order ("relevance", "newest")
     * @param langCode Optional language restriction
     * @param authenticated Whether to use authenticated calls
     * @return Flux emitting each item JsonNode
     */
    public Flux<JsonNode> streamSearchItems(String query,
                                            int maxResultsToFetch,
                                            String orderBy,
                                            String langCode,
                                            boolean authenticated) {
        final int maxResultsPerPage = 40;
        final int effectiveMax = maxResultsToFetch > 0 ? maxResultsToFetch : maxResultsPerPage;
        final int pageCount = (effectiveMax + maxResultsPerPage - 1) / maxResultsPerPage;

        return Flux.range(0, pageCount)
            .map(page -> page * maxResultsPerPage)
            .concatMap(startIndex -> {
                Mono<JsonNode> apiCall = authenticated
                    ? searchVolumesAuthenticated(query, startIndex, orderBy, langCode)
                    : searchVolumesUnauthenticated(query, startIndex, orderBy, langCode);

                ExternalApiLogger.logApiCallAttempt(log,
                    "GoogleBooks",
                    "SEARCH_PAGE",
                    String.format("%s start=%d", query, startIndex),
                    authenticated);

                return apiCall
                    .flatMapMany(responseNode -> {
                        if (responseNode != null && responseNode.has("items") && responseNode.get("items").isArray()) {
                            int count = responseNode.get("items").size();
                            ExternalApiLogger.logApiCallSuccess(log,
                                "GoogleBooks",
                                "SEARCH_PAGE",
                                String.format("%s start=%d", query, startIndex),
                                count);
                            return Flux.fromIterable(responseNode.get("items"));
                        }
                        ExternalApiLogger.logApiCallSuccess(log,
                            "GoogleBooks",
                            "SEARCH_PAGE",
                            String.format("%s start=%d", query, startIndex),
                            0);
                        log.debug("GoogleApiFetcher: {} search page for query '{}' startIndex {} returned no items.",
                                authenticated ? "Authenticated" : "Unauthenticated", query, startIndex);
                        return Flux.empty();
                    })
                    .switchIfEmpty(Flux.defer(() -> {
                        // If authenticated call returned empty (e.g., circuit breaker), gracefully end stream
                        log.debug("GoogleApiFetcher: {} search returned empty for query '{}' at startIndex {}. Stream ending gracefully.",
                                authenticated ? "Authenticated" : "Unauthenticated", query, startIndex);
                        return Flux.empty();
                    }))
                    .onErrorResume(e -> {
                        LoggingUtils.warn(log, e,
                                "GoogleApiFetcher: Error during {} search page for query '{}' at startIndex {}. Continuing stream.",
                                authenticated ? "authenticated" : "unauthenticated", query, startIndex);
                        ExternalApiLogger.logApiCallFailure(log,
                                "GoogleBooks",
                                "SEARCH_PAGE",
                                String.format("%s start=%d", query, startIndex),
                                e.getMessage());
                        return Flux.empty();
                    });
            })
            .take(effectiveMax);
    }

    /**
     * Internal implementation for searching volumes
     * 
     * @param query Search terms
     * @param startIndex Pagination start index
     * @param orderBy Sort order
     * @param langCode Language filter
     * @param authenticated Use API key for authentication
     * @return JsonNode response with search results
     */
    private Mono<JsonNode> searchVolumesInternal(String query, int startIndex, String orderBy, String langCode, boolean authenticated) {
        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(googleBooksApiUrl)
                .pathSegment("volumes")
                .queryParam("q", query) // builder will safely encode
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

        String url = builder.build(false).toUriString(); // Changed to false to allow UriComponentsBuilder to encode
        String authStatus = authenticated ? "authenticated" : "unauthenticated";
        String endpoint = "volumes/search/" + getQueryTypeForMonitoring(query) + "/" + authStatus;

        log.debug("Making Google Books API search call ({}) for query: {}, startIndex: {}, endpoint: {}",
                authStatus, query, startIndex, endpoint);

        ExternalApiLogger.logHttpRequest(log, "GET", url, authenticated);

        return webClient.get()
                .uri(url)
                .retrieve()
                .toEntity(JsonNode.class)
                .doOnSubscribe(s -> log.debug("Making Google Books API search call ({}) for query: {}, startIndex: {}", authStatus, query, startIndex))
                .timeout(Duration.ofSeconds(5)) // Add 5-second timeout to prevent blocking
                .retryWhen(Retry.backoff(1, Duration.ofSeconds(1)) // Reduced retries for faster failure
                        .filter(throwable -> {
                            if (throwable instanceof WebClientResponseException wcre) {
                                // Don't retry on 429 (rate limit) - fail fast instead
                                return wcre.getStatusCode().is5xxServerError();
                            }
                            return throwable instanceof IOException || throwable instanceof WebClientRequestException;
                        })
                        .doBeforeRetry(retrySignal -> {
                            String targetUrl = url; // Or a more generic endpoint description
                            if (retrySignal.failure() instanceof WebClientResponseException wcre) {
                                LoggingUtils.warn(log, wcre,
                                        "Retrying API search call ({}) to {} after status {}. Attempt #{}",
                                        authStatus, targetUrl, wcre.getStatusCode(), retrySignal.totalRetries() + 1);
                            } else {
                                LoggingUtils.warn(log, retrySignal.failure(),
                                        "Retrying API search call ({}) to {} after error. Attempt #{}",
                                        authStatus, targetUrl, retrySignal.totalRetries() + 1);
                            }
                        })
                        .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                            LoggingUtils.error(log, retrySignal.failure(),
                                    "All retries failed for API search call ({}) for query '{}', startIndex {}",
                                    authStatus, query, startIndex);
                            apiRequestMonitor.recordFailedRequest(endpoint, "All retries failed: " + retrySignal.failure().getMessage());
                            return retrySignal.failure();
                        }))
                .doOnSuccess(responseEntity -> {
                    if (responseEntity != null) {
                        JsonNode body = responseEntity.getBody();
                        int responseSize = body == null ? 0 : body.toString().length();
                        ExternalApiLogger.logHttpResponse(log,
                            responseEntity.getStatusCode().value(),
                            url,
                            responseSize);
                        apiRequestMonitor.recordSuccessfulRequest(endpoint);
                    }
                    if (authenticated) {
                        circuitBreakerService.recordSuccess();
                    }
                })
                .map(responseEntity -> responseEntity != null ? responseEntity.getBody() : null)
                .onErrorResume(e -> {
                    if (e instanceof PrematureCloseException) {
                        // Treat premature close as transient/cancellation; do not trip the circuit breaker
                        LoggingUtils.warn(log, e,
                            "Connection prematurely closed during Google API search ({}) for query '{}' at startIndex {}",
                            authStatus, query, startIndex);
                        apiRequestMonitor.recordFailedRequest(endpoint, "Premature close: " + e.getMessage());
                        return Mono.empty();
                    }
                    if (e instanceof WebClientResponseException wcre) {
                        LoggingUtils.error(log, wcre,
                            "Error fetching page for API search call ({}) for query '{}' at startIndex {} after retries: HTTP Status {}, Body: {}",
                            authStatus, query, startIndex, wcre.getStatusCode(), wcre.getResponseBodyAsString());
                        
                        // Record circuit breaker failure for authenticated calls
                        if (authenticated) {
                            if (wcre.getStatusCode().value() == 429) {
                                circuitBreakerService.recordRateLimitFailure();
                            } else {
                                circuitBreakerService.recordGeneralFailure();
                            }
                        }
                    } else {
                        LoggingUtils.error(log, e,
                            "Error fetching page for API search call ({}) for query '{}' at startIndex {} after retries",
                            authStatus, query, startIndex);
                        if (authenticated) {
                            circuitBreakerService.recordGeneralFailure();
                        }
                    }
                    apiRequestMonitor.recordFailedRequest(endpoint, e.getMessage());
                    ExternalApiLogger.logApiCallFailure(log,
                        "GoogleBooks",
                        "SEARCH_HTTP",
                        url,
                        e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Categorizes query for monitoring metrics
     * 
     * @param query Search query to analyze
     * @return Query type category
     */
    private String getQueryTypeForMonitoring(String query) {
        if (query.contains("intitle:")) return "title";
        if (query.contains("inauthor:")) return "author";
        if (query.contains("isbn:")) return "isbn";
        return "general";
    }

    /**
     * Checks if a Google Books API key is configured and available
     *
     * @return true if an API key is present, false otherwise
     */
    public boolean isApiKeyAvailable() {
        return googleBooksApiKey != null && !googleBooksApiKey.isEmpty();
    }

    /**
     * Exposes whether Google fallbacks are currently enabled
     *
     * @return true when Google API calls are allowed
     */
    public boolean isGoogleFallbackEnabled() {
        return googleFallbackEnabled;
    }
}
