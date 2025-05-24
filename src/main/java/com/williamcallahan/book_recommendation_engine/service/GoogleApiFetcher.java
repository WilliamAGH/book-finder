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
import com.williamcallahan.book_recommendation_engine.util.ErrorHandlingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;
import java.util.Optional;

@Service
public class GoogleApiFetcher {

    private static final Logger logger = LoggerFactory.getLogger(GoogleApiFetcher.class);

    private final WebClient webClient;
    private final ApiRequestMonitor apiRequestMonitor;
    private final ApiCircuitBreakerService circuitBreakerService;
    private final Environment environment;

    @Value("${google.books.api.base-url}")
    private String googleBooksApiUrl;

    @Value("${google.books.api.key:#{null}}") // Allow API key to be optional
    private String googleBooksApiKey;

    /**
     * Constructs GoogleApiFetcher with required dependencies
     * 
     * @param webClientBuilder WebClient builder 
     * @param apiRequestMonitor API request tracking service
     * @param circuitBreakerService Circuit breaker for API rate limiting
     * @param environment Spring Environment for profile checks
     */
    public GoogleApiFetcher(WebClient.Builder webClientBuilder,
                           ApiRequestMonitor apiRequestMonitor,
                           ApiCircuitBreakerService circuitBreakerService,
                           Environment environment) {
        this.webClient = webClientBuilder.build();
        this.apiRequestMonitor = apiRequestMonitor;
        this.circuitBreakerService = circuitBreakerService;
        this.environment = environment;
    }

    /**
     * Fetches a volume using authenticated API call
     *
     * @param bookId Google Books ID
     * @return JsonNode response from API
     */
    public Mono<JsonNode> fetchVolumeByIdAuthenticated(String bookId) {
        String endpoint = "volumes/get/" + bookId + "/authenticated";

        return Mono.fromFuture(circuitBreakerService.isApiCallAllowed())
            .flatMap(allowed -> {
                if (!allowed) {
                    logger.warn("Circuit breaker is OPEN for endpoint: {} - API call for book ID: {} blocked.", endpoint, bookId);
                    apiRequestMonitor.recordFailedRequest(endpoint, "Circuit breaker open");
                    return Mono.empty();
                }

                if (googleBooksApiKey == null || googleBooksApiKey.isEmpty()) {
                    logger.warn("Authenticated API call attempted for bookId {} without an API key for endpoint {}. This call will likely fail or be rate-limited.", bookId, endpoint);
                }
                String url = UriComponentsBuilder.fromUriString(googleBooksApiUrl)
                        .pathSegment("volumes", bookId)
                        .queryParamIfPresent("key", Optional.ofNullable(googleBooksApiKey).filter(key -> !key.isEmpty()))
                        .build(false)
                        .toUriString();
                
                if (environment.acceptsProfiles(Profiles.of("dev"))) {
                    logger.info("[ASYNC_WEBC] Making Authenticated Google Books API GET call for book ID: {}, endpoint: {}", bookId, endpoint);
                } else {
                    logger.debug("Making Authenticated Google Books API GET call for book ID: {}, endpoint: {}", bookId, endpoint);
                }

                return webClient.get()
                        .uri(url)
                        .retrieve()
                        .bodyToMono(JsonNode.class)
                        .doOnSubscribe(s -> {
                            if (environment.acceptsProfiles(Profiles.of("dev"))) {
                                logger.info("[ASYNC_WEBC] Fetching book {} from Google API (Authenticated).", bookId);
                            } else {
                                logger.debug("Fetching book {} from Google API (Authenticated).", bookId);
                            }
                        })
                        .retryWhen(ErrorHandlingUtils.createWebClientRetry(logger,
                                "Authenticated API call for book " + bookId,
                                () -> apiRequestMonitor.recordFailedRequest(endpoint, "All retries failed")))
                        .doOnSuccess(response -> {
                            apiRequestMonitor.recordSuccessfulRequest(endpoint);
                            circuitBreakerService.recordSuccess();
                        })
                        .onErrorResume(e -> {
                            if (e instanceof WebClientResponseException) {
                                WebClientResponseException wcre = (WebClientResponseException) e;
                                logger.error("Error fetching book {} from Google API (Authenticated) after retries: HTTP Status {}, Body: {}", 
                                    bookId, wcre.getStatusCode(), wcre.getResponseBodyAsString(), wcre);
                                if (wcre.getStatusCode().value() == 429) {
                                    circuitBreakerService.recordRateLimitFailure();
                                } else {
                                    circuitBreakerService.recordGeneralFailure();
                                }
                            } else {
                                logger.error("Error fetching book {} from Google API (Authenticated) after retries: {}", bookId, e.getMessage(), e);
                                circuitBreakerService.recordGeneralFailure();
                            }
                            apiRequestMonitor.recordFailedRequest(endpoint, e.getMessage());
                            return Mono.empty();
                        });
            });
    }

    /**
     * Fetches a volume using unauthenticated API call
     *
     * @param bookId Google Books ID
     * @return JsonNode response from API
     */
    public Mono<JsonNode> fetchVolumeByIdUnauthenticated(String bookId) {
        String url = UriComponentsBuilder.fromUriString(googleBooksApiUrl)
                .pathSegment("volumes", bookId)
                // No API key for unauthenticated calls
                .build(false) // Changed to false to allow UriComponentsBuilder to encode
                .toUriString();

        String endpoint = "volumes/get/" + bookId + "/unauthenticated";
        if (environment.acceptsProfiles(Profiles.of("dev"))) {
            logger.info("[ASYNC_WEBC] Making Unauthenticated Google Books API GET call for book ID: {}, endpoint: {}", bookId, endpoint);
        } else {
            logger.debug("Making Unauthenticated Google Books API GET call for book ID: {}, endpoint: {}", bookId, endpoint);
        }
        
        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .doOnSubscribe(s -> {
                    if (environment.acceptsProfiles(Profiles.of("dev"))) {
                        logger.info("[ASYNC_WEBC] Fetching book {} from Google API (Unauthenticated).", bookId);
                    } else {
                        logger.debug("Fetching book {} from Google API (Unauthenticated).", bookId);
                    }
                })
                .retryWhen(ErrorHandlingUtils.createWebClientRetry(logger,
                        "Unauthenticated API call for book " + bookId,
                        () -> apiRequestMonitor.recordFailedRequest(endpoint, "All retries failed")))
                .doOnSuccess(response -> apiRequestMonitor.recordSuccessfulRequest(endpoint))
                .onErrorResume(e -> {
                    if (e instanceof WebClientResponseException) {
                        WebClientResponseException wcre = (WebClientResponseException) e;
                        logger.error("Error fetching book {} from Google API (Unauthenticated) after retries: HTTP Status {}, Body: {}", 
                            bookId, wcre.getStatusCode(), wcre.getResponseBodyAsString(), wcre);
                    } else {
                        logger.error("Error fetching book {} from Google API (Unauthenticated) after retries: {}", bookId, e.getMessage(), e);
                    }
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
        String endpoint = "volumes/search/" + getQueryTypeForMonitoring(query) + "/authenticated"; // Define endpoint early for logging

        return Mono.fromFuture(circuitBreakerService.isApiCallAllowed())
            .flatMap(allowed -> {
                if (!allowed) {
                    logger.warn("Circuit breaker is OPEN for endpoint: {} - blocking authenticated search API call for query: {}", endpoint, query);
                    apiRequestMonitor.recordFailedRequest(endpoint, "Circuit breaker open"); // Record as failed due to CB
                    return Mono.empty();
                }

                if (googleBooksApiKey == null || googleBooksApiKey.isEmpty()) {
                    logger.warn("Authenticated search API call attempted for query '{}' without an API key for endpoint {}.", query, endpoint);
                }
                return searchVolumesInternal(query, startIndex, orderBy, langCode, true);
            });
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
        return searchVolumesInternal(query, startIndex, orderBy, langCode, false);
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

        if (environment.acceptsProfiles(Profiles.of("dev"))) {
            logger.info("[ASYNC_WEBC] Making Google Books API search call ({}) for query: {}, startIndex: {}, endpoint: {}",
                    authStatus, query, startIndex, endpoint);
        } else {
            logger.debug("Making Google Books API search call ({}) for query: {}, startIndex: {}, endpoint: {}",
                    authStatus, query, startIndex, endpoint);
        }

        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .doOnSubscribe(s -> {
                    if (environment.acceptsProfiles(Profiles.of("dev"))) {
                        logger.info("[ASYNC_WEBC] Making Google Books API search call ({}) for query: {}, startIndex: {}", authStatus, query, startIndex);
                    } else {
                        logger.debug("Making Google Books API search call ({}) for query: {}, startIndex: {}", authStatus, query, startIndex);
                    }
                })
                .retryWhen(ErrorHandlingUtils.createWebClientRetry(logger,
                        String.format("API search call (%s) for query '%s', startIndex %d", authStatus, query, startIndex),
                        () -> apiRequestMonitor.recordFailedRequest(endpoint, "All retries failed")))
                .doOnSuccess(response -> {
                    apiRequestMonitor.recordSuccessfulRequest(endpoint);
                    if (authenticated) {
                        circuitBreakerService.recordSuccess();
                    }
                })
                .onErrorResume(e -> {
                    if (e instanceof WebClientResponseException) {
                        WebClientResponseException wcre = (WebClientResponseException) e;
                        logger.error("Error fetching page for API search call ({}) for query '{}' at startIndex {} after retries: HTTP Status {}, Body: {}",
                            authStatus, query, startIndex, wcre.getStatusCode(), wcre.getResponseBodyAsString(), wcre);
                        
                        // Record circuit breaker failure for authenticated calls
                        if (authenticated) {
                            if (wcre.getStatusCode().value() == 429) {
                                circuitBreakerService.recordRateLimitFailure();
                            } else {
                                circuitBreakerService.recordGeneralFailure();
                            }
                        }
                    } else {
                        logger.error("Error fetching page for API search call ({}) for query '{}' at startIndex {} after retries: {}",
                            authStatus, query, startIndex, e.getMessage(), e);
                        if (authenticated) {
                            circuitBreakerService.recordGeneralFailure();
                        }
                    }
                    apiRequestMonitor.recordFailedRequest(endpoint, e.getMessage());
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
}
