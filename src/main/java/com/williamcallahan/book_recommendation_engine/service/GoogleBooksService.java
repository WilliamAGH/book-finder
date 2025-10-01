/**
 * Service for interacting with the Google Books API
 * This class is responsible for all direct communication with the Google Books API
 * It handles:
 * - Constructing and executing API requests for searching books and retrieving book details
 * - Applying rate limiting (via Resilience4j {@code @RateLimiter}) and circuit breaking ({@code @CircuitBreaker})
 *   to protect the application and the external API
 * - Converting JSON responses from the Google Books API into {@link Book} domain objects
 * - Providing normalized output for the Postgres-first caching pipeline
 * - Monitoring API usage and performance via {@link ApiRequestMonitor}
 * - Transforming cover image URLs for optimal quality and display
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.dto.BookListItem;
import com.williamcallahan.book_recommendation_engine.mapper.GoogleBooksMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.model.image.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.util.BookDomainMapper;
import com.williamcallahan.book_recommendation_engine.util.SearchQueryQualifierExtractor;
import com.williamcallahan.book_recommendation_engine.service.image.GoogleCoverUrlEvaluator;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.ExternalApiLogger;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import com.williamcallahan.book_recommendation_engine.service.image.ExternalCoverFetchHelper;
import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.util.cover.CoverIdentifierResolver;
import com.williamcallahan.book_recommendation_engine.util.GoogleBooksUrlEnhancer;
import com.williamcallahan.book_recommendation_engine.util.UuidUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.*;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import com.williamcallahan.book_recommendation_engine.service.image.LocalDiskCoverCacheService;
import com.williamcallahan.book_recommendation_engine.service.image.CoverCacheManager;

@Service
@Slf4j
public class GoogleBooksService {

    private final ObjectMapper objectMapper;
    private final ApiRequestMonitor apiRequestMonitor;
    private final GoogleApiFetcher googleApiFetcher;
    private final ExternalCoverFetchHelper externalCoverFetchHelper;
    private final GoogleCoverUrlEvaluator googleCoverUrlEvaluator;
    private final GoogleBooksMapper googleBooksMapper;

    private BookSearchService bookSearchService;
    private BookQueryRepository bookQueryRepository;

    private static final int ISBN_BATCH_QUERY_SIZE = 5; // Reduced batch size for ISBN OR queries
    private static final List<String> GOOGLE_ID_TAG_KEYS = List.of(
        "google_canonical_id",
        "googleVolumeId",
        "google_volume_id",
        "google_volume",
        "google_book_id"
    );
    
    @Value("${app.nyt.scheduler.google.books.api.batch-delay-ms:200}") // Configurable delay
    private long isbnBatchDelayMs;

    /**
     * Constructs a GoogleBooksService with necessary dependencies
     *
     * @param objectMapper Jackson ObjectMapper for JSON processing
     * @param apiRequestMonitor Service for monitoring API usage metrics
     * @param googleApiFetcher Service for direct Google API calls
     */
    public GoogleBooksService(
            ObjectMapper objectMapper,
            ApiRequestMonitor apiRequestMonitor,
            GoogleApiFetcher googleApiFetcher,
            ExternalCoverFetchHelper externalCoverFetchHelper,
            GoogleCoverUrlEvaluator googleCoverUrlEvaluator,
            GoogleBooksMapper googleBooksMapper) {
        this.objectMapper = objectMapper;
        this.apiRequestMonitor = apiRequestMonitor;
        this.googleApiFetcher = googleApiFetcher;
        this.externalCoverFetchHelper = externalCoverFetchHelper;
        this.googleCoverUrlEvaluator = googleCoverUrlEvaluator;
        this.googleBooksMapper = googleBooksMapper;
    }

    /**
     * Performs a search against the Google Books API using GoogleApiFetcher
     * This is a lower-level method that fetches a single page of results
     * It is protected by rate limiting and circuit breaking
     *
     * @param query Search query string
     * @param startIndex Starting index for pagination (0-based)
     * @param orderBy Result ordering preference (e.g., "newest", "relevance")
     * @param langCode Optional language code filter (e.g., "en", "fr") to restrict search results
     * @return Mono containing the raw JsonNode response from the API for a single page
     */
    @CircuitBreaker(name = "googleBooksService", fallbackMethod = "searchBooksFallback")
    @TimeLimiter(name = "googleBooksService")
    @RateLimiter(name = "googleBooksServiceRateLimiter", fallbackMethod = "searchBooksRateLimitFallback")
    public Mono<JsonNode> searchBooks(String query, int startIndex, String orderBy, String langCode) {
        // Delegate to GoogleApiFetcher for authenticated search
        // Resilience4j annotations remain on this public method
        // The actual API call and its own retry/monitoring are within GoogleApiFetcher
        // This method now primarily serves as an entry point with resilience
        log.debug("GoogleBooksService.searchBooks called for query: {}, startIndex: {}. Delegating to GoogleApiFetcher.", query, startIndex);
        // Assuming this service primarily deals with authenticated calls if it's a direct passthrough
        // The plan implies GoogleBooksService might be a thin wrapper for *authenticated* calls
        return googleApiFetcher.searchVolumesAuthenticated(query, startIndex, orderBy, langCode)
            // Record successful API call on receiving a response
            .doOnNext(response -> apiRequestMonitor.recordSuccessfulRequest(
                "volumes/search/authenticated?query=" + query + "&startIndex=" + startIndex + "&orderBy=" + orderBy + "&langCode=" + langCode
            ))
            .doOnError(e -> apiRequestMonitor.recordFailedRequest(
                "volumes/search/authenticated?query=" + query + "&startIndex=" + startIndex,
                e.getMessage()));
    }

    /**
     * Performs a comprehensive search across multiple Google Books API pages, aggregating results
     * This method orchestrates multiple calls to the single-page {@code searchBooks} method (which now uses GoogleApiFetcher)
     *
     * @param query Search query string to send to the API
     * @param langCode Optional language code to restrict results (e.g., "en", "fr")
     * @param desiredTotalResults The desired maximum number of total results to fetch across all pages
     * @param orderBy The order to sort results by (e.g., "relevance", "newest")
     * @return Mono containing a list of {@link Book} objects retrieved from the API
     *
     * @deprecated Use {@link BookSearchService} with {@link com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository}
     * DTO projections, invoking {@link GoogleApiFetcher#streamSearchItems(String, int, String, String, boolean)} directly when
     * external fallback is absolutely required.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<Book>> searchBooksAsyncReactive(String query, String langCode, int desiredTotalResults, String orderBy) {
        if (bookSearchService == null || bookQueryRepository == null) {
            return Mono.just(List.of());
        }

        final int safeDesired = desiredTotalResults > 0 ? desiredTotalResults : 20;

        return Mono.fromCallable(() -> bookSearchService.searchBooks(query, safeDesired))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(results -> assembleBaselineResults(results)
                .flatMap(baseline -> {
                    if (baseline.size() >= safeDesired || !googleApiFetcher.isGoogleFallbackEnabled()) {
                        return Mono.just(baseline);
                    }

                    int remaining = Math.max(safeDesired - baseline.size(), 0);
                    if (remaining == 0) {
                        return Mono.just(baseline);
                    }

                    return streamBooksReactive(query, langCode, remaining, orderBy)
                        .collectList()
                        .map(fallback -> mergeSearchResults(baseline, fallback, safeDesired))
                        .defaultIfEmpty(baseline)
                        .onErrorResume(ex -> {
                            log.warn("Google fallback search failed for query '{}': {}", query, ex.getMessage());
                            return Mono.just(baseline);
                        });
                }))
            .defaultIfEmpty(List.of())
            .doOnError(ex -> log.warn("Postgres search failed for query '{}': {}", query, ex.getMessage(), ex))
            .onErrorReturn(List.of());
    }

    /**
     * Streams results from Google Books, emitting authenticated results first and falling back to
     * unauthenticated calls when the authenticated path is blocked or unavailable. Results are
     * deduplicated by volume ID and capped to the desired total.
     */
    /**
     * @deprecated Stream `BookCard` DTOs via {@link com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository}
     * and call {@link GoogleApiFetcher#streamSearchItems(String, int, String, String, boolean)} directly for raw payloads.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Flux<Book> streamBooksReactive(String query,
                                          String langCode,
                                          int desiredTotalResults,
                                          String orderBy) {
        if (!googleApiFetcher.isGoogleFallbackEnabled()) {
            log.debug("GoogleBooksService.streamBooksReactive skipping because Google fallback disabled for query '{}'", query);
            return Flux.empty();
        }

        final int maxTotalResultsToFetch = (desiredTotalResults > 0) ? desiredTotalResults : 200;
        final String effectiveOrderBy = (orderBy != null && !orderBy.trim().isEmpty()) ? orderBy : "newest";
        final Map<String, Object> queryQualifiers = SearchQueryQualifierExtractor.extract(query);

        final boolean apiKeyAvailable = googleApiFetcher.isApiKeyAvailable();

        Set<String> seenIds = Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

        Flux<Book> authenticatedFlux = Flux.empty();
        if (apiKeyAvailable) {
            authenticatedFlux = googleApiFetcher.streamSearchItems(query, maxTotalResultsToFetch, effectiveOrderBy, langCode, true)
                .doOnSubscribe(subscription -> ExternalApiLogger.logApiCallAttempt(log,
                    "GoogleBooks",
                    "STREAM_AUTH",
                    query,
                    true))
                .map(this::convertJsonToBook)
                .filter(Objects::nonNull)
                .map(book -> applyQualifiers(book, queryQualifiers))
                .onErrorResume(error -> {
                    LoggingUtils.warn(log, error,
                        "GoogleBooksService.streamBooksReactive authenticated search failed for query '{}'", query);
                    apiRequestMonitor.recordFailedRequest("volumes/search/authenticated/" + query, error.getMessage());
                    ExternalApiLogger.logApiCallFailure(log,
                        "GoogleBooks",
                        "STREAM_AUTH",
                        query,
                        error.getMessage());
                    return Flux.empty();
                });
        }

        Flux<Book> unauthenticatedFlux = googleApiFetcher.streamSearchItems(query, maxTotalResultsToFetch, effectiveOrderBy, langCode, false)
            .doOnSubscribe(subscription -> ExternalApiLogger.logApiCallAttempt(log,
                "GoogleBooks",
                "STREAM_UNAUTH",
                query,
                false))
            .map(this::convertJsonToBook)
            .filter(Objects::nonNull)
            .map(book -> applyQualifiers(book, queryQualifiers))
            .onErrorResume(error -> {
                LoggingUtils.warn(log, error,
                    "GoogleBooksService.streamBooksReactive unauthenticated search failed for query '{}'", query);
                apiRequestMonitor.recordFailedRequest("volumes/search/unauthenticated/" + query, error.getMessage());
                ExternalApiLogger.logApiCallFailure(log,
                    "GoogleBooks",
                    "STREAM_UNAUTH",
                    query,
                    error.getMessage());
                return Flux.empty();
            });

        Flux<Book> combinedFlux = apiKeyAvailable
            ? Flux.concat(authenticatedFlux, unauthenticatedFlux)
            : unauthenticatedFlux;

        return combinedFlux
            .filter(book -> book.getId() != null && seenIds.add(book.getId()))
            .take(maxTotalResultsToFetch)
            .doOnSubscribe(subscription -> log.debug(
                "GoogleBooksService.streamBooksReactive starting stream for query '{}' (max {} results, orderBy={})",
                query, maxTotalResultsToFetch, effectiveOrderBy))
            .doOnNext(book -> apiRequestMonitor.recordSuccessfulRequest("volumes/search/stream"))
            .doOnComplete(() -> {
                log.debug(
                    "GoogleBooksService.streamBooksReactive completed stream for query '{}' (emitted {} results)",
                    query, seenIds.size());
                ExternalApiLogger.logApiCallSuccess(log,
                    "GoogleBooks",
                    "STREAM_COMBINED",
                    query,
                    seenIds.size());
            });
    }

    /**
     * Overloaded version of searchBooksAsyncReactive with default values
     * 
     * @param query Search query string to send to the API
     * @param langCode Optional language code to restrict results (e.g., "en", "fr")
     * @return Mono containing a list of Book objects retrieved from the API
     */
    /**
     * @deprecated See {@link #searchBooksAsyncReactive(String, String, int, String)}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<Book>> searchBooksAsyncReactive(String query, String langCode) {
        return searchBooksAsyncReactive(query, langCode, 200, "newest"); 
    }

    /**
     * Overloaded version of searchBooksAsyncReactive with no language filtering
     * 
     * @param query Search query string to send to the API
     * @return Mono containing a list of Book objects retrieved from the API
     */
    /**
     * @deprecated See {@link #searchBooksAsyncReactive(String, String, int, String)}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<Book>> searchBooksAsyncReactive(String query) {
        return searchBooksAsyncReactive(query, null, 200, "newest");
    }

    /**
     * @deprecated Populate homepage content from Postgres via
     * {@link com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository#fetchBookCards(java.util.List)}
     * and only consult {@link GoogleApiFetcher} within the orchestrator layer.
     */
    private Book applyQualifiers(Book book, Map<String, Object> queryQualifiers) {
        if (book != null && queryQualifiers != null && !queryQualifiers.isEmpty()) {
            queryQualifiers.forEach(book::addQualifier);
        }
        return book;
    }

    /**
     * Searches for books by ISBN using Google Books API
     * @param isbn The ISBN (10 or 13) to search for
     * @return Mono containing a list of books matching the ISBN
     * @deprecated Use {@link BookDataOrchestrator#fetchCanonicalBookReactive(String)} for ISBN lookups
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<Book>> searchBooksByISBN(String isbn) {
        if (!ValidationUtils.hasText(isbn) || bookSearchService == null || bookQueryRepository == null) {
            return Mono.just(List.of());
        }

        String normalized = isbn.trim();

        return Mono.fromCallable(() -> bookSearchService.searchByIsbn(normalized))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(optional -> optional
                .map(result -> Mono.fromCallable(() -> bookQueryRepository.fetchBookDetail(result.bookId()))
                    .subscribeOn(Schedulers.boundedElastic())
                    .map(detailOpt -> detailOpt
                        .map(BookDomainMapper::fromDetail)
                        .map(List::of)
                        .orElse(List.of()))
                    .onErrorResume(ex -> {
                        log.warn("Postgres ISBN detail lookup failed for '{}': {}", normalized, ex.getMessage());
                        return Mono.just(List.<Book>of());
                    }))
                .orElseGet(() -> Mono.just(List.<Book>of())))
            .onErrorResume(ex -> {
                log.warn("GoogleBooksService.searchBooksByISBN failed for '{}': {}", normalized, ex.getMessage());
                return Mono.just(List.<Book>of());
            });
    }

    /**
     * Fetches the Google Books ID for a given ISBN
     * This method is specifically for the NYT scheduler to minimize data transfer
     * when only the ID is needed
     *
     * @param isbn The ISBN (10 or 13) to search for
     * @return Mono emitting the Google Books ID, or empty if not found or error
     */
    @RateLimiter(name = "googleBooksServiceRateLimiter") // Apply rate limiting
    public Mono<String> fetchGoogleBookIdByIsbn(String isbn) {
        log.debug("Fetching Google Book ID for ISBN: {}", isbn);
        return searchBooks("isbn:" + isbn, 0, "relevance", null)
            .map(responseNode -> {
                if (responseNode != null && responseNode.has("items") && responseNode.get("items").isArray() && responseNode.get("items").size() > 0) {
                    JsonNode firstItem = responseNode.get("items").get(0);
                    if (firstItem.has("id")) {
                        String googleId = firstItem.get("id").asText();
                        log.info("Found Google Book ID: {} for ISBN: {}", googleId, isbn);
                        return googleId;
                    }
                }
                log.warn("No Google Book ID found for ISBN: {}", isbn);
                return null; // Will be filtered by filter(Objects::nonNull) or handled by switchIfEmpty
            })
            .filter(Objects::nonNull) // Ensure we only proceed if an ID was found
            .doOnError(e -> {
                LoggingUtils.error(log, e, "Error fetching Google Book ID for ISBN {}", isbn);
                apiRequestMonitor.recordFailedRequest("volumes/search/isbn_to_id/" + isbn, e.getMessage());
            })
            .onErrorResume(e -> Mono.<String>empty());
    }

    /**
     * Retrieve a specific book by its Google Books volume ID
     * This method now delegates to GoogleApiFetcher for the authenticated API call
     * and uses BookJsonParser for conversion. S3 caching is handled by BookDataOrchestrator
     * Resilience4j annotations are kept on this public method
     * 
     * @param bookId Google Books volume ID
     * @return CompletionStage containing the Book object if found, or null otherwise
     */
    @CircuitBreaker(name = "googleBooksService", fallbackMethod = "getBookByIdFallback")
    @TimeLimiter(name = "googleBooksService")
    @RateLimiter(name = "googleBooksServiceRateLimiter", fallbackMethod = "getBookByIdRateLimitFallback")
    /**
     * @deprecated Fetch canonical records via {@link BookDataOrchestrator#fetchCanonicalBookReactive(String)} or
     * {@link com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository} DTOs; direct
     * Google volumes access should go through {@link GoogleApiFetcher} utilities.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public CompletionStage<Book> getBookById(String bookId) {
        log.warn("GoogleBooksService.getBookById called directly for {}. Consider using orchestrated flow via BookCacheFacadeService/GoogleBooksCachingStrategy.", bookId);
        // Prevent misuse: do not call volumes/{id} for slugs or unsafe identifiers
        if (!com.williamcallahan.book_recommendation_engine.util.IdentifierClassifier.isSafeForGoogleBooksVolumesApi(bookId)) {
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }
        return googleApiFetcher.fetchVolumeByIdAuthenticated(bookId)
            .switchIfEmpty(googleApiFetcher.fetchVolumeByIdUnauthenticated(bookId))
            .map(this::convertJsonToBook)
            .filter(Objects::nonNull)
            // Record successful API fetch for the book
            .doOnNext(book -> apiRequestMonitor.recordSuccessfulRequest(
                "volumes/get/authenticated/" + bookId
            ))
            // Handle errors by recording failure and returning empty
            .onErrorResume(e -> {
                LoggingUtils.error(log, e, "Error fetching book by ID {}", bookId);
                apiRequestMonitor.recordFailedRequest(
                    "volumes/get/authenticated/" + bookId,
                    e.getMessage()
                );
                return Mono.<Book>empty();
            })
            .toFuture();
    }

    /**
     * Finds books similar to the given book based on author and title
     * - Creates a search query using author and title information
     * - Excludes the original book from results
     * - Limits results to a maximum of 5 similar books
     * - Returns empty list for invalid input or when no similar books found
     * - Used for book recommendation features
     * 
     * @param book Book to find similar books for
     * @return Mono containing a list of similar Book objects, limited to 5 results
     */
    /**
     * @deprecated Use {@link RecommendationService} /
     * {@link com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository#fetchBookCards(java.util.List)}
     * based pipelines to surface similar titles.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<Book>> getSimilarBooks(Book book) {
        if (book == null || book.getAuthors() == null || book.getAuthors().isEmpty() || book.getTitle() == null) {
            return Mono.just(Collections.emptyList());
        }
        
        String authorQuery = book.getAuthors().stream()
                .findFirst()
                .orElse("")
                .replace(" ", "+");

        String titleQuery = book.getTitle().replace(" ", "+");
        if (authorQuery.isEmpty() && titleQuery.isEmpty()) {
            return Mono.just(Collections.emptyList());
        }

        String query = String.format("inauthor:%s intitle:%s", authorQuery, titleQuery);
        
        // This will now use the refactored searchBooksAsyncReactive
        return searchBooksAsyncReactive(query)
            .map(similarBooksList -> similarBooksList.stream()
                .filter(similarBook -> !similarBook.getId().equals(book.getId()))
                .limit(5)
                .collect(Collectors.toList())
            );
    }

    /**
     * Fallback method for searchBooks when circuit breaker is triggered
     * - Provides graceful degradation when Google Books API is unavailable
     * - Logs warning with detailed error information
     * - Records failure in ApiRequestMonitor
     * - Returns empty Mono to allow for proper error handling in downstream components
     *
     * @param query Search query that triggered the circuit breaker
     * @param startIndex Pagination index being accessed
     * @param orderBy Sort order requested
     * @param langCode Language filter if any
     * @param t The throwable that triggered the circuit breaker
     * @return Empty Mono to indicate no results available
     */
    public Mono<JsonNode> searchBooksFallback(String query, int startIndex, String orderBy, String langCode, Throwable t) {
        log.warn("GoogleBooksService.searchBooks circuit breaker opened for query: '{}', startIndex: {}. Error: {}", 
            query, startIndex, t.getMessage());
        
        String queryType = "general"; // Simplified for fallback, specific type determined by GoogleApiFetcher now
        if (query.contains("intitle:")) queryType = "title";
        else if (query.contains("inauthor:")) queryType = "author";
        else if (query.contains("isbn:")) queryType = "isbn";
        String apiEndpoint = "volumes/search/" + queryType + "/authenticated";
        
        apiRequestMonitor.recordFailedRequest(apiEndpoint, "Circuit breaker opened for query: '" + query + "', startIndex: " + startIndex + ": " + t.getMessage());
        
        // Return an empty object node instead of Mono.empty() to avoid downstream errors
        return Mono.just(objectMapper.createObjectNode());
    }
    
    /**
     * Fallback method for searchBooks when rate limit is exceeded
     * - Handles scenario when too many search requests are made within time period
     * - Logs rate limiting information for monitoring
     * - Records rate limiting events in ApiRequestMonitor
     * - Returns empty Mono to indicate no results available
     *
     * @param query Search query that triggered the rate limiter
     * @param startIndex Pagination index being accessed
     * @param orderBy Sort order requested
     * @param langCode Language filter if any
     * @param t The throwable from the rate limiter
     * @return Empty Mono to indicate no results available
     */
    public Mono<JsonNode> searchBooksRateLimitFallback(String query, int startIndex, String orderBy, String langCode, Throwable t) {
        log.warn("GoogleBooksService.searchBooks rate limit exceeded for query: '{}', startIndex: {}. Error: {}", 
            query, startIndex, t.getMessage());
        
        apiRequestMonitor.recordMetric("api/rate-limited", "API call rate limited for search: " + query + " (via GoogleBooksService)");
        
        // Return an empty object node instead of Mono.empty() to avoid downstream errors
        return Mono.just(objectMapper.createObjectNode());
    }

    /**
     * Fallback method for getBookById when circuit breaker is triggered
     * - Provides graceful degradation when Google Books API is unavailable
     * - Logs warning with detailed error information
     * - Records failure in ApiRequestMonitor
     * - Returns empty CompletionStage to allow for proper error handling
     *
     * @param bookId The book ID that triggered the circuit breaker
     * @param t The throwable that triggered the circuit breaker
     * @return CompletedFuture with null value to indicate no book available
     */
    public CompletionStage<Book> getBookByIdFallback(String bookId, Throwable t) {
        log.warn("GoogleBooksService.getBookById circuit breaker opened for bookId: {}. Error: {}", 
            bookId, t.getMessage());
        
        apiRequestMonitor.recordFailedRequest("volumes/get/" + bookId + "/authenticated", "Circuit breaker opened for bookId: " + bookId + ": " + t.getMessage());
        
        return CompletableFuture.completedFuture(null); 
    }
    
    /**
     * Fallback method for getBookById when rate limit is exceeded
     * - Handles scenario when too many requests are made within time period
     * - Logs rate limiting information for monitoring
     * - Adds rate limit metrics to ApiRequestMonitor
     * - Returns empty CompletionStage to allow for proper error handling
     *
     * @param bookId The book ID that triggered the rate limiter
     * @param t The throwable from the rate limiter
     * @return CompletedFuture with null value to indicate no book available
     */
    public CompletionStage<Book> getBookByIdRateLimitFallback(String bookId, Throwable t) {
        log.warn("GoogleBooksService.getBookById rate limit exceeded for bookId: {}. Error: {}", 
            bookId, t.getMessage());
        
        apiRequestMonitor.recordMetric("api/rate-limited", "API call rate limited for book " + bookId + " (via GoogleBooksService)");
        
        return CompletableFuture.completedFuture(null);
    }

    /**
     * @deprecated Route Google cover hydration through
     * {@link com.williamcallahan.book_recommendation_engine.service.image.CoverSourceFetchingService}
     * so results are persisted via {@link com.williamcallahan.book_recommendation_engine.service.image.CoverPersistenceService}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public CompletableFuture<ImageDetails> fetchCoverByIsbn(
        String isbn,
        String bookIdForLog,
        ImageProvenanceData provenanceData,
        LocalDiskCoverCacheService localDiskCoverCacheService,
        CoverCacheManager coverCacheManager) {

        log.debug("Attempting Google Books API by ISBN {} (book log id: {})", isbn, bookIdForLog);

        return externalCoverFetchHelper.fetchAndCache(
            isbn,
            coverCacheManager::isKnownBadImageUrl,
            url -> coverCacheManager.addKnownBadImageUrl(url),
            () -> searchBooksByISBN(isbn)
                .toFuture()
                .thenApply(books -> {
                    if (books == null || books.isEmpty()) {
                        return Optional.empty();
                    }
                    Book googleBook = books.get(0);
                    if (googleBook == null) {
                        return Optional.empty();
                    }
                    if (googleBook.getRawJsonResponse() != null && provenanceData.getGoogleBooksApiResponse() == null) {
                        provenanceData.setGoogleBooksApiResponse(googleBook.getRawJsonResponse());
                    }
                    String googleUrl = googleBook.getExternalImageUrl();
                    if (!ValidationUtils.hasText(googleUrl) || googleUrl.contains("image-not-available.png")) {
                        return Optional.empty();
                    }
                    String enhanced = GoogleBooksUrlEnhancer.enhanceUrl(googleUrl, 0);
                    return Optional.of(new ImageDetails(
                        enhanced,
                        "GOOGLE_BOOKS",
                        googleBook.getId(),
                        com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource.GOOGLE_BOOKS,
                        ImageResolutionPreference.ORIGINAL
                    ));
                }),
            "Google ISBN: " + isbn,
            ImageSourceName.GOOGLE_BOOKS,
            "GoogleBooksAPI-ISBN",
            "google-isbn",
            provenanceData,
            bookIdForLog,
            new ExternalCoverFetchHelper.ValidationHooks(
                googleCoverUrlEvaluator::isAcceptableUrl,
                details -> googleCoverUrlEvaluator.isAcceptableUrl(details.getUrlOrPath())
            )
        );
    }

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.service.image.CoverSourceFetchingService}
     * for Google volume lookups persisted via {@link com.williamcallahan.book_recommendation_engine.service.image.CoverPersistenceService}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public CompletableFuture<ImageDetails> fetchCoverByVolumeId(
        String googleVolumeId,
        String bookIdForLog,
        ImageProvenanceData provenanceData,
        LocalDiskCoverCacheService localDiskCoverCacheService,
        CoverCacheManager coverCacheManager) {

        log.debug("Attempting Google Books API by Volume ID {} (book log id: {})", googleVolumeId, bookIdForLog);

        return externalCoverFetchHelper.fetchAndCache(
            googleVolumeId,
            coverCacheManager::isKnownBadImageUrl,
            url -> coverCacheManager.addKnownBadImageUrl(url),
            () -> getBookById(googleVolumeId)
                .toCompletableFuture()
                .thenApply(googleBook -> {
                    if (googleBook != null && googleBook.getRawJsonResponse() != null && provenanceData.getGoogleBooksApiResponse() == null) {
                        provenanceData.setGoogleBooksApiResponse(googleBook.getRawJsonResponse());
                    }
                    if (googleBook == null) {
                        return Optional.empty();
                    }
                    String googleUrl = googleBook.getExternalImageUrl();
                    if (!ValidationUtils.hasText(googleUrl) || googleUrl.contains("image-not-available.png")) {
                        return Optional.empty();
                    }
                    String enhanced = GoogleBooksUrlEnhancer.enhanceUrl(googleUrl, 0);
                    return Optional.of(new ImageDetails(
                        enhanced,
                        "GOOGLE_BOOKS",
                        googleBook.getId(),
                        com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource.GOOGLE_BOOKS,
                        ImageResolutionPreference.ORIGINAL
                    ));
                }),
            "Google VolumeID: " + googleVolumeId,
            ImageSourceName.GOOGLE_BOOKS,
            "GoogleBooksAPI-VolumeID",
            "google-volume",
            provenanceData,
            bookIdForLog,
            new ExternalCoverFetchHelper.ValidationHooks(
                googleCoverUrlEvaluator::isAcceptableUrl,
                details -> googleCoverUrlEvaluator.isAcceptableUrl(details.getUrlOrPath())
            )
        );
    }

    @Autowired(required = false)
    public void setBookSearchService(BookSearchService bookSearchService) {
        this.bookSearchService = bookSearchService;
    }

    @Autowired(required = false)
    public void setBookQueryRepository(BookQueryRepository bookQueryRepository) {
        this.bookQueryRepository = bookQueryRepository;
    }

    private List<Book> assembleOrderedLegacyBooks(LinkedHashMap<UUID, Integer> orderedIds,
                                                  List<BookListItem> items) {
        if (items == null || items.isEmpty()) {
            return List.of();
        }

        Map<UUID, Book> mapped = new HashMap<>();
        for (BookListItem item : items) {
            if (item == null) {
                continue;
            }
            UUID uuid = UuidUtils.parseUuidOrNull(item.id());
            if (uuid == null || mapped.containsKey(uuid)) {
                continue;
            }
            Book legacy = BookDomainMapper.fromListItem(item);
            if (legacy != null) {
                mapped.put(uuid, legacy);
            }
        }

        if (mapped.isEmpty()) {
            return List.of();
        }

        List<Book> ordered = new ArrayList<>(mapped.size());
        for (UUID id : orderedIds.keySet()) {
            Book book = mapped.get(id);
            if (book != null) {
                ordered.add(book);
            }
        }
        return ordered.isEmpty() ? List.of() : ordered;
    }

    private Book convertJsonToBook(JsonNode item) {
        if (item == null) {
            return null;
        }

        try {
            BookAggregate aggregate = googleBooksMapper.map(item);
            Book book = BookDomainMapper.fromAggregate(aggregate);
            if (book != null) {
                book.setRawJsonResponse(item.toString());
            }
            return book;
        } catch (Exception ex) {
            log.debug("Failed to map Google Books JSON node: {}", ex.getMessage());
            return null;
        }
    }

    private Mono<List<Book>> assembleBaselineResults(List<BookSearchService.SearchResult> results) {
        if (results == null || results.isEmpty()) {
            return Mono.just(List.of());
        }

        LinkedHashMap<UUID, Integer> orderedIds = new LinkedHashMap<>();
        for (BookSearchService.SearchResult result : results) {
            if (result != null && result.bookId() != null) {
                orderedIds.putIfAbsent(result.bookId(), orderedIds.size());
            }
        }

        if (orderedIds.isEmpty() || bookQueryRepository == null) {
            return Mono.just(List.of());
        }

        List<UUID> lookupOrder = new ArrayList<>(orderedIds.keySet());

        return Mono.fromCallable(() -> bookQueryRepository.fetchBookListItems(lookupOrder))
            .subscribeOn(Schedulers.boundedElastic())
            .map(items -> assembleOrderedLegacyBooks(orderedIds, items))
            .onErrorResume(ex -> {
                log.warn("Failed to fetch Postgres list items for search ({} ids): {}", lookupOrder.size(), ex.getMessage());
                return Mono.just(List.<Book>of());
            });
    }

    private List<Book> mergeSearchResults(List<Book> baseline, List<Book> fallback, int limit) {
        if ((baseline == null || baseline.isEmpty()) && (fallback == null || fallback.isEmpty())) {
            return List.of();
        }

        LinkedHashMap<String, Book> ordered = new LinkedHashMap<>();

        if (baseline != null) {
            baseline.stream()
                .filter(Objects::nonNull)
                .forEach(book -> ordered.putIfAbsent(searchResultKey(book), book));
        }

        if (fallback != null) {
            for (Book book : fallback) {
                if (book == null) {
                    continue;
                }
                String key = searchResultKey(book);
                if (!ordered.containsKey(key)) {
                    ordered.put(key, book);
                }
                if (ordered.size() >= limit) {
                    break;
                }
            }
        }

        if (ordered.isEmpty()) {
            return List.of();
        }

        return new ArrayList<>(ordered.values());
    }

    private String searchResultKey(Book book) {
        if (book == null) {
            return "__null__";
        }
        if (ValidationUtils.hasText(book.getSlug())) {
            return book.getSlug();
        }
        if (ValidationUtils.hasText(book.getId())) {
            return book.getId();
        }
        if (book.getTitle() != null && book.getAuthors() != null && !book.getAuthors().isEmpty()) {
            return book.getTitle() + "::" + String.join("|", book.getAuthors());
        }
        return book.getTitle() != null ? book.getTitle() : Integer.toHexString(System.identityHashCode(book));
    }

    /**
     * Fetches Google Book IDs for a list of ISBNs in batches
     *
     * @param isbns List of ISBNs (10 or 13)
     * @param langCode Optional language code
     * @return Mono emitting a Map of ISBN to Google Book ID
     */
    public Mono<Map<String, String>> fetchGoogleBookIdsForMultipleIsbns(List<String> isbns, String langCode) {
        if (isbns == null || isbns.isEmpty()) {
            return Mono.just(Collections.emptyMap());
        }

        List<List<String>> isbnBatches = new ArrayList<>();
        for (int i = 0; i < isbns.size(); i += ISBN_BATCH_QUERY_SIZE) {
            isbnBatches.add(isbns.subList(i, Math.min(i + ISBN_BATCH_QUERY_SIZE, isbns.size())));
        }

        java.util.function.BinaryOperator<String> mergeFunction = (id1, id2) -> id1; // Define merge function

        return Flux.fromIterable(isbnBatches)
            .concatMap(batch -> 
                Mono.defer(() -> { // Defer execution of each batch
                    String batchQuery = batch.stream()
                                            .map(isbn -> "isbn:" + isbn.trim())
                                            .collect(Collectors.joining(" OR "));
                    log.info("Fetching Google Book IDs for ISBN batch query: {}", batchQuery);
                    // Use searchBooksAsyncReactive, which handles pagination within the API call for that query
                    // We expect few results per ISBN, so desiredTotalResults can be batch.size() * small_multiplier
                    return searchBooksAsyncReactive(batchQuery, langCode, batch.size() * 2, "relevance")
                        .onErrorResume(e -> {
                            LoggingUtils.error(log, e, "Error fetching batch of ISBNs (query: {})", batchQuery);
                            return Mono.just(Collections.<Book>emptyList());
                        });
                })
                .delayElement(java.time.Duration.ofMillis(isbnBatchDelayMs)) // Add delay between batch executions
            )
            .flatMap(Flux::fromIterable)
            .map(book -> Map.entry(book, resolveGoogleVolumeId(book)))
            .filter(entry -> {
                Book book = entry.getKey();
                Optional<String> googleId = entry.getValue();
                if (book == null || googleId.isEmpty()) {
                    return false;
                }
                String preferredIsbn = CoverIdentifierResolver.getPreferredIsbn(book);
                return ValidationUtils.hasText(preferredIsbn) && ValidationUtils.hasText(googleId.get());
            })
            .collect(Collectors.toMap(
                entry -> CoverIdentifierResolver.getPreferredIsbn(entry.getKey()),
                entry -> entry.getValue().orElseThrow(),
                mergeFunction
            ))
            .doOnSuccess(idMap -> log.info("Successfully fetched {} Google Book IDs for {} initial ISBNs.", idMap.size(), isbns.size()))
            .onErrorReturn(Collections.<String, String>emptyMap());
    }

    private Optional<String> resolveGoogleVolumeId(Book book) {
        if (book == null) {
            return Optional.empty();
        }

        String candidate = book.getId();
        if (ValidationUtils.hasText(candidate) && UuidUtils.parseUuidOrNull(candidate) == null) {
            return Optional.of(candidate);
        }

        Map<String, Object> qualifiers = book.getQualifiers();
        if (qualifiers == null || qualifiers.isEmpty()) {
            return Optional.empty();
        }

        for (String key : GOOGLE_ID_TAG_KEYS) {
            Object value = qualifiers.get(key);
            if (value instanceof String str && ValidationUtils.hasText(str)) {
                return Optional.of(str);
            }
        }

        return Optional.empty();
    }

}
