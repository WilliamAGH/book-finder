/**
 * Service for interacting with the Google Books API
 * This class is responsible for all direct communication with the Google Books API
 * It handles:
 * - Constructing and executing API requests for searching books and retrieving book details
 * - Applying rate limiting (via Resilience4j {@code @RateLimiter}) and circuit breaking ({@code @CircuitBreaker})
 *   to protect the application and the external API
 * - Converting JSON responses from the Google Books API into {@link Book} domain objects
 * - Caching API responses in S3 via the {@link S3RetryService} for GET requests for individual books
 *   (Note: The {@link GoogleBooksCachingStrategy} is expected to be used by this service or a higher-level proxy
 *    to provide more comprehensive caching including S3, database, and in-memory layers before hitting this service)
 * - Monitoring API usage and performance via {@link ApiRequestMonitor}
 * - Transforming cover image URLs for optimal quality and display
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.model.image.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import com.williamcallahan.book_recommendation_engine.service.image.GoogleCoverUrlEvaluator;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.ReactiveErrorUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import com.williamcallahan.book_recommendation_engine.service.image.ExternalCoverFetchHelper;
import com.williamcallahan.book_recommendation_engine.util.ImageCacheUtils;
import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils.BookValidator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.*;
import java.util.stream.Collectors;
import java.util.function.Function;
import java.util.function.BinaryOperator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import com.williamcallahan.book_recommendation_engine.service.image.LocalDiskCoverCacheService;
import com.williamcallahan.book_recommendation_engine.service.image.CoverCacheManager;

@SuppressWarnings("unused")
@Service
@Slf4j
public class GoogleBooksService {

    
    private final ObjectMapper objectMapper;
    private final ApiRequestMonitor apiRequestMonitor;
    private final GoogleApiFetcher googleApiFetcher;
    private final BookDataOrchestrator bookDataOrchestrator; // Added for fetchMultipleBooksByIds
    private final ExternalCoverFetchHelper externalCoverFetchHelper;
    private final GoogleCoverUrlEvaluator googleCoverUrlEvaluator;

    private static final int ISBN_BATCH_QUERY_SIZE = 5; // Reduced batch size for ISBN OR queries
    
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
            BookDataOrchestrator bookDataOrchestrator,
            ExternalCoverFetchHelper externalCoverFetchHelper,
            GoogleCoverUrlEvaluator googleCoverUrlEvaluator) {
        this.objectMapper = objectMapper;
        this.apiRequestMonitor = apiRequestMonitor;
        this.googleApiFetcher = googleApiFetcher;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.externalCoverFetchHelper = externalCoverFetchHelper;
        this.googleCoverUrlEvaluator = googleCoverUrlEvaluator;
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
     */
    public Mono<List<Book>> searchBooksAsyncReactive(String query, String langCode, int desiredTotalResults, String orderBy) {
        final int maxResultsPerPage = 40;
        final int maxTotalResultsToFetch = (desiredTotalResults > 0) ? desiredTotalResults : 200;
        final String effectiveOrderBy = (orderBy != null && !orderBy.trim().isEmpty()) ? orderBy : "newest";

        log.debug("GoogleBooksService.searchBooksAsyncReactive with query: '{}', langCode: {}, maxResults: {}, orderBy: {}",
            query, langCode, maxTotalResultsToFetch, effectiveOrderBy);

        final Map<String, Object> queryQualifiers = BookJsonParser.extractQualifiersFromSearchQuery(query);

        // Calculate number of pages needed to fetch the desired total results
        final int numberOfPages = (int) Math.ceil((double) maxTotalResultsToFetch / maxResultsPerPage);

        // IMPORTANT: Do NOT call methods on a Mockito mock that rely on real implementation.
        // Tests stub searchVolumesAuthenticated(...), so we build the stream from that.
        // Use Flux.range to paginate through multiple pages of results
        return Flux.range(0, numberOfPages)
            .concatMap(pageIndex -> {
                int startIndex = pageIndex * maxResultsPerPage;
                log.debug("Fetching page {} (startIndex={}) for query '{}'", pageIndex, startIndex, query);

                return googleApiFetcher.searchVolumesAuthenticated(query, startIndex, effectiveOrderBy, langCode)
                    .flatMapMany(responseNode -> {
                        if (responseNode != null && responseNode.has("items") && responseNode.get("items").isArray()) {
                            return Flux.fromIterable(responseNode.get("items"));
                        }
                        return Flux.empty();
                    })
                    .onErrorResume(e -> {
                        log.warn("Error fetching page {} for query '{}': {}", pageIndex, query, e.getMessage());
                        return Flux.empty(); // Continue with other pages if one fails
                    });
            })
            .map(BookJsonParser::convertJsonToBook)
            .filter(Objects::nonNull)
            .map(book -> {
                if (!queryQualifiers.isEmpty()) {
                    queryQualifiers.forEach(book::addQualifier);
                }
                return book;
            })
            .take(maxTotalResultsToFetch) // Limit to exact number requested
            .collectList()
            .map(books -> {
                log.info("GoogleBooksService.searchBooksAsyncReactive completed for query '{}'. Retrieved {} books total.",
                    query, books.size());
                return books;
            })
            .onErrorResume(e -> {
                LoggingUtils.error(log, e, "Error in searchBooksAsyncReactive for query '{}'", query);
                apiRequestMonitor.recordFailedRequest(
                    "volumes/search/reactive/" + query,
                    e.getMessage()
                );
                return Mono.just(Collections.<Book>emptyList());
            });
    }

    /**
     * Overloaded version of searchBooksAsyncReactive with default values
     * 
     * @param query Search query string to send to the API
     * @param langCode Optional language code to restrict results (e.g., "en", "fr")
     * @return Mono containing a list of Book objects retrieved from the API
     */
    public Mono<List<Book>> searchBooksAsyncReactive(String query, String langCode) {
        return searchBooksAsyncReactive(query, langCode, 200, "newest"); 
    }

    /**
     * Overloaded version of searchBooksAsyncReactive with no language filtering
     * 
     * @param query Search query string to send to the API
     * @return Mono containing a list of Book objects retrieved from the API
     */
    public Mono<List<Book>> searchBooksAsyncReactive(String query) {
        return searchBooksAsyncReactive(query, null, 200, "newest");
    }

    public Mono<List<Book>> fetchAdditionalHomepageBooks(String query, int limit, boolean allowExternalFallback) {
        Mono<List<Book>> postgres = bookDataOrchestrator.searchBooksTiered(query, null, limit, null)
            .onErrorResume(e -> {
                log.warn("Postgres-first homepage filler lookup failed for query '{}': {}", query, e.getMessage());
                return Mono.just(Collections.<Book>emptyList());
            });

        if (!allowExternalFallback) {
            return postgres.defaultIfEmpty(List.of());
        }

        Mono<List<Book>> fallback = searchBooksAsyncReactive(query, null, limit, null)
            .onErrorResume(e -> {
                log.error("Google fallback for homepage filler failed for query '{}': {}", query, e.getMessage());
                return Mono.just(Collections.<Book>emptyList());
            });

        return postgres.flatMap(results -> (results != null && !results.isEmpty()) ? Mono.just(results) : fallback)
            .defaultIfEmpty(List.of());
    }

    /**
     * Searches books by title using the 'intitle:' Google Books API qualifier
     * 
     * @param title Book title to search for
     * @param langCode Optional language code to restrict results
     * @return Mono containing a list of Book objects matching the title
     */
    public Mono<List<Book>> searchBooksByTitle(String title, String langCode) {
        return searchBooksAsyncReactive("intitle:" + title, langCode);
    }

    /**
     * Overloaded version without language filtering
     * 
     * @param title Book title to search for
     * @return Mono containing a list of Book objects matching the title
     */
    public Mono<List<Book>> searchBooksByTitle(String title) {
        return searchBooksByTitle(title, null);
    }

    /**
     * Searches books by author using the 'inauthor:' Google Books API qualifier
     * 
     * @param author Author name to search for
     * @param langCode Optional language code to restrict results
     * @return Mono containing a list of Book objects by the specified author
     */
    public Mono<List<Book>> searchBooksByAuthor(String author, String langCode) {
        return searchBooksAsyncReactive("inauthor:" + author, langCode);
    }

    /**
     * Overloaded version without language filtering
     * 
     * @param author Author name to search for
     * @return Mono containing a list of Book objects by the specified author
     */
    public Mono<List<Book>> searchBooksByAuthor(String author) {
        return searchBooksByAuthor(author, null);
    }

    /**
     * Searches books by ISBN using the 'isbn:' Google Books API qualifier 
     * 
     * @param isbn ISBN identifier to search for
     * @param langCode Optional language code to restrict results
     * @return Mono containing a list of Book objects matching the ISBN
     */
    public Mono<List<Book>> searchBooksByISBN(String isbn, String langCode) {
        return searchBooksAsyncReactive("isbn:" + isbn, langCode);
    }

    /**
     * Overloaded version without language filtering
     * 
     * @param isbn ISBN identifier to search for
     * @return Mono containing a list of Book objects matching the ISBN
     */
    public Mono<List<Book>> searchBooksByISBN(String isbn) {
        return searchBooksByISBN(isbn, null);
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
    public CompletionStage<Book> getBookById(String bookId) {
        log.warn("GoogleBooksService.getBookById called directly for {}. Consider using orchestrated flow via BookCacheFacadeService/GoogleBooksCachingStrategy.", bookId);
        return googleApiFetcher.fetchVolumeByIdAuthenticated(bookId)
            .map(BookJsonParser::convertJsonToBook)
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
                    String enhanced = ImageCacheUtils.enhanceGoogleCoverUrl(googleUrl, "zoom=0");
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
                url -> ImageCacheUtils.isLikelyGoogleCoverUrl(url),
                details -> ImageCacheUtils.isLikelyGoogleCoverUrl(details.getUrlOrPath())
            )
        );
    }

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
                    String enhanced = ImageCacheUtils.enhanceGoogleCoverUrl(googleUrl, "zoom=0");
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
                url -> ImageCacheUtils.isLikelyGoogleCoverUrl(url),
                details -> ImageCacheUtils.isLikelyGoogleCoverUrl(details.getUrlOrPath())
            )
        );
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
            .flatMap(Flux::fromIterable) // Flatten List<Book> from each batch into a single Flux<Book>
            .filter(book -> {
                if (book == null) {
                    return false;
                }
                String preferredIsbn = BookValidator.getPreferredIsbn(book);
                return ValidationUtils.hasText(book.getId()) && ValidationUtils.hasText(preferredIsbn);
            })
            .collect(Collectors.toMap(
                BookValidator::getPreferredIsbn,
                Book::getId,
                mergeFunction // Merge Function: In case of duplicate ISBNs, take the first ID
            ))
            .doOnSuccess(idMap -> log.info("Successfully fetched {} Google Book IDs for {} initial ISBNs.", idMap.size(), isbns.size()))
            .onErrorReturn(Collections.<String, String>emptyMap());
    }

    /**
     * Fetches full book details for a list of Google Book IDs
     * Uses BookDataOrchestrator to leverage its tiered fetching and S3 caching
     *
     * @param bookIds List of Google Book IDs
     * @return Flux emitting Book objects
     */
    public Flux<Book> fetchMultipleBooksByIdsTiered(List<String> bookIds) {
        if (bookIds == null || bookIds.isEmpty()) {
            return Flux.empty();
        }
        log.info("Fetching full details for {} book IDs using BookDataOrchestrator.", bookIds.size());
        return Flux.fromIterable(bookIds)
            .publishOn(reactor.core.scheduler.Schedulers.parallel()) // Allow parallel execution of orchestrator calls
            .flatMap(bookId -> bookDataOrchestrator.getBookByIdTiered(bookId)
                                .doOnError(e -> LoggingUtils.warn(log, e, "Error fetching book ID {} via orchestrator in batch", bookId))
                                .onErrorResume(e -> Mono.<Book>empty()),
                        Math.min(bookIds.size(), 10) // Concurrency level, adjust as needed
            )
            .filter(Objects::nonNull);
    }
}
