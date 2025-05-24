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
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import com.williamcallahan.book_recommendation_engine.service.ApiRequestMonitor;
import com.williamcallahan.book_recommendation_engine.service.GoogleApiFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Service;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.function.Function;
import java.util.function.BinaryOperator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@SuppressWarnings("unused")
@Service
public class GoogleBooksService {

    private static final Logger logger = LoggerFactory.getLogger(GoogleBooksService.class);

    private final ObjectMapper objectMapper;
    private final ApiRequestMonitor apiRequestMonitor;
    private final GoogleApiFetcher googleApiFetcher;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final Environment environment;

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
            Environment environment) {
        this.objectMapper = objectMapper;
        this.apiRequestMonitor = apiRequestMonitor;
        this.googleApiFetcher = googleApiFetcher;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.environment = environment;
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
    public CompletableFuture<JsonNode> searchBooks(String query, int startIndex, String orderBy, String langCode) {
        // Delegate to GoogleApiFetcher for authenticated search
        // Resilience4j annotations remain on this public method
        // The actual API call and its own retry/monitoring are within GoogleApiFetcher
        // This method now primarily serves as an entry point with resilience
        if (environment.acceptsProfiles(Profiles.of("dev"))) {
            logger.info("[ASYNC_CF_VIA_WEBC] GoogleBooksService.searchBooks for query: {}, startIndex: {}. Delegating to GoogleApiFetcher.", query, startIndex);
        } else {
            logger.debug("GoogleBooksService.searchBooks called for query: {}, startIndex: {}. Delegating to GoogleApiFetcher.", query, startIndex);
        }
        // Assuming this service primarily deals with authenticated calls if it's a direct passthrough
        // The plan implies GoogleBooksService might be a thin wrapper for *authenticated* calls
        return googleApiFetcher.searchVolumesAuthenticated(query, startIndex, orderBy, langCode)
            // Record successful API call on receiving a response
            .doOnNext(response -> apiRequestMonitor.recordSuccessfulRequest(
                "volumes/search/authenticated?query=" + query + "&startIndex=" + startIndex + "&orderBy=" + orderBy + "&langCode=" + langCode
            ))
            .doOnError(e -> apiRequestMonitor.recordFailedRequest(
                "volumes/search/authenticated?query=" + query + "&startIndex=" + startIndex,
                e.getMessage()))
            .toFuture();
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
        
        if (environment.acceptsProfiles(Profiles.of("dev"))) {
            logger.info("[ASYNC_REACTIVE_STREAM] GoogleBooksService.searchBooksAsyncReactive with query: '{}', langCode: {}, maxResults: {}, orderBy: {}",
                query, langCode, maxTotalResultsToFetch, effectiveOrderBy);
        } else {
            logger.debug("GoogleBooksService.searchBooksAsyncReactive with query: '{}', langCode: {}, maxResults: {}, orderBy: {}",
                query, langCode, maxTotalResultsToFetch, effectiveOrderBy);
        }

        final Map<String, Object> queryQualifiers = BookJsonParser.extractQualifiersFromSearchQuery(query);

        return Flux.range(0, (maxTotalResultsToFetch + maxResultsPerPage - 1) / maxResultsPerPage)
            .map(page -> page * maxResultsPerPage)
            .concatMap((Integer startIndex) -> { // Explicitly type startIndex
                // Delegate to searchBooks and handle errors per page to avoid bubbling up
                // Using fully qualified name for Supplier here just in case, though import should cover it.
                java.util.function.Supplier<java.util.concurrent.CompletableFuture<JsonNode>> futureSupplier = () -> searchBooks(query, startIndex, effectiveOrderBy, langCode);
                Flux<JsonNode> pageResults = Mono.fromFuture(futureSupplier)
                    .flatMapMany(responseNode -> {
                        if (responseNode != null && responseNode.has("items") && responseNode.get("items").isArray()) {
                            List<JsonNode> items = new ArrayList<>();
                            responseNode.get("items").forEach(items::add);
                            return Flux.fromIterable(items);
                        }
                        return Flux.<JsonNode>empty(); // Explicit type for Flux.empty()
                    })
                    .onErrorResume(e -> {
                        logger.error("Error fetching page {} for query '{}': {}", startIndex, query, e.getMessage());
                        apiRequestMonitor.recordFailedRequest(
                            "volumes/search/reactive/" + query,
                            e.getMessage()
                        );
                        return Flux.<JsonNode>empty(); // Explicit type for Flux.empty()
                    });
                // Explicitly return pageResults for clarity, though it was implicitly returned before.
                return pageResults; 
            })
            .map(jsonNode -> BookJsonParser.convertJsonToBook(jsonNode)) // Use BookJsonParser
            .filter(Objects::nonNull)
            .map(book -> {
                if (!queryQualifiers.isEmpty()) {
                    queryQualifiers.forEach(book::addQualifier);
                }
                return book;
            })
            .collectList()
            .map(books -> {
                logger.info("GoogleBooksService.searchBooksAsyncReactive completed for query '{}'. Retrieved {} books total.",
                    query, books.size());
                if (books.size() > maxTotalResultsToFetch) {
                    return books.subList(0, maxTotalResultsToFetch);
                }
                return books;
            })
            .onErrorResume(e -> {
                logger.error("Error in searchBooksAsyncReactive for query '{}': {}", query, e.getMessage());
                apiRequestMonitor.recordFailedRequest(
                    "volumes/search/reactive/" + query,
                    e.getMessage()
                );
                // Return an empty list to handle errors gracefully
                return Mono.just(Collections.emptyList());
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
    public Mono<List<Book>> searchBooksByAuthor(String author, String langCode, int desiredTotalResults, String orderBy) {
        return searchBooksAsyncReactive("inauthor:" + author, langCode, desiredTotalResults, orderBy);
    }

    /**
     * Searches books by author using the 'inauthor:' Google Books API qualifier.
     * Defaults to fetching up to 200 results ordered by "newest".
     * 
     * @param author Author name to search for
     * @param langCode Optional language code to restrict results
     * @return Mono containing a list of Book objects by the specified author
     */
    public Mono<List<Book>> searchBooksByAuthor(String author, String langCode) {
        return searchBooksByAuthor(author, langCode, 200, "newest"); // Keep existing default behavior
    }

    /**
     * Overloaded version without language filtering.
     * Defaults to fetching up to 200 results ordered by "newest".
     * 
     * @param author Author name to search for
     * @return Mono containing a list of Book objects by the specified author
     */
    public Mono<List<Book>> searchBooksByAuthor(String author) {
        return searchBooksByAuthor(author, null, 200, "newest"); // Keep existing default behavior
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
        if (environment.acceptsProfiles(Profiles.of("dev"))) {
            logger.info("[ASYNC_CF_VIA_WEBC] Fetching Google Book ID for ISBN: {} (via searchBooks to CompletableFuture, then to Mono)", isbn);
        } else {
            logger.debug("Fetching Google Book ID for ISBN: {}", isbn);
        }
        // Explicit typing for supplier, though it should be inferred.
        java.util.function.Supplier<java.util.concurrent.CompletableFuture<JsonNode>> futureSupplier = () -> searchBooks("isbn:" + isbn, 0, "relevance", null);
        return Mono.fromFuture(futureSupplier)
            .map(responseNode -> {
                if (responseNode != null && responseNode.has("items") && responseNode.get("items").isArray() && responseNode.get("items").size() > 0) {
                    JsonNode firstItem = responseNode.get("items").get(0);
                    if (firstItem.has("id")) {
                        String googleId = firstItem.get("id").asText();
                        logger.info("Found Google Book ID: {} for ISBN: {}", googleId, isbn);
                        return googleId;
                    }
                }
                logger.warn("No Google Book ID found for ISBN: {}", isbn);
                return null; // Will be filtered by filter(Objects::nonNull) or handled by switchIfEmpty
            })
            .filter(Objects::nonNull) // Ensure we only proceed if an ID was found
            .doOnError(e -> {
                logger.error("Error fetching Google Book ID for ISBN {}: {}", isbn, e.getMessage(), e);
                apiRequestMonitor.recordFailedRequest("volumes/search/isbn_to_id/" + isbn, e.getMessage());
            })
            .onErrorResume(e -> Mono.empty()); // On error, return an empty Mono
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
        if (environment.acceptsProfiles(Profiles.of("dev"))) {
            logger.info("[ASYNC_CF_VIA_WEBC] GoogleBooksService.getBookById for bookId: {}. Delegating to GoogleApiFetcher.", bookId);
        } else {
            logger.warn("GoogleBooksService.getBookById called directly for {}. Consider using orchestrated flow via BookCacheFacadeService/GoogleBooksCachingStrategy.", bookId);
        }
        return googleApiFetcher.fetchVolumeByIdAuthenticated(bookId)
            .map(BookJsonParser::convertJsonToBook)
            .filter(Objects::nonNull)
            // Record successful API fetch for the book
            .doOnNext(book -> apiRequestMonitor.recordSuccessfulRequest(
                "volumes/get/authenticated/" + bookId
            ))
            // Handle errors by recording failure and returning empty
            .onErrorResume(e -> {
                apiRequestMonitor.recordFailedRequest(
                    "volumes/get/authenticated/" + bookId,
                    e.getMessage()
                );
                return Mono.empty();
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
    public CompletableFuture<JsonNode> searchBooksFallback(String query, int startIndex, String orderBy, String langCode, Throwable t) {
        logger.warn("GoogleBooksService.searchBooks circuit breaker opened for query: '{}', startIndex: {}. Error: {}", 
            query, startIndex, t.getMessage());
        
        String queryType = "general"; // Simplified for fallback, specific type determined by GoogleApiFetcher now
        if (query.contains("intitle:")) queryType = "title";
        else if (query.contains("inauthor:")) queryType = "author";
        else if (query.contains("isbn:")) queryType = "isbn";
        String apiEndpoint = "volumes/search/" + queryType + "/authenticated";
        
        apiRequestMonitor.recordFailedRequest(apiEndpoint, "Circuit breaker opened for query: '" + query + "', startIndex: " + startIndex + ": " + t.getMessage());
        
        // Return an empty object node instead of Mono.empty() to avoid downstream errors
        return CompletableFuture.completedFuture(objectMapper.createObjectNode());
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
    public CompletableFuture<JsonNode> searchBooksRateLimitFallback(String query, int startIndex, String orderBy, String langCode, Throwable t) {
        logger.warn("GoogleBooksService.searchBooks rate limit exceeded for query: '{}', startIndex: {}. Error: {}", 
            query, startIndex, t.getMessage());
        
        apiRequestMonitor.recordMetric("api/rate-limited", "API call rate limited for search: " + query + " (via GoogleBooksService)");
        
        // Return an empty object node instead of Mono.empty() to avoid downstream errors
        return CompletableFuture.completedFuture(objectMapper.createObjectNode());
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
        logger.warn("GoogleBooksService.getBookById circuit breaker opened for bookId: {}. Error: {}", 
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
        logger.warn("GoogleBooksService.getBookById rate limit exceeded for bookId: {}. Error: {}", 
            bookId, t.getMessage());
        
        apiRequestMonitor.recordMetric("api/rate-limited", "API call rate limited for book " + bookId + " (via GoogleBooksService)");
        
        return CompletableFuture.completedFuture(null);
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
                    logger.info("Fetching Google Book IDs for ISBN batch query: {}", batchQuery);
                    // Use searchBooksAsyncReactive, which handles pagination within the API call for that query
                    // We expect few results per ISBN, so desiredTotalResults can be batch.size() * small_multiplier
                    return searchBooksAsyncReactive(batchQuery, langCode, batch.size() * 2, "relevance")
                        .onErrorResume(e -> {
                            logger.error("Error fetching batch of ISBNs (query: {}): {}", batchQuery, e.getMessage());
                            return Mono.just(Collections.emptyList()); // Continue with other batches
                        });
                })
                .delayElement(java.time.Duration.ofMillis(isbnBatchDelayMs)) // Add delay between batch executions
            )
            .flatMap(Flux::fromIterable) // Flatten List<Book> from each batch into a single Flux<Book>
            .filter(book -> book != null && book.getId() != null && (book.getIsbn10() != null || book.getIsbn13() != null))
            .collect(Collectors.toMap(
                (Book book) -> { // Key Mapper: Extract ISBN (prefer ISBN13)
                    String isbn13 = book.getIsbn13();
                    return (isbn13 != null && !isbn13.isEmpty()) ? isbn13 : book.getIsbn10();
                },
                Book::getId, // Value Mapper: Google Book ID
                mergeFunction // Merge Function: In case of duplicate ISBNs, take the first ID
            ))
            .doOnSuccess(idMap -> logger.info("Successfully fetched {} Google Book IDs for {} initial ISBNs.", idMap.size(), isbns.size()))
            .onErrorReturn(Collections.emptyMap());
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
        logger.info("Fetching full details for {} book IDs using BookDataOrchestrator.", bookIds.size());
        return Flux.fromIterable(bookIds)
            .publishOn(reactor.core.scheduler.Schedulers.parallel()) // Allow parallel execution of orchestrator calls
            .flatMap(bookId -> bookDataOrchestrator.getBookByIdTiered(bookId)
                                .doOnError(e -> logger.warn("Error fetching book ID {} via orchestrator in batch: {}", bookId, e.getMessage()))
                                .onErrorResume(e -> Mono.empty()), // Continue if one book fails
                        Math.min(bookIds.size(), 10) // Concurrency level, adjust as needed
            )
            .filter(Objects::nonNull);
    }
}
