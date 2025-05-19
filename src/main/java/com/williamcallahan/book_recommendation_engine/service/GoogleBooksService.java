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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.time.Duration;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.springframework.web.util.UriComponentsBuilder;

@Service
public class GoogleBooksService {

    private static final Logger logger = LoggerFactory.getLogger(GoogleBooksService.class);

    private final WebClient webClient;
    private final S3RetryService s3RetryService;
    private final ObjectMapper objectMapper;
    private final ApiRequestMonitor apiRequestMonitor;

    @Value("${googlebooks.api.url}")
    private String googleBooksApiUrl;

    @Value("${googlebooks.api.key}")
    private String googleBooksApiKey;

    /**
     * Constructs a GoogleBooksService with necessary dependencies.
     * 
     * @param webClientBuilder Spring WebClient builder for constructing the reactive HTTP client
     * @param s3RetryService Service for S3 operations with retry logic, used here for caching API responses
     * @param objectMapper Jackson ObjectMapper for JSON processing
     * @param apiRequestMonitor Service for monitoring API usage metrics
     */
    @Autowired
    public GoogleBooksService(
            WebClient.Builder webClientBuilder,
            S3RetryService s3RetryService,
            ObjectMapper objectMapper,
            ApiRequestMonitor apiRequestMonitor) {
        this.webClient = webClientBuilder.build();
        this.s3RetryService = s3RetryService;
        this.objectMapper = objectMapper;
        this.apiRequestMonitor = apiRequestMonitor;
    }

    /**
     * Performs a search against the Google Books API
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
        String encodedQuery;
        try {
            encodedQuery = URLEncoder.encode(query, StandardCharsets.UTF_8.name());
        } catch (java.io.UnsupportedEncodingException e) {
            logger.error("Failed to URL encode query: {}", query, e);
            return Mono.error(e); // Or handle more gracefully
        }
        StringBuilder urlBuilder = new StringBuilder(String.format("%s/v1/volumes?q=%s&startIndex=%d&maxResults=40", 
                googleBooksApiUrl, encodedQuery, startIndex));
        
        if (googleBooksApiKey != null && !googleBooksApiKey.isEmpty()) {
            urlBuilder.append("&key=").append(googleBooksApiKey);
        }
        
        if (orderBy != null && !orderBy.isEmpty()) {
            urlBuilder.append("&orderBy=").append(orderBy);
        }
        if (langCode != null && !langCode.isEmpty()) {
            urlBuilder.append("&langRestrict=").append(langCode);
            logger.debug("Google Books API call with langRestrict: {}", langCode);
        }
        
        String url = urlBuilder.toString();
        
        // Define a specific endpoint identifier for monitoring
        // Format: volumes/search/{query-type} to track different search types
        String queryType = "general";
        if (query.contains("intitle:")) {
            queryType = "title";
        } else if (query.contains("inauthor:")) {
            queryType = "author";
        } else if (query.contains("isbn:")) {
            queryType = "isbn";
        }
        String endpoint = "volumes/search/" + queryType;
        
        logger.debug("Making Google Books API search call for query: {}, startIndex: {}, endpoint: {}", 
            query, startIndex, endpoint);
        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .doOnSubscribe(s -> logger.debug("Making Google Books API search call for query: {}, startIndex: {}", query, startIndex))
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                    .filter(throwable -> throwable instanceof IOException || throwable instanceof WebClientRequestException)
                    .doBeforeRetry(retrySignal -> {
                        logger.warn("Retrying API call for query '{}', startIndex {}. Attempt #{}. Error: {}", 
                            query, startIndex, retrySignal.totalRetries() + 1, retrySignal.failure().getMessage());
                    })
                    .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                        logger.error("All retries failed for query '{}', startIndex {}. Final error: {}", 
                            query, startIndex, retrySignal.failure().getMessage());
                        apiRequestMonitor.recordFailedRequest(endpoint, "All retries failed: " + retrySignal.failure().getMessage());
                        return retrySignal.failure();
                    }))
                .doOnSuccess(response -> {
                    // Record successful API call
                    apiRequestMonitor.recordSuccessfulRequest(endpoint);
                })
                .onErrorResume(e -> {
                    // This catches errors after retries are exhausted or if the error wasn't retryable
                    logger.error("Error fetching page for query '{}' at startIndex {} after retries or due to non-retryable error: {}", 
                        query, startIndex, e.getMessage());
                    apiRequestMonitor.recordFailedRequest(endpoint, e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Performs a comprehensive search across multiple Google Books API pages, aggregating results
     * This method orchestrates multiple calls to the single-page {@code searchBooks} method
     * 
     * @param query Search query string to send to the API
     * @param langCode Optional language code to restrict results (e.g., "en", "fr")
     * @param desiredTotalResults The desired maximum number of total results to fetch across all pages
     * @param orderBy The order to sort results by (e.g., "relevance", "newest")
     * @return Mono containing a list of {@link Book} objects retrieved from the API
     */
    public Mono<List<Book>> searchBooksAsyncReactive(String query, String langCode, int desiredTotalResults, String orderBy) {
        final int maxResultsPerPage = 40;
        final int maxTotalResultsToFetch = (desiredTotalResults > 0) ? desiredTotalResults : 200; // Default to 200 if invalid
        final String effectiveOrderBy = (orderBy != null && !orderBy.trim().isEmpty()) ? orderBy : "newest"; // Default to newest
        
        logger.debug("Starting searchBooksAsyncReactive with query: '{}', langCode: {}, maxResults: {}, orderBy: {}", 
            query, langCode, maxTotalResultsToFetch, effectiveOrderBy);

        // Extract qualifiers from the search query to add to books
        final String cleanQuery = query.trim().toLowerCase();
        final Map<String, Object> queryQualifiers = extractQualifiersFromQuery(cleanQuery);
        
        // Note: We don't need to explicitly track API calls here as they are tracked
        // in the searchBooks method which is called for each page of results
        return Flux.range(0, (maxTotalResultsToFetch + maxResultsPerPage - 1) / maxResultsPerPage)
            .map(page -> page * maxResultsPerPage)
            .concatMap(startIndex -> 
                searchBooks(query, startIndex, effectiveOrderBy, langCode) 
                    .flatMapMany(response -> {
                        if (response != null && response.has("items") && response.get("items").isArray()) {
                            List<JsonNode> items = new ArrayList<>();
                            response.get("items").forEach(items::add);
                            int itemCount = items.size();
                            logger.debug("Retrieved {} items from page starting at index {}", itemCount, startIndex);
                            return Flux.fromIterable(items);
                        } else {
                            logger.debug("No items found for page starting at index {}", startIndex);
                            return Flux.empty();
                        }
                    })
            )
            .map(this::convertGroupToBook)
            .filter(Objects::nonNull)
            .map(book -> {
                // Add the search query as qualifier for each book
                if (!queryQualifiers.isEmpty()) {
                    // Add all extracted qualifiers
                    for (Map.Entry<String, Object> entry : queryQualifiers.entrySet()) {
                        book.addQualifier(entry.getKey(), entry.getValue());
                    }
                    
                    // Also add the raw query for reference
                    book.addQualifier("searchQuery", cleanQuery);
                }
                return book;
            })
            .collectList()
            .map(books -> {
                logger.info("searchBooksAsyncReactive completed for query '{}'. Retrieved {} books total.", 
                    query, books.size());
                if (books.size() > maxTotalResultsToFetch) {
                    return books.subList(0, maxTotalResultsToFetch);
                }
                return books;
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
        // Calls the method with default desiredTotalResults and orderBy newest
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
     * Retrieve a specific book by its Google Books volume ID
     * - Fetches detailed book information using the volume ID
     * - Attempts to get data from cache before making API calls
     * - Implements retry logic with exponential backoff for transient errors
     * - Converts API response to Book domain object
     * - Returns empty Mono if book cannot be found or errors occur
     * - Tracks API calls with ApiRequestMonitor, only when actual API requests are made
     * - Properly distinguishes between S3 cache miss and S3 service errors
     * 
     * @param bookId Google Books volume ID
     * @return Mono containing the Book object if found, empty Mono otherwise
     */
    @CircuitBreaker(name = "googleBooksService", fallbackMethod = "getBookByIdFallback")
    @TimeLimiter(name = "googleBooksService")
    @RateLimiter(name = "googleBooksServiceRateLimiter", fallbackMethod = "getBookByIdRateLimitFallback")
    public CompletionStage<Book> getBookById(String bookId) {
        logger.debug("getBookById called for book ID: {}", bookId);
        
        // Step 1: Try to fetch from S3 cache with retry logic for transient errors
        return s3RetryService.fetchJsonWithRetry(bookId) // This returns CompletableFuture<S3FetchResult<String>> with retry logic
            .thenComposeAsync(s3Result -> {
                // Handle the result based on its status
                if (s3Result.isSuccess() && s3Result.getData().isPresent()) {
                    String jsonString = s3Result.getData().get();
                    try {
                        // Parse the JSON from S3
                        logger.info("Retrieved book {} JSON from S3 cache. Preparing to process for imageLinks.", bookId);
                        JsonNode s3JsonNode = objectMapper.readTree(jsonString);
                        Book bookFromS3 = convertSingleItemToBook(s3JsonNode);

                        if (bookFromS3 != null && bookFromS3.getId() != null) { // Basic validation
                            logger.info("Successfully processed book {} from S3 cache.", bookId); // Changed log slightly for clarity
                            // No API call metrics recorded here since we used the cache
                            return CompletableFuture.completedFuture(bookFromS3);
                        } else {
                            // S3 JSON is present but malformed or doesn't convert to a valid book
                            logger.warn("S3 cache for {} contained JSON, but it parsed to a null/invalid book. Falling back to API.", bookId);
                        }
                    } catch (IOException e) {
                        // S3 JSON is present but fails to parse as JSON tree
                        logger.warn("Failed to parse book JSON from S3 cache for bookId {}: {}. Falling back to API.", bookId, e.getMessage());
                    }
                } else if (s3Result.isNotFound()) {
                    // Normal cache miss - book wasn't in S3
                    logger.info("Book {} not found in S3 cache. Fetching from API.", bookId);
                } else if (s3Result.isServiceError()) {
                    // S3 service error - transient issue with S3 service
                    String errorMsg = s3Result.getErrorMessage().orElse("Unknown S3 error");
                    logger.warn("S3 service error while fetching book {}: {}. Falling back to API.", bookId, errorMsg);
                    // Track S3 service error metrics
                    apiRequestMonitor.recordMetric("s3/errors", "Error: " + errorMsg);
                } else if (s3Result.isDisabled()) {
                    // S3 is disabled or misconfigured
                    logger.info("S3 is disabled or misconfigured. Fetching book {} directly from API.", bookId);
                }

                // If any condition leads to API fallback, fetch from API
                // The circuit breaker on getBookById will handle failures from fetchFromGoogleBooksApiAndCache
                // API call metrics are recorded inside fetchFromGoogleBooksApiAndCache
                return fetchFromGoogleBooksApiAndCache(bookId).toFuture();
            });
    }

    private Mono<Book> fetchFromGoogleBooksApiAndCache(String bookId) {
        String url = UriComponentsBuilder.fromUriString(googleBooksApiUrl)
            .pathSegment("v1", "volumes", bookId)
            .queryParamIfPresent("key", Optional.ofNullable(googleBooksApiKey)
                .filter(key -> !key.isEmpty()))
            .build(true)     // true = encode
            .toUriString();
            
        String endpoint = "volumes/get/" + bookId;

        logger.debug("Making Google Books API GET call for book ID: {}, endpoint: {}", bookId, endpoint);
        
        return webClient.get()
            .uri(url)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .doOnSubscribe(s -> logger.debug("Fetching book {} from Google API.", bookId))
            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                .filter(throwable -> throwable instanceof IOException || throwable instanceof WebClientRequestException)
                .doBeforeRetry(retrySignal -> 
                    logger.warn("Retrying API call for book {}. Attempt #{}. Error: {}", 
                        bookId, retrySignal.totalRetries() + 1, retrySignal.failure().getMessage()))
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                    logger.error("All retries failed for book {}. Final error: {}", bookId, retrySignal.failure().getMessage());
                    apiRequestMonitor.recordFailedRequest(endpoint, "All retries failed: " + retrySignal.failure().getMessage());
                    return retrySignal.failure();
                }))
            .flatMap(responseNode -> { // responseNode is the JsonNode from API
                String jsonToCache;
                try {
                    jsonToCache = objectMapper.writeValueAsString(responseNode);
                } catch (JsonProcessingException e) {
                    logger.error("Error converting API response to JSON string for bookId {}: {}", bookId, e.getMessage());
                    return Mono.error(e); // Propagate error if serialization fails
                }

                // Attempt to cache to S3. If it fails, log the error and continue.
                Mono<Void> s3CacheMono = Mono.fromFuture(s3RetryService.uploadJsonWithRetry(bookId, jsonToCache))
                    .doOnSuccess(v -> logger.info("Successfully cached book {} JSON in S3 via retry service.", bookId))
                    .doOnError(e -> logger.error("Non-fatal: Failed to cache book {} JSON in S3 via retry service: {}", bookId, e.getMessage()))
                    .onErrorResume(e -> Mono.empty()); // If S3 cache fails, complete empty to allow main flow to continue.

                return s3CacheMono.then(Mono.just(responseNode)); // After S3 attempt (success or handled error), proceed with responseNode
            })
            .map(responseNode -> { // Convert JsonNode to Book object
                Book book = convertJsonToBook(responseNode);
                if (book != null) {
                    if (responseNode != null) {
                        book.setRawJsonResponse(responseNode.toString());
                    }
                    apiRequestMonitor.recordSuccessfulRequest(endpoint);
                    logger.info("Processing book {} JSON freshly retrieved from Google Books API for imageLinks.", bookId);
                } else {
                    apiRequestMonitor.recordFailedRequest(endpoint, "Converted book was null for bookId: " + bookId);
                }
                return book;
            });
    }

    /**
     * Converts a JSON item from the Google Books API response to a Book object
     * - Delegates to convertSingleItemToBook for actual conversion
     * - Handles null items safely
     * 
     * @param item JsonNode from Google Books API response
     * @return Converted Book object or null if input is null
     */
    private Book convertGroupToBook(JsonNode item) {
        if (item == null) {
            return null;
        }

        Book book = convertSingleItemToBook(item);
        
        return book;
    }

    /**
     * Converts a JSON node to a Book object
     * - Public method for external services to convert JSON to Book objects
     * - Used by the caching strategy for consistent conversion
     * 
     * @param item JsonNode containing volume information
     * @return Fully populated Book object or null if input is null
     */
    public Book convertJsonToBook(JsonNode item) {
        return convertSingleItemToBook(item);
    }

    /**
     * Converts a single volume item JSON to a complete Book object
     * - Creates new Book instance and populates all fields
     * - Stores raw JSON for future reference
     * - Extracts all book metadata from various sections of response
     * 
     * @param item JsonNode containing volume information
     * @return Fully populated Book object
     */
    private Book convertSingleItemToBook(JsonNode item) {
        Book book = new Book();
        if (item != null) {
            book.setRawJsonResponse(item.toString());
        }
        extractBookBaseInfo(item, book);
        setAdditionalFields(item, book);
        setLinks(item, book);
        
        // Extract qualifiers if they exist in the JSON
        if (item != null && item.has("qualifiers")) {
            try {
                JsonNode qualifiersNode = item.get("qualifiers");
                if (qualifiersNode != null && qualifiersNode.isObject()) {
                    qualifiersNode.fields().forEachRemaining(entry -> {
                        String key = entry.getKey();
                        JsonNode valueNode = entry.getValue();
                        
                        // Handle different types of values
                        if (valueNode.isBoolean()) {
                            book.addQualifier(key, valueNode.booleanValue());
                        } else if (valueNode.isTextual()) {
                            book.addQualifier(key, valueNode.textValue());
                        } else if (valueNode.isNumber()) {
                            book.addQualifier(key, valueNode.numberValue());
                        } else if (valueNode.isArray()) {
                            // Convert JSON array to Java List
                            List<Object> values = new ArrayList<>();
                            valueNode.elements().forEachRemaining(element -> {
                                if (element.isTextual()) {
                                    values.add(element.textValue());
                                } else if (element.isBoolean()) {
                                    values.add(element.booleanValue());
                                } else if (element.isNumber()) {
                                    values.add(element.numberValue());
                                } else {
                                    // For more complex types, use string representation
                                    values.add(element.toString());
                                }
                            });
                            book.addQualifier(key, values);
                        } else {
                            // For complex objects, we might need more sophisticated handling
                            // For now, just convert to string if not a primitive type
                            book.addQualifier(key, valueNode.toString());
                        }
                    });
                    
                    logger.debug("Extracted {} qualifiers from JSON for book {}", 
                        book.getQualifiers().size(), book.getId());
                }
            } catch (Exception e) {
                logger.warn("Error extracting qualifiers from JSON for book {}: {}", 
                    book.getId(), e.getMessage());
            }
        }

        return book;
    }
    
    /**
     * Extracts base book information from volume info JSON
     * - Sets core book metadata like title, authors, publisher
     * - Extracts dates and descriptions
     * - Processes cover image URLs
     * - Extracts edition information from industry identifiers
     * 
     * @param item Main JsonNode containing book data
     * @param book Book object to populate with extracted data
     */
    private void extractBookBaseInfo(JsonNode item, Book book) {
        if (item == null || !item.has("volumeInfo")) {
            return;
        }

        JsonNode volumeInfo = item.get("volumeInfo");

        book.setId(item.has("id") ? item.get("id").asText() : null);
        book.setTitle(volumeInfo.has("title") ? volumeInfo.get("title").asText() : null);
        book.setAuthors(getAuthorsFromVolumeInfo(volumeInfo));
        // Sanitize publisher: strip surrounding quotes if present
        String rawPublisher = volumeInfo.has("publisher") ? volumeInfo.get("publisher").asText() : null;
        if (rawPublisher != null) {
            // Remove any leading/trailing double quotes
            rawPublisher = rawPublisher.replaceAll("^\"|\"$", "");
        }
        book.setPublisher(rawPublisher);
        book.setPublishedDate(parsePublishedDate(volumeInfo));
        book.setDescription(volumeInfo.has("description") ? volumeInfo.get("description").asText() : null);
        book.setCoverImageUrl(getGoogleCoverImageFromVolumeInfo(volumeInfo));
        book.setLanguage(volumeInfo.has("language") ? volumeInfo.get("language").asText() : null);

        if (volumeInfo.has("industryIdentifiers")) {
            List<Book.EditionInfo> otherEditions = new ArrayList<>();
            for (JsonNode identifierNode : volumeInfo.get("industryIdentifiers")) {
                extractEditionInfoFromItem(identifierNode, otherEditions);

                // Populate the main book's ISBN-10 and ISBN-13 fields
                String type = identifierNode.has("type") ? identifierNode.get("type").asText() : null;
                String idValue = identifierNode.has("identifier") ? identifierNode.get("identifier").asText() : null;

                if (idValue != null && !idValue.isEmpty()) {
                    if ("ISBN_10".equals(type) && book.getIsbn10() == null) {
                        book.setIsbn10(idValue);
                    } else if ("ISBN_13".equals(type) && book.getIsbn13() == null) {
                        book.setIsbn13(idValue);
                    }
                }
            }
            book.setOtherEditions(otherEditions);
        }
    }

    /**
     * Extracts author names from volume info
     * - Processes author array from JSON
     * - Returns empty list if no authors are found
     * 
     * @param volumeInfo JsonNode containing volume information
     * @return List of author names extracted from the volume info
     */
    private List<String> getAuthorsFromVolumeInfo(JsonNode volumeInfo) {
        List<String> authors = new ArrayList<>();
        if (volumeInfo.has("authors")) {
            volumeInfo.get("authors").forEach(authorNode -> authors.add(authorNode.asText()));
        }
        return authors;
    }

    /**
     * Get the best available cover image URL from the volume info
     * - Extracts image links from Google Books API response
     * - Prioritizes higher resolution images when available
     * - Uses URL enhancement to optimize image quality
     * - Maintains backward compatibility with single URL return value
     * 
     * @param volumeInfo JsonNode containing volume information
     * @return Best available cover image URL or null if none found
     */
    private String getGoogleCoverImageFromVolumeInfo(JsonNode volumeInfo) {
        if (volumeInfo.has("imageLinks")) {
            JsonNode imageLinks = volumeInfo.get("imageLinks");
            String coverUrl = null;

            // Log all available raw image links for debugging
            if (logger.isDebugEnabled()) {
                String bookTitleForLog = volumeInfo.has("title") ? volumeInfo.get("title").asText() : "Unknown Title";
                logger.debug("Raw Google Books imageLinks for '{}': {}", bookTitleForLog, imageLinks.toString());
            }
            
            // Try to get the best available image, prioritizing larger ones
            if (imageLinks.has("extraLarge")) {
                coverUrl = imageLinks.get("extraLarge").asText();
            } else if (imageLinks.has("large")) {
                coverUrl = imageLinks.get("large").asText();
            } else if (imageLinks.has("medium")) {
                coverUrl = imageLinks.get("medium").asText();
            } else if (imageLinks.has("thumbnail")) {
                coverUrl = imageLinks.get("thumbnail").asText();
            } else if (imageLinks.has("smallThumbnail")) {
                coverUrl = imageLinks.get("smallThumbnail").asText();
            }
            
            if (coverUrl != null) {
                // Enhance the URL to get a higher resolution image
                coverUrl = enhanceGoogleCoverUrl(coverUrl, "high");
                return coverUrl;
            }
        }
        return null;
    }
    
    /**
     * Enhances a Google Books cover URL to get optimal image quality
     * - Upgrades HTTP to HTTPS for secure connections
     * - Removes or adjusts image sizing parameters for better quality
     * - Applies quality-specific optimizations based on requested level
     * - Handles URL parameter cleanup to ensure valid URLs
     * 
     * @param url The original image URL from Google Books API
     * @param quality The desired image quality ("high", "medium", or "low")
     * @return Enhanced URL optimized for requested quality level
     */
    private String enhanceGoogleCoverUrl(String url, String quality) {
        if (url == null) return null;
        
        // Make a copy of the URL to avoid modifying the original
        String enhancedUrl = url;
        
        // Remove http protocol to use https
        if (enhancedUrl.startsWith("http://")) {
            enhancedUrl = "https://" + enhancedUrl.substring(7);
        }
        
        // Enhance based on requested quality
        switch (quality) {
            case "high":
                // Prefer less aggressive upscaling. Try to get the best available quality
                // by removing common resizing parameters or setting them to fetch original/larger sizes
                // For "high" quality:
                // 1. Remove 'fife' parameter to avoid forced width
                if (enhancedUrl.contains("&fife=")) {
                    enhancedUrl = enhancedUrl.replaceAll("&fife=w\\d+", "");
                } else if (enhancedUrl.contains("?fife=")) {
                    enhancedUrl = enhancedUrl.replaceAll("\\?fife=w\\d+", "?");
                    // Clean up if '?' is now trailing
                    if (enhancedUrl.endsWith("?")) {
                        enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
                    }
                }
                // Remove trailing '&' if fife was the last parameter and removed
                if (enhancedUrl.endsWith("&")) {
                    enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
                }

                // 2. Adjust 'zoom' parameter conservatively
                //    If zoom is high (e.g., > 2), reduce it to 2, otherwise, keep existing zoom (0, 1, or 2)
                //    If no zoom, don't add one
                if (enhancedUrl.contains("zoom=")) {
                    try {
                        String zoomValueStr = enhancedUrl.substring(enhancedUrl.indexOf("zoom=") + 5);
                        if (zoomValueStr.contains("&")) {
                            zoomValueStr = zoomValueStr.substring(0, zoomValueStr.indexOf("&"));
                        }
                        int currentZoom = Integer.parseInt(zoomValueStr);
                        if (currentZoom > 2) {
                            enhancedUrl = enhancedUrl.replaceAll("zoom=\\d+", "zoom=2");
                            logger.debug("Adjusted high zoom to zoom=2 for URL: {}", enhancedUrl);
                        }
                        // Keep zoom if it's 0, 1, or 2
                    } catch (NumberFormatException e) {
                        logger.warn("Could not parse zoom value in URL: {}. Leaving zoom as is.", enhancedUrl);
                    }
                }
                break;
                
            case "medium":
                if (enhancedUrl.contains("&fife=")) {
                    enhancedUrl = enhancedUrl.replaceAll("&fife=w\\d+", "");
                } else if (enhancedUrl.contains("?fife=")) {
                     enhancedUrl = enhancedUrl.replaceAll("\\?fife=w\\d+", "?");
                     if (enhancedUrl.endsWith("?")) {
                        enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() -1);
                     }
                }
                if (enhancedUrl.endsWith("&")) {
                    enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
                }
                if (enhancedUrl.contains("zoom=")) {
                    enhancedUrl = enhancedUrl.replaceAll("zoom=\\d+", "zoom=1"); // Use zoom=1 for medium
                }
                break;
                
            case "low":
                if (enhancedUrl.contains("&fife=")) {
                    enhancedUrl = enhancedUrl.replaceAll("&fife=w\\d+", "");
                } else if (enhancedUrl.contains("?fife=")) {
                     enhancedUrl = enhancedUrl.replaceAll("\\?fife=w\\d+", "?");
                     if (enhancedUrl.endsWith("?")) {
                        enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() -1);
                     }
                }
                if (enhancedUrl.endsWith("&")) {
                    enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
                }
                if (enhancedUrl.contains("zoom=")) {
                    enhancedUrl = enhancedUrl.replaceAll("zoom=\\d+", "zoom=1"); // zoom=1 for low (thumbnail)
                }
                break;
        }
        
        return enhancedUrl;
    }
    
    /**
     * Sets additional book fields from the sale info section
     * - Extracts pricing and currency information
     * - Handles nested JSON structure safely
     * 
     * @param item JsonNode containing book data
     * @param book Book object to populate with extracted data
     */
    private void setAdditionalFields(JsonNode item, Book book) {
        if (item.has("saleInfo")) {
            JsonNode saleInfo = item.get("saleInfo");
            if (saleInfo.has("listPrice")) {
                JsonNode listPrice = saleInfo.get("listPrice");
                if (listPrice.has("amount")) {
                    book.setListPrice(listPrice.get("amount").asDouble());
                }
                if (listPrice.has("currencyCode")) {
                    book.setCurrencyCode(listPrice.get("currencyCode").asText());
                }
            }
        }
    }

    /**
     * Sets web reader and access links for the book
     * - Extracts links from the accessInfo section
     * - Sets web reader URL for browser-based reading
     * - Extracts PDF and EPUB availability information
     * 
     * @param item JsonNode containing book data
     * @param book Book object to populate with extracted links
     */
    private void setLinks(JsonNode item, Book book) {
        if (item.has("accessInfo")) {
            JsonNode accessInfo = item.get("accessInfo");
            
            // Extract web reader link
            if (accessInfo.has("webReaderLink")) {
                book.setWebReaderLink(accessInfo.get("webReaderLink").asText());
            }
            
            // Extract PDF availability information
            if (accessInfo.has("pdf")) {
                JsonNode pdfInfo = accessInfo.get("pdf");
                if (pdfInfo.has("isAvailable")) {
                    book.setPdfAvailable(pdfInfo.get("isAvailable").asBoolean());
                }
            }
            
            // Extract EPUB availability information
            if (accessInfo.has("epub")) {
                JsonNode epubInfo = accessInfo.get("epub");
                if (epubInfo.has("isAvailable")) {
                    book.setEpubAvailable(epubInfo.get("isAvailable").asBoolean());
                }
            }
        }
    }

    /**
     * Parses published date from volume info with flexible formatting
     * - Attempts to parse full date format (yyyy-MM-dd) first
     * - Falls back to year-only format (yyyy) if full date fails
     * - Returns null if date cannot be parsed
     * - Handles common date format variations in Google Books API
     * 
     * @param volumeInfo JsonNode containing volume information
     * @return Parsed Date object or null if date cannot be parsed
     */
    private Date parsePublishedDate(JsonNode volumeInfo) {
        if (volumeInfo.has("publishedDate")) {
            String dateString = volumeInfo.get("publishedDate").asText();
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            try {
                return format.parse(dateString);
            } catch (ParseException e) {
                format = new SimpleDateFormat("yyyy");
                try {
                    return format.parse(dateString);
                } catch (ParseException ex) {
                    logger.error("Failed to parse published date: {}", dateString);
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * Extracts edition information from industry identifiers
     * - Creates EditionInfo objects from identifier JSON
     * - Adds edition information to the provided list
     * - Handles ISBN and other identifier types
     * 
     * @param identifier JsonNode containing identifier information
     * @param otherEditions List to add extracted EditionInfo objects to
     */
    private void extractEditionInfoFromItem(JsonNode identifier, List<Book.EditionInfo> otherEditions) {
        if (identifier.has("type") && identifier.has("identifier")) {
            String type = identifier.get("type").asText();
            String ident = identifier.get("identifier").asText();
            Book.EditionInfo editionInfo = new Book.EditionInfo();
            editionInfo.setType(type);
            editionInfo.setIdentifier(ident);
            otherEditions.add(editionInfo);
        }
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
        logger.warn("GoogleBooksService.searchBooks circuit breaker opened for query: '{}', startIndex: {}. Error: {}", 
            query, startIndex, t.getMessage());
        
        // Define query type for specific endpoint monitoring
        String queryType = "general";
        if (query.contains("intitle:")) {
            queryType = "title";
        } else if (query.contains("inauthor:")) {
            queryType = "author";
        } else if (query.contains("isbn:")) {
            queryType = "isbn";
        }
        String apiEndpoint = "volumes/search/" + queryType; // Renamed to avoid conflict if 'endpoint' is used elsewhere
        
        // Record this failure in our metrics
        apiRequestMonitor.recordFailedRequest(apiEndpoint, "Circuit breaker opened for query: '" + query + "', startIndex: " + startIndex + ": " + t.getMessage());
        
        return Mono.empty(); // Return empty JsonNode or a default structure if appropriate
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
        logger.warn("GoogleBooksService.searchBooks rate limit exceeded for query: '{}', startIndex: {}. Error: {}", 
            query, startIndex, t.getMessage());
        
        // Record this as a rate limit event in our metrics
        apiRequestMonitor.recordMetric("api/rate-limited", "API call rate limited for search: " + query);
        
        return Mono.empty(); // Return empty JsonNode or a default structure if appropriate
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
        
        // Record this failure in our metrics
        apiRequestMonitor.recordFailedRequest("volumes/get/id", "Circuit breaker opened for bookId: " + bookId + ": " + t.getMessage());
        
        return CompletableFuture.completedFuture(null); // Return empty Book
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
        
        // Record this as a rate limit event in our metrics
        apiRequestMonitor.recordMetric("api/rate-limited", "API call rate limited for book " + bookId);
        
        return CompletableFuture.completedFuture(null); // Return empty Book
    }

    /**
     * Extract qualifiers from a search query to store with book data
     * - Identifies special terms like "bestseller", "award winner", etc.
     * - Creates a map of qualifier key-value pairs for later reference
     * 
     * @param query Search query to analyze
     * @return Map of extracted qualifier key-value pairs
     */
    private Map<String, Object> extractQualifiersFromQuery(String query) {
        Map<String, Object> qualifiers = new HashMap<>();
        
        // Normalize the query
        String normalizedQuery = query.toLowerCase().trim();
        
        // Check for New York Times Bestseller
        if (normalizedQuery.contains("new york times bestseller") || 
            normalizedQuery.contains("nyt bestseller") ||
            normalizedQuery.contains("ny times bestseller")) {
            qualifiers.put("nytBestseller", true);
        }
        
        // Check for award winners
        if (normalizedQuery.contains("award winner") || 
            normalizedQuery.contains("prize winner") ||
            normalizedQuery.contains("pulitzer") ||
            normalizedQuery.contains("nobel")) {
            qualifiers.put("awardWinner", true);
            
            // Capture specific award if mentioned
            if (normalizedQuery.contains("pulitzer")) {
                qualifiers.put("pulitzerPrize", true);
            }
            if (normalizedQuery.contains("nobel")) {
                qualifiers.put("nobelPrize", true);
            }
        }
        
        // Check for best books lists
        if (normalizedQuery.contains("best books") || 
            normalizedQuery.contains("top books") ||
            normalizedQuery.contains("must read")) {
            qualifiers.put("recommendedList", true);
        }
        
        // Store raw query terms for future reference
        qualifiers.put("queryTerms", new ArrayList<>(Arrays.asList(normalizedQuery.split("\\s+"))));
        
        return qualifiers;
    }

}
