/**
 * Service for interacting with the Google Books API
 * - Provides reactive interfaces for searching and retrieving book information
 * - Handles API communication with retry logic and error handling
 * - Converts Google Books API responses to Book domain objects
 * - Implements search by title, author, ISBN and similarity
 * - Manages cover image URL transformations for optimal quality
 * - Caches Google Books API responses in S3
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.io.IOException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
@Service
public class GoogleBooksService {

    private static final Logger logger = LoggerFactory.getLogger(GoogleBooksService.class);

    private final WebClient webClient;
    private final S3StorageService s3StorageService;
    private final ObjectMapper objectMapper;

    @Value("${googlebooks.api.url}")
    private String googleBooksApiUrl;

    @Value("${googlebooks.api.key}")
    private String googleBooksApiKey;

    /**
     * Initializes the GoogleBooksService with a WebClient for API requests, an S3StorageService for caching, and an ObjectMapper for JSON processing.
     *
     * @param webClientBuilder builder for creating the reactive WebClient
     * @param s3StorageService service used for S3-based caching of API responses
     * @param objectMapper JSON processor for serializing and deserializing data
     */
    @Autowired
    public GoogleBooksService(WebClient.Builder webClientBuilder, S3StorageService s3StorageService, ObjectMapper objectMapper) {
        this.webClient = webClientBuilder.build();
        this.s3StorageService = s3StorageService;
        this.objectMapper = objectMapper;
    }

    /**
     * Searches the Google Books API with the specified query and parameters, returning the raw JSON response.
     *
     * Executes an HTTP GET request with pagination, ordering, and optional language restriction. Automatically retries transient errors with exponential backoff and returns an empty result on failure.
     *
     * @param query the search query string
     * @param startIndex the starting index for pagination (0-based)
     * @param orderBy the result ordering preference (e.g., "newest", "relevance")
     * @param langCode the optional language code filter (e.g., "en", "fr")
     * @return a Mono emitting the raw JsonNode response from the API, or empty if the request fails
     */
    @CircuitBreaker(name = "googleBooksService", fallbackMethod = "searchBooksFallback")
    @TimeLimiter(name = "googleBooksService")
    public Mono<JsonNode> searchBooks(String query, int startIndex, String orderBy, String langCode) {
        StringBuilder urlBuilder = new StringBuilder(String.format("%s/volumes?q=%s&startIndex=%d&maxResults=40", 
                googleBooksApiUrl, query, startIndex));
        
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
        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                    .filter(throwable -> throwable instanceof IOException || throwable instanceof WebClientRequestException)
                    .doBeforeRetry(retrySignal -> logger.warn("Retrying API call for query '{}', startIndex {}. Attempt #{}. Error: {}", query, startIndex, retrySignal.totalRetries() + 1, retrySignal.failure().getMessage()))
                    .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                        logger.error("All retries failed for query '{}', startIndex {}. Final error: {}", query, startIndex, retrySignal.failure().getMessage());
                        return retrySignal.failure();
                    }))
                .onErrorResume(e -> {
                    // This now catches errors after retries are exhausted or if the error wasn't retryable
                    logger.error("Error fetching page for query '{}' at startIndex {} after retries or due to non-retryable error: {}", query, startIndex, e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * Searches for books using the Google Books API with pagination and returns a list of Book objects.
     *
     * Performs a multi-page search using the provided query, language code, and ordering, retrieving up to the specified maximum number of results. Each API response item is converted to a Book object, and the final list is limited to the desired total results.
     *
     * @param query the search query string
     * @param langCode optional language code to restrict results (e.g., "en", "fr")
     * @param desiredTotalResults the maximum number of results to retrieve
     * @param orderBy the sorting order for results (e.g., "relevance", "newest")
     * @return a Mono emitting a list of Book objects matching the search criteria
     */
    public Mono<List<Book>> searchBooksAsyncReactive(String query, String langCode, int desiredTotalResults, String orderBy) {
        final int maxResultsPerPage = 40;
        final int maxTotalResultsToFetch = (desiredTotalResults > 0) ? desiredTotalResults : 200; // Default to 200 if invalid
        final String effectiveOrderBy = (orderBy != null && !orderBy.trim().isEmpty()) ? orderBy : "newest"; // Default to newest

        return Flux.range(0, (maxTotalResultsToFetch + maxResultsPerPage - 1) / maxResultsPerPage)
            .map(page -> page * maxResultsPerPage)
            .concatMap(startIndex -> 
                searchBooks(query, startIndex, effectiveOrderBy, langCode) 
                    .flatMapMany(response -> {
                        if (response != null && response.has("items") && response.get("items").isArray()) {
                            List<JsonNode> items = new ArrayList<>();
                            response.get("items").forEach(items::add);
                            return Flux.fromIterable(items);
                        }
                        return Flux.empty();
                    })
            )
            .takeUntil(jsonNode -> !jsonNode.has("kind") ) // This condition might need review if it prematurely stops fetching
            .map(this::convertGroupToBook)
            .filter(Objects::nonNull)
            .collectList()
            .map(books -> {
                if (books.size() > maxTotalResultsToFetch) {
                    return books.subList(0, maxTotalResultsToFetch);
                }
                return books;
            });
    }

    /**
     * Searches for books using the provided query and optional language code, returning up to 200 results ordered by newest.
     *
     * @param query the search query string
     * @param langCode optional language code to restrict results (e.g., "en", "fr")
     * @return a Mono emitting a list of Book objects matching the query
     */
    public Mono<List<Book>> searchBooksAsyncReactive(String query, String langCode) {
        // Calls the method with default desiredTotalResults and orderBy newest
        return searchBooksAsyncReactive(query, langCode, 200, "newest"); 
    }

    /**
     * Searches for books using the provided query without language filtering.
     *
     * @param query the search query string
     * @return a Mono emitting a list of Book objects matching the query
     */
    public Mono<List<Book>> searchBooksAsyncReactive(String query) {
        return searchBooksAsyncReactive(query, null, 200, "newest");
    }

    /**
     * Searches for books with the specified title using the Google Books API.
     *
     * @param title the title to search for
     * @param langCode optional language code to filter results
     * @return a Mono emitting a list of books matching the given title
     */
    public Mono<List<Book>> searchBooksByTitle(String title, String langCode) {
        return searchBooksAsyncReactive("intitle:" + title, langCode);
    }

    /****
     * Searches for books by title using the Google Books API.
     *
     * @param title the book title to search for
     * @return a Mono emitting a list of books matching the given title
     */
    public Mono<List<Book>> searchBooksByTitle(String title) {
        return searchBooksByTitle(title, null);
    }

    /**
     * Searches for books by a specific author using the Google Books API.
     *
     * @param author the name of the author to search for
     * @param langCode optional language code to filter results
     * @return a Mono emitting a list of books authored by the specified individual
     */
    public Mono<List<Book>> searchBooksByAuthor(String author, String langCode) {
        return searchBooksAsyncReactive("inauthor:" + author, langCode);
    }

    /**
     * Searches for books by the specified author without applying a language filter.
     *
     * @param author the name of the author to search for
     * @return a Mono emitting a list of books by the given author
     */
    public Mono<List<Book>> searchBooksByAuthor(String author) {
        return searchBooksByAuthor(author, null);
    }

    /**
     * Searches for books by ISBN using the Google Books API.
     *
     * @param isbn the ISBN identifier to search for
     * @param langCode optional language code to restrict results
     * @return a Mono emitting a list of books matching the given ISBN
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
     * Retrieves detailed book information by Google Books volume ID, using S3 caching for efficiency.
     *
     * Attempts to load the book's JSON data from S3 cache; if unavailable or invalid, fetches from the Google Books API.
     * Converts the resulting JSON to a {@code Book} object. Returns a completed future with {@code null} if the book cannot be found or an error occurs.
     *
     * @param bookId the Google Books volume ID
     * @return a {@code CompletionStage} containing the {@code Book} if found, or {@code null} if not found or on error
     */
    @CircuitBreaker(name = "googleBooksService", fallbackMethod = "getBookByIdFallback")
    @TimeLimiter(name = "googleBooksService")
    public CompletionStage<Book> getBookById(String bookId) {
        // Step 1: Try to fetch from S3 cache first
        return s3StorageService.fetchJsonAsync(bookId) // This returns CompletableFuture<Optional<String>>
            .thenComposeAsync(optionalJsonString -> {
                if (optionalJsonString.isPresent()) {
                    String jsonString = optionalJsonString.get();
                    try {
                        // Assuming the JSON in S3 is the same structure that convertSingleItemToBook expects
                        // (i.e., the raw item from Google Books API, possibly enriched with _metadata)
                        JsonNode s3JsonNode = objectMapper.readTree(jsonString);
                        Book bookFromS3 = convertSingleItemToBook(s3JsonNode); // This will set rawJsonResponse

                        if (bookFromS3 != null && bookFromS3.getId() != null) { // Basic validation
                            logger.info("Successfully retrieved book {} from S3 cache.", bookId);
                            return CompletableFuture.completedFuture(bookFromS3);
                        } else {
                            // This case handles if S3 JSON is present but malformed or doesn't convert to a valid book
                            logger.warn("S3 cache for {} contained JSON, but it parsed to a null/invalid book. Falling back to API.", bookId);
                        }
                    } catch (IOException e) {
                        // This case handles if S3 JSON is present but fails to parse as JSON tree
                        logger.warn("Failed to parse book JSON from S3 cache for bookId {}: {}. Falling back to API.", bookId, e.getMessage());
                    }
                } else {
                    // This case handles S3 miss (NoSuchKey in fetchJsonAsync) or S3 fetch error (as fetchJsonAsync returns Optional.empty for errors)
                    logger.info("Book {} not found in S3 cache (or S3 error occurred during fetch). Fetching from API.", bookId);
                }

                // If S3 miss, S3 error, S3 JSON parsing error, or S3 JSON parsed to invalid book, then fetch from API.
                // The circuit breaker on getBookById will handle failures from fetchFromGoogleBooksApiAndCache.
                return fetchFromGoogleBooksApiAndCache(bookId);
            });
    }

    /**
     * Fetches detailed book information from the Google Books API by volume ID, enriches the JSON with a fetch timestamp, converts it to a {@code Book} object, and asynchronously caches the JSON in S3.
     *
     * If the API response is a valid JSON object, a {@code _metadata} node with the Pacific Time fetch timestamp is added before conversion and caching. The JSON is pretty-printed for S3 storage when possible. Errors during fetching, conversion, or caching are logged, and {@code null} is returned on failure.
     *
     * @param bookId the Google Books volume ID to retrieve
     * @return a {@code CompletionStage} containing the {@code Book} object, or {@code null} if retrieval or conversion fails
     */
    private CompletionStage<Book> fetchFromGoogleBooksApiAndCache(String bookId) {
        StringBuilder urlBuilder = new StringBuilder(String.format("%s/volumes/%s", googleBooksApiUrl, bookId));
        
        if (googleBooksApiKey != null && !googleBooksApiKey.isEmpty()) {
            urlBuilder.append("?key=").append(googleBooksApiKey);
        }
        
        String url = urlBuilder.toString();
        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                    .filter(throwable -> throwable instanceof IOException || throwable instanceof WebClientRequestException)
                    .doBeforeRetry(retrySignal -> logger.warn("Retrying API call for bookId {}. Attempt #{}. Error: {}", bookId, retrySignal.totalRetries() + 1, retrySignal.failure().getMessage()))
                    .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                        logger.error("All retries failed for bookId {}. Final error: {}", bookId, retrySignal.failure().getMessage());
                        return retrySignal.failure();
                    }))
                .map(originalJsonNode -> {
                    JsonNode finalNodeForBookAndS3; // This node will be used for Book object and for S3 caching
                    boolean metadataWasAdded = false;

                    if (originalJsonNode.isObject()) {
                        ObjectNode enrichedNode = objectMapper.createObjectNode();
                        enrichedNode.setAll((ObjectNode) originalJsonNode); // Copy original content

                        // Add _metadata
                        ObjectNode metadataNode = objectMapper.createObjectNode();
                        Instant nowUtc = Instant.now();
                        ZoneId pacificTimeZone = ZoneId.of("America/Los_Angeles");
                        ZonedDateTime pacificTime = nowUtc.atZone(pacificTimeZone);
                        String pacificTimestampString = pacificTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME);
                        metadataNode.put("fetchedAt", pacificTimestampString);
                        enrichedNode.set("_metadata", metadataNode);

                        finalNodeForBookAndS3 = enrichedNode;
                        metadataWasAdded = true;
                    } else {
                        logger.warn("Original JSON node for bookId {} is not an ObjectNode. Book data and S3 cache will use the original structure without added _metadata.", bookId);
                        finalNodeForBookAndS3 = originalJsonNode; // Use original as-is
                    }

                    Book book = convertSingleItemToBook(finalNodeForBookAndS3); // This sets book.rawJsonResponse to finalNodeForBookAndS3.toString()

                    if (book != null) {
                        // For S3, we want pretty-printed JSON.
                        // The book.getRawJsonResponse() will remain compact as per current convertSingleItemToBook logic.
                        String jsonForS3Cache;
                        try {
                            jsonForS3Cache = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(finalNodeForBookAndS3);
                        } catch (IOException e) {
                            logger.error("Error pretty-printing JSON for S3 cache, bookId {}: {}. Will attempt to cache compact JSON.", bookId, e.getMessage());
                            jsonForS3Cache = finalNodeForBookAndS3.toString(); // Fallback to compact string if pretty-printing fails
                        }
                        
                        final String finalJsonForS3Cache = jsonForS3Cache; // Effectively final for lambda
                        if (finalJsonForS3Cache != null && !finalJsonForS3Cache.isEmpty()) {
                            boolean finalMetadataWasAdded = metadataWasAdded; // effectively final for lambda
                            s3StorageService.uploadJsonAsync(bookId, finalJsonForS3Cache)
                                .thenRun(() -> logger.info("Successfully cached book {} JSON in S3. Metadata added: {}. Pretty-printed: {}", 
                                                            bookId, finalMetadataWasAdded, (finalJsonForS3Cache.contains("\n"))))
                                .exceptionally(ex -> {
                                    logger.error("Failed to cache book {} JSON in S3: {}", bookId, ex.getMessage());
                                    return null;
                                });
                        } else {
                            logger.warn("jsonForS3Cache is null or empty for bookId {}. S3 upload will be skipped.", bookId);
                        }
                    } else {
                         logger.warn("Book object is null after conversion for bookId {}. S3 upload will be skipped.", bookId);
                    }
                    return book;
                })
                .toFuture()
                .thenApply(book -> book) 
                .exceptionally(e -> {
                    logger.error("Error fetching book {} from Google Books API: {}", bookId, e.getMessage());
                    return null;
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

        return book;
    }
    
    /**
     * Populates a Book object with core metadata extracted from a Google Books API JSON node.
     *
     * Extracts and sets the book's ID, title, authors, publisher (with sanitized quotes), published date, description, cover image URL, language, ISBN-10, ISBN-13, and other edition identifiers.
     *
     * @param item JSON node containing the book data, expected to include a "volumeInfo" object
     * @param book Book instance to be populated with extracted metadata
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
     * Returns the highest quality available cover image URL from the given volume information.
     *
     * Selects the best resolution image from the Google Books API image links, prioritizing larger sizes, and enhances the URL for optimal quality. Returns null if no image is available.
     *
     * @param volumeInfo JSON node containing Google Books volume information
     * @return the best available cover image URL, or null if none found
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
     * Populates the book with web reader and digital format availability links from the JSON data.
     *
     * Extracts the web reader URL, and sets flags indicating PDF and EPUB availability based on the access information in the provided JSON node.
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
     * Retrieves up to five books similar to the given book, based on matching author and title.
     *
     * Constructs a search query using the first author and the title of the provided book, excluding the original book from the results. Returns an empty list if the input book is invalid or if no similar books are found.
     *
     * @param book the reference book for which to find similar books
     * @return a Mono emitting a list of up to five similar Book objects
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
     * Provides a fallback response when the searchBooks circuit breaker is triggered.
     *
     * Logs a warning and returns an empty Mono to indicate that no results are available due to service unavailability.
     *
     * @return an empty Mono indicating no search results
     */
    public Mono<JsonNode> searchBooksFallback(String query, int startIndex, String orderBy, String langCode, Throwable t) {
        logger.warn("GoogleBooksService.searchBooks circuit breaker opened for query: '{}', startIndex: {}. Error: {}", query, startIndex, t.getMessage());
        return Mono.empty(); // Return empty JsonNode or a default structure if appropriate
    }

    /**
     * Provides a fallback response when retrieving a book by ID fails due to circuit breaker activation.
     *
     * Logs a warning and returns a completed future with null to indicate the book could not be retrieved.
     *
     * @param bookId the ID of the book that was requested
     * @param t the exception that caused the fallback
     * @return a completed future containing null
     */
    public CompletionStage<Book> getBookByIdFallback(String bookId, Throwable t) {
        logger.warn("GoogleBooksService.getBookById circuit breaker opened for bookId: {}. Error: {}", bookId, t.getMessage());
        return CompletableFuture.completedFuture(null); // Return empty Book
    }

}
