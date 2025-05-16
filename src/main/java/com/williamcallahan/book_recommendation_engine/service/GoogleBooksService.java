package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import java.time.Duration;
import java.io.IOException;
import java.util.concurrent.CompletableFuture; // Added import

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for interacting with the Google Books API
 * - Provides reactive interfaces for searching and retrieving book information
 * - Handles API communication with retry logic and error handling
 * - Converts Google Books API responses to Book domain objects
 * - Implements search by title, author, ISBN and similarity
 * - Manages cover image URL transformations for optimal quality
 *
 * @author William Callahan
 */
@Service
public class GoogleBooksService {

    private static final Logger logger = LoggerFactory.getLogger(GoogleBooksService.class);

    private final WebClient webClient;

    @Value("${googlebooks.api.url}")
    private String googleBooksApiUrl;

    @Value("${googlebooks.api.key}")
    private String googleBooksApiKey;

    /**
     * Constructs a GoogleBooksService with configured WebClient
     * - Initializes reactive HTTP client for API communication
     * 
     * @param webClientBuilder Spring WebClient builder for constructing the client
     */
    public GoogleBooksService(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }

    /**
     * Performs a search against the Google Books API
     * - Executes an HTTP GET request with the provided query parameters
     * - Implements retry logic with exponential backoff for transient errors
     * - Returns raw JsonNode response for further processing
     * - Handles pagination through startIndex parameter
     * 
     * @param query Search query string
     * @param startIndex Starting index for pagination (0-based)
     * @param orderBy Result ordering preference (e.g., "newest", "relevance")
     * @param langCode Optional language code filter (e.g., "en", "fr")
     * @return Mono containing the raw JsonNode response from the API
     */
    @CircuitBreaker(name = "googleBooksService", fallbackMethod = "searchBooksFallback")
    @TimeLimiter(name = "googleBooksService")
    public Mono<JsonNode> searchBooks(String query, int startIndex, String orderBy, String langCode) {
        String url = String.format("%s/volumes?q=%s&startIndex=%d&maxResults=40&key=%s",
                googleBooksApiUrl, query, startIndex, googleBooksApiKey);
        if (orderBy != null && !orderBy.isEmpty()) {
            url += "&orderBy=" + orderBy;
        }
        if (langCode != null && !langCode.isEmpty()) {
            url += "&langRestrict=" + langCode;
            logger.debug("Google Books API call with langRestrict: {}", langCode);
        }
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
                    if (e instanceof io.netty.handler.ssl.SslHandshakeTimeoutException) {
                        logger.error("SSL Handshake Timeout for query '{}' at startIndex {} after retries. This often indicates network connectivity or firewall issues to www.googleapis.com:443. Error: {}", query, startIndex, e.getMessage());
                    } else {
                        logger.error("Error fetching page for query '{}' at startIndex {} after retries or due to non-retryable error: {}", query, startIndex, e.getMessage());
                    }
                    return Mono.empty();
                });
    }

    /**
     * Performs a comprehensive search across multiple Google Books API pages
     * - Implements pagination across multiple results pages 
     * - Processes each page concurrently using reactive streams
     * - Converts API response items to Book domain objects
     * - Enforces maximum result limits to prevent excessive API calls
     * 
     * @param query Search query string to send to the API
     * @param langCode Optional language code to restrict results (e.g., "en", "fr")
     * @return Mono containing a list of Book objects retrieved from the API
     */
    public Mono<List<Book>> searchBooksAsyncReactive(String query, String langCode) {
        final int maxResultsPerPage = 40;
        final int maxTotalResults = 200;

        return Flux.range(0, (maxTotalResults + maxResultsPerPage - 1) / maxResultsPerPage)
            .map(page -> page * maxResultsPerPage)
            .concatMap(startIndex -> 
                searchBooks(query, startIndex, "newest", langCode)
                    .flatMapMany(response -> {
                        if (response != null && response.has("items") && response.get("items").isArray()) {
                            List<JsonNode> items = new ArrayList<>();
                            response.get("items").forEach(items::add);
                            return Flux.fromIterable(items);
                        }
                        return Flux.empty();
                    })
            )
            .takeUntil(jsonNode -> !jsonNode.has("kind") )
            .map(this::convertGroupToBook)
            .filter(Objects::nonNull)
            .collectList()
            .map(books -> {
                if (books.size() > maxTotalResults) {
                    return books.subList(0, maxTotalResults);
                }
                return books;
            });
    }

    /**
     * Search books with default language setting
     * - Convenience method that calls searchBooksAsyncReactive without language restriction
     * 
     * @param query Search query string to send to the API
     * @return Mono containing a list of Book objects retrieved from the API
     */
    public Mono<List<Book>> searchBooksAsyncReactive(String query) {
        return searchBooksAsyncReactive(query, null);
    }

    /**
     * Search books by title with language filtering
     * - Uses the 'intitle:' Google Books API qualifier
     * - Allows restricting results to specific language
     * 
     * @param title Book title to search for
     * @param langCode Optional language code to restrict results
     * @return Mono containing a list of Book objects matching the title
     */
    public Mono<List<Book>> searchBooksByTitle(String title, String langCode) {
        return searchBooksAsyncReactive("intitle:" + title, langCode);
    }

    /**
     * Search books by title with default language setting
     * - Convenience method that calls searchBooksByTitle without language restriction
     * 
     * @param title Book title to search for
     * @return Mono containing a list of Book objects matching the title
     */
    public Mono<List<Book>> searchBooksByTitle(String title) {
        return searchBooksByTitle(title, null);
    }

    /**
     * Search books by author with language filtering
     * - Uses the 'inauthor:' Google Books API qualifier
     * - Allows restricting results to specific language
     * - Executes search across Google Books catalog
     * - Converts API responses to domain objects
     * 
     * @param author Author name to search for
     * @param langCode Optional language code to restrict results
     * @return Mono containing a list of Book objects by the specified author
     */
    public Mono<List<Book>> searchBooksByAuthor(String author, String langCode) {
        return searchBooksAsyncReactive("inauthor:" + author, langCode);
    }

    /**
     * Search books by author with default language setting
     * - Convenience method that calls searchBooksByAuthor without language restriction
     * 
     * @param author Author name to search for
     * @return Mono containing a list of Book objects by the specified author
     */
    public Mono<List<Book>> searchBooksByAuthor(String author) {
        return searchBooksByAuthor(author, null);
    }

    /**
     * Search books by ISBN with language filtering
     * - Uses the 'isbn:' Google Books API qualifier for precise matching
     * - Allows restricting results to specific language
     * 
     * @param isbn ISBN identifier to search for
     * @param langCode Optional language code to restrict results
     * @return Mono containing a list of Book objects matching the ISBN
     */
    public Mono<List<Book>> searchBooksByISBN(String isbn, String langCode) {
        return searchBooksAsyncReactive("isbn:" + isbn, langCode);
    }

    /**
     * Search books by ISBN with default language setting
     * - Convenience method that calls searchBooksByISBN without language restriction
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
     * - Implements retry logic with exponential backoff for transient errors
     * - Converts API response to Book domain object
     * - Returns empty Mono if book cannot be found or errors occur
     * 
     * @param bookId Google Books volume ID
     * @return Mono containing the Book object if found, empty Mono otherwise
     */
    @CircuitBreaker(name = "googleBooksService", fallbackMethod = "getBookByIdFallback")
    @TimeLimiter(name = "googleBooksService")
    public CompletableFuture<Book> getBookById(String bookId) {
        String url = String.format("%s/volumes/%s?key=%s", googleBooksApiUrl, bookId, googleBooksApiKey);
        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(1))
                    .filter(throwable -> throwable instanceof IOException || throwable instanceof WebClientRequestException)
                    .doBeforeRetry(retrySignal -> logger.warn("Retrying API call for book ID {}. Attempt #{}. Error: {}", bookId, retrySignal.totalRetries() + 1, retrySignal.failure().getMessage()))
                    .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                        logger.error("All retries failed for book ID {}. Final error: {}", bookId, retrySignal.failure().getMessage());
                        return retrySignal.failure();
                    }))
                .map(item -> {
                    if (item != null) {
                        return convertSingleItemToBook(item);
                    }
                    return null;
                })
                .onErrorResume(e -> {
                    if (e instanceof io.netty.handler.ssl.SslHandshakeTimeoutException) {
                        logger.error("SSL Handshake Timeout for book ID {} after retries. This often indicates network connectivity or firewall issues to www.googleapis.com:443. Error: {}", bookId, e.getMessage());
                    } else {
                        logger.error("Error fetching book by ID {} after retries or due to non-retryable error: {}", bookId, e.getMessage());
                    }
                    return Mono.empty(); // Return an empty Mono on error
                })
                .toFuture(); // Convert Mono to CompletableFuture
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
            for (JsonNode identifier : volumeInfo.get("industryIdentifiers")) {
                extractEditionInfoFromItem(identifier, otherEditions);
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
     * 
     * @param item JsonNode containing book data
     * @param book Book object to populate with extracted links
     */
    private void setLinks(JsonNode item, Book book) {
        if (item.has("accessInfo")) {
            JsonNode accessInfo = item.get("accessInfo");
            if (accessInfo.has("webReaderLink")) {
                book.setWebReaderLink(accessInfo.get("webReaderLink").asText());
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

    // Fallback methods
    public Mono<JsonNode> searchBooksFallback(String query, int startIndex, String orderBy, String langCode, Throwable t) {
        logger.warn("GoogleBooksService.searchBooks circuit breaker opened for query: '{}', startIndex: {}. Error: {}", query, startIndex, t.getMessage());
        return Mono.empty(); // Return empty JsonNode or a default structure if appropriate
    }

    public CompletableFuture<Book> getBookByIdFallback(String bookId, Throwable t) {
        logger.warn("GoogleBooksService.getBookById circuit breaker opened for bookId: {}. Error: {}", bookId, t.getMessage());
        return CompletableFuture.completedFuture(null); // Return a completed CompletableFuture with null or an empty Book
    }

    /**
     * Circuit breaker fallbacks for higher-level search methods
     * - Root cause handled by searchBooks/getBookById fallbacks
     * - No additional fallbacks required for derived methods
     */
}
