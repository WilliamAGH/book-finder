/**
 * Service for interacting with OpenLibrary's data API to fetch book information
 *
 * @author William Callahan
 *
 * Features:
 * - Provides methods to fetch book data from OpenLibrary using ISBN
 * - Enables book title-based search against OpenLibrary API
 * - Implements resilience patterns with circuit breakers and rate limiting
 * - Handles API response mapping to Book domain objects
 * - Provides fallback behavior for API failures
 * - Uses reactive programming model with Mono/Flux return types
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.ExternalApiLogger;
import com.williamcallahan.book_recommendation_engine.util.DateParsingUtils;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.PrematureCloseException;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.Objects; // Added import
import java.time.Duration;

@Service
@Slf4j
public class OpenLibraryBookDataService {

        private final WebClient webClient;
    private final boolean externalFallbackEnabled;

    public OpenLibraryBookDataService(WebClient.Builder webClientBuilder,
                                   @Value("${OPENLIBRARY_API_URL:https://openlibrary.org}") String openLibraryApiUrl,
                                   @Value("${app.features.external-fallback.enabled:${app.features.google-fallback.enabled:true}}") boolean externalFallbackEnabled) {
        this.webClient = webClientBuilder.baseUrl(openLibraryApiUrl).build();
        this.externalFallbackEnabled = externalFallbackEnabled;
    }

    /**
     * Fetches book data from OpenLibrary by ISBN
     *
     * @param isbn The ISBN of the book
     * @return A Mono emitting the Book object if found, or Mono.empty() otherwise
     */
    @RateLimiter(name = "openLibraryDataService")
    @CircuitBreaker(name = "openLibraryDataService", fallbackMethod = "fetchBookFallback")
    public Mono<Book> fetchBookByIsbn(String isbn) {
        if (isbn == null || isbn.trim().isEmpty()) {
            log.warn("ISBN is null or empty. Cannot fetch book from OpenLibrary.");
            return Mono.empty();
        }
        if (!externalFallbackEnabled) {
            log.debug("External fallback disabled; skipping OpenLibrary fetch for ISBN {}.", isbn);
            return Mono.empty();
        }
        log.info("Attempting to fetch book data from OpenLibrary for ISBN: {}", isbn);

        String bibkey = "ISBN:" + isbn;

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/api/books")
                        .queryParam("bibkeys", bibkey)
                        .queryParam("format", "json")
                        .queryParam("jscmd", "data")
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .timeout(Duration.ofSeconds(5))
                .onErrorResume(PrematureCloseException.class, e -> {
                    log.debug("OpenLibrary connection closed early for ISBN {}: {}", isbn, e.toString());
                    return Mono.empty();
                })
                .flatMap(responseNode -> {
                    JsonNode bookDataNode = responseNode.path(bibkey);
                    if (bookDataNode.isMissingNode() || bookDataNode.isEmpty()) {
                        log.warn("No data found in OpenLibrary response for bibkey: {}", bibkey);
                        return Mono.empty();
                    }
                    Book book = parseOpenLibraryBook(bookDataNode, isbn);
                    return book != null ? Mono.just(book) : Mono.empty();
                })
                .doOnError(e -> LoggingUtils.error(log, e, "Error fetching book by ISBN {} from OpenLibrary", isbn))
                .onErrorResume(e -> { // Ensure fallback behavior on error before circuit breaker
                    LoggingUtils.warn(log, e, "Error during OpenLibrary fetch for ISBN {}, returning empty", isbn);
                    return Mono.empty();
                });
    }

    /**
     * Searches for books on OpenLibrary by title (and optionally author)
     *
     * @param title The title of the book
     * @return A Flux emitting Book objects matching the search query
     */
    @RateLimiter(name = "openLibraryDataService")
    @CircuitBreaker(name = "openLibraryDataService", fallbackMethod = "searchBooksFallback")
    public Flux<Book> searchBooksByTitle(String title) {
        if (title == null || title.trim().isEmpty()) {
            log.warn("Title is null or empty. Cannot search books on OpenLibrary.");
            return Flux.empty();
        }
        if (!externalFallbackEnabled) {
            log.debug("External fallback disabled; skipping OpenLibrary search for title: {}", title);
            return Flux.empty();
        }
        // Example endpoint: /search.json?q={title} or /search.json?title={title}&author={author}
        log.info("Attempting to search books from OpenLibrary for title: {}", title);
        ExternalApiLogger.logApiCallAttempt(log, "OpenLibrary", "SEARCH_TITLE", title, false);
        // Placeholder:
        // return webClient.get().uri("/search.json?q=" + title)
        // .retrieve()
        // .bodyToFlux(Book.class) // This will need proper mapping and extraction from search results
        // .doOnError(e -> log.error("Error searching books by title '{}' from OpenLibrary: {}", title, e.getMessage()));
        // return Flux.empty(); // Placeholder
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/search.json")
                        .queryParam("title", title) // Using title parameter for more specific search
                        // .queryParam("q", title) // General query, 'title' is often more direct
                        .queryParam("limit", 20) // Limit results for now
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .timeout(Duration.ofSeconds(5))
                .onErrorResume(PrematureCloseException.class, e -> {
                    log.debug("OpenLibrary search connection closed early for title '{}': {}", title, e.toString());
                    return Mono.empty();
                })
                .flatMapMany(responseNode -> {
                    if (!responseNode.has("docs") || !responseNode.get("docs").isArray()) {
                        ExternalApiLogger.logApiCallSuccess(log, "OpenLibrary", "SEARCH_TITLE", title, 0);
                        return Flux.empty();
                    }
                    int count = responseNode.get("docs").size();
                    ExternalApiLogger.logApiCallSuccess(log, "OpenLibrary", "SEARCH_TITLE", title, count);
                    return Flux.fromIterable(responseNode.get("docs"))
                               .map(this::parseOpenLibrarySearchDoc)
                               .filter(Objects::nonNull);
                })
                .doOnError(e -> LoggingUtils.error(log, e, "Error searching books by title '{}' from OpenLibrary", title))
                .onErrorResume(e -> {
                     LoggingUtils.warn(log, e, "Error during OpenLibrary search for title '{}', returning empty Flux", title);
                     ExternalApiLogger.logApiCallFailure(log, "OpenLibrary", "SEARCH_TITLE", title, e.getMessage());
                     return Flux.empty();
                });
    }

    @RateLimiter(name = "openLibraryDataService")
    @CircuitBreaker(name = "openLibraryDataService", fallbackMethod = "searchBooksFallback")
    public Flux<Book> searchBooksByAuthor(String author) {
        if (author == null || author.trim().isEmpty()) {
            log.warn("Author is null or empty. Cannot search books on OpenLibrary.");
            return Flux.empty();
        }
        if (!externalFallbackEnabled) {
            log.debug("External fallback disabled; skipping OpenLibrary author search for: {}", author);
            return Flux.empty();
        }

        log.info("Attempting to search OpenLibrary for author: {}", author);
        ExternalApiLogger.logApiCallAttempt(log, "OpenLibrary", "SEARCH_AUTHOR", author, false);

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/search.json")
                        .queryParam("author", author)
                        .queryParam("limit", 20)
                        .build())
                .retrieve()
                .bodyToMono(JsonNode.class)
                .timeout(Duration.ofSeconds(5))
                .onErrorResume(PrematureCloseException.class, e -> {
                    log.debug("OpenLibrary author search connection closed early for '{}': {}", author, e.toString());
                    return Mono.empty();
                })
                .flatMapMany(responseNode -> {
                    if (!responseNode.has("docs") || !responseNode.get("docs").isArray()) {
                        ExternalApiLogger.logApiCallSuccess(log, "OpenLibrary", "SEARCH_AUTHOR", author, 0);
                        return Flux.empty();
                    }
                    int count = responseNode.get("docs").size();
                    ExternalApiLogger.logApiCallSuccess(log, "OpenLibrary", "SEARCH_AUTHOR", author, count);
                    return Flux.fromIterable(responseNode.get("docs"))
                            .map(this::parseOpenLibrarySearchDoc)
                            .filter(Objects::nonNull);
                })
                .doOnError(e -> LoggingUtils.error(log, e, "Error searching books by author '{}' from OpenLibrary", author))
                .onErrorResume(e -> {
                    LoggingUtils.warn(log, e, "Error during OpenLibrary search for author '{}', returning empty Flux", author);
                    ExternalApiLogger.logApiCallFailure(log, "OpenLibrary", "SEARCH_AUTHOR", author, e.getMessage());
                    return Flux.empty();
                });
    }

    /**
     * Unified entry point that selects title or author search heuristics.
     */
    public Flux<Book> searchBooks(String query, boolean treatAsAuthor) {
        return treatAsAuthor ? searchBooksByAuthor(query) : searchBooksByTitle(query);
    }

    // --- Fallback Methods ---

    public Mono<Book> fetchBookFallback(String isbn, Throwable t) {
        LoggingUtils.warn(log, t, "OpenLibraryBookDataService.fetchBookByIsbn fallback triggered for ISBN: {}", isbn);
        return Mono.empty();
    }

    public Flux<Book> searchBooksFallback(String title, Throwable t) {
        LoggingUtils.warn(log, t, "OpenLibraryBookDataService.searchBooksByTitle fallback triggered for title: '{}'", title);
        return Flux.empty();
    }

    private Book parseOpenLibraryBook(JsonNode bookDataNode, String originalIsbn) {
        if (bookDataNode == null || bookDataNode.isMissingNode() || !bookDataNode.isObject()) {
            log.warn("Book data node is null, missing, or not an object for ISBN: {}", originalIsbn);
            return null;
        }

        Book book = new Book();

        String olid = bookDataNode.path("key").asText(null);
        if (olid != null && olid.contains("/")) {
            olid = olid.substring(olid.lastIndexOf('/') + 1);
        }
        book.setId(olid);

        // Normalize title to proper case
        String rawTitle = bookDataNode.path("title").asText(null);
        book.setTitle(com.williamcallahan.book_recommendation_engine.util.TextUtils.normalizeBookTitle(rawTitle));

        List<String> authors = new ArrayList<>();
        if (bookDataNode.has("authors")) {
            for (JsonNode authorNode : bookDataNode.path("authors")) {
                String rawAuthor = authorNode.path("name").asText(null);
                // Normalize author name to proper case
                authors.add(com.williamcallahan.book_recommendation_engine.util.TextUtils.normalizeAuthorName(rawAuthor));
            }
        }
        book.setAuthors(authors.isEmpty() ? null : authors);

        String publisher = null;
        if (bookDataNode.has("publishers") && bookDataNode.path("publishers").isArray() && bookDataNode.path("publishers").size() > 0) {
            publisher = bookDataNode.path("publishers").get(0).path("name").asText(null);
        }
        book.setPublisher(publisher);

        String publishedDateStr = bookDataNode.path("publish_date").asText(null);
        if (publishedDateStr != null && !publishedDateStr.trim().isEmpty()) {
            Date parsedDate = DateParsingUtils.parseFlexibleDate(publishedDateStr);
            if (parsedDate == null) {
                log.warn("Could not parse OpenLibrary publish_date '{}' for ISBN {} via DateParsingUtils.", publishedDateStr, originalIsbn);
            }
            book.setPublishedDate(parsedDate);
        }


        // Description is often not in the 'data' view, might be in 'details' view.
        // book.setDescription(bookDataNode.path("description").path("value").asText(null));

        String isbn10 = null;
        String isbn13 = null;
        if (bookDataNode.has("identifiers")) {
            JsonNode identifiers = bookDataNode.path("identifiers");
            if (identifiers.has("isbn_10") && identifiers.path("isbn_10").isArray() && identifiers.path("isbn_10").size() > 0) {
                isbn10 = identifiers.path("isbn_10").get(0).asText(null);
            }
            if (identifiers.has("isbn_13") && identifiers.path("isbn_13").isArray() && identifiers.path("isbn_13").size() > 0) {
                isbn13 = identifiers.path("isbn_13").get(0).asText(null);
            }
        }
         // Fallback to original ISBN if not found in identifiers
        if (isbn10 == null && originalIsbn != null && originalIsbn.length() == 10) {
            isbn10 = originalIsbn;
        }
        if (isbn13 == null && originalIsbn != null && originalIsbn.length() == 13) {
            isbn13 = originalIsbn;
        }
        book.setIsbn10(isbn10);
        book.setIsbn13(isbn13);

        Integer pageCount = bookDataNode.has("number_of_pages") ? bookDataNode.path("number_of_pages").asInt(0) : null;
        if (pageCount != null && pageCount == 0) { // Distinguish missing from actual zero
             book.setPageCount(null);
        } else {
            book.setPageCount(pageCount);
        }


        List<String> categories = new ArrayList<>();
        if (bookDataNode.has("subjects")) {
            for (JsonNode subjectNode : bookDataNode.path("subjects")) {
                categories.add(subjectNode.path("name").asText(subjectNode.asText(null))); // Prefer name if subject is an object
            }
        }
        book.setCategories(categories.isEmpty() ? null : categories);

        String thumbnailUrl = null;
        String smallThumbnailUrl = null;
        if (bookDataNode.has("cover")) {
            JsonNode coverNode = bookDataNode.path("cover");
            thumbnailUrl = coverNode.path("medium").asText(null); // Typically used as primary cover
            smallThumbnailUrl = coverNode.path("small").asText(null);
        }
        book.setExternalImageUrl(thumbnailUrl); // original external source
        // s3ImagePath remains null here; S3 upload may occur later in orchestration
        // Keep small thumbnail as fallback external URL if main is null
        if (book.getExternalImageUrl() == null) {
            book.setExternalImageUrl(smallThumbnailUrl);
        }

        book.setInfoLink(bookDataNode.path("url").asText(null));
        book.setRawJsonResponse(bookDataNode.toString());

        // Fields not typically in OpenLibrary 'data' view or not parsed yet:
        // book.setGoogleBookId(null); // This is an OpenLibrary sourced book
        // book.setDescription(null); // Handled above, usually not in this view
        // book.setAverageRating(null);
        // book.setRatingsCount(null);
        // book.setLanguage(null);
        // book.setPreviewLink(null);
        // book.setPurchaseLink(null);
        // book.setListPrice(null);
        // book.setCurrencyCode(null);
        // book.setWebReaderLink(null);
        // book.setPdfAvailable(null);
        // book.setEpubAvailable(null);
        // book.setCoverImageWidth(null);
        // book.setCoverImageHeight(null);
        // book.setIsCoverHighResolution(null);
        // book.setCoverImages(null); // This is a complex object, might need separate mapping
        // book.setOtherEditions(null);
        // book.setAsin(null);

        return book;
    }

    private Book parseOpenLibrarySearchDoc(JsonNode docNode) {
        if (docNode == null || docNode.isMissingNode() || !docNode.isObject()) {
            return null;
        }

        Book book = new Book();
        String key = docNode.path("key").asText(null);
        if (key != null && key.contains("/")) {
            book.setId(key.substring(key.lastIndexOf('/') + 1)); // Use OLID as ID
        }

        // Normalize title to proper case
        String rawTitle = docNode.path("title").asText(null);
        book.setTitle(com.williamcallahan.book_recommendation_engine.util.TextUtils.normalizeBookTitle(rawTitle));

        List<String> authors = new ArrayList<>();
        if (docNode.has("author_name") && docNode.get("author_name").isArray()) {
            for (JsonNode authorNameNode : docNode.get("author_name")) {
                String rawAuthor = authorNameNode.asText(null);
                // Normalize author name to proper case
                authors.add(com.williamcallahan.book_recommendation_engine.util.TextUtils.normalizeAuthorName(rawAuthor));
            }
        }
        book.setAuthors(authors.isEmpty() ? null : authors);

        // Search results often don't have detailed publisher/date directly
        // book.setPublisher(docNode.path("publisher").get(0).asText(null)); // Example if available
        // book.setPublishedDate(parsePublishedDate(docNode.path("first_publish_year").asText(null))); // Example

        List<String> isbnList = new ArrayList<>();
        if (docNode.has("isbn") && docNode.get("isbn").isArray()) {
            for (JsonNode isbnNode : docNode.get("isbn")) {
                String currentIsbn = isbnNode.asText(null);
                if (currentIsbn != null) {
                    isbnList.add(currentIsbn);
                    if (currentIsbn.length() == 13 && book.getIsbn13() == null) {
                        book.setIsbn13(currentIsbn);
                    } else if (currentIsbn.length() == 10 && book.getIsbn10() == null) {
                        book.setIsbn10(currentIsbn);
                    }
                }
            }
        }
        // If specific ISBNs not set, try to pick first valid ones from the list
        if (book.getIsbn13() == null && book.getIsbn10() == null && !isbnList.isEmpty()) {
            for (String currentIsbn : isbnList) {
                 if (currentIsbn.length() == 13) { book.setIsbn13(currentIsbn); break; }
            }
            for (String currentIsbn : isbnList) {
                 if (currentIsbn.length() == 10) { book.setIsbn10(currentIsbn); break; }
            }
        }


        // Cover ID might be present, construct URL if needed
        // Example: book.setCoverImageUrl("http://covers.openlibrary.org/b/id/" + docNode.path("cover_i").asText(null) + "-M.jpg");

        // Raw JSON for search result item might be useful for debugging or further processing
        book.setRawJsonResponse(docNode.toString());
        
        // Only return book if it has at least an ID and title
        if (book.getId() != null && book.getTitle() != null) {
            return book;
        }
        return null;
    }
}
