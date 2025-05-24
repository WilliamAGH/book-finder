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
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Service
public class OpenLibraryBookDataService {

    private static final Logger logger = LoggerFactory.getLogger(OpenLibraryBookDataService.class);
    private final WebClient webClient;
    private final Environment environment;

    public OpenLibraryBookDataService(WebClient.Builder webClientBuilder,
                                   @Value("${OPENLIBRARY_API_URL:https://openlibrary.org}") String openLibraryApiUrl,
                                   Environment environment) {
        this.webClient = webClientBuilder.baseUrl(openLibraryApiUrl).build();
        this.environment = environment;
    }

    /**
     * Fetches book data from OpenLibrary by ISBN
     *
     * @param isbn The ISBN of the book
     * @return A Mono emitting the Book object if found, or Mono.empty() otherwise
     */
    @RateLimiter(name = "openLibraryRateLimiter")
    @CircuitBreaker(name = "openLibraryDataService", fallbackMethod = "fetchBookFallback")
    @TimeLimiter(name = "openLibraryDataService")
    public CompletableFuture<Book> fetchBookByIsbn(String isbn) {
        if (isbn == null || isbn.trim().isEmpty()) {
            logger.warn("ISBN is null or empty. Cannot fetch book from OpenLibrary.");
            return CompletableFuture.completedFuture(null);
        }
        if (environment.acceptsProfiles(Profiles.of("dev"))) {
            logger.info("[ASYNC_CF_VIA_WEBC] Attempting to fetch book data from OpenLibrary for ISBN: {}", isbn);
        } else {
            logger.info("Attempting to fetch book data from OpenLibrary for ISBN: {}", isbn);
        }

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
                .flatMap(responseNode -> {
                    JsonNode bookDataNode = responseNode.path(bibkey);
                    if (bookDataNode.isMissingNode() || bookDataNode.isEmpty()) {
                        logger.warn("No data found in OpenLibrary response for bibkey: {}", bibkey);
                        return Mono.empty();
                    }
                    Book book = parseOpenLibraryBook(bookDataNode, isbn);
                    return book != null ? Mono.just(book) : Mono.empty();
                })
                .doOnError(e -> logger.error("Error fetching book by ISBN {} from OpenLibrary: {}", isbn, e.getMessage()))
                .onErrorResume(e -> { // Ensure fallback behavior on error before circuit breaker
                    logger.warn("Error during OpenLibrary fetch for ISBN {}, returning empty. Error: {}", isbn, e.getMessage());
                    return Mono.empty();
                })
                .toFuture();
    }

    /**
     * Searches for books on OpenLibrary by title (and optionally author)
     *
     * @param title The title of the book
     * @return A Flux emitting Book objects matching the search query
     */
    @RateLimiter(name = "openLibraryRateLimiter")
    @CircuitBreaker(name = "openLibraryDataService", fallbackMethod = "searchBooksFallback")
    public Flux<Book> searchBooksByTitle(String title) {
        if (title == null || title.trim().isEmpty()) {
            logger.warn("Title is null or empty. Cannot search books on OpenLibrary.");
            return Flux.empty();
        }
        if (environment.acceptsProfiles(Profiles.of("dev"))) {
            logger.info("[ASYNC_REACTIVE_STREAM] Attempting to search books from OpenLibrary for title: {}", title);
        } else {
            logger.info("Attempting to search books from OpenLibrary for title: {}", title);
        }
        // Placeholder:
        // return webClient.get().uri("/search.json?q=" + title)
        // .retrieve()
        // .bodyToFlux(Book.class) // This will need proper mapping and extraction from search results
        // .doOnError(e -> logger.error("Error searching books by title '{}' from OpenLibrary: {}", title, e.getMessage()));
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
                .flatMapMany(responseNode -> {
                    if (!responseNode.has("docs") || !responseNode.get("docs").isArray()) {
                        return Flux.empty();
                    }
                    return Flux.fromIterable(responseNode.get("docs"))
                               .map(this::parseOpenLibrarySearchDoc)
                               .filter(Objects::nonNull);
                })
                .doOnError(e -> logger.error("Error searching books by title '{}' from OpenLibrary: {}", title, e.getMessage()))
                .onErrorResume(e -> {
                     logger.warn("Error during OpenLibrary search for title '{}', returning empty Flux. Error: {}", title, e.getMessage());
                     return Flux.empty();
                });
    }

    // --- Fallback Methods ---

    public CompletableFuture<Book> fetchBookFallback(String isbn, Throwable t) {
        logger.warn("OpenLibraryBookDataService.fetchBookByIsbn fallback triggered for ISBN: {}. Error: {}", isbn, t.getMessage());
        return CompletableFuture.completedFuture(null);
    }

    public Flux<Book> searchBooksFallback(String title, Throwable t) {
        logger.warn("OpenLibraryBookDataService.searchBooksByTitle fallback triggered for title: '{}'. Error: {}", title, t.getMessage());
        return Flux.empty();
    }

    private Book parseOpenLibraryBook(JsonNode bookDataNode, String originalIsbn) {
        if (bookDataNode == null || bookDataNode.isMissingNode() || !bookDataNode.isObject()) {
            logger.warn("Book data node is null, missing, or not an object for ISBN: {}", originalIsbn);
            return null;
        }

        Book book = new Book();

        String olid = bookDataNode.path("key").asText(null);
        if (olid != null && olid.contains("/")) {
            olid = olid.substring(olid.lastIndexOf('/') + 1);
        }
        book.setId(olid);

        book.setTitle(bookDataNode.path("title").asText(null));

        List<String> authors = new ArrayList<>();
        if (bookDataNode.has("authors")) {
            for (JsonNode authorNode : bookDataNode.path("authors")) {
                authors.add(authorNode.path("name").asText(null));
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
            Date parsedDate = null;
            List<SimpleDateFormat> dateFormats = Arrays.asList(
                    new SimpleDateFormat("MMMM d, yyyy"), // "January 1, 2020"
                    new SimpleDateFormat("dd MMMM yyyy"),  // "15 January 2020"
                    new SimpleDateFormat("yyyy-MM-dd"),    // "2020-01-15"
                    new SimpleDateFormat("MM/dd/yyyy"),    // "01/15/2020"
                    new SimpleDateFormat("yyyy/MM/dd"),    // "2020/01/15"
                    new SimpleDateFormat("MMMM yyyy"),     // "January 2020"
                    new SimpleDateFormat("yyyy-MM"),       // "2020-01"
                    new SimpleDateFormat("yyyy")           // "2020"
            );

            for (SimpleDateFormat sdf : dateFormats) {
                try {
                    sdf.setLenient(false); // Be strict about matching the format
                    parsedDate = sdf.parse(publishedDateStr);
                    if (parsedDate != null) {
                        break; // Successfully parsed
                    }
                } catch (ParseException e) {
                    // Try next format
                }
            }

            if (parsedDate == null) {
                logger.warn("Could not parse OpenLibrary publish_date '{}' for ISBN {} with any of the attempted formats.", publishedDateStr, originalIsbn);
            } else {
                // Convert Date to LocalDate
                book.setPublishedDate(parsedDate.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDate());
            }
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
        book.setCoverImageUrl(thumbnailUrl);
        book.setImageUrl(smallThumbnailUrl); // Or some other logic for alternative image

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

        book.setTitle(docNode.path("title").asText(null));

        List<String> authors = new ArrayList<>();
        if (docNode.has("author_name") && docNode.get("author_name").isArray()) {
            for (JsonNode authorNameNode : docNode.get("author_name")) {
                authors.add(authorNameNode.asText(null));
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
