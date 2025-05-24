/**
 * REST controller for handling book-related API requests
 *
 * @author William Callahan
 *
 * Features:
 * - Provides reactive endpoints for book searches, details, and recommendations
 * - Supports search by general query, title, author, and ISBN
 * - Offers optimized cover image URL resolution for API responses
 * - Handles image resolution preferences and cover source options
 * - Tracks recently viewed books for personalized recommendations
 * - Implements similar book recommendations based on source books
 */

package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookCacheFacadeService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
import com.williamcallahan.book_recommendation_engine.service.S3RetryService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.concurrent.CompletableFuture;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.CompletionException;
import reactor.core.scheduler.Schedulers;

@RestController
@RequestMapping("/api/books")
public class BookController {
    private static final Logger logger = LoggerFactory.getLogger(BookController.class);
    
    private final BookCacheFacadeService bookCacheFacadeService;
    private final RecentlyViewedService recentlyViewedService;
    private final RecommendationService recommendationService;
    private final BookImageOrchestrationService bookImageOrchestrationService;
    private final S3RetryService s3RetryService;

    private boolean isYearFilteringEnabled;
    
    /**
     * Constructs the BookController with all required services
     *
     * @param bookCacheFacadeService Service for caching and retrieving book data
     * @param recentlyViewedService Service for tracking recently viewed books
     * @param recommendationService Service for generating book recommendations
     * @param bookImageOrchestrationService Service for book cover image processing
     * @param s3RetryService Service for S3 operations with retries
     */
    public BookController(BookCacheFacadeService bookCacheFacadeService,
                          RecentlyViewedService recentlyViewedService,
                          RecommendationService recommendationService,
                          BookImageOrchestrationService bookImageOrchestrationService,
                          S3RetryService s3RetryService) {
        this.bookCacheFacadeService = bookCacheFacadeService;
        this.recentlyViewedService = recentlyViewedService;
        this.recommendationService = recommendationService;
        this.bookImageOrchestrationService = bookImageOrchestrationService;
        this.s3RetryService = s3RetryService;
    }
    
    /**
     * Search books by keyword with pagination and filtering options
     *
     * @param query Search query string
     * @param startIndex Start index for pagination
     * @param maxResults Maximum results to return
     * @param coverSource Preferred cover image source
     * @param resolution Preferred image resolution
     * @param publishedYear Filter by publication year
     * @return Mono with search results and pagination details
     */
    @GetMapping("/search")
    public Mono<ResponseEntity<Map<String, Object>>> searchBooks(
            @RequestParam String query,
            @RequestParam(required = false, defaultValue = "0") int startIndex,
            @RequestParam(required = false, defaultValue = "10") int maxResults,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution,
            @RequestParam(required = false) Integer publishedYear) {
        
        logger.info("Searching books with query: {}, startIndex: {}, maxResults: {}, coverSource: {}, resolution: {}, publishedYear: {}", 
                query, startIndex, maxResults, coverSource, resolution, publishedYear);
        
        final CoverImageSource effectivelyFinalPreferredSource = 
            getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = 
            getImageResolutionPreferenceFromString(resolution);
        
        String finalQueryForProcessing = query;
        Integer finalEffectivePublishedYear = publishedYear; // Start with the request parameter

        if (!isYearFilteringEnabled) {
            finalEffectivePublishedYear = null; // Force null if feature is disabled
            logger.info("Year filtering is disabled. Ignoring year parameter and query parsing for year.");
            // finalQueryForProcessing remains the original query
        } else {
            // Year filtering is enabled.
            // If publishedYear param was not provided, try to parse from query.
            if (finalEffectivePublishedYear == null && finalQueryForProcessing != null && finalQueryForProcessing.matches(".*\\b(19|20)\\d{2}\\b.*")) {
                Matcher yearMatcher = Pattern.compile("\\b(19|20)\\d{2}\\b").matcher(finalQueryForProcessing);
                if (yearMatcher.find()) {
                    String yearStr = yearMatcher.group();
                    try {
                        Integer yearFromQueryVal = Integer.parseInt(yearStr);
                        // Only use yearFromQuery if publishedYear parameter was not set
                        if (publishedYear == null) { 
                            finalEffectivePublishedYear = yearFromQueryVal;
                            finalQueryForProcessing = finalQueryForProcessing.replaceAll("\\b" + yearStr + "\\b", "").trim();
                            logger.info("Extracted year {} from query. Modified query: '{}'", finalEffectivePublishedYear, finalQueryForProcessing);
                        }
                    } catch (NumberFormatException e) {
                        logger.warn("Failed to parse year from query: {}", finalQueryForProcessing);
                        // finalEffectivePublishedYear remains as it was (null if publishedYear was also null)
                    }
                }
            }
            
            if (finalEffectivePublishedYear != null) {
                logger.info("Searching with effective published year filter: {}", finalEffectivePublishedYear);
            }
        }
        
        // If finalQueryForProcessing is empty after potential year extraction, use a wildcard search
        if (finalQueryForProcessing == null || finalQueryForProcessing.isEmpty()) {
            finalQueryForProcessing = "*";
            logger.info("Query was empty after year processing, using wildcard search '*' for API call.");
        }
        
        // Create effectively final versions for use in lambdas
        final String actualQueryForApi = finalQueryForProcessing;
        final Integer actualPublishedYearForApi = finalEffectivePublishedYear;

        return bookCacheFacadeService.searchBooksReactive(actualQueryForApi, startIndex, maxResults, actualPublishedYearForApi, null, null)
            .flatMap(paginatedBooks -> {
                List<Book> currentPaginatedBooks = (paginatedBooks == null) ? Collections.emptyList() : paginatedBooks;

                if (currentPaginatedBooks.isEmpty()) {
                    Map<String, Object> response = new HashMap<>();
                    response.put("resultsInPage", 0);
                    response.put("results", Collections.emptyList());
                    response.put("count", 0);
                    response.put("startIndex", startIndex);
                    response.put("query", query); // Original query for response
                    return Mono.just(ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(response));
                }

                return Flux.fromIterable(currentPaginatedBooks)
                    .filter(book -> { // Year filtering moved before enrichment
                        if (actualPublishedYearForApi == null) {
                            return true; 
                        }
                        if (book != null && book.getPublishedDate() != null) {
                            Calendar calendar = Calendar.getInstance();
                            calendar.setTime(book.getPublishedDate());
                            int bookYear = calendar.get(Calendar.YEAR);
                            return bookYear == actualPublishedYearForApi;
                        }
                        return false;
                    })
                    .flatMap(book -> {
                        if (book == null) { // Should ideally not happen if list is pre-filtered for nulls
                            return Mono.just(book); 
                        }
                        return Mono.fromFuture(bookImageOrchestrationService.getBestCoverUrlAsync(book, effectivelyFinalPreferredSource, effectivelyFinalResolutionPreference))
                            .map(processedBookFromService -> processedBookFromService)
                            .onErrorResume(e -> {
                                logger.warn("Error in async cover processing for book ID {}: {}. Book may have defaults.", book.getId(), e.getMessage());
                                if (book.getCoverImages() == null) {
                                    String currentCoverUrl = book.getCoverImageUrl() != null ? book.getCoverImageUrl() : "/images/placeholder-book-cover.svg";
                                    book.setCoverImages(new com.williamcallahan.book_recommendation_engine.types.CoverImages(currentCoverUrl, currentCoverUrl));
                                }
                                if (book.getCoverImageUrl() == null) {
                                    book.setCoverImageUrl("/images/placeholder-book-cover.svg");
                                }
                                return Mono.just(book);
                            });
                    })
                    .collectList()
                    .map(filteredAndEnrichedBooks -> { // Renamed from processedBooks
                        // Year filtering is already done.
                                
                        // Include query-specific metadata for search results
                        for (Book book : filteredAndEnrichedBooks) { // Use filteredAndEnrichedBooks
                            boolean qualifiersUpdated = false;
                            
                            if (book.getQualifiers() != null && book.hasQualifier("searchQuery")) {
                                // Book already has query information stored
                                // We could potentially add visual markers or badges in UI based on qualifiers
                                logger.debug("Book {} has stored query qualifiers: {}", book.getId(), 
                                    book.getQualifiers().keySet());
                            } else {
                                // If no qualifiers yet (maybe from a previous cache), add the current query
                                book.addQualifier("searchQuery", query);
                                qualifiersUpdated = true;
                            }
                            
                            // Extract potential qualifiers from current query
                            if (query.toLowerCase().contains("new york times bestseller") || 
                                query.toLowerCase().contains("nyt bestseller")) {
                                if (!book.hasQualifier("nytBestseller")) {
                                    book.addQualifier("nytBestseller", true);
                                    qualifiersUpdated = true;
                                }
                            }
                            
                            // More qualifier extractions could be added here
                            
                            // If qualifiers were updated, persist them to S3
                            if (qualifiersUpdated) {
                                Schedulers.boundedElastic().schedule(() -> {
                                    try {
                                        s3RetryService.updateBookJsonWithRetry(book)
                                            .whenComplete((result, ex) -> {
                                                if (ex != null) {
                                                    Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                                                    logger.warn("Failed to update S3 with new qualifiers for book {}: {}", 
                                                        book.getId(), cause.getMessage());
                                                } else {
                                                    logger.info("Successfully updated S3 with new qualifiers for book {}", 
                                                        book.getId());
                                                }
                                            }).join(); // Ensure completion or exception propagation
                                    } catch (CompletionException ce) {
                                        logger.error("CompletionException during S3 update for book {}: {}", 
                                            book.getId(), ce.getCause() != null ? ce.getCause().getMessage() : ce.getMessage());
                                    } catch (Exception e) {
                                        logger.error("Error during S3 update for book {}: {}", 
                                            book.getId(), e.getMessage());
                                    }
                                });
                            }
                        }

                        // Create response with metadata
                        Map<String, Object> response = new HashMap<>();
                        response.put("query", query);
                        response.put("resultsInPage", filteredAndEnrichedBooks.size()); // Use filteredAndEnrichedBooks
                        response.put("results", filteredAndEnrichedBooks); // Use filteredAndEnrichedBooks
                        response.put("count", filteredAndEnrichedBooks.size()); // Use filteredAndEnrichedBooks
                        response.put("totalAvailableResults", filteredAndEnrichedBooks.size()); // This is an estimate for client pagination
                        
                        // Save any query-related metadata we parsed
                        Map<String, Object> metadata = new HashMap<>();
                        if (actualPublishedYearForApi != null) { // Use the effectively final variable
                            metadata.put("publishedYear", actualPublishedYearForApi);
                        }
                        response.put("metadata", metadata);
                        
                        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(response);
                    });
            })
            .onErrorResume(e -> {
                logger.error("Error searching books for query '{}': {}", query, e.getMessage(), e);
                if (e instanceof ResponseStatusException rse) {
                    return Mono.error(rse); 
                }
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occurred while searching books", e));
            });
    }
    
    /**
     * Converts string to CoverImageSource enum
     *
     * @param source String representation of cover image source
     * @return CoverImageSource enum value or ANY if invalid
     */
    private CoverImageSource getCoverImageSourceFromString(String source) {
        try {
            return CoverImageSource.valueOf(source.toUpperCase());
        } catch (IllegalArgumentException e) {
            return CoverImageSource.ANY;
        }
    }

    /**
     * Converts string to ImageResolutionPreference enum
     *
     * @param resolution String representation of resolution preference
     * @return ImageResolutionPreference enum value or ANY if invalid
     */
    private ImageResolutionPreference getImageResolutionPreferenceFromString(String resolution) {
        try {
            return ImageResolutionPreference.valueOf(resolution.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ImageResolutionPreference.ANY;
        }
    }
    
    /**
     * Search books by title with filtering options
     *
     * @param title Book title to search for
     * @param coverSource Preferred cover image source
     * @param resolution Preferred image resolution
     * @return Mono with search results filtered by title
     */
    @GetMapping("/search/title")
    public Mono<ResponseEntity<Map<String, Object>>> searchBooksByTitle(
            @RequestParam String title,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Searching books by title: {}, coverSource: {}, resolution: {}", title, coverSource, resolution);

        final CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = getImageResolutionPreferenceFromString(resolution);

        // Use BookCacheFacadeService for searching books by title
        return bookCacheFacadeService.searchBooksReactive("intitle:" + title, 0, 40, null, null, null) // Pass null for publishedYear, langCode, orderBy
            .flatMap(books -> {
                List<Book> currentBooks = (books == null) ? Collections.emptyList() : books;
                if (currentBooks.isEmpty()) {
                    Map<String, Object> response = new HashMap<>();
                    response.put("results", Collections.emptyList());
                    response.put("count", 0);
                    response.put("title", title);
                    return Mono.just(ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(response));
                }

                return Flux.fromIterable(currentBooks)
                    .flatMap(book -> {
                        if (book == null) return Mono.just(book);
                        return Mono.fromFuture(bookImageOrchestrationService.getBestCoverUrlAsync(book, effectivelyFinalPreferredSource, effectivelyFinalResolutionPreference))
                            .map(processedBookFromService -> processedBookFromService)
                            .onErrorResume(e -> {
                                logger.warn("Error in async cover processing for book ID {}: {}. Book may have defaults.", book.getId(), e.getMessage());
                                if (book.getCoverImages() == null) {
                                    String currentCoverUrl = book.getCoverImageUrl() != null ? book.getCoverImageUrl() : "/images/placeholder-book-cover.svg";
                                    book.setCoverImages(new com.williamcallahan.book_recommendation_engine.types.CoverImages(currentCoverUrl, currentCoverUrl));
                                }
                                if (book.getCoverImageUrl() == null) {
                                    book.setCoverImageUrl("/images/placeholder-book-cover.svg");
                                }
                                return Mono.just(book);
                            });
                    })
                    .collectList()
                    .map(processedBooks -> {
                        // Process books for qualifiers and update S3 if needed
                        for (Book book : processedBooks) {
                            updateBookQualifiersAsync(book, "searchQuery", "intitle:" + title);
                        }
                        
                        Map<String, Object> response = new HashMap<>();
                        response.put("results", processedBooks);
                        response.put("count", processedBooks.size());
                        response.put("title", title);
                        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(response);
                    });
            })
            .onErrorResume(e -> {
                logger.error("Error searching books by title '{}': {}", title, e.getMessage(), e);
                if (e instanceof ResponseStatusException rse) return Mono.error(rse);
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occurred while searching books by title", e));
            });
    }
    
    /**
     * Search books by author with filtering options
     *
     * @param author Author name to search for
     * @param coverSource Preferred cover image source
     * @param resolution Preferred image resolution
     * @return Mono with search results filtered by author
     */
    @GetMapping("/search/author")
    public Mono<ResponseEntity<Map<String, Object>>> searchBooksByAuthor(
            @RequestParam String author,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Searching books by author: {}, coverSource: {}, resolution: {}", author, coverSource, resolution);

        final CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = getImageResolutionPreferenceFromString(resolution);

        // Use BookCacheFacadeService for searching books by author
        return bookCacheFacadeService.searchBooksReactive("inauthor:" + author, 0, 40, null, null, null) // Pass null for publishedYear, langCode, orderBy
            .flatMap(books -> {
                List<Book> currentBooks = (books == null) ? Collections.emptyList() : books;
                if (currentBooks.isEmpty()) {
                    Map<String, Object> response = new HashMap<>();
                    response.put("results", Collections.emptyList());
                    response.put("count", 0);
                    response.put("author", author);
                    return Mono.just(ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(response));
                }

                return Flux.fromIterable(currentBooks)
                    .flatMap(book -> {
                        if (book == null) return Mono.just(book);
                        return Mono.fromFuture(bookImageOrchestrationService.getBestCoverUrlAsync(book, effectivelyFinalPreferredSource, effectivelyFinalResolutionPreference))
                            .map(processedBookFromService -> processedBookFromService)
                            .onErrorResume(e -> {
                                logger.warn("Error in async cover processing for book ID {}: {}. Book may have defaults.", book.getId(), e.getMessage());
                                if (book.getCoverImages() == null) {
                                    String currentCoverUrl = book.getCoverImageUrl() != null ? book.getCoverImageUrl() : "/images/placeholder-book-cover.svg";
                                    book.setCoverImages(new com.williamcallahan.book_recommendation_engine.types.CoverImages(currentCoverUrl, currentCoverUrl));
                                }
                                if (book.getCoverImageUrl() == null) {
                                    book.setCoverImageUrl("/images/placeholder-book-cover.svg");
                                }
                                return Mono.just(book);
                            });
                    })
                    .collectList()
                    .map(processedBooks -> {
                        // Process books for qualifiers and update S3 if needed
                        for (Book book : processedBooks) {
                            updateBookQualifiersAsync(book, "searchQuery", "inauthor:" + author);
                        }
                        
                        Map<String, Object> response = new HashMap<>();
                        response.put("results", processedBooks);
                        response.put("count", processedBooks.size());
                        response.put("author", author);
                        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(response);
                    });
            })
            .onErrorResume(e -> {
                logger.error("Error searching books by author '{}': {}", author, e.getMessage(), e);
                if (e instanceof ResponseStatusException rse) return Mono.error(rse);
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occurred while searching books by author", e));
            });
    }
    
    /**
     * Search books by ISBN with filtering options
     *
     * @param isbn Book ISBN number
     * @param coverSource Preferred cover image source
     * @param resolution Preferred image resolution
     * @return Mono with search results for the specific ISBN
     */
    @GetMapping("/search/isbn")
    public Mono<ResponseEntity<Map<String, Object>>> searchBooksByISBN(
            @RequestParam String isbn,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Searching books by ISBN: {}, coverSource: {}, resolution: {}", isbn, coverSource, resolution);

        final CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = getImageResolutionPreferenceFromString(resolution);

        // Use BookCacheFacadeService for searching books by ISBN
        return bookCacheFacadeService.getBooksByIsbnReactive(isbn)
            .flatMap(books -> {
                List<Book> currentBooks = (books == null) ? Collections.emptyList() : books;
                if (currentBooks.isEmpty()) {
                    Map<String, Object> response = new HashMap<>();
                    response.put("results", Collections.emptyList());
                    response.put("count", 0);
                    response.put("isbn", isbn);
                    return Mono.just(ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(response));
                }

                return Flux.fromIterable(currentBooks)
                    .flatMap(book -> {
                        if (book == null) return Mono.just(book);
                        return Mono.fromFuture(bookImageOrchestrationService.getBestCoverUrlAsync(book, effectivelyFinalPreferredSource, effectivelyFinalResolutionPreference))
                            .map(processedBookFromService -> processedBookFromService)
                            .onErrorResume(e -> {
                                logger.warn("Error in async cover processing for book ID {}: {}. Book may have defaults.", book.getId(), e.getMessage());
                                if (book.getCoverImages() == null) {
                                    String currentCoverUrl = book.getCoverImageUrl() != null ? book.getCoverImageUrl() : "/images/placeholder-book-cover.svg";
                                    book.setCoverImages(new com.williamcallahan.book_recommendation_engine.types.CoverImages(currentCoverUrl, currentCoverUrl));
                                }
                                if (book.getCoverImageUrl() == null) {
                                    book.setCoverImageUrl("/images/placeholder-book-cover.svg");
                                }
                                return Mono.just(book);
                            });
                    })
                    .collectList()
                    .map(processedBooks -> {
                        // Process books for qualifiers and update S3 if needed
                        for (Book book : processedBooks) {
                            updateBookQualifiersAsync(book, "searchQuery", "isbn:" + isbn);
                        }
                        
                        Map<String, Object> response = new HashMap<>();
                        response.put("results", processedBooks);
                        response.put("count", processedBooks.size());
                        response.put("isbn", isbn);
                        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(response);
                    });
            })
            .onErrorResume(e -> {
                logger.error("Error searching books by ISBN '{}': {}", isbn, e.getMessage(), e);
                if (e instanceof ResponseStatusException rse) return Mono.error(rse);
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occurred while searching books by ISBN", e));
            });
    }
    
    /**
     * Get book details by ID with filtering options
     *
     * @param id Book identifier to retrieve
     * @param coverSource Preferred cover image source
     * @param resolution Preferred image resolution
     * @return Mono with book details or not found status
     */
    @GetMapping("/{id}")
    public Mono<ResponseEntity<Object>> getBookById(
            @PathVariable String id,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Getting book by ID: {}, coverSource: {}, resolution: {}", id, coverSource, resolution);

        final CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = getImageResolutionPreferenceFromString(resolution);

        // Use BookCacheFacadeService to get book by ID
        return bookCacheFacadeService.getBookByIdReactive(id)
            // If not found by volume ID, fallback to ISBN-based search
            .switchIfEmpty(
                bookCacheFacadeService.getBooksByIsbnReactive(id)
                    .filter(list -> list != null && !list.isEmpty())
                    .map(list -> list.get(0))
                    .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Book not found with ID or ISBN: " + id, null)))
            )
            .flatMap(book -> // This flatMap only executes if book was found
                Mono.fromFuture(bookImageOrchestrationService.getBestCoverUrlAsync(book, effectivelyFinalPreferredSource, effectivelyFinalResolutionPreference))
                    .map(processedBookFromService -> processedBookFromService)
                    .onErrorResume(e -> { // Handle errors during cover enrichment
                        logger.warn("Error in async cover processing for book ID {}: {}. Book may have defaults.", book.getId(), e.getMessage());
                        if (book.getCoverImages() == null) {
                            String currentCoverUrl = book.getCoverImageUrl() != null ? book.getCoverImageUrl() : "/images/placeholder-book-cover.svg";
                            book.setCoverImages(new com.williamcallahan.book_recommendation_engine.types.CoverImages(currentCoverUrl, currentCoverUrl));
                        }
                        if (book.getCoverImageUrl() == null) {
                            book.setCoverImageUrl("/images/placeholder-book-cover.svg");
                        }
                        return Mono.just(book); // Return the book with default/existing cover info
                    })
            )
            .doOnSuccess(recentlyViewedService::addToRecentlyViewed) // No null check needed as stream errors out if book not found
            .map(book -> ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body((Object)book)) // Map to ResponseEntity
            // The switchIfEmpty below is now less likely to be the primary "not found" path for the book ID itself,
            // but could still handle cases where bookImageOrchestrationService returns an empty Mono (if its internal onErrorResume was removed)
            // However, with ResponseStatusException, onErrorResume will catch it
            .switchIfEmpty(Mono.just(ResponseEntity.notFound().build())) 
            .onErrorResume(e -> {
                logger.error("Error getting book by ID '{}': {}", id, e.getMessage(), e);
                if (e instanceof ResponseStatusException rse) {
                    // If it's already a ResponseStatusException (like our NOT_FOUND), return it directly
                    // or wrap it in a Mono.error to be handled by Spring's default error handling
                    // For clarity, we can re-throw it if it's one we expect, or map to a generic error
                    if (rse.getStatusCode() == HttpStatus.NOT_FOUND) {
                        Map<String, String> errorResponse = new HashMap<>();
                        errorResponse.put("error", "Not Found");
                        errorResponse.put("message", rse.getReason());
                        return Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .contentType(MediaType.APPLICATION_JSON)
                            .body((Object)errorResponse));
                    }
                    return Mono.error(rse);
                }
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occurred while getting book by ID", e));
            });
    }
    
    /**
     * Handle validation errors for request parameters
     *
     * @param ex IllegalArgumentException from request processing
     * @return ResponseEntity with error details and 400 status
     */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ResponseEntity<Map<String, String>> handleValidationExceptions(IllegalArgumentException ex) {
        Map<String, String> errors = new HashMap<>();
        errors.put("error", ex.getMessage());
        return ResponseEntity.badRequest().body(errors);
    }
    
    /**
     * Get similar books recommendations for a specific book
     *
     * @param id Source book ID to find similar books for
     * @param count Number of recommendations to return
     * @param coverSource Preferred cover image source
     * @param resolution Preferred image resolution
     * @return Mono with similar book recommendations
     */
    @GetMapping("/{id}/similar")
    public Mono<ResponseEntity<Map<String, Object>>> getSimilarBooks(
            @PathVariable String id,
            @RequestParam(required = false, defaultValue = "6") int count,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        
        logger.info("Getting similar books for book ID: {}, count: {}, coverSource: {}, resolution: {}", id, count, coverSource, resolution);

        final CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = getImageResolutionPreferenceFromString(resolution);

        return recommendationService.getSimilarBooks(id, count) 
            .flatMap(similarBooksList -> {
                List<Book> currentSimilarBooks = (similarBooksList == null) ? Collections.emptyList() : similarBooksList;
                if (currentSimilarBooks.isEmpty()) {
                            Map<String, Object> response = new HashMap<>();
                            response.put("results", Collections.emptyList());
                            response.put("count", 0);
                            response.put("sourceBookId", id);
                            return Mono.just(ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(response));
                        }

                        return Flux.fromIterable(currentSimilarBooks)
                            .flatMap(book -> {
                                if (book == null) return Mono.just(book);
                                return Mono.fromFuture(bookImageOrchestrationService.getBestCoverUrlAsync(book, effectivelyFinalPreferredSource, effectivelyFinalResolutionPreference))
                                    .map(processedBookFromService -> processedBookFromService)
                                    .onErrorResume(e -> {
                                        logger.warn("Error in async cover processing for similar book ID {}: {}. Book may have defaults.", book.getId(), e.getMessage());
                                        if (book.getCoverImages() == null) {
                                            String currentCoverUrl = book.getCoverImageUrl() != null ? book.getCoverImageUrl() : "/images/placeholder-book-cover.svg";
                                            book.setCoverImages(new com.williamcallahan.book_recommendation_engine.types.CoverImages(currentCoverUrl, currentCoverUrl));
                                        }
                                        if (book.getCoverImageUrl() == null) {
                                            book.setCoverImageUrl("/images/placeholder-book-cover.svg");
                                        }
                                        return Mono.just(book);
                                    });
                            })
                            .collectList()
                            .map(processedSimilarBooks -> {
                                Map<String, Object> response = new HashMap<>();
                                response.put("results", processedSimilarBooks);
                                response.put("count", processedSimilarBooks.size());
                                response.put("sourceBookId", id);
                                return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(response);
                            });
                    })
            .onErrorResume(e -> {
                logger.error("Error getting similar books for book ID '{}': {}", id, e.getMessage(), e);
                if (e instanceof ResponseStatusException rse) return Mono.error(rse);
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occurred while getting similar books", e));
            });
    }

    /**
     * Updates book qualifiers and persists to S3 asynchronously
     *
     * @param book Book to update qualifiers for
     * @param qualifier Qualifier name
     * @param value Qualifier value
     */
    private void updateBookQualifiersAsync(Book book, String qualifier, String value) {
        boolean qualifiersUpdated = false;
        
        if (!book.hasQualifier(qualifier)) {
            book.addQualifier(qualifier, value);
            qualifiersUpdated = true;
        }
        
        if (qualifiersUpdated) {
            Schedulers.boundedElastic().schedule(() -> {
                try {
                    CompletableFuture<Void> future = s3RetryService.updateBookJsonWithRetry(book);
                    if (future == null) {
                        logger.error("Error during S3 update for book {}: S3RetryService returned null CompletableFuture", book.getId());
                        return;
                    }
                    
                    future.whenComplete((result, ex) -> {
                        if (ex != null) {
                            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                            logger.warn("Failed to update S3 with new qualifiers for book {}: {}", 
                                book.getId(), cause.getMessage());
                        } else {
                            logger.debug("Successfully updated S3 with {} qualifiers for book {}", 
                                qualifier, book.getId());
                        }
                    });
                } catch (CompletionException ce) {
                    logger.error("CompletionException during S3 update for book {}: {}", 
                        book.getId(), ce.getCause() != null ? ce.getCause().getMessage() : ce.getMessage());
                } catch (Exception e) {
                    logger.error("Error during S3 update for book {}: {}", 
                        book.getId(), e.getMessage());
                }
            });
        }
    }

    /**
     * Creates a new book resource
     *
     * @param book Book object to create
     * @return Mono with created book and 201 status or error
     */
    @PostMapping
    public Mono<ResponseEntity<Book>> createBook(@RequestBody Book book) {
        logger.info("Attempting to create book: {}", book.getTitle());
        return Mono.defer(() -> {
            bookCacheFacadeService.cacheBook(book); // This call can throw IllegalArgumentException
            // Assuming book.getId() is populated by cacheBook or by the service call.
            // If not, the URI creation will be problematic.
            if (book.getId() == null) {
                logger.error("Book ID is null after caching for book title '{}'. Cannot create resource without a stable ID.", book.getTitle());
                // Throw an exception to be caught by onErrorResume
                return Mono.error(new IllegalStateException("Book ID was not assigned during persistence. Cannot create resource."));
            }
            // If ID is present, proceed to build the URI:
            URI location = ServletUriComponentsBuilder
                    .fromCurrentRequest()
                    .path("/{id}")
                    .buildAndExpand(book.getId()) // book.getId() is guaranteed non-null here
                    .toUri();
            return Mono.just(ResponseEntity.created(location).body(book));
        })
        .onErrorResume(IllegalArgumentException.class, e -> {
            logger.error("Validation error creating book with title '{}': {}", book.getTitle(), e.getMessage());
            // Consider creating a more informative error response body if desired
            return Mono.just(ResponseEntity.badRequest().build()); 
        })
        .onErrorResume(IllegalStateException.class, e -> { // Specifically catch our new exception
            logger.error("State error creating book with title '{}': {}", book.getTitle(), e.getMessage());
            // Map to a 500 Internal Server Error, or a 400 if it's considered a client-induced state issue
            return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
        })
        .onErrorResume(e -> { // Catch other, non-specified errors
            logger.error("Generic error creating book with title '{}': {}", book.getTitle(), e.getMessage(), e);
            return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
        });
    }
    
    /**
     * Deletes a book resource by ID
     *
     * @param id Book ID to delete
     * @return Mono with empty response if successful
     */
    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<Void>> deleteBook(@PathVariable String id) {
        logger.info("Attempting to delete book with ID: {}", id);
        // Always attempt deletion and return 200 OK, even if book was not found
        return Mono.fromRunnable(() -> bookCacheFacadeService.removeBook(id))
            .then(Mono.just(ResponseEntity.ok().<Void>build()))
            .onErrorResume(IllegalArgumentException.class, e -> {
                logger.error("Validation error deleting book with ID {}: {}", id, e.getMessage());
                return Mono.just(ResponseEntity.badRequest().build());
            })
            .onErrorResume(e -> {
                logger.error("Error deleting book with ID {}: {}", id, e.getMessage(), e);
                return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
            });
    }
    
    /**
     * Updates an existing book resource
     *
     * @param id Book ID to update
     * @param bookUpdate Updated book data
     * @return Mono with updated book if successful
     */
    @PutMapping("/{id}")
    public Mono<ResponseEntity<Book>> updateBook(@PathVariable String id, @RequestBody Book bookUpdate) {
        logger.info("Attempting to update book with ID: {}", id);
        
        // Ensure the ID in the path matches the ID in the book object
        if (bookUpdate.getId() != null && !bookUpdate.getId().equals(id)) {
            return Mono.just(ResponseEntity.badRequest().<Book>build()); // ID mismatch between path and body
        }
        
        // Set the ID from the path
        bookUpdate.setId(id);
        
        return bookCacheFacadeService.getBookByIdReactive(id)
            // If book does not exist, still proceed to update (upsert behavior)
            .defaultIfEmpty(bookUpdate)
            .flatMap(existingBook -> {
                // Update the book in the cache/database
                return Mono.fromCallable(() -> {
                    bookCacheFacadeService.updateBook(bookUpdate);
                    return bookUpdate;
                })
                .map(updatedBook -> ResponseEntity.ok().body(updatedBook));
            })
            .onErrorResume(IllegalArgumentException.class, e -> {
                logger.error("Validation error updating book with ID {}: {}", id, e.getMessage());
                return Mono.just(ResponseEntity.badRequest().<Book>build());
            })
            .onErrorResume(e -> {
                if (e instanceof ResponseStatusException) {
                    return Mono.error(e); // Propagate status exceptions
                }
                logger.error("Error updating book with ID {}: {}", id, e.getMessage(), e);
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, 
                    "Error occurred while updating book", e));
            });
    }
}
