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
import com.williamcallahan.book_recommendation_engine.service.BookCacheService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/books")
public class BookController {
    private static final Logger logger = LoggerFactory.getLogger(BookController.class);
    
    private final BookCacheService bookCacheService; 
    private final RecentlyViewedService recentlyViewedService;
    private final RecommendationService recommendationService;
    private final BookImageOrchestrationService bookImageOrchestrationService;
    
    /**
     * Constructs the BookController with all required services
     *
     * @param bookCacheService Service for caching and retrieving book data from various sources
     * @param recentlyViewedService Service for tracking and managing recently viewed books
     * @param recommendationService Service for generating book recommendations based on various criteria
     * @param bookImageOrchestrationService Service for orchestrating book cover image retrieval and processing
     */
    @Autowired
    public BookController(BookCacheService bookCacheService, 
                          RecentlyViewedService recentlyViewedService,
                          RecommendationService recommendationService,
                          BookImageOrchestrationService bookImageOrchestrationService) {
        this.bookCacheService = bookCacheService; 
        this.recentlyViewedService = recentlyViewedService;
        this.recommendationService = recommendationService;
        this.bookImageOrchestrationService = bookImageOrchestrationService;
    }
    
    /**
     * Search books by keyword with support for pagination, cover source preferences, and image resolution filtering
     * 
     * @param query Search query string to find matching books
     * @param startIndex Start index for pagination (optional, defaults to 0)
     * @param maxResults Maximum number of results to return (optional, defaults to 10)
     * @param coverSource Preferred source for book cover images (optional, defaults to ANY)
     * @param resolution Preferred resolution for book cover images (optional, defaults to ANY)
     * @return Mono containing ResponseEntity with map of search results including pagination details
     */
    @GetMapping("/search")
    public Mono<ResponseEntity<Map<String, Object>>> searchBooks(
            @RequestParam String query,
            @RequestParam(required = false, defaultValue = "0") int startIndex,
            @RequestParam(required = false, defaultValue = "10") int maxResults,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        
        logger.info("Searching books with query: {}, startIndex: {}, maxResults: {}, coverSource: {}, resolution: {}", 
                query, startIndex, maxResults, coverSource, resolution);
        
        final CoverImageSource effectivelyFinalPreferredSource = 
            getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = 
            getImageResolutionPreferenceFromString(resolution);
        return bookCacheService.searchBooksReactive(query, startIndex, maxResults)
            .flatMap(paginatedBooks -> {
                List<Book> currentPaginatedBooks = (paginatedBooks == null) ? Collections.emptyList() : paginatedBooks;
                int totalResultsInPage = currentPaginatedBooks.size(); 

                if (currentPaginatedBooks.isEmpty()) {
                    Map<String, Object> response = new HashMap<>();
                    response.put("resultsInPage", 0);
                    response.put("results", Collections.emptyList());
                    response.put("count", 0);
                    response.put("startIndex", startIndex);
                    response.put("query", query);
                    return Mono.just(ResponseEntity.ok(response));
                }

                return Flux.fromIterable(currentPaginatedBooks)
                    .flatMap(book -> {
                        if (book == null) {
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
                    .map(processedBooks -> {
                        List<Book> finalBooksToReturn = processedBooks;
                        if (effectivelyFinalResolutionPreference == ImageResolutionPreference.HIGH_ONLY) {
                            finalBooksToReturn = processedBooks.stream()
                                .filter(b -> b != null && b.getIsCoverHighResolution() != null && b.getIsCoverHighResolution())
                                .collect(Collectors.toList());
                            logger.info("Filtered paginated list for HIGH_ONLY, new count: {}", finalBooksToReturn.size());
                        } else if (effectivelyFinalResolutionPreference == ImageResolutionPreference.HIGH_FIRST) {
                            List<Book> sortableBooks = new ArrayList<>(processedBooks.stream().filter(java.util.Objects::nonNull).toList());
                            sortableBooks.sort(Comparator.comparing((Book b) -> b.getIsCoverHighResolution() != null && b.getIsCoverHighResolution(), Comparator.reverseOrder()));
                            finalBooksToReturn = sortableBooks;
                            logger.info("Sorted paginated list for HIGH_FIRST");
                        }

                        Map<String, Object> response = new HashMap<>();
                        response.put("resultsInPage", totalResultsInPage); 
                        response.put("results", finalBooksToReturn);
                        response.put("count", finalBooksToReturn.size());
                        response.put("startIndex", startIndex);
                        response.put("query", query);
                        return ResponseEntity.ok(response);
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
     * Converts a string representation of cover image source to the corresponding enum value
     * Safely handles invalid values by returning the default CoverImageSource.ANY
     *
     * @param source String representation of a cover image source
     * @return The corresponding CoverImageSource enum value, or ANY if not valid
     */
    private CoverImageSource getCoverImageSourceFromString(String source) {
        try {
            return CoverImageSource.valueOf(source.toUpperCase());
        } catch (IllegalArgumentException e) {
            return CoverImageSource.ANY;
        }
    }

    /**
     * Converts a string representation of image resolution preference to the corresponding enum value
     * Safely handles invalid values by returning the default ImageResolutionPreference.ANY
     *
     * @param resolution String representation of an image resolution preference
     * @return The corresponding ImageResolutionPreference enum value, or ANY if not valid
     */
    private ImageResolutionPreference getImageResolutionPreferenceFromString(String resolution) {
        try {
            return ImageResolutionPreference.valueOf(resolution.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ImageResolutionPreference.ANY;
        }
    }
    
    /**
     * Search books by title with support for cover source preferences and image resolution filtering
     * Uses "intitle:" advanced search operator to find books with matching titles
     * 
     * @param title Book title to search for
     * @param coverSource Preferred source for book cover images (optional, defaults to ANY)
     * @param resolution Preferred resolution for book cover images (optional, defaults to ANY)
     * @return Mono containing ResponseEntity with map of search results filtered by title
     */
    @GetMapping("/search/title")
    public Mono<ResponseEntity<Map<String, Object>>> searchBooksByTitle(
            @RequestParam String title,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Searching books by title: {}, coverSource: {}, resolution: {}", title, coverSource, resolution);

        final CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = getImageResolutionPreferenceFromString(resolution);

        // Use BookCacheService for searching books by title
        return bookCacheService.searchBooksReactive("intitle:" + title, 0, 40) // Assuming max 40 for title search, adjust as needed
            .flatMap(books -> {
                List<Book> currentBooks = (books == null) ? Collections.emptyList() : books;
                if (currentBooks.isEmpty()) {
                    Map<String, Object> response = new HashMap<>();
                    response.put("results", Collections.emptyList());
                    response.put("count", 0);
                    response.put("title", title);
                    return Mono.just(ResponseEntity.ok(response));
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
                        Map<String, Object> response = new HashMap<>();
                        response.put("results", processedBooks);
                        response.put("count", processedBooks.size());
                        response.put("title", title);
                        return ResponseEntity.ok(response);
                    });
            })
            .onErrorResume(e -> {
                logger.error("Error searching books by title '{}': {}", title, e.getMessage(), e);
                if (e instanceof ResponseStatusException rse) return Mono.error(rse);
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occurred while searching books by title", e));
            });
    }
    
    /**
     * Search books by author with support for cover source preferences and image resolution filtering
     * Uses "inauthor:" advanced search operator to find books by specified author
     * 
     * @param author Author name to search for
     * @param coverSource Preferred source for book cover images (optional, defaults to ANY)
     * @param resolution Preferred resolution for book cover images (optional, defaults to ANY)
     * @return Mono containing ResponseEntity with map of search results filtered by author
     */
    @GetMapping("/search/author")
    public Mono<ResponseEntity<Map<String, Object>>> searchBooksByAuthor(
            @RequestParam String author,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Searching books by author: {}, coverSource: {}, resolution: {}", author, coverSource, resolution);

        final CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = getImageResolutionPreferenceFromString(resolution);

        // Use BookCacheService for searching books by author
        return bookCacheService.searchBooksReactive("inauthor:" + author, 0, 40) // Assuming max 40 for author search
            .flatMap(books -> {
                List<Book> currentBooks = (books == null) ? Collections.emptyList() : books;
                if (currentBooks.isEmpty()) {
                    Map<String, Object> response = new HashMap<>();
                    response.put("results", Collections.emptyList());
                    response.put("count", 0);
                    response.put("author", author);
                    return Mono.just(ResponseEntity.ok(response));
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
                        Map<String, Object> response = new HashMap<>();
                        response.put("results", processedBooks);
                        response.put("count", processedBooks.size());
                        response.put("author", author);
                        return ResponseEntity.ok(response);
                    });
            })
            .onErrorResume(e -> {
                logger.error("Error searching books by author '{}': {}", author, e.getMessage(), e);
                if (e instanceof ResponseStatusException rse) return Mono.error(rse);
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occurred while searching books by author", e));
            });
    }
    
    /**
     * Search books by ISBN with support for cover source preferences and image resolution filtering
     * Supports both ISBN-10 and ISBN-13 formats for precise book identification
     * 
     * @param isbn Book ISBN number (can be ISBN-10 or ISBN-13 format)
     * @param coverSource Preferred source for book cover images (optional, defaults to ANY)
     * @param resolution Preferred resolution for book cover images (optional, defaults to ANY)
     * @return Mono containing ResponseEntity with map of search results for the specific ISBN
     */
    @GetMapping("/search/isbn")
    public Mono<ResponseEntity<Map<String, Object>>> searchBooksByISBN(
            @RequestParam String isbn,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Searching books by ISBN: {}, coverSource: {}, resolution: {}", isbn, coverSource, resolution);

        final CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = getImageResolutionPreferenceFromString(resolution);

        // Use BookCacheService for searching books by ISBN
        return bookCacheService.getBooksByIsbnReactive(isbn)
            .flatMap(books -> {
                List<Book> currentBooks = (books == null) ? Collections.emptyList() : books;
                if (currentBooks.isEmpty()) {
                    Map<String, Object> response = new HashMap<>();
                    response.put("results", Collections.emptyList());
                    response.put("count", 0);
                    response.put("isbn", isbn);
                    return Mono.just(ResponseEntity.ok(response));
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
                        Map<String, Object> response = new HashMap<>();
                        response.put("results", processedBooks);
                        response.put("count", processedBooks.size());
                        response.put("isbn", isbn);
                        return ResponseEntity.ok(response);
                    });
            })
            .onErrorResume(e -> {
                logger.error("Error searching books by ISBN '{}': {}", isbn, e.getMessage(), e);
                if (e instanceof ResponseStatusException rse) return Mono.error(rse);
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occurred while searching books by ISBN", e));
            });
    }
    
    /**
     * Get book details by ID with support for cover source preferences and image resolution filtering
     * Adds viewed book to recently viewed history for recommendation tracking
     * 
     * @param id Book identifier to retrieve
     * @param coverSource Preferred source for book cover images (optional, defaults to ANY)
     * @param resolution Preferred resolution for book cover images (optional, defaults to ANY)
     * @return Mono containing ResponseEntity with the book if found, or not found status if not available
     */
    @GetMapping("/{id}")
    public Mono<ResponseEntity<Book>> getBookById(
            @PathVariable String id,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Getting book by ID: {}, coverSource: {}, resolution: {}", id, coverSource, resolution);

        final CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = getImageResolutionPreferenceFromString(resolution);

        // Use BookCacheService to get book by ID
        return bookCacheService.getBookByIdReactive(id)
            .flatMap(book -> {
                if (book == null) { // book can be null if not found by BookCacheService
                    return Mono.empty(); // This will trigger switchIfEmpty later
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
            .doOnSuccess(b -> {
                if (b != null) recentlyViewedService.addToRecentlyViewed(b);
            })
            .map(ResponseEntity::ok)
            .switchIfEmpty(Mono.just(ResponseEntity.notFound().build()))
            .onErrorResume(e -> {
                logger.error("Error getting book by ID '{}': {}", id, e.getMessage(), e);
                if (e instanceof ResponseStatusException rse) return Mono.error(rse);
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occurred while getting book by ID", e));
            });
    }
    
    /**
     * Handle validation errors for request parameters across all endpoints
     * Converts IllegalArgumentException to proper HTTP 400 Bad Request responses
     * 
     * @param ex The IllegalArgumentException thrown during request processing
     * @return ResponseEntity with error details and 400 status code
     */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ResponseEntity<Map<String, String>> handleValidationExceptions(IllegalArgumentException ex) {
        Map<String, String> errors = new HashMap<>();
        errors.put("error", ex.getMessage());
        return ResponseEntity.badRequest().body(errors);
    }
    
    /**
     * Get similar books recommendations for a specific book with support for cover source and resolution preferences
     * Uses the recommendation engine to find books similar to the given book ID based on various criteria
     * 
     * @param id Source book ID to find similar books for
     * @param count Number of recommendations to return (optional, defaults to 6)
     * @param coverSource Preferred source for book cover images (optional, defaults to ANY)
     * @param resolution Preferred resolution for book cover images (optional, defaults to ANY)
     * @return Mono containing ResponseEntity with map of similar book recommendations
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
                            return Mono.just(ResponseEntity.ok(response));
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
                                return ResponseEntity.ok(response);
                            });
                    })
            .onErrorResume(e -> {
                logger.error("Error getting similar books for book ID '{}': {}", id, e.getMessage(), e);
                if (e instanceof ResponseStatusException rse) return Mono.error(rse);
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occurred while getting similar books", e));
            });
    }
}
