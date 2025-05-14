/**
 * REST controller for handling book-related API requests such as search, details, and recommendations.
 * Optimizes cover image URL resolution for API responses.
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookCacheService;
// import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
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
     * Search books by keyword
     * @param query Search query
     * @param startIndex Start index for pagination (optional)
     * @param maxResults Maximum number of results (optional)
     * @return List of books matching the query
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
        
        final BookImageOrchestrationService.CoverImageSource effectivelyFinalPreferredSource = 
            getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = 
            getImageResolutionPreferenceFromString(resolution);

        // Use BookCacheService for searching books.
        // Note: BookCacheService.searchBooksReactive currently handles pagination internally based on startIndex and maxResults.
        // It fetches a larger set from GoogleBooksService and then paginates.
        // For accurate totalAvailableResults, BookCacheService.searchBooksReactive would need to be modified
        // to return a structure containing both the paginated list and the total count from the initial Google fetch.
        // For now, totalAvailableResults will reflect the count of the paginated list from BookCacheService.
        return bookCacheService.searchBooksReactive(query, startIndex, maxResults)
            .flatMap(paginatedBooks -> {
                List<Book> currentPaginatedBooks = (paginatedBooks == null) ? Collections.emptyList() : paginatedBooks;
                // This totalResults is the count of books returned by BookCacheService for the current page,
                // not the grand total available for the query. This is a known simplification for now.
                int totalResultsInPage = currentPaginatedBooks.size(); 

                if (currentPaginatedBooks.isEmpty()) {
                    Map<String, Object> response = new HashMap<>();
                    // Since BookCacheService already paginates, if it's empty, it means no results for this page.
                    // We cannot accurately report totalAvailableResults for the entire query without modifying BookCacheService.
                    response.put("totalAvailableResults", 0); // Or reflect what BookCacheService can provide
                    response.put("results", Collections.emptyList());
                    response.put("count", 0);
                    response.put("startIndex", startIndex);
                    response.put("query", query);
                    return Mono.just(ResponseEntity.ok(response));
                }

                return Flux.fromIterable(currentPaginatedBooks)
                    .flatMap(book -> {
                        if (book == null) {
                            return Mono.just(book); // Should ideally not happen if list is filtered
                        }
                        return Mono.fromFuture(bookImageOrchestrationService.getBestCoverUrlAsync(book, effectivelyFinalPreferredSource, effectivelyFinalResolutionPreference))
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
                        // Again, totalAvailableResults is not the grand total here.
                        response.put("totalAvailableResults", totalResultsInPage); 
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
    
    private BookImageOrchestrationService.CoverImageSource getCoverImageSourceFromString(String source) {
        try {
            return BookImageOrchestrationService.CoverImageSource.valueOf(source.toUpperCase());
        } catch (IllegalArgumentException e) {
            return BookImageOrchestrationService.CoverImageSource.ANY;
        }
    }

    private ImageResolutionPreference getImageResolutionPreferenceFromString(String resolution) {
        try {
            return ImageResolutionPreference.valueOf(resolution.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ImageResolutionPreference.ANY;
        }
    }
    
    /**
     * Search books by title
     * @param title Book title
     * @return List of books matching the title
     */
    @GetMapping("/search/title")
    public Mono<ResponseEntity<Map<String, Object>>> searchBooksByTitle(
            @RequestParam String title,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Searching books by title: {}, coverSource: {}, resolution: {}", title, coverSource, resolution);

        final BookImageOrchestrationService.CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
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
     * Search books by author
     * @param author Book author
     * @return List of books matching the author
     */
    @GetMapping("/search/author")
    public Mono<ResponseEntity<Map<String, Object>>> searchBooksByAuthor(
            @RequestParam String author,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Searching books by author: {}, coverSource: {}, resolution: {}", author, coverSource, resolution);

        final BookImageOrchestrationService.CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
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
     * Search books by ISBN
     * @param isbn Book ISBN (can be ISBN-10 or ISBN-13)
     * @return List of books matching the ISBN
     */
    @GetMapping("/search/isbn")
    public Mono<ResponseEntity<Map<String, Object>>> searchBooksByISBN(
            @RequestParam String isbn,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Searching books by ISBN: {}, coverSource: {}, resolution: {}", isbn, coverSource, resolution);

        final BookImageOrchestrationService.CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = getImageResolutionPreferenceFromString(resolution);

        // Use BookCacheService for searching books by ISBN
        return bookCacheService.getBooksByIsbnReactive(isbn) // This returns Mono<List<Book>>
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
     * Get book by ID
     * @param id Book ID
     * @return Book details if found
     */
    @GetMapping("/{id}")
    public Mono<ResponseEntity<Book>> getBookById(
            @PathVariable String id,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Getting book by ID: {}, coverSource: {}, resolution: {}", id, coverSource, resolution);

        final BookImageOrchestrationService.CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = getImageResolutionPreferenceFromString(resolution);

        // Use BookCacheService to get book by ID
        return bookCacheService.getBookByIdReactive(id)
            .flatMap(book -> {
                if (book == null) { // book can be null if not found by BookCacheService
                    return Mono.empty(); // This will trigger switchIfEmpty later
                }
                return Mono.fromFuture(bookImageOrchestrationService.getBestCoverUrlAsync(book, effectivelyFinalPreferredSource, effectivelyFinalResolutionPreference))
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
     * Handle validation errors
     * @param ex The exception
     * @return Error response
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
     * @param id Book ID to find similar books for
     * @param count Number of recommendations to return (optional)
     * @return List of similar books
     */
    @GetMapping("/{id}/similar")
    public Mono<ResponseEntity<Map<String, Object>>> getSimilarBooks(
            @PathVariable String id,
            @RequestParam(required = false, defaultValue = "6") int count,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        
        logger.info("Getting similar books for book ID: {}, count: {}, coverSource: {}, resolution: {}", id, count, coverSource, resolution);

        final BookImageOrchestrationService.CoverImageSource effectivelyFinalPreferredSource = getCoverImageSourceFromString(coverSource);
        final ImageResolutionPreference effectivelyFinalResolutionPreference = getImageResolutionPreferenceFromString(resolution);

        // RecommendationService already uses BookCacheService for the source book, so this part is fine.
        // We just need to ensure the sourceBook for the recommendationService.getSimilarBooks call is fetched via BookCacheService if it were done here,
        // but since RecommendationService handles that internally, we only need to process the results.
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
