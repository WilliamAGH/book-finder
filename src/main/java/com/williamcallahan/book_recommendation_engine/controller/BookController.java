/**
 * REST controller for handling book-related API requests such as search, details, and recommendations.
 * Optimizes cover image URL resolution for API responses.
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
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
    
    private final GoogleBooksService googleBooksService;
    private final RecentlyViewedService recentlyViewedService;
    private final RecommendationService recommendationService;
    private final BookImageOrchestrationService bookImageOrchestrationService;
    
    @Autowired
    public BookController(GoogleBooksService googleBooksService, 
                          RecentlyViewedService recentlyViewedService,
                          RecommendationService recommendationService,
                          BookImageOrchestrationService bookImageOrchestrationService) {
        this.googleBooksService = googleBooksService;
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
    public ResponseEntity<Map<String, Object>> searchBooks(
            @RequestParam String query,
            @RequestParam(required = false, defaultValue = "0") int startIndex,
            @RequestParam(required = false, defaultValue = "10") int maxResults,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        
        logger.info("Searching books with query: {}, startIndex: {}, maxResults: {}, coverSource: {}, resolution: {}", 
                query, startIndex, maxResults, coverSource, resolution);
        
        try {
            List<Book> allBooks = googleBooksService.searchBooksAsyncReactive(query).block();
            if (allBooks == null) allBooks = Collections.emptyList();

            int totalResults = allBooks.size();
            List<Book> paginatedBooks;

            // Apply pagination FIRST
            if (startIndex >= totalResults) {
                paginatedBooks = Collections.emptyList();
            } else {
                int endIndex = Math.min(startIndex + maxResults, totalResults);
                paginatedBooks = allBooks.subList(startIndex, endIndex);
            }
            
            // Now, process covers ONLY for the paginated slice
            if (!paginatedBooks.isEmpty() && (!coverSource.equals("ANY") || !resolution.equals("ANY"))) {
                BookImageOrchestrationService.CoverImageSource preferredSource;
                try {
                    preferredSource = BookImageOrchestrationService.CoverImageSource.valueOf(coverSource.toUpperCase());
                } catch (IllegalArgumentException e) {
                    preferredSource = BookImageOrchestrationService.CoverImageSource.ANY;
                }
                
                ImageResolutionPreference resolutionPreference;
                try {
                    resolutionPreference = ImageResolutionPreference.valueOf(resolution.toUpperCase());
                } catch (IllegalArgumentException e) {
                    resolutionPreference = ImageResolutionPreference.ANY;
                }
                
                for (Book book : paginatedBooks) { // Iterate over paginatedBooks
                    if (book != null && book.getId() != null) {
                        try {
                            String coverUrl = bookImageOrchestrationService
                                .getBestCoverUrlAsync(book, preferredSource, resolutionPreference)
                                .join();
                            book.setCoverImageUrl(coverUrl);
                            // Note: book.isCoverHighResolution() will be set by getBestCoverUrlAsync if ImageDetails are updated
                        } catch (Exception e) {
                            logger.warn("Error applying cover preferences for book {}: {}", 
                                book.getId(), e.getMessage());
                        }
                    }
                }
            }

            // Apply resolution-based filtering or sorting to the paginated (and cover-processed) list
            ImageResolutionPreference finalResolutionPreference;
            try {
                finalResolutionPreference = ImageResolutionPreference.valueOf(resolution.toUpperCase());
            } catch (IllegalArgumentException e) {
                finalResolutionPreference = ImageResolutionPreference.ANY;
            }

            List<Book> finalBooksToReturn = paginatedBooks; // Start with the paginated and cover-processed list

            if (finalResolutionPreference == ImageResolutionPreference.HIGH_ONLY) {
                finalBooksToReturn = paginatedBooks.stream()
                             .filter(b -> b.getIsCoverHighResolution() != null && b.getIsCoverHighResolution())
                             .collect(Collectors.toList());
                logger.info("Filtered paginated list for HIGH_ONLY, new count: {}", finalBooksToReturn.size());
            } else if (finalResolutionPreference == ImageResolutionPreference.HIGH_FIRST) {
                // Create a mutable list for sorting if paginatedBooks is not already mutable (e.g. from subList)
                List<Book> sortablePaginatedBooks = new ArrayList<>(paginatedBooks);
                sortablePaginatedBooks.sort(Comparator.comparing((Book b) -> b.getIsCoverHighResolution() != null && b.getIsCoverHighResolution(), Comparator.reverseOrder()));
                finalBooksToReturn = sortablePaginatedBooks;
                logger.info("Sorted paginated list for HIGH_FIRST");
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("totalAvailableResults", totalResults); // totalResults is from before pagination
            response.put("results", finalBooksToReturn);
            response.put("count", finalBooksToReturn.size());
            response.put("startIndex", startIndex);
            response.put("query", query);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error searching books: {}", e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, 
                    "Error occurred while searching books", e);
        }
    }
    
    /**
     * Search books by title
     * @param title Book title
     * @return List of books matching the title
     */
    @GetMapping("/search/title")
    public ResponseEntity<Map<String, Object>> searchBooksByTitle(
            @RequestParam String title,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Searching books by title: {}", title);
        
        try {
            List<Book> books = googleBooksService.searchBooksByTitle(title).block();
            if (books == null) books = Collections.emptyList();
            
            // Apply cover source and resolution preferences if specified
            if (books.size() > 0 && (!coverSource.equals("ANY") || !resolution.equals("ANY"))) {
                BookImageOrchestrationService.CoverImageSource preferredSource;
                try {
                    preferredSource = BookImageOrchestrationService.CoverImageSource.valueOf(coverSource.toUpperCase());
                } catch (IllegalArgumentException e) {
                    preferredSource = BookImageOrchestrationService.CoverImageSource.ANY;
                }
                
                ImageResolutionPreference resolutionPreference;
                try {
                    resolutionPreference = ImageResolutionPreference.valueOf(resolution.toUpperCase());
                } catch (IllegalArgumentException e) {
                    resolutionPreference = ImageResolutionPreference.ANY;
                }
                
                // Process each book to apply the cover source and resolution preferences
                for (Book book : books) {
                    if (book != null && book.getId() != null) {
                        try {
                            // Get the preferred cover URL and update the book
                            String coverUrl = bookImageOrchestrationService
                                .getBestCoverUrlAsync(book, preferredSource, resolutionPreference)
                                .join(); // Wait for the result
                            book.setCoverImageUrl(coverUrl);
                        } catch (Exception e) {
                            logger.warn("Error applying cover preferences for book {}: {}", 
                                book.getId(), e.getMessage());
                        }
                    }
                }
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("results", books);
            response.put("count", books.size());
            response.put("title", title);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error searching books by title: {}", e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, 
                    "Error occurred while searching books by title", e);
        }
    }
    
    /**
     * Search books by author
     * @param author Book author
     * @return List of books matching the author
     */
    @GetMapping("/search/author")
    public ResponseEntity<Map<String, Object>> searchBooksByAuthor(
            @RequestParam String author,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Searching books by author: {}", author);
        
        try {
            List<Book> books = googleBooksService.searchBooksByAuthor(author).block();
            if (books == null) books = Collections.emptyList();
            
            // Apply cover source and resolution preferences if specified
            if (books.size() > 0 && (!coverSource.equals("ANY") || !resolution.equals("ANY"))) {
                BookImageOrchestrationService.CoverImageSource preferredSource;
                try {
                    preferredSource = BookImageOrchestrationService.CoverImageSource.valueOf(coverSource.toUpperCase());
                } catch (IllegalArgumentException e) {
                    preferredSource = BookImageOrchestrationService.CoverImageSource.ANY;
                }
                
                ImageResolutionPreference resolutionPreference;
                try {
                    resolutionPreference = ImageResolutionPreference.valueOf(resolution.toUpperCase());
                } catch (IllegalArgumentException e) {
                    resolutionPreference = ImageResolutionPreference.ANY;
                }
                
                // Process each book to apply the cover source and resolution preferences
                for (Book book : books) {
                    if (book != null && book.getId() != null) {
                        try {
                            // Get the preferred cover URL and update the book
                            String coverUrl = bookImageOrchestrationService
                                .getBestCoverUrlAsync(book, preferredSource, resolutionPreference)
                                .join(); // Wait for the result
                            book.setCoverImageUrl(coverUrl);
                        } catch (Exception e) {
                            logger.warn("Error applying cover preferences for book {}: {}", 
                                book.getId(), e.getMessage());
                        }
                    }
                }
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("results", books);
            response.put("count", books.size());
            response.put("author", author);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error searching books by author: {}", e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, 
                    "Error occurred while searching books by author", e);
        }
    }
    
    /**
     * Search books by ISBN
     * @param isbn Book ISBN (can be ISBN-10 or ISBN-13)
     * @return List of books matching the ISBN
     */
    @GetMapping("/search/isbn")
    public ResponseEntity<Map<String, Object>> searchBooksByISBN(
            @RequestParam String isbn,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Searching books by ISBN: {}", isbn);
        
        try {
            List<Book> books = googleBooksService.searchBooksByISBN(isbn).block();
            if (books == null) books = Collections.emptyList();
            
            // Apply cover source and resolution preferences if specified
            if (books.size() > 0 && (!coverSource.equals("ANY") || !resolution.equals("ANY"))) {
                BookImageOrchestrationService.CoverImageSource preferredSource;
                try {
                    preferredSource = BookImageOrchestrationService.CoverImageSource.valueOf(coverSource.toUpperCase());
                } catch (IllegalArgumentException e) {
                    preferredSource = BookImageOrchestrationService.CoverImageSource.ANY;
                }
                
                ImageResolutionPreference resolutionPreference;
                try {
                    resolutionPreference = ImageResolutionPreference.valueOf(resolution.toUpperCase());
                } catch (IllegalArgumentException e) {
                    resolutionPreference = ImageResolutionPreference.ANY;
                }
                
                // Process each book to apply the cover source and resolution preferences
                for (Book book : books) {
                    if (book != null && book.getId() != null) {
                        try {
                            // Get the preferred cover URL and update the book
                            String coverUrl = bookImageOrchestrationService
                                .getBestCoverUrlAsync(book, preferredSource, resolutionPreference)
                                .join(); // Wait for the result
                            book.setCoverImageUrl(coverUrl);
                        } catch (Exception e) {
                            logger.warn("Error applying cover preferences for book {}: {}", 
                                book.getId(), e.getMessage());
                        }
                    }
                }
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("results", books);
            response.put("count", books.size());
            response.put("isbn", isbn);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error searching books by ISBN: {}", e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, 
                    "Error occurred while searching books by ISBN", e);
        }
    }
    
    /**
     * Get book by ID
     * @param id Book ID
     * @return Book details if found
     */
    @GetMapping("/{id}")
    public ResponseEntity<Book> getBookById(
            @PathVariable String id,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        logger.info("Getting book by ID: {}", id);
        
        try {
            Book book = googleBooksService.getBookById(id).blockOptional().orElse(null);
            
            if (book == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Book not found with ID: " + id);
            }
            
            // Apply cover source and resolution preferences if specified
            if (!coverSource.equals("ANY") || !resolution.equals("ANY")) {
                BookImageOrchestrationService.CoverImageSource preferredSource;
                try {
                    preferredSource = BookImageOrchestrationService.CoverImageSource.valueOf(coverSource.toUpperCase());
                } catch (IllegalArgumentException e) {
                    preferredSource = BookImageOrchestrationService.CoverImageSource.ANY;
                }
                
                ImageResolutionPreference resolutionPreference;
                try {
                    resolutionPreference = ImageResolutionPreference.valueOf(resolution.toUpperCase());
                } catch (IllegalArgumentException e) {
                    resolutionPreference = ImageResolutionPreference.ANY;
                }
                
                try {
                    // Get the preferred cover URL and update the book
                    String coverUrl = bookImageOrchestrationService
                        .getBestCoverUrlAsync(book, preferredSource, resolutionPreference)
                        .join(); // Wait for the result
                    book.setCoverImageUrl(coverUrl);
                } catch (Exception e) {
                    logger.warn("Error applying cover preferences for book {}: {}", 
                        book.getId(), e.getMessage());
                }
            }
            
            // Add book to recently viewed books
            recentlyViewedService.addToRecentlyViewed(book);
            
            return ResponseEntity.ok(book);
        } catch (ResponseStatusException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Error getting book by ID: {}", e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, 
                    "Error occurred while getting book by ID", e);
        }
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
    public ResponseEntity<Map<String, Object>> getSimilarBooks(
            @PathVariable String id,
            @RequestParam(required = false, defaultValue = "6") int count,
            @RequestParam(required = false, defaultValue = "ANY") String coverSource,
            @RequestParam(required = false, defaultValue = "ANY") String resolution) {
        
        logger.info("Getting similar books for book ID: {}, count: {}", id, count);
        
        try {
            // Ensure the source book exists
            Book sourceBook = googleBooksService.getBookById(id).blockOptional().orElse(null);
            if (sourceBook == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Book not found with ID: " + id);
            }
            
            // Get recommendations
            List<Book> similarBooks = recommendationService.getSimilarBooks(id, count);
            
            // Apply cover source and resolution preferences if specified
            if ((!coverSource.equals("ANY") || !resolution.equals("ANY")) && !similarBooks.isEmpty()) {
                BookImageOrchestrationService.CoverImageSource preferredSource;
                try {
                    preferredSource = BookImageOrchestrationService.CoverImageSource.valueOf(coverSource.toUpperCase());
                } catch (IllegalArgumentException e) {
                    preferredSource = BookImageOrchestrationService.CoverImageSource.ANY;
                }
                
                ImageResolutionPreference resolutionPreference;
                try {
                    resolutionPreference = ImageResolutionPreference.valueOf(resolution.toUpperCase());
                } catch (IllegalArgumentException e) {
                    resolutionPreference = ImageResolutionPreference.ANY;
                }
                
                // Process each book to apply the cover source and resolution preferences
                for (Book book : similarBooks) {
                    if (book != null && book.getId() != null) {
                        try {
                            // Get the preferred cover URL and update the book
                            String coverUrl = bookImageOrchestrationService
                                .getBestCoverUrlAsync(book, preferredSource, resolutionPreference)
                                .join(); // Wait for the result
                            book.setCoverImageUrl(coverUrl);
                        } catch (Exception e) {
                            logger.warn("Error applying cover preferences for book {}: {}", 
                                book.getId(), e.getMessage());
                        }
                    }
                }
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("sourceBook", sourceBook);
            response.put("recommendations", similarBooks);
            response.put("count", similarBooks.size());
            
            return ResponseEntity.ok(response);
        } catch (ResponseStatusException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Error getting similar books: {}", e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, 
                    "Error occurred while getting similar books", e);
        }
    }
}
