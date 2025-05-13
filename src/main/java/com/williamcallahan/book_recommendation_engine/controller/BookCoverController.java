package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Controller for book cover related operations
 */
@RestController
@RequestMapping("/api/covers")
public class BookCoverController {
    private static final Logger logger = LoggerFactory.getLogger(BookCoverController.class);
    
    private final GoogleBooksService googleBooksService;
    private final BookImageOrchestrationService bookImageOrchestrationService;
    
    @Autowired
    public BookCoverController(
            GoogleBooksService googleBooksService,
            BookImageOrchestrationService bookImageOrchestrationService) {
        this.googleBooksService = googleBooksService;
        this.bookImageOrchestrationService = bookImageOrchestrationService;
    }
    
    /**
     * Get the best cover URL for a book with an optional source preference
     * 
     * @param id Book ID
     * @param source Optional source preference (GOOGLE_BOOKS, OPEN_LIBRARY, LONGITOOD, or ANY)
     * @return The best cover URL for the book
     */
    @GetMapping("/{id}")
    public ResponseEntity<Map<String, Object>> getBookCover(
            @PathVariable String id,
            @RequestParam(required = false, defaultValue = "ANY") String source) {
        
        logger.info("Getting book cover for book ID: {} with source preference: {}", id, source);
        
        try {
            // Parse the source parameter
            CoverImageSource preferredSource;
            try {
                preferredSource = CoverImageSource.valueOf(source.toUpperCase());
            } catch (IllegalArgumentException e) {
                logger.warn("Invalid source parameter: {}. Defaulting to ANY.", source);
                preferredSource = CoverImageSource.ANY;
            }
            
            // Get the book
            Book book = googleBooksService.getBookById(id).blockOptional().orElse(null);
            if (book == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Book not found with ID: " + id);
            }
            
            // Get the best cover URL with the preferred source
            CompletableFuture<String> coverUrlFuture = bookImageOrchestrationService.getBestCoverUrlAsync(book, preferredSource);
            String coverUrl = coverUrlFuture.get();
            
            Map<String, Object> response = new HashMap<>();
            response.put("bookId", id);
            response.put("coverUrl", coverUrl);
            response.put("source", preferredSource.name());
            
            return ResponseEntity.ok(response);
        } catch (ResponseStatusException e) {
            throw e;
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error getting book cover: {}", e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, 
                    "Error occurred while getting book cover", e);
        } catch (Exception e) {
            logger.error("Unexpected error getting book cover: {}", e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, 
                    "Unexpected error occurred while getting book cover", e);
        }
    }
    
    /**
     * Handle validation errors
     * 
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
}
