package com.williamcallahan.book_recommendation_engine.controller;

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
import java.util.concurrent.CompletionException;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;

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
    public CompletableFuture<ResponseEntity<Map<String, Object>>> getBookCover(
            @PathVariable String id,
            @RequestParam(required = false, defaultValue = "ANY") String source) {
        logger.info("Getting book cover for book ID: {} with source preference: {}", id, source);
        final CoverImageSource preferredSource = parsePreferredSource(source);
        return googleBooksService.getBookById(id)
            .toFuture()
            .thenCompose(book -> {
                if (book == null) {
                    CompletableFuture<ResponseEntity<Map<String, Object>>> notFound = new CompletableFuture<>();
                    notFound.completeExceptionally(new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "Book not found with ID: " + id));
                    return notFound;
                }
                return bookImageOrchestrationService
                    .getBestCoverUrlAsync(book, preferredSource)
                    .thenApply(updatedBook -> {
                        Map<String, Object> response = new HashMap<>();
                        response.put("bookId", id);
                        response.put("coverUrl", updatedBook.getCoverImageUrl());
                        if (updatedBook.getCoverImages() != null) {
                            response.put("preferredUrl", updatedBook.getCoverImages().getPreferredUrl());
                            response.put("fallbackUrl", updatedBook.getCoverImages().getFallbackUrl());
                        } else {
                            response.put("preferredUrl", updatedBook.getCoverImageUrl());
                            response.put("fallbackUrl", updatedBook.getCoverImageUrl());
                        }
                        response.put("requestedSourcePreference", preferredSource.name());
                        return ResponseEntity.ok(response);
                    });
            })
            .exceptionally(ex -> {
                Throwable cause = (ex instanceof CompletionException && ex.getCause() != null)
                    ? ex.getCause() : ex;
                if (cause instanceof ResponseStatusException rse) {
                    throw rse;
                }
                logger.error("Error getting book cover: {}", cause.getMessage(), cause);
                throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Error occurred while getting book cover", cause);
            });
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

    @ExceptionHandler(AsyncRequestTimeoutException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public ResponseEntity<Map<String, String>> handleAsyncTimeout(AsyncRequestTimeoutException ex) {
        Map<String, String> error = new HashMap<>();
        error.put("error", "Request timeout");
        error.put("message", "The request took too long to process. Please try again later.");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(error);
    }

    /**
     * Safely parse the preferred cover image source, defaulting to ANY on invalid input.
     */
    private CoverImageSource parsePreferredSource(String sourceParam) {
        try {
            return CoverImageSource.valueOf(sourceParam.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid source parameter: {}. Defaulting to ANY.", sourceParam);
            return CoverImageSource.ANY;
        }
    }
}
