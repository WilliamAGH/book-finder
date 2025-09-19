package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;
import reactor.core.publisher.Mono;
import reactor.core.Disposable;

/**
 * Controller for book cover image operations and retrieval
 * 
 * @author William Callahan
 * 
 * Features:
 * - Provides API endpoints for retrieving book cover images
 * - Supports source preferences for cover images (Google Books, Open Library, etc.)
 * - Orchestrates multi-source image fetching with fallbacks
 * - Handles async processing for optimal response times
 * - Manages error cases with appropriate HTTP status codes
 */
@RestController
@RequestMapping("/api/covers")
public class BookCoverController {
    private static final Logger logger = LoggerFactory.getLogger(BookCoverController.class);
    
    private final GoogleBooksService googleBooksService;
    private final BookImageOrchestrationService bookImageOrchestrationService;
    
    /**
     * Constructs BookCoverController with required services
     * - Injects GoogleBooksService for book metadata retrieval
     * - Injects BookImageOrchestrationService for cover image processing
     * 
     * @param googleBooksService Service for retrieving book information from Google Books API
     * @param bookImageOrchestrationService Service for orchestrating book cover image operations
     */
    public BookCoverController(
            GoogleBooksService googleBooksService,
            BookImageOrchestrationService bookImageOrchestrationService) {
        this.googleBooksService = googleBooksService;
        this.bookImageOrchestrationService = bookImageOrchestrationService;
    }
    
    /**
     * Get the best cover URL for a book with an optional source preference
     * - Retrieves cover URL based on book ID
     * - Supports source preference parameter for choosing image provider
     * - Handles asynchronous processing with timeout
     * - Returns structured JSON response with cover URLs
     * 
     * @param id Book ID
     * @param source Optional source preference (GOOGLE_BOOKS, OPEN_LIBRARY, LONGITOOD, or ANY)
     * @return The best cover URL for the book
     */
    @GetMapping("/{id}")
    public DeferredResult<ResponseEntity<Map<String, Object>>> getBookCover(
            @PathVariable String id,
            @RequestParam(required = false, defaultValue = "ANY") String source) {
        logger.info("Getting book cover for book ID: {} with source preference: {}", id, source);
        final CoverImageSource preferredSource = parsePreferredSource(source);

        long timeoutValue = 120_000L; // 120 seconds in milliseconds
        DeferredResult<ResponseEntity<Map<String, Object>>> deferredResult = 
            new DeferredResult<>(timeoutValue);

        Mono<ResponseEntity<Map<String, Object>>> responseMono = Mono.fromCompletionStage(googleBooksService.getBookById(id)) // Convert CompletionStage to Mono
            .flatMap(book -> {
                // bookImageOrchestrationService.getBestCoverUrlAsync returns CompletableFuture<Book>
                return Mono.fromFuture(bookImageOrchestrationService.getBestCoverUrlAsync(book, preferredSource))
                    .map(updatedBook -> {
                        Map<String, Object> response = new HashMap<>();
                        response.put("bookId", id);
                        response.put("coverUrl", updatedBook.getS3ImagePath());
                        if (updatedBook.getCoverImages() != null) {
                            response.put("preferredUrl", updatedBook.getCoverImages().getPreferredUrl());
                            response.put("fallbackUrl", updatedBook.getCoverImages().getFallbackUrl());
                        } else {
                            response.put("preferredUrl", updatedBook.getS3ImagePath());
                            response.put("fallbackUrl", updatedBook.getS3ImagePath());
                        }
                        response.put("requestedSourcePreference", preferredSource.name());
                        return ResponseEntity.ok(response);
                    });
            })
            .switchIfEmpty(Mono.defer(new java.util.function.Supplier<Mono<ResponseEntity<Map<String, Object>>>>() {
                @Override
                public Mono<ResponseEntity<Map<String, Object>>> get() {
                    logger.warn("Book not found with ID: {} when processing getBookCover.", id);
                    return Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Book not found with ID: " + id));
                }
            }));

        // Subscribe to the Mono and bridge to DeferredResult
        Disposable subscription = responseMono.subscribe(
            result -> {
                if (!deferredResult.isSetOrExpired()) {
                    deferredResult.setResult(result);
                }
            },
            error -> {
                if (deferredResult.isSetOrExpired()) return;
                
                Throwable cause = error;
                // Check if cause is wrapped, e.g. in CompletionException if Mono.fromFuture passed it
                if (error instanceof java.util.concurrent.CompletionException && error.getCause() != null) {
                    cause = error.getCause();
                }

                if (cause instanceof ResponseStatusException rse) {
                    deferredResult.setErrorResult(ResponseEntity.status(rse.getStatusCode()).body(createErrorMap(rse.getReason())));
                } else {
                    logger.error("Error processing getBookCover reactive chain: {}", cause.getMessage(), cause);
                    deferredResult.setErrorResult(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body(createErrorMap("Error occurred while getting book cover")));
                }
            }
        );

        deferredResult.onTimeout(() -> {
            if (deferredResult.isSetOrExpired()) return;
            Map<String, String> error = new HashMap<>();
            error.put("error", "Request timeout");
            error.put("message", "The request to get book cover took too long to process. Please try again later.");
            if (!deferredResult.isSetOrExpired()) {
                deferredResult.setErrorResult(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(error));
            }
            if (subscription != null && !subscription.isDisposed()) {
                subscription.dispose();
            }
        });
        
        deferredResult.onCompletion(() -> {
            if (subscription != null && !subscription.isDisposed()) {
                subscription.dispose();
            }
        });

        // This onError handles errors in DeferredResult's own processing,
        // or if an error is set via deferredResult.setErrorResult explicitly before the Mono completes.
        deferredResult.onError(ex -> {
             if (deferredResult.isSetOrExpired()) return;
             Throwable cause = (ex instanceof CompletionException && ex.getCause() != null)
                ? ex.getCause() : ex;
            if (cause instanceof ResponseStatusException rse) {
                 if (!deferredResult.isSetOrExpired()) deferredResult.setErrorResult(ResponseEntity.status(rse.getStatusCode()).body(createErrorMap(rse.getReason())));
            } else {
                logger.error("Error in DeferredResult for getBookCover: {}", cause.getMessage(), cause);
                 if (!deferredResult.isSetOrExpired()) deferredResult.setErrorResult(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorMap("Error occurred while getting book cover")));
            }
        });
            
        return deferredResult;
    }

    private Map<String, String> createErrorMap(String message) {
        Map<String, String> error = new HashMap<>();
        error.put("error", message);
        return error;
    }
    
    /**
     * Handle validation errors for request parameters
     * - Converts IllegalArgumentException to HTTP 400 Bad Request
     * - Provides structured error response with explanation
     * - Maps exception message to error field in response
     * - Returns consistent error format for client consumption
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
     * Handle asynchronous request timeouts
     * - Captures AsyncRequestTimeoutException for long-running requests
     * - Returns 503 Service Unavailable with friendly message
     * - Prompts client to retry the request later
     * - Provides standardized error response format
     * - Handles timeout for any async method not using DeferredResult
     * 
     * @param ex The AsyncRequestTimeoutException thrown when request times out
     * @return ResponseEntity with timeout details and 503 status code
     */
    @ExceptionHandler(AsyncRequestTimeoutException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public ResponseEntity<Map<String, String>> handleAsyncTimeout(AsyncRequestTimeoutException ex) {
        Map<String, String> error = new HashMap<>();
        error.put("error", "Request timeout");
        error.put("message", "The request took too long to process. Please try again later.");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(error);
    }

    /**
     * Safely parse the preferred cover image source from request parameter
     * - Converts string source parameter to CoverImageSource enum
     * - Handles invalid values gracefully by defaulting to ANY
     * - Logs warning when input source is invalid
     * - Prevents exceptions from invalid source parameters
     * 
     * @param sourceParam String representation of cover image source
     * @return The corresponding CoverImageSource enum value
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
