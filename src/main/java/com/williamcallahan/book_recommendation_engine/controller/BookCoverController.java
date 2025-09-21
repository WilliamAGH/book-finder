package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService;
import com.williamcallahan.book_recommendation_engine.util.EnumParsingUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import com.williamcallahan.book_recommendation_engine.controller.support.ErrorResponseUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;
import org.springframework.web.context.request.async.DeferredResult;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

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
@Slf4j
public class BookCoverController {
        private final BookDataOrchestrator bookDataOrchestrator;

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
            BookImageOrchestrationService bookImageOrchestrationService,
            BookDataOrchestrator bookDataOrchestrator) {
        this.googleBooksService = googleBooksService;
        this.bookImageOrchestrationService = bookImageOrchestrationService;
        this.bookDataOrchestrator = bookDataOrchestrator;
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
        log.info("Getting book cover for book ID: {} with source preference: {}", id, source);
        final CoverImageSource preferredSource = parsePreferredSource(source);

        long timeoutValue = 120_000L; // 120 seconds in milliseconds
        DeferredResult<ResponseEntity<Map<String, Object>>> deferredResult = 
            new DeferredResult<>(timeoutValue);

        Mono<Book> canonicalBookMono = bookDataOrchestrator.getBookByIdTiered(id)
            .switchIfEmpty(Mono.defer(() -> Mono.fromCompletionStage(googleBooksService.getBookById(id))
                .flatMap(book -> book == null ? Mono.empty() : Mono.just(book))));

        Mono<ResponseEntity<Map<String, Object>>> responseMono = canonicalBookMono
            .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Book not found")))
            .flatMap(book -> {
                // bookImageOrchestrationService.getBestCoverUrlAsync returns CompletableFuture<Book>
                return Mono.fromFuture(bookImageOrchestrationService.getBestCoverUrlAsync(book, preferredSource))
                    .map(updatedBook -> {
                        Map<String, Object> response = new HashMap<>();
                        response.put("bookId", id);

                        // Improved fallback logic with proper coalescing
                        String s3Path = updatedBook.getS3ImagePath();
                        String preferredFromImages = updatedBook.getCoverImages() != null ?
                            updatedBook.getCoverImages().getPreferredUrl() : null;
                        String fallbackFromImages = updatedBook.getCoverImages() != null ?
                            updatedBook.getCoverImages().getFallbackUrl() : null;
                        String externalUrl = updatedBook.getExternalImageUrl();
                        String placeholder = "/images/placeholder-book-cover.svg";

                        // Priority: S3 > preferred > external > placeholder
                        String coverUrl = firstNonBlank(s3Path, preferredFromImages, externalUrl, placeholder);
                        response.put("coverUrl", coverUrl);

                        if (updatedBook.getCoverImages() != null) {
                            response.put("preferredUrl", firstNonBlank(preferredFromImages, coverUrl));
                            response.put("fallbackUrl", firstNonBlank(fallbackFromImages, coverUrl));
                        } else {
                            String fallback = firstNonBlank(s3Path, externalUrl, placeholder);
                            response.put("preferredUrl", fallback);
                            response.put("fallbackUrl", fallback);
                        }
                        response.put("requestedSourcePreference", preferredSource.name());
                        return ResponseEntity.ok(response);
                    });
            })
            .switchIfEmpty(Mono.defer(new java.util.function.Supplier<Mono<ResponseEntity<Map<String, Object>>>>() {
                @Override
                public Mono<ResponseEntity<Map<String, Object>>> get() {
                    log.warn("Book not found with ID: {} when processing getBookCover.", id);
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
                    deferredResult.setErrorResult(ErrorResponseUtils.error(HttpStatus.valueOf(rse.getStatusCode().value()), rse.getReason()));
                } else {
                    log.error("Error processing getBookCover reactive chain: {}", cause.getMessage(), cause);
                    deferredResult.setErrorResult(ErrorResponseUtils.internalServerError(
                        "Error occurred while getting book cover",
                        cause.getMessage()
                    ));
                }
            }
        );

        deferredResult.onTimeout(() -> {
            if (deferredResult.isSetOrExpired()) return;
            if (!deferredResult.isSetOrExpired()) {
                deferredResult.setErrorResult(ErrorResponseUtils.error(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "Request timeout",
                    "The request to get book cover took too long to process. Please try again later."
                ));
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
                 if (!deferredResult.isSetOrExpired()) deferredResult.setErrorResult(ErrorResponseUtils.error(HttpStatus.valueOf(rse.getStatusCode().value()), rse.getReason()));
            } else {
                log.error("Error in DeferredResult for getBookCover: {}", cause.getMessage(), cause);
                 if (!deferredResult.isSetOrExpired()) deferredResult.setErrorResult(ErrorResponseUtils.internalServerError(
                    "Error occurred while getting book cover",
                    cause.getMessage()
                 ));
            }
        });

        return deferredResult;
    }

    /**
     * Helper method to find the first non-blank string from a list of values
     * @param values Variable number of string values to check
     * @return The first non-blank string, or null if all are blank
     */
    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String v : values) {
            if (ValidationUtils.hasText(v)) return v;
        }
        return null;
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
        return ErrorResponseUtils.badRequest(ex.getMessage(), null);
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
        return ErrorResponseUtils.error(
            HttpStatus.SERVICE_UNAVAILABLE,
            "Request timeout",
            "The request took too long to process. Please try again later."
        );
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
        return EnumParsingUtils.parseOrDefault(
                sourceParam,
                CoverImageSource.class,
                CoverImageSource.ANY,
                invalid -> log.warn("Invalid source parameter: {}. Defaulting to ANY.", invalid)
        );
    }
}
