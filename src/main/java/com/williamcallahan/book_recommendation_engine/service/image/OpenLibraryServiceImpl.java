/**
 * Implementation of the Open Library book cover service for retrieving cover images
 * 
 * @author William Callahan
 * 
 * Features:
 * - Fetches book cover images from Open Library's free API
 * - Supports ISBN-based lookups for precise image matching
 * - Implements rate limiting protection for API compliance
 * - Handles multiple cover sizes (small, medium, large)
 * - Provides fallback mechanisms when service is unavailable
 */
package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.model.image.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import com.williamcallahan.book_recommendation_engine.util.cover.CoverIdentifierResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Service
public class OpenLibraryServiceImpl implements OpenLibraryService {

    private static final Logger logger = LoggerFactory.getLogger(OpenLibraryServiceImpl.class);
    private static final String OPEN_LIBRARY_SOURCE_NAME = "OpenLibrary";

    private final CoverCacheManager coverCacheManager;
    private final ExternalCoverFetchHelper externalCoverFetchHelper;

    public OpenLibraryServiceImpl(ExternalCoverFetchHelper externalCoverFetchHelper,
                                  CoverCacheManager coverCacheManager) {
        this.externalCoverFetchHelper = externalCoverFetchHelper;
        this.coverCacheManager = coverCacheManager;
    }

    /**
     * Fetches book cover image from Open Library
     *
     * @param book The book to fetch a cover for
     * @return A CompletableFuture emitting ImageDetails for the large-size cover, or null if not available
     */
    @Override
    @RateLimiter(name = "openLibraryServiceRateLimiter", fallbackMethod = "fetchCoverRateLimitFallback")
    public CompletableFuture<Optional<ImageDetails>> fetchCover(Book book) {
        String isbn = CoverIdentifierResolver.getPreferredIsbn(book);

        if (!ValidationUtils.hasText(isbn)) {
            logger.warn("No ISBN found for book ID: {}, cannot fetch cover from OpenLibrary.", book.getId());
            return CompletableFuture.completedFuture(Optional.empty());
        }

        // Provide the Large size URL as the primary candidate
        String largeUrl = String.format("https://covers.openlibrary.org/b/isbn/%s-L.jpg", isbn);
        String sourceSystemId = String.format("%s-L-%s", OPEN_LIBRARY_SOURCE_NAME, isbn);

        ImageDetails imageDetails = new ImageDetails(
            largeUrl,
            OPEN_LIBRARY_SOURCE_NAME,
            sourceSystemId,
            CoverImageSource.OPEN_LIBRARY,
            ImageResolutionPreference.LARGE
        );

        return CompletableFuture.completedFuture(Optional.of(imageDetails));
    }

    /**
     * Fetches cover image details from OpenLibrary for a specific ISBN and size
     *
     * @param isbn The ISBN of the book
     * @param sizeSuffix The size suffix for the cover (e.g., "L", "M", "S")
     * @return A CompletableFuture emitting ImageDetails for the specified OpenLibrary cover, or null if ISBN or size is invalid
     */
    public CompletableFuture<Optional<ImageDetails>> fetchOpenLibraryCoverDetails(String isbn, String sizeSuffix) {
        if (isbn == null || isbn.trim().isEmpty()) {
            logger.warn("No ISBN provided, cannot fetch cover details from OpenLibrary for size suffix: {}", sizeSuffix);
            return CompletableFuture.completedFuture(Optional.empty());
        }
        // Ensure sizeSuffix is valid before proceeding to switch
        if (sizeSuffix == null || (!sizeSuffix.equals("L") && !sizeSuffix.equals("M") && !sizeSuffix.equals("S"))) {
            logger.warn("Invalid or unsupported size suffix '{}' for ISBN {}. Cannot fetch OpenLibrary cover details.", sizeSuffix, isbn);
            return CompletableFuture.completedFuture(Optional.empty());
        }

        String url = String.format("https://covers.openlibrary.org/b/isbn/%s-%s.jpg", isbn, sizeSuffix);
        String sourceSystemId = String.format("%s-%s-%s", OPEN_LIBRARY_SOURCE_NAME, sizeSuffix, isbn);

        ImageResolutionPreference resolutionPreference;
        switch (sizeSuffix) {
            case "L":
                resolutionPreference = ImageResolutionPreference.LARGE;
                break;
            case "M":
                resolutionPreference = ImageResolutionPreference.MEDIUM;
                break;
            case "S":
                resolutionPreference = ImageResolutionPreference.SMALL;
                break;
            default:
                // Fallback case
                resolutionPreference = ImageResolutionPreference.ORIGINAL;
                logger.debug("Unknown size suffix '{}' for OpenLibrary, using ORIGINAL preference for ISBN {}.", sizeSuffix, isbn);
                break;
        }

        ImageDetails details = new ImageDetails(
            url,
            OPEN_LIBRARY_SOURCE_NAME,
            sourceSystemId,
            CoverImageSource.OPEN_LIBRARY,
            resolutionPreference
        );
        return CompletableFuture.completedFuture(Optional.of(details));
    }

    public CompletableFuture<ImageDetails> fetchAndCacheCover(String isbn, String bookIdForLog, String sizeSuffix, ImageProvenanceData provenanceData) {
        return externalCoverFetchHelper.fetchAndCache(
            isbn,
            coverCacheManager::isKnownBadOpenLibraryIsbn,
            coverCacheManager::addKnownBadOpenLibraryIsbn,
            () -> fetchOpenLibraryCoverDetails(isbn, sizeSuffix),
            "OpenLibrary ISBN: " + isbn + ", size: " + sizeSuffix,
            ImageSourceName.OPEN_LIBRARY,
            "OpenLibrary-" + sizeSuffix,
            "ol-" + sizeSuffix,
            provenanceData,
            bookIdForLog,
            null
        );
    }
    
    /**
     * Fallback method for rate limiting in fetchCover
     * - Called when the rate limit for OpenLibrary API is exceeded
     * - Logs the rate limiting event for monitoring
     * - Returns empty result to indicate no image is available
     *
     * @param book The book that triggered the rate limiter
     * @param t The throwable from the rate limiter
     * @return Empty Optional wrapped in CompletableFuture
     */
    public CompletableFuture<Optional<ImageDetails>> fetchCoverRateLimitFallback(Book book, Throwable t) {
        String isbn = CoverIdentifierResolver.getPreferredIsbn(book);
        logger.warn("OpenLibraryService rate limit exceeded for book ID: {}, ISBN: {}. Error: {}", 
            book.getId(), isbn, t.getMessage());
        return CompletableFuture.completedFuture(Optional.empty());
    }
}
