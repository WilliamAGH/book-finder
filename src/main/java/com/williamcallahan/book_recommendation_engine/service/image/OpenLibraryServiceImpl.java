package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.types.OpenLibraryService; 
import com.williamcallahan.book_recommendation_engine.types.ImageDetails; // Added import
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
// import reactor.core.publisher.Mono; // Removed unused import
import java.util.concurrent.CompletableFuture; // Added import

/**
 * Implementation of the Open Library book cover service
 *
 * @author William Callahan
 *
 * Features:
 * - Fetches book cover images from the Open Library API
 * - Supports multiple image sizes (small, medium, large)
 * - Provides ISBN-based lookup for consistent cover retrieval
 * - Returns detailed metadata about image source and resolution 
 * - Handles cover retrieval without requiring actual HTTP calls
 */
@Service
public class OpenLibraryServiceImpl implements OpenLibraryService {

    private static final Logger logger = LoggerFactory.getLogger(OpenLibraryServiceImpl.class);
    private static final String OPEN_LIBRARY_SOURCE_NAME = "OpenLibrary";

    /**
     * Fetches book cover image from Open Library
     *
     * @param book The book to fetch a cover for
     * @return A CompletableFuture emitting ImageDetails for the large-size cover, or null if not available
     */
    @Override
    public CompletableFuture<ImageDetails> fetchCover(Book book) {
        String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();

        if (isbn == null || isbn.trim().isEmpty()) {
            logger.warn("No ISBN found for book ID: {}, cannot fetch cover from OpenLibrary.", book.getId());
            return CompletableFuture.completedFuture(null);
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

        return CompletableFuture.completedFuture(imageDetails);
    }

    /**
     * Fetches cover image details from OpenLibrary for a specific ISBN and size
     *
     * @param isbn The ISBN of the book
     * @param sizeSuffix The size suffix for the cover (e.g., "L", "M", "S")
     * @return A CompletableFuture emitting ImageDetails for the specified OpenLibrary cover, or null if ISBN or size is invalid
     */
    public CompletableFuture<ImageDetails> fetchOpenLibraryCoverDetails(String isbn, String sizeSuffix) {
        if (isbn == null || isbn.trim().isEmpty()) {
            logger.warn("No ISBN provided, cannot fetch cover details from OpenLibrary for size suffix: {}", sizeSuffix);
            return CompletableFuture.completedFuture(null);
        }
        // Ensure sizeSuffix is valid before proceeding to switch
        if (sizeSuffix == null || (!sizeSuffix.equals("L") && !sizeSuffix.equals("M") && !sizeSuffix.equals("S"))) {
            logger.warn("Invalid or unsupported size suffix '{}' for ISBN {}. Cannot fetch OpenLibrary cover details.", sizeSuffix, isbn);
            return CompletableFuture.completedFuture(null); // Return null if suffix is null or not L, M, S
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
                // This case should ideally be handled by the check above, 
                // but as a fallback or if other suffixes become valid
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
        return CompletableFuture.completedFuture(details);
    }
}
