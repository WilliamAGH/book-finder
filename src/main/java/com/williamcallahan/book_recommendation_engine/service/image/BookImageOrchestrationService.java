/**
 * Orchestrates the retrieval and management of book cover image URLs
 * 
 * @author William Callahan
 * 
 * This service coordinates the process of retrieving book cover images from various sources,
 * prioritizing cached images when available before fetching from external services -- it works
 * with BookCoverCacheService to provide fast initial image URLs while triggering
 * background processing for optimal image quality
 * 
 * Key responsibilities:
 * - Providing immediate cover URLs for fast page rendering
 * - Managing fallback URLs when primary sources are unavailable
 * - Coordinating with caching services for improved performance
 * - Supporting different image resolution preferences
 */
package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.types.CoverImages;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;

@Service
public class BookImageOrchestrationService {

    /** Logger for this class */
    private static final Logger logger = LoggerFactory.getLogger(BookImageOrchestrationService.class);
    
    /** Default placeholder image path to use when no cover is available */
    private static final String DEFAULT_PLACEHOLDER_IMAGE = "/images/placeholder-book-cover.svg";

    /** Service for handling cover image caching and background processing */
    private final BookCoverManagementService bookCoverManagementService;

    /** Flag indicating whether S3 storage is enabled */
    @Value("${s3.enabled:true}")
    private boolean s3Enabled;

    /** Flag indicating whether to prefer S3 cached images over other sources */
    @Value("${app.cover-sources.prefer-s3:true}")
    private boolean preferS3;

    /** Directory name for storing cached cover images */
    @Value("${app.cover-cache.dir:covers}")
    private String cacheDirName;


    /**
     * Constructs a new BookImageOrchestrationService with the required dependencies.
     *
     * @param bookCoverManagementService The service for cache management and background processing
     */
    public BookImageOrchestrationService(BookCoverManagementService bookCoverManagementService) {
        this.bookCoverManagementService = bookCoverManagementService;
    }

    /**
     * Asynchronously retrieves the best cover URL for a given book using default preferences.
     * 
     * This method uses CoverImageSource.ANY and ImageResolutionPreference.ANY as the default 
     * preferences, allowing the service to select the optimal source based on availability 
     * and cached data.
     *
     * @param book The book to fetch the cover for
     * @return A CompletableFuture that resolves to the updated book with cover image details
     * @see #getBestCoverUrlAsync(Book, CoverImageSource, ImageResolutionPreference)
     */
    public CompletableFuture<Book> getBestCoverUrlAsync(Book book) {
        return getBestCoverUrlAsync(book, CoverImageSource.ANY, ImageResolutionPreference.ANY);
    }
    
    /**
     * Asynchronously retrieves the best cover URL for a given book from a specified source.
     * 
     * This method allows specifying a preferred source while using the default
     * ImageResolutionPreference.ANY for resolution selection.
     *
     * @param book The book to fetch the cover for
     * @param preferredSource The preferred source to fetch the cover from
     * @return A CompletableFuture that resolves to the updated book with cover image details
     * @see #getBestCoverUrlAsync(Book, CoverImageSource, ImageResolutionPreference)
     */
    public CompletableFuture<Book> getBestCoverUrlAsync(Book book, CoverImageSource preferredSource) {
        return getBestCoverUrlAsync(book, preferredSource, ImageResolutionPreference.ANY);
    }
    
    /**
     * Asynchronously populates the book object with preferred and fallback cover image URLs.
     * 
     * This is the primary implementation method that handles all the logic for fetching
     * and managing book cover images. It performs the following operations:
     * 
     * - Sets the main book.coverImageUrl to the preferred URL
     * - Populates the CoverImages object with both preferred and fallback URLs
     * - Triggers background processing via BookCoverCacheService for optimal image quality
     * 
     * The method prioritizes fast initial response while ensuring high-quality images 
     * are eventually provided through background processing.
     *
     * @param book The book to fetch the cover for
     * @param preferredSource The preferred image source (may be overridden based on availability)
     * @param resolutionPreference The preferred image resolution quality
     * @return A CompletableFuture that resolves to the updated Book object with image details
     * @throws NullPointerException if the BookCoverCacheService returns null results
     */
    public CompletableFuture<Book> getBestCoverUrlAsync(Book book, CoverImageSource preferredSource, ImageResolutionPreference resolutionPreference) {
        if (book == null) {
            logger.warn("Book object is null. Creating a placeholder book structure for cover images.");
            Book placeholderBook = new Book();
            placeholderBook.setId("null-book");
            placeholderBook.setTitle("Unknown Book");
            placeholderBook.setCoverImageUrl(DEFAULT_PLACEHOLDER_IMAGE);
            placeholderBook.setCoverImages(new CoverImages(DEFAULT_PLACEHOLDER_IMAGE, DEFAULT_PLACEHOLDER_IMAGE));
            placeholderBook.setCoverImageWidth(0);
            placeholderBook.setCoverImageHeight(0);
            placeholderBook.setIsCoverHighResolution(false);
            return CompletableFuture.completedFuture(placeholderBook);
        }

        if (book.getId() == null) {
            logger.warn("Book ID is null for book object title: '{}'. Setting defaults and using placeholder for cover images.", book.getTitle());
            // Modify the existing book object
            book.setCoverImageUrl(DEFAULT_PLACEHOLDER_IMAGE);
            book.setCoverImages(new CoverImages(DEFAULT_PLACEHOLDER_IMAGE, DEFAULT_PLACEHOLDER_IMAGE));
            book.setCoverImageWidth(0);
            book.setCoverImageHeight(0);
            book.setIsCoverHighResolution(false);
            return CompletableFuture.completedFuture(book);
        }

        // Capture the original cover URL from the book to use as a fallback
        String originalSourceUrl = book.getCoverImageUrl(); // Assumes this is populated by GoogleBooksService
        if (originalSourceUrl == null || originalSourceUrl.isEmpty()) {
            originalSourceUrl = book.getImageUrl(); // Check imageUrl as another possible field for original
        }
        if (originalSourceUrl == null || originalSourceUrl.isEmpty() || originalSourceUrl.equals(DEFAULT_PLACEHOLDER_IMAGE)) {
            originalSourceUrl = DEFAULT_PLACEHOLDER_IMAGE; 
        }
        final String finalFallbackUrl = originalSourceUrl;

        logger.debug("Fetching initial cover for book {} (ID: {}), preferred source: {}, resolution: {}. Fallback will be: {}", 
            book.getTitle(), book.getId(), preferredSource, resolutionPreference, finalFallbackUrl);

        // Convert Mono<CoverImages> to CompletableFuture
        return bookCoverManagementService.getInitialCoverUrlAndTriggerBackgroundUpdate(book, true)
            .toFuture() // Converts Mono<CoverImages> to CompletableFuture<CoverImages>
            .thenApply(coverImagesResult -> {
                String initialPreferredUrl = (coverImagesResult != null && coverImagesResult.getPreferredUrl() != null) ? coverImagesResult.getPreferredUrl() : DEFAULT_PLACEHOLDER_IMAGE;

                // Populate CoverImages structure
                CoverImageSource sourceFromResult = (coverImagesResult != null && coverImagesResult.getSource() != null) ? coverImagesResult.getSource() : CoverImageSource.UNDEFINED;
                
                if (coverImagesResult == null) {
                    logger.warn("coverImagesResult was null for book ID {}. Using default placeholder.", book.getId());
                    book.setCoverImages(new CoverImages(DEFAULT_PLACEHOLDER_IMAGE, finalFallbackUrl, CoverImageSource.LOCAL_CACHE));
                    book.setCoverImageUrl(DEFAULT_PLACEHOLDER_IMAGE);
                } else {
                    book.setCoverImages(new CoverImages(initialPreferredUrl, finalFallbackUrl, sourceFromResult));
                    book.setCoverImageUrl(initialPreferredUrl);
                }

                // Set resolution details to null for background processing
                book.setCoverImageWidth(null);
                book.setCoverImageHeight(null);
                book.setIsCoverHighResolution(null);

                logger.debug("Book ID {}: CoverImages set - Preferred URL: '{}', Fallback URL: '{}'. Background processing initiated by cache service.",
                             book.getId(), book.getCoverImageUrl(), finalFallbackUrl);
                
                return book;
            })
            .exceptionally(ex -> {
                logger.error("Error processing cover images for book ID {}: {}. Using placeholders.", book.getId(), ex.getMessage(), ex);
                book.setCoverImageUrl(DEFAULT_PLACEHOLDER_IMAGE);
                book.setCoverImages(new CoverImages(DEFAULT_PLACEHOLDER_IMAGE, finalFallbackUrl, CoverImageSource.LOCAL_CACHE));
                book.setCoverImageWidth(null);
                book.setCoverImageHeight(null);
                book.setIsCoverHighResolution(null);
                return book;
            });
    }
}
