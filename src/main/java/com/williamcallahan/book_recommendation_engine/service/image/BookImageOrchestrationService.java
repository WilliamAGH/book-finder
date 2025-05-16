/**
 * Orchestrates the retrieval and management of book cover image URLs.
 * 
 * @author William Callahan
 * 
 * This service coordinates the process of retrieving book cover images from various sources,
 * prioritizing cached images when available before fetching from external services. It works
 * with BookCoverCacheService to provide fast initial image URLs while triggering
 * background processing for optimal image quality.
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
import org.springframework.beans.factory.annotation.Autowired;
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
    private final BookCoverManagementService bookCoverManagementService; // Renamed

    /** Flag indicating whether S3 storage is enabled */
    @Value("${s3.enabled:true}")
    private boolean s3Enabled;

    /** Flag indicating whether to prefer S3 cached images over other sources */
    @Value("${app.cover-sources.prefer-s3:true}")
    private boolean preferS3;

    /** Directory name for storing cached cover images */
    @Value("${app.cover-cache.dir:covers}")
    private String cacheDirName;

    // Removed maxFileSizeBytes as it's primarily used in S3BookCoverService

    /**
     * Constructs a new BookImageOrchestrationService with the required dependencies.
     *
     * @param bookCoverManagementService The service for cache management and background processing
     */
    @Autowired
    public BookImageOrchestrationService(BookCoverManagementService bookCoverManagementService) { // Renamed parameter
        this.bookCoverManagementService = bookCoverManagementService; // Renamed field
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
        // Pass through resolution preference, though it's less critical for this initial setup
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

        // Capture the original cover URL from the book (e.g., from Google Books API) to use as a fallback
        // This should be the URL as initially fetched before any caching logic modifies it
        String originalSourceUrl = book.getCoverImageUrl(); // Assumes this is populated by GoogleBooksService
        if (originalSourceUrl == null || originalSourceUrl.isEmpty()) {
            originalSourceUrl = book.getImageUrl(); // Check imageUrl as another possible field for original
        }
        if (originalSourceUrl == null || originalSourceUrl.isEmpty() || originalSourceUrl.equals(DEFAULT_PLACEHOLDER_IMAGE)) {
            // If no meaningful original URL, use a default placeholder for the fallback as well
            // However, BookCoverCacheService might itself return a placeholder, which is fine
            // The goal is that fallbackUrl is _truly_ the original, or a placeholder if none existed
            originalSourceUrl = DEFAULT_PLACEHOLDER_IMAGE; 
        }
        final String finalFallbackUrl = originalSourceUrl;

        logger.debug("Fetching initial cover for book {} (ID: {}), preferred source: {}, resolution: {}. Fallback will be: {}", 
            book.getTitle(), book.getId(), preferredSource, resolutionPreference, finalFallbackUrl);

        // BookCoverManagementService will provide an initial URL (cached or original) and trigger background processing
        CoverImages coverImagesResult = bookCoverManagementService.getInitialCoverUrlAndTriggerBackgroundUpdate(book); // Use renamed service
        String initialPreferredUrl = (coverImagesResult != null && coverImagesResult.getPreferredUrl() != null) ? coverImagesResult.getPreferredUrl() : DEFAULT_PLACEHOLDER_IMAGE;

        // Now, populate our new CoverImages structure
        // The 'initialPreferredUrl' is what BookCoverManagementService decided is best for now
        // 'finalFallbackUrl' is what it captured as the original
        // Use the source from the coverImagesResult obtained from BookCoverManagementService
        CoverImageSource sourceFromResult = (coverImagesResult != null && coverImagesResult.getSource() != null) ? coverImagesResult.getSource() : CoverImageSource.UNDEFINED;
        book.setCoverImages(new CoverImages(initialPreferredUrl, finalFallbackUrl, sourceFromResult));
        
        // Assuming BookCoverManagementService.getInitialCoverUrlAndTriggerBackgroundUpdate might set book.coverImageUrl directly
        // or we rely on the returned CoverImages.preferredUrl.
        // For clarity, let's ensure book.coverImageUrl is set from the result here.
        book.setCoverImageUrl(initialPreferredUrl);


        // Explicitly set resolution details to null. These will be updated by background processing
        // once the actual image is fetched and analyzed by BookCoverCacheService or S3BookCoverService
        book.setCoverImageWidth(null);
        book.setCoverImageHeight(null);
        book.setIsCoverHighResolution(null);

        logger.debug("Book ID {}: CoverImages set - Preferred URL: '{}', Fallback URL: '{}'. Background processing initiated by cache service.",
                     book.getId(), initialPreferredUrl, finalFallbackUrl);
        
        return CompletableFuture.completedFuture(book);
    }
}
