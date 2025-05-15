/**
 * Orchestrates the retrieval of book cover image URLs.
 * Prioritizes fetching from S3 cache, then delegates to BookCoverCacheService
 * to provide a fast initial URL and trigger background processing for optimal image quality.
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

    private static final Logger logger = LoggerFactory.getLogger(BookImageOrchestrationService.class);
    private static final String DEFAULT_PLACEHOLDER_IMAGE = "/images/placeholder-book-cover.svg";

    // Removed S3BookCoverService field as it's no longer directly used here
    // private final S3BookCoverService s3BookCoverService;
    private final BookCoverCacheService bookCoverCacheService;

    // s3Enabled, preferS3, cacheDirName, maxFileSizeBytes are related to S3/cache behavior configuration that might still be relevant
    // for logging or decisions, or simply passed down. If BookCoverCacheService now fully owns S3 logic,
    // these might only be relevant there. For now, keeping them unless further refactoring is done.
    @Value("${s3.enabled:true}")
    private boolean s3Enabled;

    @Value("${app.cover-sources.prefer-s3:true}")
    private boolean preferS3;

    @Value("${app.cover-cache.dir:covers}")
    private String cacheDirName;

    @Value("${app.cover-cache.max-file-size-bytes:5242880}") // 5MB default
    private long maxFileSizeBytes; // Potentially used by BookCoverCacheService or S3 service

    @Autowired
    public BookImageOrchestrationService(BookCoverCacheService bookCoverCacheService) { // Removed S3BookCoverService from constructor
        // this.s3BookCoverService = s3BookCoverService;
        this.bookCoverCacheService = bookCoverCacheService;
    }

    /**
     * Asynchronously retrieves the best cover URL for a given book
     *
     * @param book The book to fetch the cover for
     * @return A CompletableFuture that resolves to the updated book with cover image details
     */
    public CompletableFuture<Book> getBestCoverUrlAsync(Book book) {
        return getBestCoverUrlAsync(book, CoverImageSource.ANY, ImageResolutionPreference.ANY);
    }
    
    /**
     * Asynchronously retrieves the best cover URL for a given book from a specified source
     *
     * @param book The book to fetch the cover for
     * @param preferredSource The preferred source to fetch the cover from
     * @return A CompletableFuture that resolves to the updated book with cover image details
     */
    public CompletableFuture<Book> getBestCoverUrlAsync(Book book, CoverImageSource preferredSource) {
        // Pass through resolution preference, though it's less critical for this initial setup
        return getBestCoverUrlAsync(book, preferredSource, ImageResolutionPreference.ANY);
    }
    
    /**
     * Asynchronously populates the book object with preferred and fallback cover image URLs
     * The main book.coverImageUrl will be set to the preferred URL
     * Background processing for optimal image caching is triggered by BookCoverCacheService
     *
     * @param book The book to fetch the cover for
     * @param preferredSource The preferred source to fetch the cover from (currently less emphasized in this new logic for initial URL)
     * @param resolutionPreference The preferred resolution quality (currently less emphasized for initial URL)
     * @return A CompletableFuture that resolves to the updated Book object
     */
    public CompletableFuture<Book> getBestCoverUrlAsync(Book book, CoverImageSource preferredSource, ImageResolutionPreference resolutionPreference) {
        if (book == null) {
            logger.warn("Book object is null. Creating a placeholder book structure for cover images.");
            Book placeholderBook = new Book(); // Basic placeholder
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

        // REMOVED S3 Pre-check block. S3 interaction is handled by BookCoverCacheService in background

        logger.debug("Fetching initial cover for book {} (ID: {}), preferred source: {}, resolution: {}. Fallback will be: {}", 
            book.getTitle(), book.getId(), preferredSource, resolutionPreference, finalFallbackUrl);

        // BookCoverCacheService will provide an initial URL (cached or original) and trigger background processing
        CoverImages coverImagesResult = bookCoverCacheService.getInitialCoverUrlAndTriggerBackgroundUpdate(book);
        String initialPreferredUrl = (coverImagesResult != null && coverImagesResult.getPreferredUrl() != null) ? coverImagesResult.getPreferredUrl() : DEFAULT_PLACEHOLDER_IMAGE;

        // Now, populate our new CoverImages structure
        // The 'initialPreferredUrl' is what BookCoverCacheService decided is best for now
        // 'finalFallbackUrl' is what we captured as the original
        // Use the source from the coverImagesResult obtained from BookCoverCacheService
        CoverImageSource sourceFromResult = (coverImagesResult != null && coverImagesResult.getSource() != null) ? coverImagesResult.getSource() : CoverImageSource.UNDEFINED;
        book.setCoverImages(new CoverImages(initialPreferredUrl, finalFallbackUrl, sourceFromResult));
        
        // book.setCoverImageUrl() is already updated by bookCoverCacheService.getInitialCoverUrlAndTriggerBackgroundUpdate()
        // So, the top-level coverImageUrl will reflect the preferred URL

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
