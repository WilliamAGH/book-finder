package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.EnvironmentService;
import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.CoverImages;
import com.williamcallahan.book_recommendation_engine.types.ImageDetails;
import com.williamcallahan.book_recommendation_engine.types.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.util.ImageCacheUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
// import reactor.core.scheduler.Schedulers; // Removed unused import

import java.nio.file.Files; // Added import
import java.nio.file.Path;   // Added import
import java.nio.file.Paths;  // Added import

/**
 * Orchestrates the retrieval, caching, and background updating of book cover images
 * - Provides initial cover URLs quickly for UI rendering
 * - Manages background processes to find and cache the best quality images from various sources
 * - Coordinates with local disk cache, S3 storage, and in-memory caches
 *
 * @author William Callahan
 */
@Service
public class BookCoverManagementService {

    private static final Logger logger = LoggerFactory.getLogger(BookCoverManagementService.class);

    @Value("${app.cover-cache.enabled:true}")
    private boolean cacheEnabled;
    
    // CDN URL for S3-stored cover images
    @Value("${s3.cdn-url}")
    private String s3CdnUrl;

    @Value("${s3.public-cdn-url:${S3_PUBLIC_CDN_URL:}}")
    private String s3PublicCdnUrl;


    private final CoverCacheManager coverCacheManager;
    private final CoverSourceFetchingService coverSourceFetchingService;
    private final S3BookCoverService s3BookCoverService; // Enhanced version
    private final LocalDiskCoverCacheService localDiskCoverCacheService; // For placeholder path
    private final ApplicationEventPublisher eventPublisher;
    private final EnvironmentService environmentService; // For debug mode checks

    /**
     * Constructs the BookCoverManagementService
     * @param coverCacheManager Manager for in-memory caches
     * @param coverSourceFetchingService Service for fetching covers from various sources
     * @param s3BookCoverService Service for S3 interactions (enhanced)
     * @param localDiskCoverCacheService Service for local disk cache operations
     * @param eventPublisher Publisher for application events
     * @param environmentService Service for environment-specific configurations
     */
    @Autowired
    public BookCoverManagementService(
            CoverCacheManager coverCacheManager,
            CoverSourceFetchingService coverSourceFetchingService,
            S3BookCoverService s3BookCoverService,
            LocalDiskCoverCacheService localDiskCoverCacheService,
            ApplicationEventPublisher eventPublisher,
            EnvironmentService environmentService) {
        this.coverCacheManager = coverCacheManager;
        this.coverSourceFetchingService = coverSourceFetchingService;
        this.s3BookCoverService = s3BookCoverService;
        this.localDiskCoverCacheService = localDiskCoverCacheService;
        this.eventPublisher = eventPublisher;
        this.environmentService = environmentService;
    }

    /**
     * Gets the initial cover URL for immediate display and triggers background processing for updates
     * - Checks S3, final, and provisional caches for an existing image
     * - Uses book's existing cover URL or a placeholder if no cached version is found
     * - Initiates an asynchronous background process to find and cache the best quality cover
     * @param book The book to retrieve the cover image for
     * @return CoverImages object containing preferred and fallback URLs, and the source
     */
    public CoverImages getInitialCoverUrlAndTriggerBackgroundUpdate(Book book) {
        CoverImages result = new CoverImages();
        result.setSource(CoverImageSource.UNDEFINED);
        String localPlaceholderPath = localDiskCoverCacheService.getLocalPlaceholderPath();

        if (!cacheEnabled) {
            logger.warn("Cover caching is disabled, returning placeholder for book ID: {}", book != null ? book.getId() : "null");
            result.setPreferredUrl(localPlaceholderPath);
            result.setFallbackUrl(localPlaceholderPath);
            result.setSource(CoverImageSource.LOCAL_CACHE);
            return result;
        }

        if (book == null || (book.getIsbn13() == null && book.getIsbn10() == null && book.getId() == null)) {
            logger.warn("Book or all relevant identifiers are null. Cannot process for initial cover URL");
            result.setPreferredUrl(localPlaceholderPath);
            result.setFallbackUrl(localPlaceholderPath);
            result.setSource(CoverImageSource.LOCAL_CACHE);
            return result;
        }

        String identifierKey = ImageCacheUtils.getIdentifierKey(book);
        if (identifierKey == null) {
            logger.warn("Could not determine a valid identifierKey for book with ID: {}. Returning placeholder", book.getId());
            result.setPreferredUrl(localPlaceholderPath);
            result.setFallbackUrl(localPlaceholderPath);
            result.setSource(CoverImageSource.LOCAL_CACHE);
            return result;
        }

        // 1. Check S3 (via S3BookCoverService which might have its own internal cache for S3 object existence)
        // S3BookCoverService.fetchCover should ideally return ImageDetails with an S3 URL if found.
        try {
            // s3BookCoverService.fetchCover now returns CompletableFuture.
            // The blocking S3 SDK calls within its reactive chain are already on Schedulers.boundedElastic().
            ImageDetails s3Details = s3BookCoverService.fetchCover(book).join(); // Use join() for CompletableFuture
            if (s3Details != null && s3Details.getUrlOrPath() != null && s3Details.getCoverImageSource() == CoverImageSource.S3_CACHE) {
                logger.debug("Initial check: Found S3 cover for identifier {}: {}", identifierKey, s3Details.getUrlOrPath());
                result.setPreferredUrl(s3Details.getUrlOrPath());
                result.setSource(CoverImageSource.S3_CACHE);
                result.setFallbackUrl((book.getCoverImageUrl() != null && !book.getCoverImageUrl().equals(localPlaceholderPath)) ? book.getCoverImageUrl() : localPlaceholderPath);
                coverCacheManager.putFinalImageDetails(identifierKey, s3Details); // Cache S3 details as final
                return result;
            }
        } catch (Exception e) {
            logger.warn("Error during initial S3 check for identifier {}: {}", identifierKey, e.getMessage());
            // Proceed to other caches
        }
        
        // 2. Check final in-memory cache (already processed and best image known)
        ImageDetails finalCachedImageDetails = coverCacheManager.getFinalImageDetails(identifierKey);
        if (finalCachedImageDetails != null && finalCachedImageDetails.getUrlOrPath() != null) {
            logger.debug("Returning final cached ImageDetails for identifierKey {}: Path: {}, Source: {}",
                identifierKey, finalCachedImageDetails.getUrlOrPath(), finalCachedImageDetails.getCoverImageSource());
            result.setPreferredUrl(finalCachedImageDetails.getUrlOrPath());
            result.setSource(finalCachedImageDetails.getCoverImageSource() != null ? finalCachedImageDetails.getCoverImageSource() : CoverImageSource.UNDEFINED);
            result.setFallbackUrl(determineFallbackUrl(book, finalCachedImageDetails.getUrlOrPath(), localPlaceholderPath));
            return result;
        }

        // 3. Check provisional in-memory cache
        String provisionalUrl = coverCacheManager.getProvisionalUrl(identifierKey);
        String urlToUseAsPreferred;
        CoverImageSource inferredProvisionalSource = CoverImageSource.UNDEFINED;

        if (provisionalUrl != null) {
            logger.debug("Returning provisional cached URL for identifierKey {}: {}", identifierKey, provisionalUrl);
            urlToUseAsPreferred = provisionalUrl;
            inferredProvisionalSource = inferSourceFromUrl(provisionalUrl, localPlaceholderPath);
        } else {
            // 4. Use book's existing cover URL if available and not placeholder
            if (book.getCoverImageUrl() != null && !book.getCoverImageUrl().isEmpty() && !book.getCoverImageUrl().equals(localPlaceholderPath)) {
                urlToUseAsPreferred = book.getCoverImageUrl();
                logger.debug("Using existing coverImageUrl from book object as provisional for identifierKey {}: {}", identifierKey, urlToUseAsPreferred);
                inferredProvisionalSource = inferSourceFromUrl(urlToUseAsPreferred, localPlaceholderPath);
            } else {
                // 5. Fallback to placeholder
                urlToUseAsPreferred = localPlaceholderPath;
                inferredProvisionalSource = CoverImageSource.LOCAL_CACHE;
                logger.debug("No provisional URL for identifierKey {}, will use placeholder and process in background", identifierKey);
            }
            coverCacheManager.putProvisionalUrl(identifierKey, urlToUseAsPreferred);
        }
        
        result.setPreferredUrl(urlToUseAsPreferred);
        result.setSource(inferredProvisionalSource);
        result.setFallbackUrl(determineFallbackUrl(book, urlToUseAsPreferred, localPlaceholderPath));

        // Trigger background processing
        processCoverInBackground(book, urlToUseAsPreferred.equals(localPlaceholderPath) ? null : urlToUseAsPreferred);
        return result;
    }

    /**
     * Infers the CoverImageSource from a given URL string
     * @param url The URL to infer source from
     * @param localPlaceholderPath Path to the local placeholder image
     * @return The inferred CoverImageSource
     */
    private CoverImageSource inferSourceFromUrl(String url, String localPlaceholderPath) {
        if (url == null || url.isEmpty()) return CoverImageSource.UNDEFINED;
        if (url.equals(localPlaceholderPath)) return CoverImageSource.LOCAL_CACHE;
        if (url.startsWith("/" + localDiskCoverCacheService.getCacheDirName())) return CoverImageSource.LOCAL_CACHE;
        if (url.contains("googleapis.com/books") || url.contains("books.google.com/books")) return CoverImageSource.GOOGLE_BOOKS;
        if (url.contains("openlibrary.org")) return CoverImageSource.OPEN_LIBRARY;
        if (url.contains("longitood.com")) return CoverImageSource.LONGITOOD;
        if (url.contains(s3CdnUrl) || (s3PublicCdnUrl != null && !s3PublicCdnUrl.isEmpty() && url.contains(s3PublicCdnUrl))) return CoverImageSource.S3_CACHE;
        return CoverImageSource.ANY; // Default if no specific source is identified
    }
    
    /**
     * Determines the appropriate fallback URL
     * @param book The book object
     * @param preferredUrl The preferred URL that was selected
     * @param localPlaceholderPath Path to the local placeholder image
     * @return The fallback URL string
     */
    private String determineFallbackUrl(Book book, String preferredUrl, String localPlaceholderPath) {
        String bookOriginalCover = book.getCoverImageUrl();
        if (preferredUrl.equals(localPlaceholderPath)) {
            // If preferred is placeholder, use original book cover if it's valid and not placeholder
            if (bookOriginalCover != null && !bookOriginalCover.isEmpty() && !bookOriginalCover.equals(localPlaceholderPath)) {
                return bookOriginalCover;
            }
        } else {
            // If preferred is not placeholder, use original book cover if it's different and valid
            if (bookOriginalCover != null && !bookOriginalCover.isEmpty() && !bookOriginalCover.equals(preferredUrl)) {
                return bookOriginalCover;
            }
        }
        return localPlaceholderPath; // Default fallback
    }

    /**
     * Asynchronously processes the book cover in the background to find the best quality version
     * - Delegates to CoverSourceFetchingService to find the best image
     * - If a better local image is found, triggers an S3 upload via S3BookCoverService
     * - Updates in-memory caches (final and provisional)
     * - Publishes a BookCoverUpdatedEvent
     * @param book The book to find the cover image for
     * @param provisionalUrlHint A hint URL that might have been used for initial display
     */
    @Async
    public void processCoverInBackground(Book book, String provisionalUrlHint) {
        if (!cacheEnabled || book == null) {
            logger.debug("Background processing skipped: cache disabled or book is null");
            return;
        }

        ImageProvenanceData provenanceData = new ImageProvenanceData();
        String identifierKey = ImageCacheUtils.getIdentifierKey(book);
        if (identifierKey == null) {
            logger.warn("Background: Could not determine identifierKey for book with ID: {}. Aborting", book.getId());
            return;
        }
        final String bookIdForLog = book.getId() != null ? book.getId() : identifierKey;
        provenanceData.setBookId(bookIdForLog);

        logger.info("Background: Starting full cover processing for identifierKey: {}, Book ID: {}, Title: {}",
            identifierKey, bookIdForLog, book.getTitle());

        coverSourceFetchingService.getBestCoverImageUrlAsync(book, provisionalUrlHint, provenanceData)
            .thenAcceptAsync(finalImageDetails -> {
                if (finalImageDetails == null || finalImageDetails.getUrlOrPath() == null || 
                    finalImageDetails.getUrlOrPath().equals(localDiskCoverCacheService.getLocalPlaceholderPath())) {
                    
                    logger.warn("Background: Final processing for {} (BookID {}) yielded placeholder or null. Final cache updated with placeholder", identifierKey, bookIdForLog);
                    ImageDetails placeholderDetails = localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "background-fetch-failed");
                    coverCacheManager.putFinalImageDetails(identifierKey, placeholderDetails);
                    coverCacheManager.invalidateProvisionalUrl(identifierKey); // Remove potentially misleading provisional URL
                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, placeholderDetails.getUrlOrPath(), book.getId(), placeholderDetails.getCoverImageSource()));
                    // Log provenance if debug mode is on
                    if (environmentService.isBookCoverDebugMode()) {
                         // Consider logging provenanceData to a file or separate system if it's large/complex
                        logger.info("Background Provenance (Placeholder) for {}: {}", identifierKey, provenanceData.toString());
                    }
                    return;
                }

                // A valid image was found
                logger.info("Background: Best image found for {} (BookID {}): URL/Path: {}, Source: {}",
                    identifierKey, bookIdForLog, finalImageDetails.getUrlOrPath(), finalImageDetails.getCoverImageSource());

                // If the best image is locally cached (not from S3 directly), try to upload it to S3
                if (finalImageDetails.getCoverImageSource() != CoverImageSource.S3_CACHE &&
                    finalImageDetails.getUrlOrPath().startsWith("/" + localDiskCoverCacheService.getCacheDirName())) {
                    
                    logger.info("Background: Image for {} (BookID {}) is locally cached from {}. Triggering S3 upload",
                        identifierKey, bookIdForLog, finalImageDetails.getCoverImageSource());
                    
                    // S3BookCoverService's enhanced method will handle processing and upload
                    // It should return the new S3 ImageDetails upon success
                    // We need to read the file bytes first
                    try {
                        Path localImagePath = Paths.get(localDiskCoverCacheService.getCacheDirString(), finalImageDetails.getUrlOrPath().substring(("/" + localDiskCoverCacheService.getCacheDirName() + "/").length()));
                        byte[] imageBytes = Files.readAllBytes(localImagePath);
                        String fileExtension = ImageCacheUtils.getFileExtensionFromUrl(finalImageDetails.getUrlOrPath());
                        // MimeType determined based on file extension
                        // Width and height values from finalImageDetails
                        Integer width = finalImageDetails.getWidth() != null ? finalImageDetails.getWidth() : 0;
                        Integer height = finalImageDetails.getHeight() != null ? finalImageDetails.getHeight() : 0;

                        s3BookCoverService.uploadProcessedCoverToS3Async(
                            imageBytes,
                            fileExtension,
                            null, // MimeType - S3BookCoverService's imageProcessingService should handle this
                            width,
                            height,
                            bookIdForLog, // bookId for S3 key generation
                            finalImageDetails.getSourceName(), // original source for S3 key
                            provenanceData // Pass provenance data along
                        )
                        .doOnSuccess(s3UploadedDetails -> { // Use doOnSuccess for side effects with Mono
                            if (s3UploadedDetails != null && s3UploadedDetails.getCoverImageSource() == CoverImageSource.S3_CACHE) {
                                logger.info("Background: Successfully uploaded to S3 for {}. New S3 URL: {}. Updating final cache and publishing event",
                                    identifierKey, s3UploadedDetails.getUrlOrPath());
                                coverCacheManager.putFinalImageDetails(identifierKey, s3UploadedDetails);
                                eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, s3UploadedDetails.getUrlOrPath(), book.getId(), CoverImageSource.S3_CACHE));
                            } else {
                                // S3 upload failed or didn't result in S3_CACHE, stick with the locally found best image
                                logger.warn("Background: S3 upload failed or did not return S3_CACHE for {}. Using locally found best image: {}", identifierKey, finalImageDetails.getUrlOrPath());
                                coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                                eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), finalImageDetails.getCoverImageSource()));
                            }
                        })
                        .doOnError(s3Ex -> { // Handle errors from the S3 upload Mono
                            logger.error("Background: Exception during S3 upload chain for {}: {}. Using locally found best image.", identifierKey, s3Ex.getMessage(), s3Ex);
                            coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails); // Fallback to local
                            eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), finalImageDetails.getCoverImageSource()));
                        })
                        .subscribe(); // Subscribe to trigger the Mono execution
                    } catch (java.io.IOException e) {
                        logger.error("Background: Failed to read local image file {} for S3 upload for BookID {}: {}", 
                            finalImageDetails.getUrlOrPath(), bookIdForLog, e.getMessage());
                        // Fallback to using the local details if reading fails
                        coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                        eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), finalImageDetails.getCoverImageSource()));
                    }
                } else {
                    // Image is already from S3 or not a local cache candidate for S3 upload, just update cache and publish
                    coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), finalImageDetails.getCoverImageSource()));
                }
                
                coverCacheManager.invalidateProvisionalUrl(identifierKey); // Clean up provisional cache

                if (environmentService.isBookCoverDebugMode()) {
                    logger.info("Background Provenance (Success) for {}: {}", identifierKey, provenanceData.toString());
                }

            }, java.util.concurrent.ForkJoinPool.commonPool()) // Use a common pool for CPU-bound tasks after IO
            .exceptionally(ex -> {
                logger.error("Background: Top-level exception in processCoverInBackground for {} (BookID {}): {}",
                    identifierKey, bookIdForLog, ex.getMessage(), ex);
                // Ensure caches are cleaned up or set to placeholder on top-level failure
                coverCacheManager.invalidateProvisionalUrl(identifierKey);
                coverCacheManager.putFinalImageDetails(identifierKey, localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "background-exception"));
                eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, localDiskCoverCacheService.getLocalPlaceholderPath(), book.getId(), CoverImageSource.LOCAL_CACHE));
                return null;
            });
    }
}
