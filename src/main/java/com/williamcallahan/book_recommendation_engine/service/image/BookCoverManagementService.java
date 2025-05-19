/**
 * Orchestrates the retrieval, caching, and background updating of book cover images
 *
 * @author William Callahan
 *
 * Features:
 * - Provides initial cover URLs quickly for UI rendering
 * - Manages background processes to find and cache best quality images from various sources
 * - Coordinates with local disk cache, S3 storage, and in-memory caches
 * - Publishes events when cover images are updated
 * - Implements optimized caching strategy with fallback mechanisms
 */
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
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private final S3BookCoverService s3BookCoverService;
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
     * Creates placeholder cover images when actual covers cannot be found
     * 
     * @param bookIdForLog identifier for logging purposes
     * @return CoverImages object with placeholder paths
     */
    private CoverImages createPlaceholderCoverImages(String bookIdForLog) {
        CoverImages placeholder = new CoverImages();
        String localPlaceholderPath = localDiskCoverCacheService.getLocalPlaceholderPath();
        placeholder.setPreferredUrl(localPlaceholderPath);
        placeholder.setFallbackUrl(localPlaceholderPath);
        placeholder.setSource(CoverImageSource.LOCAL_CACHE);
        logger.warn("Returning placeholder for book ID: {}", bookIdForLog);
        return placeholder;
    }

    /**
     * Gets the initial cover URL for immediate display and triggers background processing for updates
     * - Checks S3, final, and provisional caches for an existing image
     * - Uses book's existing cover URL or a placeholder if no cached version is found
     * - Initiates an asynchronous background process to find and cache the best quality cover
     * @param book The book to retrieve the cover image for
     * @return Mono<CoverImages> object containing preferred and fallback URLs, and the source
     */
    public Mono<CoverImages> getInitialCoverUrlAndTriggerBackgroundUpdate(Book book) {
        String localPlaceholderPath = localDiskCoverCacheService.getLocalPlaceholderPath();

        if (!cacheEnabled) {
            return Mono.just(createPlaceholderCoverImages(book != null ? book.getId() : "null (cache disabled)"));
        }

        if (book == null || (book.getIsbn13() == null && book.getIsbn10() == null && book.getId() == null)) {
            return Mono.just(createPlaceholderCoverImages("null (book or identifiers null)"));
        }

        String identifierKey = ImageCacheUtils.getIdentifierKey(book);
        if (identifierKey == null) {
            return Mono.just(createPlaceholderCoverImages(book.getId() + " (identifierKey null)"));
        }

        // 1. Check S3 (via S3BookCoverService which might have its own internal cache for S3 object existence)
        // Convert CompletableFuture to Mono
        return Mono.fromFuture(s3BookCoverService.fetchCover(book)) // This now returns CompletableFuture<Optional<ImageDetails>>
            .flatMap(imageDetailsOptionalFromS3 -> { // imageDetailsOptionalFromS3 is Optional<ImageDetails>
                if (imageDetailsOptionalFromS3.isPresent()) {
                    ImageDetails imageDetailsFromS3 = imageDetailsOptionalFromS3.get();
                    if (imageDetailsFromS3.getUrlOrPath() != null && imageDetailsFromS3.getCoverImageSource() == CoverImageSource.S3_CACHE) {
                        logger.debug("Initial check: Found S3 cover for identifier {}: {}", identifierKey, imageDetailsFromS3.getUrlOrPath());
                        CoverImages s3Result = new CoverImages();
                        s3Result.setPreferredUrl(imageDetailsFromS3.getUrlOrPath());
                        s3Result.setSource(CoverImageSource.S3_CACHE);
                        s3Result.setFallbackUrl((book.getCoverImageUrl() != null && !book.getCoverImageUrl().equals(localPlaceholderPath)) ? book.getCoverImageUrl() : localPlaceholderPath);
                        coverCacheManager.putFinalImageDetails(identifierKey, imageDetailsFromS3); // Cache S3 ImageDetails
                        return Mono.just(s3Result);
                    }
                    logger.debug("S3 check: Optional<ImageDetails> was present but content not valid S3 cache for identifier {}: {}", identifierKey, imageDetailsFromS3);
                } else {
                     logger.debug("S3 check: Optional<ImageDetails> was empty for identifier {}", identifierKey);
                }
                // S3 miss, invalid details, or empty Optional, proceed to check other caches
                return checkMemoryCachesAndDefaults(book, identifierKey, localPlaceholderPath);
            })
            .onErrorResume(e -> { // This catches errors from s3BookCoverService.fetchCover(book) or the flatMap processing
                logger.warn("Error during S3 fetch or processing for identifier {}: {}", identifierKey, e.getMessage());
                // Error in S3 fetch or its processing, proceed to check other caches
                return checkMemoryCachesAndDefaults(book, identifierKey, localPlaceholderPath);
            })
            .defaultIfEmpty(createPlaceholderCoverImages(book.getId() + " (all checks failed or resulted in empty)"));
    }

    /**
     * Checks memory caches for cover image details and falls back to default options
     * 
     * @param book book object to find cover for
     * @param identifierKey cache key for lookups
     * @param localPlaceholderPath path to placeholder image
     * @return Mono with best available cover images
     */
    private Mono<CoverImages> checkMemoryCachesAndDefaults(Book book, String identifierKey, String localPlaceholderPath) {
        // Check final in-memory cache
        ImageDetails finalCachedImageDetails = coverCacheManager.getFinalImageDetails(identifierKey);
        if (finalCachedImageDetails != null && finalCachedImageDetails.getUrlOrPath() != null) {
            logger.debug("Returning final cached ImageDetails for identifierKey {}: Path: {}, Source: {}",
                identifierKey, finalCachedImageDetails.getUrlOrPath(), finalCachedImageDetails.getCoverImageSource());
            CoverImages finalCacheResult = new CoverImages();
            finalCacheResult.setPreferredUrl(finalCachedImageDetails.getUrlOrPath());
            finalCacheResult.setSource(finalCachedImageDetails.getCoverImageSource() != null ? finalCachedImageDetails.getCoverImageSource() : CoverImageSource.UNDEFINED);
            finalCacheResult.setFallbackUrl(determineFallbackUrl(book, finalCachedImageDetails.getUrlOrPath(), localPlaceholderPath));
            return Mono.just(finalCacheResult);
        }

        // Check provisional or use book's URL or placeholder
        CoverImages provisionalResult = new CoverImages();
        String provisionalUrl = coverCacheManager.getProvisionalUrl(identifierKey);
        String urlToUseAsPreferred;
        CoverImageSource inferredProvisionalSource;

        if (provisionalUrl != null) {
            urlToUseAsPreferred = provisionalUrl;
            inferredProvisionalSource = inferSourceFromUrl(provisionalUrl, localPlaceholderPath);
        } else {
            if (book.getCoverImageUrl() != null && !book.getCoverImageUrl().isEmpty() && !book.getCoverImageUrl().equals(localPlaceholderPath)) {
                urlToUseAsPreferred = book.getCoverImageUrl();
                inferredProvisionalSource = inferSourceFromUrl(urlToUseAsPreferred, localPlaceholderPath);
            } else {
                urlToUseAsPreferred = localPlaceholderPath;
                inferredProvisionalSource = CoverImageSource.LOCAL_CACHE;
            }
            coverCacheManager.putProvisionalUrl(identifierKey, urlToUseAsPreferred);
        }
        
        provisionalResult.setPreferredUrl(urlToUseAsPreferred);
        provisionalResult.setSource(inferredProvisionalSource);
        provisionalResult.setFallbackUrl(determineFallbackUrl(book, urlToUseAsPreferred, localPlaceholderPath));
        
        // Trigger background processing
        processCoverInBackground(book, urlToUseAsPreferred.equals(localPlaceholderPath) ? null : urlToUseAsPreferred);
        return Mono.just(provisionalResult);
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
                String localPlaceholderPath = localDiskCoverCacheService.getLocalPlaceholderPath();
                if (finalImageDetails == null || finalImageDetails.getUrlOrPath() == null || 
                    finalImageDetails.getUrlOrPath().equals(localPlaceholderPath)) {
                    
                    logger.warn("Background: Final processing for {} (BookID {}) yielded placeholder or null. Final cache updated with placeholder", identifierKey, bookIdForLog);
                    ImageDetails placeholderDetails = localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "background-fetch-failed");
                    
                    if (placeholderDetails == null) {
                        // As per review suggestion, throw an exception instead of silently returning
                        logger.error("CRITICAL: localDiskCoverCacheService.createPlaceholderImageDetails returned null for BookID {}. Throwing exception.", bookIdForLog);
                        if (identifierKey != null) {
                            coverCacheManager.invalidateProvisionalUrl(identifierKey); // Attempt to clean up provisional URL
                        }
                        throw new IllegalStateException("createPlaceholderImageDetails returned null for BookID " + bookIdForLog + " with identifierKey " + identifierKey);
                    }

                    coverCacheManager.putFinalImageDetails(identifierKey, placeholderDetails);
                    coverCacheManager.invalidateProvisionalUrl(identifierKey);
                    // Ensure placeholderDetails and its getUrlOrPath() are not null before publishing event
                    if (placeholderDetails.getUrlOrPath() != null && placeholderDetails.getCoverImageSource() != null) {
                        eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, placeholderDetails.getUrlOrPath(), book.getId(), placeholderDetails.getCoverImageSource()));
                    } else {
                        logger.error("CRITICAL: placeholderDetails has null URL or Source for BookID {}. Event not published.", bookIdForLog);
                    }
                    if (environmentService.isBookCoverDebugMode()) {
                        logger.info("Background Provenance (Placeholder) for {}: {}", identifierKey, provenanceData.toString());
                    }
                    return;
                }

                logger.info("Background: Best image found for {} (BookID {}): URL/Path: {}, Source: {}",
                    identifierKey, bookIdForLog, finalImageDetails.getUrlOrPath(), finalImageDetails.getCoverImageSource());

                if (finalImageDetails.getCoverImageSource() != CoverImageSource.S3_CACHE &&
                    finalImageDetails.getUrlOrPath() != null &&
                    finalImageDetails.getUrlOrPath().startsWith("/" + localDiskCoverCacheService.getCacheDirName())) {
                    
                    logger.info("Background: Image for {} (BookID {}) is locally cached from {}. Triggering S3 upload",
                        identifierKey, bookIdForLog, finalImageDetails.getCoverImageSource());
                    
                    try {
                        Path cacheDir = Paths.get(localDiskCoverCacheService.getCacheDirString());
                        // Ensure finalImageDetails.getUrlOrPath() is not null before creating Path
                        Path relativeImagePath = Paths.get(finalImageDetails.getUrlOrPath()).getFileName(); 
                        Path localImagePath = cacheDir.resolve(relativeImagePath);

                        if (!Files.exists(localImagePath)) {
                            logger.warn("Background: Local image {} does not exist for BookID {}, cannot upload to S3. Using local details.", localImagePath, bookIdForLog);
                            coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                            eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), finalImageDetails.getCoverImageSource()));
                        } else {
                            byte[] imageBytes = Files.readAllBytes(localImagePath);
                            String fileExtension = ImageCacheUtils.getFileExtensionFromUrl(finalImageDetails.getUrlOrPath());
                            Integer width = finalImageDetails.getWidth() != null ? finalImageDetails.getWidth() : 0;
                            Integer height = finalImageDetails.getHeight() != null ? finalImageDetails.getHeight() : 0;

                            s3BookCoverService.uploadProcessedCoverToS3Async(
                                imageBytes, fileExtension, null, width, height,
                                bookIdForLog, finalImageDetails.getSourceName(), provenanceData
                            )
                            .doOnSuccess(s3UploadedDetails -> {
                                if (s3UploadedDetails != null && s3UploadedDetails.getCoverImageSource() == CoverImageSource.S3_CACHE) {
                                    logger.info("Background: Successfully uploaded to S3 for {}. New S3 URL: {}. Updating final cache.",
                                        identifierKey, s3UploadedDetails.getUrlOrPath());
                                    coverCacheManager.putFinalImageDetails(identifierKey, s3UploadedDetails);
                                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, s3UploadedDetails.getUrlOrPath(), book.getId(), CoverImageSource.S3_CACHE));
                                } else {
                                    logger.warn("Background: S3 upload failed or didn't return S3_CACHE for {}. Using local: {}", identifierKey, finalImageDetails.getUrlOrPath());
                                    coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), finalImageDetails.getCoverImageSource()));
                                }
                            })
                            .doOnError(s3Ex -> {
                                logger.error("Background: Exception in S3 upload chain for {}: {}. Using local.", identifierKey, s3Ex.getMessage(), s3Ex);
                                coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                                eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), finalImageDetails.getCoverImageSource()));
                            })
                            .subscribe(); // This makes the S3 upload non-blocking
                            // The logic after this if/else will execute immediately if S3 path is taken.
                            // No explicit return here, so flow continues.
                        }
                    } catch (java.io.IOException e) {
                        logger.error("Background: IOException for local image {} for S3 upload (BookID {}): {}", 
                            finalImageDetails.getUrlOrPath(), bookIdForLog, e.getMessage());
                        coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                        eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), finalImageDetails.getCoverImageSource()));
                    }
                    // If S3 upload path was taken, it will return here to avoid the redundant cache update below
                    // The S3 callbacks will handle their own cache updates
                    return;
                } else { // Not S3 cache and not a local disk cache candidate for S3 upload
                    logger.info("Background: Image for {} (BookID {}) is not a local cache candidate for S3 upload (Source: {}, Path: {}). Using details as is.",
                        identifierKey, bookIdForLog, finalImageDetails.getCoverImageSource(), finalImageDetails.getUrlOrPath());
                    coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), finalImageDetails.getCoverImageSource()));
                    // Explicit return after handling this case
                    return;
                }
            }, java.util.concurrent.ForkJoinPool.commonPool())
            .exceptionally(ex -> {
                logger.error("Background: Top-level exception in processCoverInBackground for identifierKey='{}', BookID='{}': {}",
                    identifierKey,
                    bookIdForLog,
                    ex.getMessage(), ex);

                coverCacheManager.invalidateProvisionalUrl(identifierKey); // identifierKey should be non-null here due to earlier check

                ImageDetails placeholderOnError = localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "background-exception");

                coverCacheManager.putFinalImageDetails(identifierKey, placeholderOnError);
                if (placeholderOnError != null && placeholderOnError.getUrlOrPath() != null && placeholderOnError.getCoverImageSource() != null) {
                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey,
                                                                      placeholderOnError.getUrlOrPath(),
                                                                      bookIdForLog,
                                                                      placeholderOnError.getCoverImageSource()));
                } else {
                    logger.error("Background: placeholderOnError or its properties are null for BookID {}. Event not published from exceptionally block.", bookIdForLog);
                }
                return null;
            });
    }
}
