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
import com.williamcallahan.book_recommendation_engine.service.DuplicateBookService;
import com.williamcallahan.book_recommendation_engine.service.EnvironmentService;
import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.util.UrlUtils;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.ReactiveErrorUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import com.williamcallahan.book_recommendation_engine.util.cover.CoverIdentifierResolver;
import com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
@Service
@Slf4j
public class BookCoverManagementService {

    private static final String DEPRECATED_S3 = "S3";
    private static final String DEPRECATED_UNKNOWN = "UNKNOWN";
    private static final CoverImageSource PLACEHOLDER_SOURCE = CoverImageSource.NONE;

    
    @Value("${app.cover-cache.enabled:true}")
    private boolean cacheEnabled = true;
    
    // CDN URL for S3-stored cover images
    @Value("${s3.cdn-url:${S3_CDN_URL:}}")
    private String s3CdnUrl;

    @Value("${s3.public-cdn-url:${S3_PUBLIC_CDN_URL:}}")
    private String s3PublicCdnUrl;

    @Value("${app.features.external-fallback.enabled:${app.features.google-fallback.enabled:true}}")
    private boolean externalFallbackEnabled;

    private final CoverCacheManager coverCacheManager;
    private final CoverSourceFetchingService coverSourceFetchingService;
    private final S3BookCoverService s3BookCoverService;
    private final LocalDiskCoverCacheService localDiskCoverCacheService; // For placeholder path
    private final ApplicationEventPublisher eventPublisher;
    private final EnvironmentService environmentService; // For debug mode checks
    private final DuplicateBookService duplicateBookService;
    private final CoverPersistenceService coverPersistenceService;

    /**
     * Constructs the BookCoverManagementService
     * @param coverCacheManager Manager for in-memory caches
     * @param coverSourceFetchingService Service for fetching covers from various sources
     * @param s3BookCoverService Service for S3 interactions (enhanced)
     * @param localDiskCoverCacheService Service for local disk cache operations
     * @param eventPublisher Publisher for application events
     * @param environmentService Service for environment-specific configurations
     */
    public BookCoverManagementService(
            CoverCacheManager coverCacheManager,
            CoverSourceFetchingService coverSourceFetchingService,
            S3BookCoverService s3BookCoverService,
            LocalDiskCoverCacheService localDiskCoverCacheService,
            ApplicationEventPublisher eventPublisher,
            EnvironmentService environmentService,
            DuplicateBookService duplicateBookService,
            CoverPersistenceService coverPersistenceService) {
        this.coverCacheManager = coverCacheManager;
        this.coverSourceFetchingService = coverSourceFetchingService;
        this.s3BookCoverService = s3BookCoverService;
        this.localDiskCoverCacheService = localDiskCoverCacheService;
        this.eventPublisher = eventPublisher;
        this.environmentService = environmentService;
        this.duplicateBookService = duplicateBookService;
        this.coverPersistenceService = coverPersistenceService;
    }

    private static boolean isStoredInS3(ImageDetails details) {
        if (details == null) {
            return false;
        }
        String storageLocation = details.getStorageLocation();
        return storageLocation != null && storageLocation.equalsIgnoreCase("S3");
    }

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.CoverSourceMapper#sanitize(CoverImageSource)} instead.
     * This method duplicates source sanitization logic that is now centralized in CoverSourceMapper.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * CoverImageSource clean = sanitizeSource(source);
     * 
     * // New:
     * CoverImageSource clean = CoverSourceMapper.sanitize(source);
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    private static CoverImageSource sanitizeSource(CoverImageSource source) {
        if (source == null) {
            return CoverImageSource.UNDEFINED;
        }
        String name = source.name();
        if (DEPRECATED_S3.equals(name) || DEPRECATED_UNKNOWN.equals(name)) {
            return CoverImageSource.UNDEFINED;
        }
        return source;
    }

    /**
     * Creates placeholder cover images when actual covers cannot be found
     * 
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.CoverImagesFactory#createPlaceholder(String)} instead.
     * This method duplicates CoverImages placeholder creation logic. The factory provides a centralized
     * and consistent way to create placeholder CoverImages objects.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * CoverImages placeholder = createPlaceholderCoverImages(bookId);
     * 
     * // New:
     * String placeholderPath = localDiskCoverCacheService.getLocalPlaceholderPath();
     * CoverImages placeholder = CoverImagesFactory.createPlaceholder(placeholderPath);
     * }</pre>
     * 
     * @param bookIdForLog identifier for logging purposes
     * @return CoverImages object with placeholder paths
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    private CoverImages createPlaceholderCoverImages(String bookIdForLog) {
        CoverImages placeholder = new CoverImages();
        String localPlaceholderPath = localDiskCoverCacheService.getLocalPlaceholderPath();
        placeholder.setPreferredUrl(localPlaceholderPath);
        placeholder.setFallbackUrl(localPlaceholderPath);
        placeholder.setSource(PLACEHOLDER_SOURCE);  // Placeholder has no data source
        log.warn("Returning placeholder for book ID: {}", bookIdForLog);
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
        String localPlaceholderPath = ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH;

        if (!cacheEnabled) {
            return Mono.just(createPlaceholderCoverImages(book != null ? book.getId() : "null (cache disabled)"));
        }

        if (book == null || !CoverIdentifierResolver.hasIdentifier(book)) {
            return Mono.just(createPlaceholderCoverImages("null (book or identifiers null)"));
        }

        String identifierKey = CoverIdentifierResolver.resolve(book);
        if (identifierKey == null) {
            return Mono.just(createPlaceholderCoverImages(book.getId() + " (identifierKey null)"));
        }

        // Check memory cache first (fast path)
        ImageDetails cached = coverCacheManager.getFinalImageDetails(identifierKey);
        if (cached != null && cached.getUrlOrPath() != null) {
            log.debug("Memory cache hit for {}", identifierKey);
            CoverImages result = new CoverImages();
            result.setPreferredUrl(cached.getUrlOrPath());
            result.setFallbackUrl(determineFallbackUrl(book, cached.getUrlOrPath(), localPlaceholderPath));
            result.setSource(cached.getCoverImageSource());
            return Mono.just(result);
        }

        // Check S3 with reasonable timeout - balance responsiveness with success rate
        return Mono.fromFuture(s3BookCoverService.fetchCover(book))
            .timeout(Duration.ofMillis(500)) // 500ms timeout allows S3 to respond in most cases
            .subscribeOn(Schedulers.boundedElastic()) // Run on background thread
            .onErrorResume(e -> {
                log.debug("S3 cover fetch timed out or failed for {}: {}", identifierKey, e.getMessage());
                return Mono.just(Optional.<ImageDetails>empty());
            })
            .flatMap(imageDetailsOptionalFromS3 -> { // imageDetailsOptionalFromS3 is Optional<ImageDetails>
                if (imageDetailsOptionalFromS3.isPresent()) {
                    ImageDetails imageDetailsFromS3 = imageDetailsOptionalFromS3.get();
                    boolean isS3Stored = isStoredInS3(imageDetailsFromS3);

                    if (imageDetailsFromS3.getUrlOrPath() != null && isS3Stored) {
                        log.debug("Initial check: Found S3 cover for identifier {}: {}", identifierKey, imageDetailsFromS3.getUrlOrPath());
                        CoverImages s3Result = new CoverImages();
                        s3Result.setPreferredUrl(imageDetailsFromS3.getUrlOrPath());
                        s3Result.setSource(sanitizeSource(imageDetailsFromS3.getCoverImageSource()));
                        s3Result.setFallbackUrl(determineFallbackUrl(book, imageDetailsFromS3.getUrlOrPath(), localPlaceholderPath));
                        coverCacheManager.putFinalImageDetails(identifierKey, imageDetailsFromS3); // Cache S3 ImageDetails
                        return Mono.just(s3Result);
                    }
                    log.debug("S3 check: Optional<ImageDetails> was present but content not valid S3 cache for identifier {}: {}", identifierKey, imageDetailsFromS3);
                } else {
                     log.debug("S3 check: Optional<ImageDetails> was empty for identifier {}", identifierKey);
                }
                // S3 miss, invalid details, or empty Optional, proceed to check other caches
                return checkMemoryCachesAndDefaults(book, identifierKey, localPlaceholderPath);
            })
            .onErrorResume(e -> {
                ReactiveErrorUtils.logError(e, "BookCoverManagementService.resolveInitialCover(" + identifierKey + ")");
                return checkMemoryCachesAndDefaults(book, identifierKey, localPlaceholderPath);
            })
            .defaultIfEmpty(createPlaceholderCoverImages(book.getId() + " (all checks failed or resulted in empty)"));
    }

    /**
     * Prepares a single book for display by fetching its cover and ensuring fallbacks.
     * Returns book immediately with placeholder, then resolves cover asynchronously.
     */
    /**
     * @deprecated Prepare API responses via {@link com.williamcallahan.book_recommendation_engine.controller.dto.BookDto}
     * mapping and the cover DTO pipeline managed by {@link CoverPersistenceService}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<Book> prepareBookForDisplay(Book book) {
        if (book == null) {
            return Mono.empty();
        }

        // Set placeholder immediately for fast rendering
        if (!ValidationUtils.hasText(book.getS3ImagePath())) {
            book.setS3ImagePath(ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH);
        }
        if (book.getCoverImages() == null) {
            book.setCoverImages(createPlaceholderCoverImages(book.getId()));
        }
        ensureSlug(book);

        // Trigger async cover resolution in background (fire and forget)
        getInitialCoverUrlAndTriggerBackgroundUpdate(book)
            .timeout(Duration.ofMillis(50)) // Very short timeout for sync path
            .subscribeOn(Schedulers.parallel())
            .subscribe(
                coverImages -> {
                    // Update if we got better cover quickly
                    if (coverImages != null && coverImages.getSource() != PLACEHOLDER_SOURCE) {
                        book.setCoverImages(coverImages);
                        coverImagesOptional(coverImages)
                            .filter(ValidationUtils::hasText)
                            .ifPresent(book::setS3ImagePath);
                    }
                },
                e -> log.debug("Background cover fetch failed for {}: {}", book.getId(), e.getMessage())
            );

        // Return book immediately with placeholder
        return Mono.just(book);
    }

    /**
     * Prepares a collection of books for display.
     * Optimized: Sets placeholders immediately, skips individual cover resolution.
     */
    /**
     * @deprecated Use DTO projections (e.g. {@link com.williamcallahan.book_recommendation_engine.dto.BookCard})
     * that already include cover metadata instead of mutating legacy {@link Book} entities.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<Book>> prepareBooksForDisplay(List<Book> books) {
        if (books == null || books.isEmpty()) {
            return Mono.just(List.of());
        }
        
        // Fast path: Set placeholders on all books immediately without async lookups
        List<Book> prepared = books.stream()
            .filter(Objects::nonNull)
            .peek(book -> {
                // Set placeholder immediately
                if (!ValidationUtils.hasText(book.getS3ImagePath())) {
                    book.setS3ImagePath(ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH);
                }
                if (book.getCoverImages() == null) {
                    book.setCoverImages(createPlaceholderCoverImages(book.getId()));
                }
                ensureSlug(book);
            })
            .toList();
        
        // Deduplicate and return immediately
        List<Book> deduplicated = deduplicateAndFilter(prepared);
        log.debug("Fast-prepared {} books for display (from {} input)", deduplicated.size(), books.size());
        
        return Mono.just(deduplicated);
    }

    private Optional<String> coverImagesOptional(CoverImages coverImages) {
        return Optional.ofNullable(coverImages)
            .map(CoverImages::getPreferredUrl)
            .filter(ValidationUtils::hasText);
    }

    private void ensureSlug(Book book) {
        if (!ValidationUtils.hasText(book.getSlug())) {
            book.setSlug(book.getId());
        }
    }

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.CoverDeduplicationUtils}
     * once the DTO-first pipeline is complete. This legacy helper still inspects deprecated cover enums.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private List<Book> deduplicateAndFilter(List<Book> books) {
        if (books == null || books.isEmpty()) {
            return List.of();
        }

        LinkedHashMap<String, Book> realCoverBooks = new LinkedHashMap<>();
        LinkedHashMap<String, Book> placeholderBooks = new LinkedHashMap<>();
        String placeholderPath = ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH;

        for (Book book : books) {
            if (book == null) {
                continue;
            }
            String key = ValidationUtils.hasText(book.getId()) ? book.getId() : book.getSlug();
            if (!ValidationUtils.hasText(key)) {
                continue;
            }

            boolean hasRealCover = ValidationUtils.BookValidator.hasActualCover(book, placeholderPath);
            if (hasRealCover) {
                realCoverBooks.putIfAbsent(key, book);
            } else {
                placeholderBooks.putIfAbsent(key, book);
            }
        }

        List<Book> combined = new ArrayList<>(realCoverBooks.values());

        placeholderBooks.forEach((key, book) -> {
            if (!realCoverBooks.containsKey(key)) {
                combined.add(book);
            }
        });

        combined.forEach(duplicateBookService::populateDuplicateEditions);
        return combined;
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
            log.debug("Returning final cached ImageDetails for identifierKey {}: Path: {}, Source: {}",
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
            if (book.getS3ImagePath() != null && !book.getS3ImagePath().isEmpty() && !book.getS3ImagePath().equals(localPlaceholderPath)) {
                urlToUseAsPreferred = book.getS3ImagePath();
                inferredProvisionalSource = inferSourceFromUrl(urlToUseAsPreferred, localPlaceholderPath);
            } else {
                urlToUseAsPreferred = localPlaceholderPath;
                inferredProvisionalSource = PLACEHOLDER_SOURCE;
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
    /**
     * Infers the CoverImageSource from a given URL string
     *
     * @deprecated Deprecated 2025-10-01. Use {@link com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector#detectSource(String)} instead.
     *             This logic is centralized in UrlSourceDetector.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private CoverImageSource inferSourceFromUrl(String url, String localPlaceholderPath) {
        if (url == null || url.isEmpty()) return CoverImageSource.UNDEFINED;
        if (url.equals(localPlaceholderPath)) return PLACEHOLDER_SOURCE;
        if (url.startsWith("/" + localDiskCoverCacheService.getCacheDirName())) return PLACEHOLDER_SOURCE;
        if ((s3CdnUrl != null && !s3CdnUrl.isEmpty() && url.contains(s3CdnUrl)) ||
            (s3PublicCdnUrl != null && !s3PublicCdnUrl.isEmpty() && url.contains(s3PublicCdnUrl))) {
            return CoverImageSource.UNDEFINED;
        }
        CoverImageSource detected = UrlSourceDetector.detectSource(url);
        // If detectSource returns UNDEFINED for external URLs, map to ANY
        // Otherwise use the detected source (GOOGLE_BOOKS, OPEN_LIBRARY, LONGITOOD, etc.)
        if (detected == CoverImageSource.UNDEFINED && UrlSourceDetector.isExternalUrl(url)) {
            return CoverImageSource.ANY;
        }
        return detected;
    }
    
    /**
     * Determines the appropriate fallback URL
     * @param book The book object
     * @param preferredUrl The preferred URL that was selected
     * @param localPlaceholderPath Path to the local placeholder image
     * @return The fallback URL string
     */
    private String determineFallbackUrl(Book book, String preferredUrl, String localPlaceholderPath) {
        String bookOriginalCover = book.getExternalImageUrl();
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
     * - If book has external cover URL but no S3 path, triggers background S3 upload (Bug #4)
     * - Updates in-memory caches (final and provisional)
     * - Publishes a BookCoverUpdatedEvent
     * @param book The book to find the cover image for
     * @param provisionalUrlHint A hint URL that might have been used for initial display
     */
    @Async
    public void processCoverInBackground(Book book, String provisionalUrlHint) {
        if (!cacheEnabled || book == null) {
            log.debug("Background processing skipped: cache disabled or book is null");
            return;
        }

        // Bug #4: Check if book has external cover that needs S3 migration
        if (shouldMigrateExternalCoverToS3(book)) {
            log.info("Background: Book {} has external cover without S3 path. Triggering S3 migration.", book.getId());
            migrateExternalCoverToS3(book);
            return; // Early return - migration handles its own caching and events
        }

        // Always attempt background processing; underlying services will no-op if disabled

        ImageProvenanceData provenanceData = new ImageProvenanceData();
        String identifierKey = CoverIdentifierResolver.resolve(book);
        if (identifierKey == null) {
            log.warn("Background: Could not determine identifierKey for book with ID: {}. Aborting", book.getId());
            return;
        }
        final String bookIdForLog = book.getId() != null ? book.getId() : identifierKey;
        provenanceData.setBookId(bookIdForLog);

        log.info("Background: Starting full cover processing for identifierKey: {}, Book ID: {}, Title: {}",
            identifierKey, bookIdForLog, book.getTitle());

        CompletableFuture<ImageDetails> coverFetchFuture = coverSourceFetchingService.getBestCoverImageUrlAsync(book, provisionalUrlHint, provenanceData);

        if (coverFetchFuture == null) {
            LoggingUtils.error(log, null,
                "Background: coverSourceFetchingService returned null CompletableFuture for identifierKey='{}', BookID='{}'. Falling back to placeholder.",
                identifierKey,
                bookIdForLog);

            coverCacheManager.invalidateProvisionalUrl(identifierKey);
            ImageDetails placeholderOnNull = localDiskCoverCacheService.placeholderImageDetails(bookIdForLog, "background-null-future");

            if (placeholderOnNull != null) {
                coverCacheManager.putFinalImageDetails(identifierKey, placeholderOnNull);
                if (placeholderOnNull.getUrlOrPath() != null && placeholderOnNull.getCoverImageSource() != null) {
                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey,
                        placeholderOnNull.getUrlOrPath(),
                        book.getId(),
                        sanitizeSource(placeholderOnNull.getCoverImageSource())));
                } else {
                    LoggingUtils.error(log, null,
                        "Background: placeholderOnNull has null URL or Source for BookID {}. Event not published from null future handler.",
                        bookIdForLog);
                }
            } else {
                LoggingUtils.error(log, null,
                    "Background: placeholderImageDetails returned null while handling null future for BookID {}.",
                    bookIdForLog);
            }
            return;
        }

        coverFetchFuture
            .thenAcceptAsync(finalImageDetails -> {
                String localPlaceholderPath = ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH;
                if (finalImageDetails == null || finalImageDetails.getUrlOrPath() == null || 
                    finalImageDetails.getUrlOrPath().equals(localPlaceholderPath)) {
                    
                    log.warn("Background: Final processing for {} (BookID {}) yielded placeholder or null. Final cache updated with placeholder", identifierKey, bookIdForLog);
                    ImageDetails placeholderDetails = localDiskCoverCacheService.placeholderImageDetails(bookIdForLog, "background-fetch-failed");
                    
                    if (placeholderDetails == null) {
                        // As per review suggestion, throw an exception instead of silently returning
                        LoggingUtils.error(log, null,
                            "CRITICAL: localDiskCoverCacheService.buildPlaceholderImageDetails returned null for BookID {}. Throwing exception.",
                            bookIdForLog);
                        if (identifierKey != null) {
                            coverCacheManager.invalidateProvisionalUrl(identifierKey); // Attempt to clean up provisional URL
                        }
                        throw new IllegalStateException("buildPlaceholderImageDetails returned null for BookID " + bookIdForLog + " with identifierKey " + identifierKey);
                    }

                    coverCacheManager.putFinalImageDetails(identifierKey, placeholderDetails);
                    coverCacheManager.invalidateProvisionalUrl(identifierKey);
                    // Ensure placeholderDetails and its getUrlOrPath() are not null before publishing event
                    if (placeholderDetails.getUrlOrPath() != null && placeholderDetails.getCoverImageSource() != null) {
                        eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, placeholderDetails.getUrlOrPath(), book.getId(), placeholderDetails.getCoverImageSource()));
                    } else {
                        LoggingUtils.error(log, null,
                            "CRITICAL: placeholderDetails has null URL or Source for BookID {}. Event not published.",
                            bookIdForLog);
                    }
                    if (environmentService.isBookCoverDebugMode()) {
                        log.info("Background Provenance (Placeholder) for {}: {}", identifierKey, provenanceData.toString());
                    }
                    return;
                }

                log.info("Background: Best image found for {} (BookID {}): URL/Path: {}, Source: {}",
                    identifierKey, bookIdForLog, finalImageDetails.getUrlOrPath(), finalImageDetails.getCoverImageSource());

                if (!isStoredInS3(finalImageDetails) &&
                    finalImageDetails.getUrlOrPath() != null &&
                    finalImageDetails.getUrlOrPath().startsWith("/" + localDiskCoverCacheService.getCacheDirName())) {
                    
                    log.info("Background: Image for {} (BookID {}) is locally cached from {}. Triggering S3 upload",
                        identifierKey, bookIdForLog, finalImageDetails.getCoverImageSource());
                    
                    try {
                        String cacheDirString = localDiskCoverCacheService.getCacheDirString();
                        if (cacheDirString == null || cacheDirString.isBlank()) {
                            log.warn("Background: Cache directory unavailable for {}. Skipping S3 upload and using local details.", identifierKey);
                            coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                            eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), sanitizeSource(finalImageDetails.getCoverImageSource())));
                            return;
                        }
                        Path cacheDir = Paths.get(cacheDirString);
                        if (finalImageDetails.getUrlOrPath() == null) {
                            log.warn("Background: Image path for {} is null; skipping S3 upload.", identifierKey);
                            coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                            eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, localPlaceholderPath, book.getId(), PLACEHOLDER_SOURCE));
                            return;
                        }
                        Path relativeImagePath = Paths.get(finalImageDetails.getUrlOrPath()).getFileName(); 
                        Path localImagePath = cacheDir.resolve(relativeImagePath);

                        if (!Files.exists(localImagePath)) {
                            log.warn("Background: Local image {} does not exist for BookID {}, cannot upload to S3. Using local details.", localImagePath, bookIdForLog);
                            coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                            eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), sanitizeSource(finalImageDetails.getCoverImageSource())));
                        } else {
                            byte[] imageBytes = Files.readAllBytes(localImagePath);
                            String fileExtension = UrlUtils.extractFileExtension(finalImageDetails.getUrlOrPath());
                            Integer width = finalImageDetails.getWidth() != null ? finalImageDetails.getWidth() : 0;
                            Integer height = finalImageDetails.getHeight() != null ? finalImageDetails.getHeight() : 0;

                            s3BookCoverService.uploadProcessedCoverToS3Async(
                                imageBytes, fileExtension, null, width, height,
                                bookIdForLog, finalImageDetails.getSourceName(), provenanceData
                            )
                            .subscribe(s3UploadedDetails -> {
                                if (s3UploadedDetails == null) {
                                    log.warn("Background: S3 upload returned null details for {}. Using local copy {}", identifierKey, finalImageDetails.getUrlOrPath());
                                    coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), sanitizeSource(finalImageDetails.getCoverImageSource())));
                                    return;
                                }

                                boolean uploadedToS3 = isStoredInS3(s3UploadedDetails);

                                if (uploadedToS3) {
                                    log.info("Background: Successfully uploaded to S3 for {}. New S3 URL: {}. Updating final cache.",
                                        identifierKey, s3UploadedDetails.getUrlOrPath());
                                    coverCacheManager.putFinalImageDetails(identifierKey, s3UploadedDetails);
                                    CoverImageSource sanitizedSource = sanitizeSource(s3UploadedDetails.getCoverImageSource());
                                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, s3UploadedDetails.getUrlOrPath(), book.getId(), sanitizedSource));
                                    // Persist S3 as primary in background
                                    try {
                                        java.util.UUID uuid = java.util.UUID.fromString(book.getId());
                                        CoverPersistenceService.PersistenceResult persistenceResult = coverPersistenceService.updateAfterS3Upload(
                                            uuid,
                                            s3UploadedDetails.getSourceSystemId(),
                                            s3UploadedDetails.getUrlOrPath(),
                                            s3UploadedDetails.getWidth(),
                                            s3UploadedDetails.getHeight(),
                                            sanitizedSource
                                        );
                                        if (!persistenceResult.success()) {
                                            log.warn("CoverPersistenceService reported no update after S3 upload for {}", identifierKey);
                                        }
                                    } catch (IllegalArgumentException ex) {
                                        log.warn("Book ID is not a valid UUID; skipping primary S3 persistence: {}", book.getId());
                                    }
                                } else {
                                    log.warn("Background: S3 upload failed or did not mark storage as S3 for {}. Using local: {}", identifierKey, finalImageDetails.getUrlOrPath());
                                    coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), sanitizeSource(finalImageDetails.getCoverImageSource())));
                                }
                            }, s3Ex -> {
                                LoggingUtils.error(log, s3Ex, "Background: Exception in S3 upload chain for {}. Using local.", identifierKey);
                                coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                                eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), sanitizeSource(finalImageDetails.getCoverImageSource())));
                            }); // non-blocking subscription with error handler
                            // The logic after this if/else will execute immediately if S3 path is taken.
                            // No explicit return here, so flow continues.
                        }
                    } catch (java.io.IOException e) {
                        LoggingUtils.error(log, e,
                            "Background: IOException for local image {} for S3 upload (BookID {})",
                            finalImageDetails.getUrlOrPath(), bookIdForLog);
                        coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                        eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), sanitizeSource(finalImageDetails.getCoverImageSource())));
                    }
                    // If S3 upload path was taken, it will return here to avoid the redundant cache update below
                    // The S3 callbacks will handle their own cache updates
                    return;
                } else { // Not S3 cache and not a local disk cache candidate for S3 upload
                    log.info("Background: Image for {} (BookID {}) is not a local cache candidate for S3 upload (Source: {}, Path: {}). Using details as is.",
                        identifierKey, bookIdForLog, finalImageDetails.getCoverImageSource(), finalImageDetails.getUrlOrPath());
                    coverCacheManager.putFinalImageDetails(identifierKey, finalImageDetails);
                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), sanitizeSource(finalImageDetails.getCoverImageSource())));
                    // Explicit return after handling this case
                    return;
                }
            }, java.util.concurrent.ForkJoinPool.commonPool())
            .exceptionally(ex -> {
                LoggingUtils.error(log, ex,
                    "Background: Top-level exception in processCoverInBackground for identifierKey='{}', BookID='{}'",
                    identifierKey,
                    bookIdForLog);

                coverCacheManager.invalidateProvisionalUrl(identifierKey); // identifierKey should be non-null here due to earlier check

                ImageDetails placeholderOnError = localDiskCoverCacheService.placeholderImageDetails(bookIdForLog, "background-exception");

                coverCacheManager.putFinalImageDetails(identifierKey, placeholderOnError);
                if (placeholderOnError != null && placeholderOnError.getUrlOrPath() != null && placeholderOnError.getCoverImageSource() != null) {
                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey,
                                                                      placeholderOnError.getUrlOrPath(),
                                                                      bookIdForLog,
                                                                      sanitizeSource(placeholderOnError.getCoverImageSource())));
                } else {
                    LoggingUtils.error(log, null,
                        "Background: placeholderOnError or its properties are null for BookID {}. Event not published from exceptionally block.",
                        bookIdForLog);
                }
                return null;
            });
    }

    /**
     * Determines if a book's external cover should be migrated to S3.
     * 
     * Conditions for migration (Bug #4):
     * 1. Book has an external image URL
     * 2. Book does NOT have S3 path, or S3 path is placeholder
     * 3. External URL is not a placeholder
     * 4. S3 is enabled
     * 
     * @param book Book to check
     * @return true if migration should occur
     */
    private boolean shouldMigrateExternalCoverToS3(Book book) {
        if (!s3BookCoverService.isS3Enabled()) {
            return false;
        }
        
        String externalUrl = book.getExternalImageUrl();
        String s3Path = book.getS3ImagePath();
        String localPlaceholder = ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH;
        
        // Must have external URL
        if (externalUrl == null || externalUrl.isEmpty() || externalUrl.equals(localPlaceholder)) {
            return false;
        }
        
        // Must NOT have valid S3 path
        boolean hasValidS3Path = s3Path != null && !s3Path.isEmpty() && !s3Path.equals(localPlaceholder);
        if (hasValidS3Path) {
            return false;
        }
        
        // External URL must be HTTP(S) URL, not a local path
        return externalUrl.startsWith("http://") || externalUrl.startsWith("https://");
    }

    /**
     * Migrates an external cover URL to S3 storage (Bug #4).
     * 
     * Process:
     * 1. Download image from external URL
     * 2. Process and upload to S3
     * 3. Update book_image_links with S3 metadata
     * 4. Update in-memory caches
     * 5. Publish BookCoverUpdatedEvent
     * 
     * Idempotent: Uses S3BookCoverService's built-in existence checks.
     * 
     * @param book Book with external cover to migrate
     */
    private void migrateExternalCoverToS3(Book book) {
        String externalUrl = book.getExternalImageUrl();
        String bookId = book.getId();
        String identifierKey = CoverIdentifierResolver.resolve(book);
        
        if (identifierKey == null) {
            log.warn("Bug #4: Cannot migrate cover for book {}: no identifier key", bookId);
            return;
        }
        
        // Infer source from URL
        String localPlaceholder = localDiskCoverCacheService.getLocalPlaceholderPath();
        CoverImageSource inferredSource = inferSourceFromUrl(externalUrl, localPlaceholder);
        String sourceString = getSourceStringForS3(inferredSource);
        
        log.info("Bug #4: Starting external cover → S3 migration for book {}: {} (source: {})",
            bookId, externalUrl, sourceString);
        
        // Trigger S3 upload asynchronously (reactive chain)
        Mono<ImageDetails> uploadPublisher = s3BookCoverService.uploadCoverToS3Async(externalUrl, bookId, sourceString);
        if (uploadPublisher == null) {
            log.warn("Bug #4: uploadCoverToS3Async returned null for book {}. Keeping external URL cached.", bookId);
            ImageDetails externalDetails = new ImageDetails(
                externalUrl, sourceString, externalUrl, inferredSource,
                com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference.ORIGINAL
            );
            coverCacheManager.putFinalImageDetails(identifierKey, externalDetails);
            eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, externalUrl, bookId, sanitizeSource(inferredSource)));
            return;
        }

        uploadPublisher
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                s3ImageDetails -> {
                    if (s3ImageDetails == null || !isStoredInS3(s3ImageDetails)) {
                        log.warn("Bug #4: S3 upload failed for book {}. Details: {}", bookId, s3ImageDetails);
                        // Keep external URL in cache as fallback
                        ImageDetails externalDetails = new ImageDetails(
                            externalUrl, sourceString, externalUrl, inferredSource, 
                            com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference.ORIGINAL
                        );
                        coverCacheManager.putFinalImageDetails(identifierKey, externalDetails);
                        eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, externalUrl, bookId, inferredSource));
                        return;
                    }
                    
                    // S3 upload succeeded
                    String s3Url = s3ImageDetails.getUrlOrPath();
                    log.info("Bug #4: Successfully migrated external cover to S3 for book {}: {}",
                        bookId, s3Url);
                    
                    // Update in-memory cache
                    coverCacheManager.putFinalImageDetails(identifierKey, s3ImageDetails);
                    coverCacheManager.invalidateProvisionalUrl(identifierKey);
                    
                    // Publish event for cache invalidation and UI updates
                    CoverImageSource sanitizedSource = sanitizeSource(s3ImageDetails.getCoverImageSource());
                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(
                        identifierKey, s3Url, bookId, sanitizedSource
                    ));
                    try {
                        java.util.UUID uuid = java.util.UUID.fromString(bookId);
                        CoverPersistenceService.PersistenceResult persistenceResult = coverPersistenceService.updateAfterS3Upload(
                            uuid,
                            s3ImageDetails.getSourceSystemId(),
                            s3Url,
                            s3ImageDetails.getWidth(),
                            s3ImageDetails.getHeight(),
                            sanitizedSource
                        );
                        if (!persistenceResult.success()) {
                            log.warn("Bug #4: CoverPersistenceService did not persist migration for book {}", bookId);
                        }
                    } catch (IllegalArgumentException ex) {
                        log.warn("Bug #4: Book ID {} is not a valid UUID; skipping Postgres persistence", bookId);
                    }
                },
                error -> {
                    LoggingUtils.error(log, error, 
                        "Bug #4: Exception during external cover → S3 migration for book {}", bookId);
                    
                    // Fall back to external URL in cache
                    ImageDetails externalDetails = new ImageDetails(
                        externalUrl, sourceString, externalUrl, inferredSource,
                        com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference.ORIGINAL
                    );
                    coverCacheManager.putFinalImageDetails(identifierKey, externalDetails);
                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, externalUrl, bookId, sanitizeSource(inferredSource)));
                }
            );
    }

    /**
     * Maps CoverImageSource to S3 key-compatible source string.
     */
    private String getSourceStringForS3(CoverImageSource source) {
        return com.williamcallahan.book_recommendation_engine.util.cover.CoverSourceMapper.toS3KeySegment(source);
    }
}
