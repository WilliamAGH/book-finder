/**
 * Service for fetching book cover images from various sources
 * - Implements a strategy to try multiple sources in a defined order
 * - Coordinates with local disk caching and S3 services
 * - Tracks provenance of image fetching attempts
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service.image;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.GoogleApiFetcher;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.mapper.GoogleBooksMapper;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageAttemptStatus;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.model.image.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.util.BookDomainMapper;
import com.williamcallahan.book_recommendation_engine.util.GoogleBooksUrlEnhancer;
import com.williamcallahan.book_recommendation_engine.util.StringUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import com.williamcallahan.book_recommendation_engine.util.cover.CoverIdentifierResolver;
import com.williamcallahan.book_recommendation_engine.util.cover.CoverSourceMapper;
import com.williamcallahan.book_recommendation_engine.util.cover.ImageDimensionUtils;
import com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
@Service
public class CoverSourceFetchingService {

    private static final Logger logger = LoggerFactory.getLogger(CoverSourceFetchingService.class);

    private final LocalDiskCoverCacheService localDiskCoverCacheService;
    private final S3BookCoverService s3BookCoverService;
    private final OpenLibraryServiceImpl openLibraryService;
    private final LongitoodService longitoodService;
    private final GoogleBooksService googleBooksService;
    private final GoogleApiFetcher googleApiFetcher;
    private final CoverCacheManager coverCacheManager;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final ImageSelectionService imageSelectionService;
    private final ImageProvenanceHandler imageProvenanceHandler;
    private final GoogleBooksMapper googleBooksMapper;

    /**
     * Constructs the CoverSourceFetchingService
     * @param localDiskCoverCacheService Service for local disk caching
     * @param s3BookCoverService Service for S3 interactions
     * @param openLibraryService Service for OpenLibrary covers
     * @param longitoodService Service for Longitood covers
     * @param googleBooksService Service for Google Books API
     * @param coverCacheManager Manager for in-memory caches
     */
    public CoverSourceFetchingService(
            LocalDiskCoverCacheService localDiskCoverCacheService,
            S3BookCoverService s3BookCoverService,
            OpenLibraryServiceImpl openLibraryService,
            LongitoodService longitoodService,
            GoogleBooksService googleBooksService,
            GoogleApiFetcher googleApiFetcher,
            CoverCacheManager coverCacheManager,
            BookDataOrchestrator bookDataOrchestrator,
            ImageSelectionService imageSelectionService,
            ImageProvenanceHandler imageProvenanceHandler,
            GoogleBooksMapper googleBooksMapper) {
        this.localDiskCoverCacheService = localDiskCoverCacheService;
        this.s3BookCoverService = s3BookCoverService;
        this.openLibraryService = openLibraryService;
        this.longitoodService = longitoodService;
        this.googleBooksService = googleBooksService;
        this.googleApiFetcher = googleApiFetcher;
        this.coverCacheManager = coverCacheManager;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.imageSelectionService = imageSelectionService;
        this.imageProvenanceHandler = imageProvenanceHandler;
        this.googleBooksMapper = googleBooksMapper;
    }

    /**
     * Asynchronously retrieves the best available cover image for a book.
     * It processes a provisional URL hint, then tries S3, then other external sources,
     * and finally selects the best image from all gathered candidates.
     * @param book The book to find the cover image for
     * @param provisionalUrlHint A hint URL that might contain a usable cover image
     * @param provenanceData Container for tracking image source details and attempts
     * @return CompletableFuture containing ImageDetails of the best found image, or a placeholder
     */
    public CompletableFuture<ImageDetails> getBestCoverImageUrlAsync(Book book, String provisionalUrlHint, ImageProvenanceData provenanceData) {
        String resolvedIdentifier = CoverIdentifierResolver.resolve(book);
        String bookIdForLog = resolvedIdentifier != null ? resolvedIdentifier : "unknown_book_id";
        if (provenanceData.getBookId() == null) { // Ensure bookId is set in provenance
            provenanceData.setBookId(bookIdForLog);
        }
        if (provenanceData.getAttemptedImageSources() == null) {
            provenanceData.setAttemptedImageSources(new ArrayList<>());
        }

        CompletableFuture<List<ImageDetails>> hintProcessingChain = processProvisionalHintAsync(book, provisionalUrlHint, bookIdForLog, provenanceData);

        return hintProcessingChain
            .thenCompose(hintCandidates ->
                fetchFromS3AndThenRemainingSources(book, bookIdForLog, provenanceData, hintCandidates)
            )
            .exceptionally(overallEx -> { // Catch-all for catastrophic failure in the chain
                logger.error("Overall catastrophic exception in getBestCoverImageUrlAsync for Book ID {}: {}. Returning placeholder.", bookIdForLog, overallEx.getMessage(), overallEx);
                return placeholder(bookIdForLog, "overall-fetch-exception");
            });
    }

    private CompletableFuture<List<ImageDetails>> processProvisionalHintAsync(Book book, String provisionalUrlHint, String bookIdForLog, ImageProvenanceData provenanceData) {
        if (provisionalUrlHint == null || provisionalUrlHint.isEmpty() ||
            provisionalUrlHint.equals(ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH) ||
            provisionalUrlHint.startsWith("/" + localDiskCoverCacheService.getCacheDirName())) {
            logger.debug("Book ID {}: No valid provisional hint provided or hint is local cache/placeholder.", bookIdForLog);
            return CompletableFuture.completedFuture(new ArrayList<>());
        }

        ImageSourceName hintSourceName = CoverSourceMapper.fromString("ProvisionalHint"); // Default
        if (provisionalUrlHint.contains("googleapis.com/books") || provisionalUrlHint.contains("books.google.com/books")) {
            hintSourceName = ImageSourceName.GOOGLE_BOOKS;
        } else if (provisionalUrlHint.contains("openlibrary.org")) {
            hintSourceName = ImageSourceName.OPEN_LIBRARY;
        } else if (provisionalUrlHint.contains("longitood.com")) {
            hintSourceName = ImageSourceName.LONGITOOD;
        }
        // S3 hints are typically direct CDN URLs and will be validated if encountered or picked up by S3 specific logic later.

        final ImageSourceName finalHintSourceName = hintSourceName;
        /**
         * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.ImageDimensionUtils#MIN_ACCEPTABLE_NON_GOOGLE} instead.
         * Image dimension validation constants should be centralized in the dimension utilities.
         * Will be removed in version 1.0.0.
         * 
         * <p><b>Migration Example:</b></p>
         * <pre>{@code
         * // Old:
         * final int MIN_ACCEPTABLE_DIMENSION_NON_GOOGLE = 200;
         * if (width >= MIN_ACCEPTABLE_DIMENSION_NON_GOOGLE) { ... }
         * 
         * // New:
         * if (width >= ImageDimensionUtils.MIN_ACCEPTABLE_NON_GOOGLE) { ... }
         * }</pre>
         */
        @Deprecated(since = "0.9.0", forRemoval = true)
        final int MIN_ACCEPTABLE_DIMENSION_NON_GOOGLE = 200;

        if (finalHintSourceName == ImageSourceName.GOOGLE_BOOKS) {
            List<CompletableFuture<ImageDetails>> googleHintFutures = new ArrayList<>();
            
            String originalHintEnhanced = GoogleBooksUrlEnhancer.enhanceUrl(provisionalUrlHint); // Enhance with default zoom level
            if (isLikelyGoogleCoverUrl(originalHintEnhanced, bookIdForLog, "ProvisionalHint-Original")) {
                logger.debug("Book ID {}: Processing Google hint (enhanced as-is): {}", bookIdForLog, originalHintEnhanced);
                googleHintFutures.add(
                    cacheImageLocally(originalHintEnhanced, bookIdForLog, provenanceData, "GoogleHint-AsIs")
                );
            } else {
                 logger.debug("Book ID {}: Google hint (enhanced as-is) {} was deemed unlikely to be a cover. Skipping.", bookIdForLog, originalHintEnhanced);
            }

            String urlZoom0 = GoogleBooksUrlEnhancer.enhanceUrl(provisionalUrlHint, 0);
            if (urlZoom0 != null && !urlZoom0.equals(originalHintEnhanced) && isLikelyGoogleCoverUrl(urlZoom0, bookIdForLog, "ProvisionalHint-Zoom0")) {
                logger.debug("Book ID {}: Processing Google hint (zoom=0 variant): {}", bookIdForLog, urlZoom0);
                googleHintFutures.add(
                    cacheImageLocally(urlZoom0, bookIdForLog, provenanceData, "GoogleHint-Zoom0")
                );
            } else if (urlZoom0 != null && !urlZoom0.equals(originalHintEnhanced)) {
                 logger.debug("Book ID {}: Google hint (zoom=0 variant) {} was deemed unlikely to be a cover. Skipping.", bookIdForLog, urlZoom0);
            }
            
            if (googleHintFutures.isEmpty()) {
                logger.debug("Book ID {}: No suitable Google hints to process after likelihood check.", bookIdForLog);
                return CompletableFuture.completedFuture(new ArrayList<>());
            }

            return CompletableFuture.allOf(googleHintFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> googleHintFutures.stream()
                    .map(CompletableFuture::join) // .join() is safe here after allOf
                    .filter(this::isValidImageDetails)
                    .collect(Collectors.toList()))
                .exceptionally(ex -> {
                     logger.error("Exception processing Google provisionalUrlHint variations for Book ID {}. Error: {}", bookIdForLog, ex.getMessage(), ex);
                     return new ArrayList<>(); // Return empty list on failure
                });

        } else { // Non-Google Hint
            return cacheImageLocally(provisionalUrlHint, bookIdForLog, provenanceData, finalHintSourceName.name())
                .thenApply(cachedFromHintDetails -> {
                    if (isValidImageDetails(cachedFromHintDetails)) {
                        boolean isGoodQualityProvisional = cachedFromHintDetails.getWidth() != null && cachedFromHintDetails.getHeight() != null &&
                                                           cachedFromHintDetails.getWidth() >= MIN_ACCEPTABLE_DIMENSION_NON_GOOGLE &&
                                                           cachedFromHintDetails.getHeight() >= MIN_ACCEPTABLE_DIMENSION_NON_GOOGLE;
                        if (isGoodQualityProvisional) {
                            logger.debug("Book ID {}: Non-Google provisional hint {} processed successfully as a candidate.", bookIdForLog, provisionalUrlHint);
                            return List.of(cachedFromHintDetails);
                        }
                        logger.debug("Non-Google provisional URL hint {} for Book ID {} was not good enough ({}x{}). Discarding as initial candidate.",
                                provisionalUrlHint, bookIdForLog, cachedFromHintDetails.getWidth(), cachedFromHintDetails.getHeight());
                    }
                    return new ArrayList<ImageDetails>();
                })
                .exceptionally(ex -> {
                    logger.error("Exception processing non-Google provisionalUrlHint {} for Book ID {}. Error: {}", provisionalUrlHint, bookIdForLog, ex.getMessage());
                    return new ArrayList<>(); // Return empty list on failure
                });
        }
    }

    /**
     * Fetches from S3 and then proceeds to fetch from remaining external sources.
     * @param book The book object
     * @param bookIdForLog Identifier for logging
     * @param provenanceData Container for tracking attempts
     * @param existingCandidates Candidates gathered from prior steps (e.g., hints)
     * @return CompletableFuture with the best ImageDetails found
     */
    /**
     * @deprecated Use {@link BookCoverManagementService#getInitialCoverUrlAndTriggerBackgroundUpdate}
     * which coordinates S3 and external fallbacks via {@link CoverPersistenceService}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private CompletableFuture<ImageDetails> fetchFromS3AndThenRemainingSources(Book book, String bookIdForLog, ImageProvenanceData provenanceData, List<ImageDetails> existingCandidates) {
        List<ImageDetails> candidatesSoFar = new ArrayList<>(existingCandidates);

        return tryS3(book, bookIdForLog, provenanceData)
            .thenCompose(s3Details -> {
                if (isValidImageDetails(s3Details)) {
                    logger.debug("Book ID {}: S3 provided valid image details. Adding to candidates.", bookIdForLog);
                    candidatesSoFar.add(s3Details);
                } else {
                    logger.debug("Book ID {}: S3 did not provide valid image details or returned placeholder. Not adding to candidates from S3.", bookIdForLog);
                }
                // Provenance for S3 attempt is handled within tryS3.
                return fetchFromRemainingExternalSources(book, bookIdForLog, provenanceData, candidatesSoFar);
            })
            .exceptionallyCompose(s3Ex -> { // Exception from tryS3 or its chain
                logger.error("Exception during S3 fetch for Book ID {}: {}. Proceeding to other external sources.", bookIdForLog, s3Ex.getMessage(), s3Ex);
                // S3 attempt provenance should have been added in tryS3 (if it reached that part).
                // Pass existingCandidates (which are from hints) to the next stage.
                return fetchFromRemainingExternalSources(book, bookIdForLog, provenanceData, candidatesSoFar);
            });
    }

    private CompletableFuture<ImageDetails> tryTieredCoverLookup(String identifier, String bookIdForLog, ImageProvenanceData provenanceData) {
        if (bookDataOrchestrator == null) {
            return CompletableFuture.completedFuture(null);
        }

        return bookDataOrchestrator.fetchCanonicalBookReactive(identifier)
            .flatMap(book -> {
                if (book == null) {
                    return Mono.<Optional<ImageDetails>>empty();
                }

                ImageDetails fromBook = imageDetailsFromBook(book, identifier);
                if (isValidImageDetails(fromBook)) {
                    imageProvenanceHandler.recordAttempt(
                        provenanceData,
                        CoverSourceMapper.toImageSourceName(fromBook.getCoverImageSource()),
                        "postgres-tier:" + identifier,
                        ImageAttemptStatus.SUCCESS,
                        null,
                        fromBook
                    );
                    return Mono.just(Optional.of(fromBook));
                }

                return Mono.fromFuture(tryS3(book, book.getId() != null ? book.getId() : bookIdForLog, provenanceData))
                    .flatMap(detail -> {
                        if (detail == null || !isValidImageDetails(detail)) {
                            return Mono.empty();
                        }
                        return Mono.just(Optional.of(detail));
                    });
            })
            .onErrorResume(ex -> {
                logger.warn("Postgres-first cover lookup failed for {}: {}", identifier, ex.getMessage());
                return Mono.just(Optional.empty());
            })
            .defaultIfEmpty(Optional.empty())
            .flatMap(opt -> opt.map(Mono::just).orElseGet(Mono::empty))
            .toFuture();
    }

    private ImageDetails imageDetailsFromBook(Book book, String fallbackSourceId) {
        String coverUrl = null;
        CoverImageSource source = null;

        if (book.getCoverImages() != null) {
            CoverImages coverImages = book.getCoverImages();
            coverUrl = StringUtils.coalesce(coverImages.getPreferredUrl(), coverImages.getFallbackUrl());
            source = coverImages.getSource();
        }

        String s3Path = book.getS3ImagePath();
        String externalUrl = book.getExternalImageUrl();
        coverUrl = StringUtils.coalesce(coverUrl, s3Path, externalUrl);

        if (coverUrl == null || coverUrl.isBlank()) {
            return null;
        }

        if (source == null || source == CoverImageSource.ANY) {
            source = UrlSourceDetector.detectSource(coverUrl);
        }

        String storageLocation = null;
        if (s3Path != null && coverUrl.equals(s3Path)) {
            storageLocation = "S3";
        } else {
            String cacheDirName = localDiskCoverCacheService.getCacheDirName();
            if (cacheDirName != null && coverUrl.startsWith("/" + cacheDirName)) {
                storageLocation = "LOCAL";
            }
        }

        ImageDetails details = new ImageDetails(
            coverUrl,
            "POSTGRES_CACHE",
            book.getId() != null ? book.getId() : fallbackSourceId,
            source,
            ImageResolutionPreference.ORIGINAL,
            ImageDimensionUtils.normalize(book.getCoverImageWidth()),
            ImageDimensionUtils.normalize(book.getCoverImageHeight())
        );

        if (storageLocation != null) {
            details.setStorageLocation(storageLocation);
            if ("S3".equals(storageLocation)) {
                details.setStorageKey(s3Path);
            } else if ("LOCAL".equals(storageLocation)) {
                details.setStorageKey(coverUrl);
            }
        }

        return details;
    }

    /**
     * Fetches images from remaining external sources (Google API, OpenLibrary, Longitood).
     * This method is called after hints and S3 have been processed.
     * @param book The book object
     * @param bookIdForLog Identifier for logging
     * @param provenanceData Container for tracking attempts
     * @param existingCandidates Candidates gathered from prior steps (hints, S3)
     * @return CompletableFuture with the best ImageDetails found from all sources
     */
    /**
     * @deprecated External source prioritisation now lives in
     * {@link BookCoverManagementService} and {@link CoverPersistenceService}. Use those orchestrators instead.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private CompletableFuture<ImageDetails> fetchFromRemainingExternalSources(Book book, String bookIdForLog, ImageProvenanceData provenanceData, List<ImageDetails> existingCandidates) {
        List<CompletableFuture<ImageDetails>> sourceFutures = new ArrayList<>();
        
        // S3 is handled by fetchFromS3AndThenRemainingSources before this method.
        
        String isbn = CoverIdentifierResolver.getPreferredIsbn(book);
        if (ValidationUtils.hasText(isbn)) {
            sourceFutures.add(tryGoogleBooksApiByIsbn(isbn, bookIdForLog, provenanceData));
            sourceFutures.add(openLibraryService.fetchAndCacheCover(isbn, bookIdForLog, "L", provenanceData));
            sourceFutures.add(tryOpenLibrary(isbn, bookIdForLog, "M", provenanceData));
            sourceFutures.add(tryOpenLibrary(isbn, bookIdForLog, "S", provenanceData));
            sourceFutures.add(longitoodService.fetchAndCacheCover(book, bookIdForLog, provenanceData));
        } else if (ValidationUtils.hasText(book.getId())) { // Google Volume ID
            sourceFutures.add(tryGoogleBooksApiByVolumeId(book.getId(), bookIdForLog, provenanceData));
        }

        if (sourceFutures.isEmpty()) {
            if (existingCandidates.isEmpty()) {
                logger.warn("Book ID {}: No external sources to try and no prior candidates. Returning placeholder.", bookIdForLog);
                return CompletableFuture.completedFuture(placeholder(bookIdForLog, "no-sources-or-candidates"));
            } else {
                logger.debug("Book ID {}: No further external APIs to query. Selecting from {} existing candidates.", bookIdForLog, existingCandidates.size());
                return CompletableFuture.completedFuture(selectBestImageDetails(existingCandidates, bookIdForLog, provenanceData));
            }
        }

        return CompletableFuture.allOf(sourceFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                List<ImageDetails> allFetchedCandidates = new ArrayList<>(existingCandidates);
                for (CompletableFuture<ImageDetails> future : sourceFutures) {
                    try {
                        ImageDetails details = future.join(); // join is safe after allOf
                        if (isValidImageDetails(details)) {
                            allFetchedCandidates.add(details);
                        }
                    } catch (Exception e) {
                        logger.warn("Book ID {}: Exception joining future in fetchFromRemainingExternalSources: {}", bookIdForLog, e.getMessage());
                    }
                }
                
                if (allFetchedCandidates.isEmpty()) {
                    logger.warn("Book ID {}: No valid images found from any source (including prior candidates). Returning placeholder.", bookIdForLog);
                    return placeholder(bookIdForLog, "all-sources-failed-or-empty");
                }
                
                return selectBestImageDetails(allFetchedCandidates, bookIdForLog, provenanceData);
            })
            .exceptionally(ex -> {
                logger.error("Overall exception in fetchFromRemainingExternalSources for Book ID {}: {}. Selecting from prior or returning placeholder.", bookIdForLog, ex.getMessage(), ex);
                if (!existingCandidates.isEmpty()) {
                    logger.warn("Book ID {}: Falling back to selecting from {} existing candidates due to exception in remaining sources.", bookIdForLog, existingCandidates.size());
                    return selectBestImageDetails(existingCandidates, bookIdForLog, provenanceData);
                }
                return placeholder(bookIdForLog, "process-remaining-sources-exception");
            });
    }
    
    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.service.image.OpenLibraryServiceImpl}
     * via the orchestrated CoverSourceFetchingService entry points that persist with CoverPersistenceService.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private CompletableFuture<ImageDetails> tryOpenLibrary(String isbn, String bookIdForLog, String size, ImageProvenanceData provenanceData) {
        logger.debug("Attempting OpenLibrary (size={}) for ISBN {} (Book ID for log: {})", size, isbn, bookIdForLog);
        return openLibraryService.fetchAndCacheCover(isbn, bookIdForLog, size, provenanceData)
            .exceptionally(ex -> {
                logger.warn("OpenLibrary fetchAndCacheCover exception for ISBN {} (size {}): {}", isbn, size, ex.getMessage());
                return placeholder(bookIdForLog, "openlibrary-" + size.toLowerCase() + "-exception");
            });
    }
    
    private ImageDetails selectBestImageDetails(List<ImageDetails> candidates, String bookIdForLog, ImageProvenanceData provenanceData) {
        ImageSelectionService.SelectionResult selectionResult = imageSelectionService.selectBest(
            candidates,
            ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH,
            bookIdForLog
        );

        if (!selectionResult.hasSelection()) {
            String fallbackReason = selectionResult.fallbackReason() != null
                ? selectionResult.fallbackReason()
                : "no-candidates";
            return placeholder(bookIdForLog, fallbackReason);
        }

        ImageDetails bestImage = selectionResult.bestImage();
        imageProvenanceHandler.recordSelection(
            provenanceData,
            CoverSourceMapper.toImageSourceName(bestImage.getCoverImageSource()),
            bestImage,
            "Selected by dimension and source preference"
        );
        return bestImage;
    }

    private boolean isValidImageDetails(ImageDetails imageDetails) {
        if (imageDetails == null || !ValidationUtils.hasText(imageDetails.getUrlOrPath())) {
            return false;
        }
        if (ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH.equals(imageDetails.getUrlOrPath())) {
            return false;
        }
        return ImageDimensionUtils.hasAcceptableDimensions(imageDetails);
    }

    private ImageDetails placeholder(String bookIdForLog, String reasonSuffix) {
        return localDiskCoverCacheService.placeholderImageDetails(bookIdForLog, reasonSuffix);
    }

    private CompletableFuture<ImageDetails> cacheImageLocally(String imageUrl,
                                                               String bookIdForLog,
                                                               ImageProvenanceData provenanceData,
                                                               String sourceLabel) {
        return s3BookCoverService
            .uploadCoverToS3Async(imageUrl, bookIdForLog, sourceLabel, provenanceData)
            .toFuture();
    }

    /**
     * Attempts to fetch cover from S3
     * @param book The book object
     * @param bookIdForLog Identifier for logging
     * @param provenanceData Container for tracking attempts
     * @return CompletableFuture with ImageDetails from S3, or placeholder
     */
    /**
     * @deprecated Prefer {@link BookCoverManagementService#getInitialCoverUrlAndTriggerBackgroundUpdate}
     * which handles S3 lookups and persistence atomically.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private CompletableFuture<ImageDetails> tryS3(Book book, String bookIdForLog, ImageProvenanceData provenanceData) {
        logger.debug("Attempting S3 for Book ID {}", bookIdForLog);
        // Direct S3 check through HEAD request
        // Returns S3 URL without downloading content
        return s3BookCoverService.fetchCover(book)
            .thenApply(s3RemoteDetailsOptional -> {
                if (s3RemoteDetailsOptional.isPresent()) {
                    ImageDetails s3RemoteDetails = s3RemoteDetailsOptional.get();
                    boolean isS3Stored = isStoredInS3(s3RemoteDetails);
                    ImageSourceName provenanceSource = determineProvenanceSource(s3RemoteDetails);

                    if (isValidImageDetails(s3RemoteDetails) && isS3Stored) {
                        logger.info("S3 provided valid image for Book ID {}: {}", bookIdForLog, s3RemoteDetails.getUrlOrPath());
                        imageProvenanceHandler.recordAttempt(
                            provenanceData,
                            provenanceSource,
                            "S3 Direct Fetch",
                            ImageAttemptStatus.SUCCESS,
                            null,
                            s3RemoteDetails
                        );
                        return s3RemoteDetails;
                    }
                    String s3UrlAttempted = s3RemoteDetails.getSourceSystemId() != null ? s3RemoteDetails.getSourceSystemId() : "S3 Direct Fetch";
                    imageProvenanceHandler.recordAttempt(
                        provenanceData,
                        provenanceSource,
                        s3UrlAttempted,
                        ImageAttemptStatus.FAILURE_INVALID_DETAILS,
                        "S3 details not valid or storage mismatch",
                        s3RemoteDetails
                    );
                    logger.debug("S3 provided Optional<ImageDetails> but it was not valid or not stored in S3 for Book ID {}. Details: {}", bookIdForLog, s3RemoteDetails);
                } else {
                    imageProvenanceHandler.recordAttempt(
                        provenanceData,
                        ImageSourceName.INTERNAL_PROCESSING,
                        "S3 Direct Fetch",
                        ImageAttemptStatus.FAILURE_NOT_FOUND,
                        "No details from S3 fetchCover",
                        null
                    );
                    logger.debug("S3 did not provide Optional<ImageDetails> for Book ID {}.", bookIdForLog);
                }
                return placeholder(bookIdForLog, "s3-miss-or-invalid");
            })
            .exceptionally(ex -> {
                logger.error("Exception trying S3 for Book ID {}: {}", bookIdForLog, ex.getMessage());
                imageProvenanceHandler.recordAttempt(
                    provenanceData,
                    ImageSourceName.INTERNAL_PROCESSING,
                    "S3 Direct Fetch",
                    ImageAttemptStatus.FAILURE_GENERIC,
                    ex.getMessage(),
                    null
                );
                return placeholder(bookIdForLog, "s3-exception");
            });
    }

    /**
     * @deprecated Use {@link GoogleBooksService#fetchCoverByIsbn(String, String, com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData, LocalDiskCoverCacheService, CoverCacheManager)}
     * in combination with {@link CoverPersistenceService}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private CompletableFuture<ImageDetails> tryGoogleBooksApiByIsbn(String isbn, String bookIdForLog, ImageProvenanceData provenanceData) {
        logger.debug("Attempting Google Books API by ISBN {} (Book ID for log: {})", isbn, bookIdForLog);
        return tryTieredCoverLookup(isbn, bookIdForLog, provenanceData)
            .exceptionally(ex -> {
                logger.warn("Tiered image lookup for ISBN {} failed: {}", isbn, ex.getMessage());
                return null;
            })
            .thenCompose(details -> {
                if (isValidImageDetails(details)) {
                    return CompletableFuture.completedFuture(details);
                }
                return googleBooksService.fetchCoverByIsbn(
                    isbn,
                    bookIdForLog,
                    provenanceData,
                    localDiskCoverCacheService,
                    coverCacheManager
                );
            });
    }

    private boolean isStoredInS3(ImageDetails details) {
        if (details == null) {
            return false;
        }
        if ("S3".equals(details.getStorageLocation())) {
            return true;
        }
        CoverImageSource source = details.getCoverImageSource();
        if (source == null) {
            return false;
        }
        String name = source.name();
        return "S3_CACHE".equals(name) || "S3".equals(name);
    }

    private ImageSourceName determineProvenanceSource(ImageDetails details) {
        if (details == null) {
            return ImageSourceName.UNKNOWN;
        }
        ImageSourceName mapped = CoverSourceMapper.toImageSourceName(details.getCoverImageSource());
        if (mapped == ImageSourceName.UNKNOWN) {
            String storage = details.getStorageLocation();
            if ("S3".equals(storage) || "LOCAL".equals(storage)) {
                return ImageSourceName.INTERNAL_PROCESSING;
            }
        }
        return mapped;
    }

    /**
     * @deprecated Use {@link GoogleBooksService#fetchCoverByVolumeId(String, String, com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData, LocalDiskCoverCacheService, CoverCacheManager)}
     * coupled with {@link CoverPersistenceService} for canonical persistence.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private CompletableFuture<ImageDetails> tryGoogleBooksApiByVolumeId(String googleVolumeId, String bookIdForLog, ImageProvenanceData provenanceData) {
        logger.debug("Attempting Google Books API by Volume ID {} (Book ID for log: {})", googleVolumeId, bookIdForLog);
        return tryTieredCoverLookup(googleVolumeId, bookIdForLog, provenanceData)
            .exceptionally(ex -> {
                logger.warn("Tiered image lookup for volume {} failed: {}", googleVolumeId, ex.getMessage());
                return null;
            })
            .thenCompose(details -> {
                if (isValidImageDetails(details)) {
                    return CompletableFuture.completedFuture(details);
                }
                return fallbackCoverFromVolume(googleVolumeId, bookIdForLog, provenanceData);
            });
    }

    private CompletableFuture<ImageDetails> fallbackCoverFromVolume(String googleVolumeId,
                                                                    String bookIdForLog,
                                                                    ImageProvenanceData provenanceData) {
        CompletableFuture<ImageDetails> canonicalFuture;

        if (bookDataOrchestrator == null) {
            canonicalFuture = CompletableFuture.completedFuture(null);
        } else {
            canonicalFuture = bookDataOrchestrator.fetchCanonicalBookReactive(googleVolumeId)
                .map(book -> imageDetailsFromBook(book, bookIdForLog))
                .filter(this::isValidImageDetails)
                .doOnNext(detail -> imageProvenanceHandler.recordAttempt(
                    provenanceData,
                    ImageSourceName.INTERNAL_PROCESSING,
                    "canonical-volume:" + googleVolumeId,
                    ImageAttemptStatus.SUCCESS,
                    null,
                    detail))
                .onErrorResume(ex -> {
                    logger.warn("Canonical cover lookup failed for volume {}: {}", googleVolumeId, ex.getMessage());
                    return Mono.empty();
                })
                .toFuture();
        }

        return canonicalFuture.thenCompose(detail -> {
            if (detail != null && isValidImageDetails(detail)) {
                return CompletableFuture.completedFuture(detail);
            }
            return fetchCoverFromGoogleVolumeApi(googleVolumeId, bookIdForLog, provenanceData);
        });
    }

    private CompletableFuture<ImageDetails> fetchCoverFromGoogleVolumeApi(String googleVolumeId,
                                                                          String bookIdForLog,
                                                                          ImageProvenanceData provenanceData) {
        if (googleApiFetcher == null) {
            logger.debug("GoogleApiFetcher unavailable for volume {}", googleVolumeId);
            return CompletableFuture.completedFuture(placeholder(bookIdForLog, "google-volume-fetcher-missing"));
        }

        return googleApiFetcher.fetchVolumeByIdAuthenticated(googleVolumeId)
            .switchIfEmpty(googleApiFetcher.fetchVolumeByIdUnauthenticated(googleVolumeId))
            .map(this::convertJsonToBook)
            .filter(Objects::nonNull)
            .flatMap(googleBook -> {
                if (googleBook.getRawJsonResponse() != null && provenanceData.getGoogleBooksApiResponse() == null) {
                    provenanceData.setGoogleBooksApiResponse(googleBook.getRawJsonResponse());
                }

                String googleUrl = googleBook.getExternalImageUrl();
                if (!ValidationUtils.hasText(googleUrl) || coverCacheManager.isKnownBadImageUrl(googleUrl)) {
                    imageProvenanceHandler.recordAttempt(
                        provenanceData,
                        ImageSourceName.GOOGLE_BOOKS,
                        "Google VolumeID: " + googleVolumeId,
                        ImageAttemptStatus.FAILURE_NOT_FOUND,
                        "No usable image returned from Google",
                        null
                    );
                    return Mono.just(placeholder(bookIdForLog, "google-volume-inline-no-image"));
                }

                String enhancedGoogleUrl = GoogleBooksUrlEnhancer.enhanceUrl(googleUrl, 0);
                return Mono.fromFuture(cacheImageLocally(
                        enhancedGoogleUrl,
                        bookIdForLog,
                        provenanceData,
                        "GoogleBooksAPI-VolumeID"))
                    .onErrorResume(ex -> {
                        logger.error("Exception downloading Google cover for volume {}: {}", googleVolumeId, ex.getMessage());
                        imageProvenanceHandler.recordAttempt(
                            provenanceData,
                            ImageSourceName.GOOGLE_BOOKS,
                            "Google VolumeID: " + googleVolumeId,
                            ImageAttemptStatus.FAILURE_GENERIC,
                            ex.getMessage(),
                            null
                        );
                        return Mono.just(placeholder(bookIdForLog, "google-volume-inline-exception"));
                    });
            })
            .switchIfEmpty(Mono.just(placeholder(bookIdForLog, "google-volume-inline-no-image")))
            .toFuture();
    }

    private Book convertJsonToBook(JsonNode node) {
        if (node == null) {
            return null;
        }

        try {
            BookAggregate aggregate = googleBooksMapper.map(node);
            Book book = BookDomainMapper.fromAggregate(aggregate);
            if (book != null) {
                book.setRawJsonResponse(node.toString());
            }
            return book;
        } catch (Exception ex) {
            logger.debug("Failed to map Google Books JSON node: {}", ex.getMessage());
            return null;
        }
    }
        private boolean isLikelyGoogleCoverUrl(String url, String bookIdForLog, String contextHint) {
        if (!ValidationUtils.hasText(url)) {
            logger.warn("Book ID {}: Likelihood check received null or empty URL. Context: {}", bookIdForLog, contextHint);
            return false;
        }

        boolean likely = GoogleBooksUrlEnhancer.isGoogleBooksUrl(url);

        if (likely) {
            if (GoogleBooksUrlEnhancer.hasFrontCoverHint(url)) {
                logger.debug("Book ID {}: URL {} has front-cover hint. Context: {}. Treating as likely cover.", bookIdForLog, url, contextHint);
            } else {
                logger.debug("Book ID {}: URL {} passed heuristics without explicit front-cover hint. Context: {}.", bookIdForLog, url, contextHint);
            }
        } else {
            logger.debug("Book ID {}: URL {} failed Google cover heuristics. Context: {}.", bookIdForLog, url, contextHint);
        }

        return likely;
    }
}
