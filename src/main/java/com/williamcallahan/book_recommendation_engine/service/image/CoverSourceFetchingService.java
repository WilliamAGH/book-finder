/**
 * Service for fetching book cover images from various sources
 * - Implements a strategy to try multiple sources in a defined order
 * - Coordinates with local disk caching and S3 services
 * - Tracks provenance of image fetching attempts
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageAttemptStatus;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.model.image.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.util.ImageCacheUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils.BookValidator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
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
    private final CoverCacheManager coverCacheManager;
    private final BookDataOrchestrator bookDataOrchestrator;

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
            CoverCacheManager coverCacheManager,
            BookDataOrchestrator bookDataOrchestrator) {
        this.localDiskCoverCacheService = localDiskCoverCacheService;
        this.s3BookCoverService = s3BookCoverService;
        this.openLibraryService = openLibraryService;
        this.longitoodService = longitoodService;
        this.googleBooksService = googleBooksService;
        this.coverCacheManager = coverCacheManager;
        this.bookDataOrchestrator = bookDataOrchestrator;
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
        String bookIdForLog = ImageCacheUtils.getIdentifierKey(book) != null ? ImageCacheUtils.getIdentifierKey(book) : "unknown_book_id";
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
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "overall-fetch-exception");
            });
    }

    private CompletableFuture<List<ImageDetails>> processProvisionalHintAsync(Book book, String provisionalUrlHint, String bookIdForLog, ImageProvenanceData provenanceData) {
        if (provisionalUrlHint == null || provisionalUrlHint.isEmpty() ||
            provisionalUrlHint.equals(localDiskCoverCacheService.getLocalPlaceholderPath()) ||
            provisionalUrlHint.startsWith("/" + localDiskCoverCacheService.getCacheDirName())) {
            logger.debug("Book ID {}: No valid provisional hint provided or hint is local cache/placeholder.", bookIdForLog);
            return CompletableFuture.completedFuture(new ArrayList<>());
        }

        ImageSourceName hintSourceName = ImageCacheUtils.mapStringToImageSourceName("ProvisionalHint"); // Default
        if (provisionalUrlHint.contains("googleapis.com/books") || provisionalUrlHint.contains("books.google.com/books")) {
            hintSourceName = ImageSourceName.GOOGLE_BOOKS;
        } else if (provisionalUrlHint.contains("openlibrary.org")) {
            hintSourceName = ImageSourceName.OPEN_LIBRARY;
        } else if (provisionalUrlHint.contains("longitood.com")) {
            hintSourceName = ImageSourceName.LONGITOOD;
        }
        // S3 hints are typically direct CDN URLs and will be validated if encountered or picked up by S3 specific logic later.

        final ImageSourceName finalHintSourceName = hintSourceName;
        final int MIN_ACCEPTABLE_DIMENSION_NON_GOOGLE = 200;

        if (finalHintSourceName == ImageSourceName.GOOGLE_BOOKS) {
            List<CompletableFuture<ImageDetails>> googleHintFutures = new ArrayList<>();
            
            String originalHintEnhanced = ImageCacheUtils.enhanceGoogleCoverUrl(provisionalUrlHint, null); // Enhance but keep original zoom if present or add default
            if (isLikelyGoogleCoverUrl(originalHintEnhanced, bookIdForLog, "ProvisionalHint-Original")) {
                logger.debug("Book ID {}: Processing Google hint (enhanced as-is): {}", bookIdForLog, originalHintEnhanced);
                googleHintFutures.add(
                    localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(originalHintEnhanced, bookIdForLog, provenanceData, "GoogleHint-AsIs")
                );
            } else {
                 logger.debug("Book ID {}: Google hint (enhanced as-is) {} was deemed unlikely to be a cover. Skipping.", bookIdForLog, originalHintEnhanced);
            }

            String urlZoom0 = ImageCacheUtils.enhanceGoogleCoverUrl(provisionalUrlHint, "zoom=0");
            if (urlZoom0 != null && !urlZoom0.equals(originalHintEnhanced) && isLikelyGoogleCoverUrl(urlZoom0, bookIdForLog, "ProvisionalHint-Zoom0")) {
                logger.debug("Book ID {}: Processing Google hint (zoom=0 variant): {}", bookIdForLog, urlZoom0);
                googleHintFutures.add(
                    localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(urlZoom0, bookIdForLog, provenanceData, "GoogleHint-Zoom0")
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
            return localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(provisionalUrlHint, bookIdForLog, provenanceData, finalHintSourceName.name())
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

        return bookDataOrchestrator.getBookByIdTiered(identifier)
            .flatMap(book -> {
                if (book == null) {
                    return Mono.<Optional<ImageDetails>>empty();
                }

                ImageDetails fromBook = imageDetailsFromBook(book, identifier);
                if (isValidImageDetails(fromBook)) {
                    ImageCacheUtils.addAttemptToProvenance(provenanceData,
                        ImageCacheUtils.mapCoverImageSourceToImageSourceName(fromBook.getCoverImageSource()),
                        "postgres-tier:" + identifier,
                        ImageAttemptStatus.SUCCESS,
                        null,
                        fromBook);
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
            coverUrl = ImageCacheUtils.firstNonBlank(coverImages.getPreferredUrl(), coverImages.getFallbackUrl());
            source = coverImages.getSource();
        }

        String s3Path = book.getS3ImagePath();
        String externalUrl = book.getExternalImageUrl();
        coverUrl = ImageCacheUtils.firstNonBlank(coverUrl, s3Path, externalUrl);

        if (coverUrl == null || coverUrl.isBlank()) {
            return null;
        }

        if (source == null || source == CoverImageSource.UNDEFINED || source == CoverImageSource.ANY) {
            if (s3Path != null && coverUrl.equals(s3Path)) {
                source = CoverImageSource.S3_CACHE;
            } else if (externalUrl != null && coverUrl.equals(externalUrl)) {
                source = CoverImageSource.GOOGLE_BOOKS;
            } else {
                source = CoverImageSource.UNDEFINED;
            }
        }

        return new ImageDetails(
            coverUrl,
            "POSTGRES_CACHE",
            book.getId() != null ? book.getId() : fallbackSourceId,
            source,
            ImageResolutionPreference.ORIGINAL,
            ImageCacheUtils.normalizeImageDimension(book.getCoverImageWidth()),
            ImageCacheUtils.normalizeImageDimension(book.getCoverImageHeight())
        );
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
    private CompletableFuture<ImageDetails> fetchFromRemainingExternalSources(Book book, String bookIdForLog, ImageProvenanceData provenanceData, List<ImageDetails> existingCandidates) {
        List<CompletableFuture<ImageDetails>> sourceFutures = new ArrayList<>();
        
        // S3 is handled by fetchFromS3AndThenRemainingSources before this method.
        
        String isbn = BookValidator.getPreferredIsbn(book);
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
                return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "no-sources-or-candidates"));
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
                    return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "all-sources-failed-or-empty");
                }
                
                return selectBestImageDetails(allFetchedCandidates, bookIdForLog, provenanceData);
            })
            .exceptionally(ex -> {
                logger.error("Overall exception in fetchFromRemainingExternalSources for Book ID {}: {}. Selecting from prior or returning placeholder.", bookIdForLog, ex.getMessage(), ex);
                if (!existingCandidates.isEmpty()) {
                    logger.warn("Book ID {}: Falling back to selecting from {} existing candidates due to exception in remaining sources.", bookIdForLog, existingCandidates.size());
                    return selectBestImageDetails(existingCandidates, bookIdForLog, provenanceData);
                }
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "process-remaining-sources-exception");
            });
    }
    
    private CompletableFuture<ImageDetails> tryOpenLibrary(String isbn, String bookIdForLog, String size, ImageProvenanceData provenanceData) {
        logger.debug("Attempting OpenLibrary (size={}) for ISBN {} (Book ID for log: {})", size, isbn, bookIdForLog);
        return openLibraryService.fetchAndCacheCover(isbn, bookIdForLog, size, provenanceData)
            .exceptionally(ex -> {
                logger.warn("OpenLibrary fetchAndCacheCover exception for ISBN {} (size {}): {}", isbn, size, ex.getMessage());
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "openlibrary-" + size.toLowerCase() + "-exception");
            });
    }
    
    private ImageDetails selectBestImageDetails(List<ImageDetails> candidates, String bookIdForLog, ImageProvenanceData provenanceData) {
        ImageCacheUtils.ImageSelectionResult selectionResult = ImageCacheUtils.selectBestImageDetails(
            candidates,
            localDiskCoverCacheService.getLocalPlaceholderPath(),
            localDiskCoverCacheService.getCacheDirName(),
            bookIdForLog,
            logger
        );

        if (!selectionResult.hasSelection()) {
            String fallbackReason = selectionResult.fallbackReason() != null
                ? selectionResult.fallbackReason()
                : "no-candidates";
            return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, fallbackReason);
        }

        ImageDetails bestImage = selectionResult.bestImage();
        ImageCacheUtils.updateSelectedImageInfo(
            provenanceData,
            ImageCacheUtils.mapCoverImageSourceToImageSourceName(bestImage.getCoverImageSource()),
            bestImage,
            "Selected by dimension and source preference",
            localDiskCoverCacheService.getCacheDirName(),
            logger
        );
        return bestImage;
    }

    private boolean isValidImageDetails(ImageDetails imageDetails) {
        return ImageCacheUtils.isValidImageDetails(imageDetails, localDiskCoverCacheService.getLocalPlaceholderPath());
    }

    /**
     * Attempts to fetch cover from S3
     * @param book The book object
     * @param bookIdForLog Identifier for logging
     * @param provenanceData Container for tracking attempts
     * @return CompletableFuture with ImageDetails from S3, or placeholder
     */
    private CompletableFuture<ImageDetails> tryS3(Book book, String bookIdForLog, ImageProvenanceData provenanceData) {
        logger.debug("Attempting S3 for Book ID {}", bookIdForLog);
        // Direct S3 check through HEAD request
        // Returns S3 URL without downloading content
        return s3BookCoverService.fetchCover(book)
            .thenApply(s3RemoteDetailsOptional -> {
                if (s3RemoteDetailsOptional.isPresent()) {
                    ImageDetails s3RemoteDetails = s3RemoteDetailsOptional.get();
                    if (isValidImageDetails(s3RemoteDetails) && s3RemoteDetails.getCoverImageSource() == CoverImageSource.S3_CACHE) {
                        logger.info("S3 provided valid image for Book ID {}: {}", bookIdForLog, s3RemoteDetails.getUrlOrPath());
                        ImageCacheUtils.addAttemptToProvenance(provenanceData, ImageSourceName.S3_CACHE, "S3 Direct Fetch", ImageAttemptStatus.SUCCESS, null, s3RemoteDetails);
                        return s3RemoteDetails;
                    }
                    String s3UrlAttempted = s3RemoteDetails.getSourceSystemId() != null ? s3RemoteDetails.getSourceSystemId() : "S3 Direct Fetch";
                    ImageCacheUtils.addAttemptToProvenance(provenanceData, ImageSourceName.S3_CACHE, s3UrlAttempted, ImageAttemptStatus.FAILURE_INVALID_DETAILS, "S3 details not valid or not S3_CACHE source", s3RemoteDetails);
                    logger.debug("S3 provided Optional<ImageDetails> but it was not valid or not from S3_CACHE for Book ID {}. Details: {}", bookIdForLog, s3RemoteDetails);
                } else {
                    ImageCacheUtils.addAttemptToProvenance(provenanceData, ImageSourceName.S3_CACHE, "S3 Direct Fetch", ImageAttemptStatus.FAILURE_NOT_FOUND, "No details from S3 fetchCover", null);
                    logger.debug("S3 did not provide Optional<ImageDetails> for Book ID {}.", bookIdForLog);
                }
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "s3-miss-or-invalid");
            })
            .exceptionally(ex -> {
                logger.error("Exception trying S3 for Book ID {}: {}", bookIdForLog, ex.getMessage());
                ImageCacheUtils.addAttemptToProvenance(provenanceData, ImageSourceName.S3_CACHE, "S3 Direct Fetch", ImageAttemptStatus.FAILURE_GENERIC, ex.getMessage(), null); // Use FAILURE_GENERIC
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "s3-exception");
            });
    }

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
                // Inline fallback using getBookById to cooperate with unit test mocks
                return googleBooksService.getBookById(googleVolumeId)
                    .toCompletableFuture()
                    .thenCompose(googleBook -> {
                        if (googleBook != null && googleBook.getRawJsonResponse() != null && provenanceData.getGoogleBooksApiResponse() == null) {
                            provenanceData.setGoogleBooksApiResponse(googleBook.getRawJsonResponse());
                        }
                        String googleUrl = googleBook != null ? googleBook.getExternalImageUrl() : null;
                        if (ValidationUtils.hasText(googleUrl) && !coverCacheManager.isKnownBadImageUrl(googleUrl)) {
                            String enhancedGoogleUrl = ImageCacheUtils.enhanceGoogleCoverUrl(googleUrl, "zoom=0");
                            return localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(
                                enhancedGoogleUrl,
                                bookIdForLog,
                                provenanceData,
                                "GoogleBooksAPI-VolumeID");
                        }
                        ImageCacheUtils.addAttemptToProvenance(
                            provenanceData,
                            ImageSourceName.GOOGLE_BOOKS,
                            "Google VolumeID: " + googleVolumeId,
                            ImageAttemptStatus.FAILURE_NOT_FOUND,
                            "No usable image from Google/VolumeID inline fallback",
                            null
                        );
                        return CompletableFuture.completedFuture(
                            localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "google-volume-inline-no-image"));
                    })
                    .exceptionally(ex -> {
                        logger.error("Exception during inline Google VolumeID fallback for {}: {}", googleVolumeId, ex.getMessage());
                        ImageCacheUtils.addAttemptToProvenance(
                            provenanceData,
                            ImageSourceName.GOOGLE_BOOKS,
                            "Google VolumeID: " + googleVolumeId,
                            ImageAttemptStatus.FAILURE_GENERIC,
                            ex.getMessage(),
                            null
                        );
                        return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "google-volume-inline-exception");
                    });
            });
    }
        private boolean isLikelyGoogleCoverUrl(String url, String bookIdForLog, String contextHint) {
        if (!ValidationUtils.hasText(url)) {
            logger.warn("Book ID {}: Likelihood check received null or empty URL. Context: {}", bookIdForLog, contextHint);
            return false;
        }

        boolean likely = ImageCacheUtils.isLikelyGoogleCoverUrl(url);

        if (likely) {
            if (ImageCacheUtils.hasGoogleFrontCoverHint(url)) {
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
