package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.ImageAttemptStatus;
import com.williamcallahan.book_recommendation_engine.types.ImageDetails;
import com.williamcallahan.book_recommendation_engine.types.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.types.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.util.ImageCacheUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

/**
 * Service for fetching book cover images from various sources
 * - Implements a strategy to try multiple sources in a defined order
 * - Coordinates with local disk caching and S3 services
 * - Tracks provenance of image fetching attempts
 *
 * @author William Callahan
 */
@Service
public class CoverSourceFetchingService {

    private static final Logger logger = LoggerFactory.getLogger(CoverSourceFetchingService.class);

    private final LocalDiskCoverCacheService localDiskCoverCacheService;
    private final S3BookCoverService s3BookCoverService;
    private final OpenLibraryServiceImpl openLibraryService;
    private final LongitoodServiceImpl longitoodService;
    private final GoogleBooksService googleBooksService;
    private final CoverCacheManager coverCacheManager;

    /**
     * Constructs the CoverSourceFetchingService
     * @param localDiskCoverCacheService Service for local disk caching
     * @param s3BookCoverService Service for S3 interactions
     * @param openLibraryService Service for OpenLibrary covers
     * @param longitoodService Service for Longitood covers
     * @param googleBooksService Service for Google Books API
     * @param coverCacheManager Manager for in-memory caches
     */
    @Autowired
    public CoverSourceFetchingService(
            LocalDiskCoverCacheService localDiskCoverCacheService,
            S3BookCoverService s3BookCoverService,
            OpenLibraryServiceImpl openLibraryService,
            LongitoodServiceImpl longitoodService,
            GoogleBooksService googleBooksService,
            CoverCacheManager coverCacheManager) {
        this.localDiskCoverCacheService = localDiskCoverCacheService;
        this.s3BookCoverService = s3BookCoverService;
        this.openLibraryService = openLibraryService;
        this.longitoodService = longitoodService;
        this.googleBooksService = googleBooksService;
        this.coverCacheManager = coverCacheManager;
    }

    /**
     * Asynchronously retrieves the best available cover image for a book, trying multiple sources
     * - Uses a provisional URL hint if provided and valid
     * - Falls back to a systematic search through S3, Google Books, OpenLibrary, and Longitood
     * - Populates ImageProvenanceData with details of all attempts
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


        // Try provisional URL hint first if it's a valid external URL
        if (provisionalUrlHint != null && !provisionalUrlHint.isEmpty() &&
            !provisionalUrlHint.equals(localDiskCoverCacheService.getLocalPlaceholderPath()) &&
            !provisionalUrlHint.startsWith("/" + localDiskCoverCacheService.getCacheDirName())) {

            ImageSourceName hintSourceName = ImageCacheUtils.mapStringToImageSourceName("ProvisionalHint"); // Default
            if (provisionalUrlHint.contains("googleapis.com/books") || provisionalUrlHint.contains("books.google.com/books")) {
                hintSourceName = ImageSourceName.GOOGLE_BOOKS;
            } else if (provisionalUrlHint.contains("openlibrary.org")) {
                hintSourceName = ImageSourceName.OPEN_LIBRARY;
            } else if (provisionalUrlHint.contains("longitood.com")) {
                hintSourceName = ImageSourceName.LONGITOOD;
            }
            // S3 hints are handled by direct CDN URL patterns

            final ImageSourceName finalHintSourceName = hintSourceName;
            final int MIN_ACCEPTABLE_DIMENSION = 200; // Minimum width or height for a provisional image

            String urlToDownload = provisionalUrlHint;
            // Optimize Google Books URL if it's the hint
            if (finalHintSourceName == ImageSourceName.GOOGLE_BOOKS) {
                logger.debug("Provisional hint is from Google Books. Ensuring URL parameters are optimal: {}", urlToDownload);
                if (urlToDownload.contains("zoom=")) urlToDownload = urlToDownload.replaceAll("zoom=\\d+", "zoom=0");
                if (urlToDownload.contains("&fife=")) urlToDownload = urlToDownload.replaceAll("&fife=w\\d+", "");
                else if (urlToDownload.contains("?fife=")) {
                    urlToDownload = urlToDownload.replaceAll("\\?fife=w\\d+", "?");
                    if (urlToDownload.endsWith("?")) urlToDownload = urlToDownload.substring(0, urlToDownload.length() -1);
                }
                if (urlToDownload.endsWith("&")) urlToDownload = urlToDownload.substring(0, urlToDownload.length() - 1);
                logger.debug("Optimized Google Books URL for download: {}", urlToDownload);
            }
            
            return localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(urlToDownload, bookIdForLog, provenanceData, finalHintSourceName.name())
                .thenCompose(cachedFromHintDetails -> {
                    boolean isGoodQualityProvisional = cachedFromHintDetails.getWidth() != null && cachedFromHintDetails.getHeight() != null &&
                                                       cachedFromHintDetails.getWidth() >= MIN_ACCEPTABLE_DIMENSION &&
                                                       cachedFromHintDetails.getHeight() >= MIN_ACCEPTABLE_DIMENSION &&
                                                       !cachedFromHintDetails.getUrlOrPath().equals(localDiskCoverCacheService.getLocalPlaceholderPath());

                    // If hint is NOT Google Books AND it's good quality, take early exit
                    if (finalHintSourceName != ImageSourceName.GOOGLE_BOOKS && isGoodQualityProvisional) {
                        updateSelectedImageInfo(provenanceData, finalHintSourceName, cachedFromHintDetails);
                        return CompletableFuture.completedFuture(cachedFromHintDetails);
                    }
                    // Otherwise (Google hint OR not good quality), proceed to full source scan
                    logger.debug("Provisional URL hint {} (Source: {}) for Book ID {} was either from Google or not good enough ({}x{}). Proceeding to full source scan.",
                            provisionalUrlHint, finalHintSourceName, bookIdForLog, cachedFromHintDetails.getWidth(), cachedFromHintDetails.getHeight());
                    return processCoverSourcesSequentially(book, bookIdForLog, provenanceData);
                })
                .exceptionallyCompose(ex -> {
                    logger.error("Exception processing provisionalUrlHint {} for Book ID {}. Falling back. Error: {}", provisionalUrlHint, bookIdForLog, ex.getMessage());
                    return processCoverSourcesSequentially(book, bookIdForLog, provenanceData); // Return the CF directly
                });
        }
        // No valid provisional hint, or hint was local cache path, proceed to full scan
        return processCoverSourcesSequentially(book, bookIdForLog, provenanceData);
    }

    /**
     * Systematically tries multiple cover sources in a defined order
     * - S3 Cache
     * - Google Books API (by ISBN, then by Volume ID)
     * - OpenLibrary (L, M, S sizes)
     * - Longitood
     * @param book The book object
     * @param bookIdForLog Identifier for logging
     * @param provenanceData Container for tracking attempts
     * @return CompletableFuture with ImageDetails of the best found image, or placeholder
     */
    private CompletableFuture<ImageDetails> processCoverSourcesSequentially(Book book, String bookIdForLog, ImageProvenanceData provenanceData) {
        return tryS3(book, bookIdForLog, provenanceData)
            .thenCompose(detailsS3 -> {
                if (isValidImageDetails(detailsS3)) {
                    updateSelectedImageInfo(provenanceData, ImageSourceName.S3_CACHE, detailsS3);
                    return CompletableFuture.completedFuture(detailsS3);
                }
                String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();
                if (isbn != null && !isbn.isEmpty()) {
                    return tryGoogleBooksApiByIsbn(isbn, bookIdForLog, provenanceData)
                        .thenCompose(detailsGoogleIsbn -> {
                            if (isValidImageDetails(detailsGoogleIsbn)) {
                                updateSelectedImageInfo(provenanceData, ImageSourceName.GOOGLE_BOOKS, detailsGoogleIsbn);
                                return CompletableFuture.completedFuture(detailsGoogleIsbn);
                            }
                            return tryOpenLibrary(isbn, bookIdForLog, "L", provenanceData);
                        })
                        .thenCompose(detailsL -> {
                            if (isValidImageDetails(detailsL)) {
                                updateSelectedImageInfo(provenanceData, ImageSourceName.OPEN_LIBRARY, detailsL);
                                return CompletableFuture.completedFuture(detailsL);
                            }
                            return tryOpenLibrary(isbn, bookIdForLog, "M", provenanceData);
                        })
                        .thenCompose(detailsM -> {
                            if (isValidImageDetails(detailsM)) {
                                updateSelectedImageInfo(provenanceData, ImageSourceName.OPEN_LIBRARY, detailsM);
                                return CompletableFuture.completedFuture(detailsM);
                            }
                            return tryOpenLibrary(isbn, bookIdForLog, "S", provenanceData);
                        })
                        .thenCompose(detailsS -> {
                            if (isValidImageDetails(detailsS)) {
                                updateSelectedImageInfo(provenanceData, ImageSourceName.OPEN_LIBRARY, detailsS);
                                return CompletableFuture.completedFuture(detailsS);
                            }
                            return tryLongitood(book, bookIdForLog, provenanceData);
                        })
                        .thenCompose(detailsLongitood -> {
                             if (isValidImageDetails(detailsLongitood)) {
                                 updateSelectedImageInfo(provenanceData, ImageSourceName.LONGITOOD, detailsLongitood);
                                 return CompletableFuture.completedFuture(detailsLongitood);
                             }
                             // Final fallback if ISBN present but all failed
                             return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "all-isbn-sources-failed"));
                        });
                } else if (book.getId() != null && !book.getId().isEmpty()) { // Google Volume ID
                    return tryGoogleBooksApiByVolumeId(book.getId(), bookIdForLog, provenanceData)
                        .thenCompose(detailsGoogleId -> {
                            if (isValidImageDetails(detailsGoogleId)) {
                                updateSelectedImageInfo(provenanceData, ImageSourceName.GOOGLE_BOOKS, detailsGoogleId);
                                return CompletableFuture.completedFuture(detailsGoogleId);
                            }
                            return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "google-volid-failed"));
                        });
                } else {
                    logger.warn("No usable identifier (ISBN or Google Volume ID) for Book ID {}. Returning placeholder.", bookIdForLog);
                    return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "no-identifier"));
                }
            })
            .exceptionally(ex -> {
                logger.error("Overall exception in processCoverSourcesSequentially for Book ID {}: {}", bookIdForLog, ex.getMessage(), ex);
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "process-sources-exception");
            });
    }

    /**
     * Checks if ImageDetails represents a valid, non-placeholder image
     * @param imageDetails The ImageDetails to check
     * @return True if valid, false otherwise
     */
    private boolean isValidImageDetails(ImageDetails imageDetails) {
        return imageDetails != null &&
               imageDetails.getUrlOrPath() != null &&
               !imageDetails.getUrlOrPath().equals(localDiskCoverCacheService.getLocalPlaceholderPath()) &&
               imageDetails.getWidth() != null && imageDetails.getWidth() > 0 && // Assuming 0 width means placeholder or error
               imageDetails.getHeight() != null && imageDetails.getHeight() > 0;
    }
    
    /**
     * Updates the SelectedImageInfo in ImageProvenanceData if not already set
     * @param provenanceData The ImageProvenanceData object
     * @param sourceName The source from which the image was selected
     * @param imageDetails The details of the selected image
     */
    private void updateSelectedImageInfo(ImageProvenanceData provenanceData, ImageSourceName sourceName, ImageDetails imageDetails) {
        if (provenanceData != null && provenanceData.getSelectedImageInfo() == null && imageDetails != null) {
            ImageProvenanceData.SelectedImageInfo selectedInfo = new ImageProvenanceData.SelectedImageInfo();
            selectedInfo.setSourceName(sourceName);
            selectedInfo.setFinalUrl(imageDetails.getUrlOrPath());
            selectedInfo.setResolution(imageDetails.getResolutionPreference() != null ? imageDetails.getResolutionPreference().name() : "ORIGINAL");
            selectedInfo.setStorageLocation(imageDetails.getUrlOrPath().startsWith("/" + localDiskCoverCacheService.getCacheDirName()) ? "LocalCache" : "Remote");
            if (imageDetails.getCoverImageSource() == CoverImageSource.S3_CACHE) {
                selectedInfo.setStorageLocation("S3");
                selectedInfo.setS3Key(imageDetails.getSourceSystemId());
            }
            provenanceData.setSelectedImageInfo(selectedInfo);
        }
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
        return s3BookCoverService.fetchCover(book) // Assuming this now returns CompletableFuture<Optional<ImageDetails>>
            .thenCompose(s3RemoteDetailsOptional -> { // s3RemoteDetailsOptional is Optional<ImageDetails>
                if (s3RemoteDetailsOptional.isPresent()) {
                    ImageDetails s3RemoteDetails = s3RemoteDetailsOptional.get();
                    if (isValidImageDetails(s3RemoteDetails) && s3RemoteDetails.getCoverImageSource() == CoverImageSource.S3_CACHE) {
                        // S3 URL directly usable without local caching
                        logger.info("S3 provided valid image for Book ID {}: {}", bookIdForLog, s3RemoteDetails.getUrlOrPath());
                        return CompletableFuture.completedFuture(s3RemoteDetails);
                    }
                    logger.debug("S3 provided Optional<ImageDetails> but it was not valid or not from S3_CACHE for Book ID {}. Details: {}", bookIdForLog, s3RemoteDetails);
                } else {
                    logger.debug("S3 did not provide Optional<ImageDetails> for Book ID {}.", bookIdForLog);
                }
                return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "s3-failed-or-not-s3-or-empty-optional"));
            })
            .exceptionally(ex -> {
                logger.error("Exception trying S3 for Book ID {}: {}", bookIdForLog, ex.getMessage());
                // Add S3 attempt to provenance if not already handled by S3BookCoverService
                if (provenanceData.getAttemptedImageSources().stream().noneMatch(a -> a.getSourceName() == ImageSourceName.S3_CACHE)) {
                     ImageProvenanceData.AttemptedSourceInfo s3Attempt = new ImageProvenanceData.AttemptedSourceInfo(ImageSourceName.S3_CACHE, "S3 Direct Fetch for " + bookIdForLog, ImageAttemptStatus.FAILURE_GENERIC);
                     s3Attempt.setFailureReason(ex.getMessage());
                     provenanceData.getAttemptedImageSources().add(s3Attempt);
                }
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "s3-exception");
            });
    }

    /**
     * Attempts to fetch cover from OpenLibrary
     * @param isbn The ISBN of the book
     * @param bookIdForLog Identifier for logging
     * @param sizeSuffix Size suffix for OpenLibrary URL (L, M, S)
     * @param provenanceData Container for tracking attempts
     * @return CompletableFuture with ImageDetails from OpenLibrary, or placeholder
     */
    private CompletableFuture<ImageDetails> tryOpenLibrary(String isbn, String bookIdForLog, String sizeSuffix, ImageProvenanceData provenanceData) {
        if (coverCacheManager.isKnownBadOpenLibraryIsbn(isbn)) {
            logger.debug("Skipping OpenLibrary for known bad ISBN: {}", isbn);
            // Add skipped attempt to provenance
            ImageProvenanceData.AttemptedSourceInfo olAttempt = new ImageProvenanceData.AttemptedSourceInfo(ImageSourceName.OPEN_LIBRARY, "isbn:" + isbn + ", size:" + sizeSuffix, ImageAttemptStatus.SKIPPED_BAD_URL);
            provenanceData.getAttemptedImageSources().add(olAttempt);
            return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "ol-known-bad-" + sizeSuffix));
        }
        logger.debug("Attempting OpenLibrary for ISBN {}, size {} (Book ID for log: {})", isbn, sizeSuffix, bookIdForLog);
        final String finalIsbn = isbn; // For use in lambda

        return openLibraryService.fetchOpenLibraryCoverDetails(isbn, sizeSuffix) // Assuming this now returns CompletableFuture<Optional<ImageDetails>>
            .thenCompose(remoteImageDetailsOptional -> { // remoteImageDetailsOptional is Optional<ImageDetails>
                if (remoteImageDetailsOptional.isPresent()) {
                    ImageDetails remoteImageDetails = remoteImageDetailsOptional.get();
                    if (remoteImageDetails.getUrlOrPath() != null && !remoteImageDetails.getUrlOrPath().isEmpty()) {
                        return localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(remoteImageDetails.getUrlOrPath(), bookIdForLog, provenanceData, "OpenLibrary-" + sizeSuffix)
                            .thenApply(cachedDetails -> { // Ensure we return the details of the *cached* version
                                 if (isValidImageDetails(cachedDetails)) return cachedDetails;
                                 coverCacheManager.addKnownBadOpenLibraryIsbn(finalIsbn); // Mark as bad if download/cache failed
                                 return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "ol-" + sizeSuffix + "-dl-fail");
                            });
                    } else {
                        // Optional was present, but ImageDetails inside had no URL
                         logger.warn("OpenLibraryService provided ImageDetails but no URL for ISBN {} size {}.", finalIsbn, sizeSuffix);
                    }
                } else {
                     // Optional was empty
                    logger.warn("OpenLibraryService did not provide ImageDetails for ISBN {} size {}.", finalIsbn, sizeSuffix);
                }
                // Common failure path if not returned earlier
                ImageProvenanceData.AttemptedSourceInfo olAttempt = new ImageProvenanceData.AttemptedSourceInfo(ImageSourceName.OPEN_LIBRARY, "isbn:" + finalIsbn + ", size:" + sizeSuffix, ImageAttemptStatus.FAILURE_404);
                provenanceData.getAttemptedImageSources().add(olAttempt);
                return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "ol-" + sizeSuffix + "-no-url-or-empty"));
            })
            .exceptionally(ex -> {
                logger.error("Exception trying OpenLibrary for ISBN {}, size {}: {}", finalIsbn, sizeSuffix, ex.getMessage());
                coverCacheManager.addKnownBadOpenLibraryIsbn(finalIsbn);
                ImageProvenanceData.AttemptedSourceInfo olAttempt = new ImageProvenanceData.AttemptedSourceInfo(ImageSourceName.OPEN_LIBRARY, "isbn:" + isbn + ", size:" + sizeSuffix, ImageAttemptStatus.FAILURE_GENERIC);
                olAttempt.setFailureReason(ex.getMessage());
                provenanceData.getAttemptedImageSources().add(olAttempt);
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "ol-" + sizeSuffix + "-exception");
            });
    }
    
    /**
     * Attempts to fetch cover from Google Books API by ISBN
     * @param isbn The ISBN of the book
     * @param bookIdForLog Identifier for logging
     * @param provenanceData Container for tracking attempts
     * @return CompletableFuture with ImageDetails from Google, or placeholder
     */
    private CompletableFuture<ImageDetails> tryGoogleBooksApiByIsbn(String isbn, String bookIdForLog, ImageProvenanceData provenanceData) {
        logger.debug("Attempting Google Books API by ISBN {} (Book ID for log: {})", isbn, bookIdForLog);
        // Directly use the CompletableFuture from searchBooksByISBN if it's adapted,
        // or convert Mono<List<Book>> to CompletableFuture<List<Book>>
        // For now, assuming searchBooksByISBN still returns Mono<List<Book>> and needs conversion
        return googleBooksService.searchBooksByISBN(isbn)
            .toFuture()
            .thenComposeAsync(books -> {
                if (books != null && !books.isEmpty()) {
                    Book gBook = books.get(0); // Take the first result
                    if (gBook != null && gBook.getRawJsonResponse() != null && provenanceData.getGoogleBooksApiResponse() == null) {
                        provenanceData.setGoogleBooksApiResponse(gBook.getRawJsonResponse());
                    }
                    if (gBook != null && gBook.getCoverImageUrl() != null && !gBook.getCoverImageUrl().isEmpty() &&
                        !coverCacheManager.isKnownBadImageUrl(gBook.getCoverImageUrl()) &&
                        !gBook.getCoverImageUrl().contains("image-not-available.png")) {
                        return localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(gBook.getCoverImageUrl(), bookIdForLog, provenanceData, "GoogleBooksAPI-ISBN");
                    }
                }
                logger.warn("Google Books API (by ISBN) did not yield a usable image for ISBN {} (Book ID for log: {})", isbn, bookIdForLog);
                ImageProvenanceData.AttemptedSourceInfo gbAttempt = new ImageProvenanceData.AttemptedSourceInfo(ImageSourceName.GOOGLE_BOOKS, "isbn:" + isbn, ImageAttemptStatus.FAILURE_404);
                provenanceData.getAttemptedImageSources().add(gbAttempt);
                return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "google-isbn-no-image"));
            })
            .exceptionally(ex -> {
                logger.error("Exception trying Google Books API by ISBN {} (Book ID for log: {}): {}", isbn, bookIdForLog, ex.getMessage(), ex);
                ImageProvenanceData.AttemptedSourceInfo gbAttempt = new ImageProvenanceData.AttemptedSourceInfo(ImageSourceName.GOOGLE_BOOKS, "isbn:" + isbn, ImageAttemptStatus.FAILURE_GENERIC);
                gbAttempt.setFailureReason(ex.getMessage());
                provenanceData.getAttemptedImageSources().add(gbAttempt);
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "google-isbn-exception");
            });
    }

    /**
     * Attempts to fetch cover from Google Books API by Volume ID
     * @param googleVolumeId The Google Books Volume ID
     * @param bookIdForLog Identifier for logging
     * @param provenanceData Container for tracking attempts
     * @return CompletableFuture with ImageDetails from Google, or placeholder
     */
    private CompletableFuture<ImageDetails> tryGoogleBooksApiByVolumeId(String googleVolumeId, String bookIdForLog, ImageProvenanceData provenanceData) {
        logger.debug("Attempting Google Books API by Volume ID {} (Book ID for log: {})", googleVolumeId, bookIdForLog);
        return googleBooksService.getBookById(googleVolumeId) // This now returns CompletableFuture<Book>
            // No .toFuture() needed here as it's already a CompletableFuture
            .thenComposeAsync(gBook -> {
                if (gBook != null && gBook.getRawJsonResponse() != null && provenanceData.getGoogleBooksApiResponse() == null) {
                    provenanceData.setGoogleBooksApiResponse(gBook.getRawJsonResponse());
                }
                if (gBook != null && gBook.getCoverImageUrl() != null && !gBook.getCoverImageUrl().isEmpty() &&
                    !coverCacheManager.isKnownBadImageUrl(gBook.getCoverImageUrl()) &&
                    !gBook.getCoverImageUrl().contains("image-not-available.png")) {
                    return localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(gBook.getCoverImageUrl(), bookIdForLog, provenanceData, "GoogleBooksAPI-VolumeID");
                }
                logger.warn("Google Books API (by Volume ID) did not yield a usable image for Volume ID {} (Book ID for log: {})", googleVolumeId, bookIdForLog);
                ImageProvenanceData.AttemptedSourceInfo gbAttempt = new ImageProvenanceData.AttemptedSourceInfo(ImageSourceName.GOOGLE_BOOKS, "volumeId:" + googleVolumeId, ImageAttemptStatus.FAILURE_404);
                provenanceData.getAttemptedImageSources().add(gbAttempt);
                return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "google-volid-no-image"));
            })
            .exceptionally(ex -> {
                logger.error("Exception trying Google Books API by Volume ID {} (Book ID for log: {}): {}", googleVolumeId, bookIdForLog, ex.getMessage(), ex);
                ImageProvenanceData.AttemptedSourceInfo gbAttempt = new ImageProvenanceData.AttemptedSourceInfo(ImageSourceName.GOOGLE_BOOKS, "volumeId:" + googleVolumeId, ImageAttemptStatus.FAILURE_GENERIC);
                gbAttempt.setFailureReason(ex.getMessage());
                provenanceData.getAttemptedImageSources().add(gbAttempt);
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "google-volid-exception");
            });
    }
    
    /**
     * Attempts to fetch cover from Longitood
     * @param book The book object
     * @param bookIdForLog Identifier for logging
     * @param provenanceData Container for tracking attempts
     * @return CompletableFuture with ImageDetails from Longitood, or placeholder
     */
    private CompletableFuture<ImageDetails> tryLongitood(Book book, String bookIdForLog, ImageProvenanceData provenanceData) {
        String isbn = ImageCacheUtils.getIdentifierKey(book); // Prefer ISBN for Longitood
        if (isbn == null) { 
            logger.warn("Longitood requires ISBN, not found for Book ID for log: {}", bookIdForLog);
            return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "longitood-no-isbn"));
        }
        if (coverCacheManager.isKnownBadLongitoodIsbn(isbn)) {
            logger.debug("Skipping Longitood for known bad ISBN: {}", isbn);
            ImageProvenanceData.AttemptedSourceInfo ltAttempt = new ImageProvenanceData.AttemptedSourceInfo(ImageSourceName.LONGITOOD, "isbn:" + isbn, ImageAttemptStatus.SKIPPED_BAD_URL);
            provenanceData.getAttemptedImageSources().add(ltAttempt);
            return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "longitood-known-bad"));
        }
        logger.debug("Attempting Longitood for ISBN {} (Book ID for log: {})", isbn, bookIdForLog);

        final String finalIsbn = isbn; // For use in lambda

        return longitoodService.fetchCover(book) // Assuming this now returns CompletableFuture<Optional<ImageDetails>>
            // .toFuture() // Removed redundant .toFuture()
            .thenCompose(remoteImageDetailsOptional -> { // remoteImageDetailsOptional is Optional<ImageDetails>
                if (remoteImageDetailsOptional.isPresent()) {
                    ImageDetails remoteImageDetails = remoteImageDetailsOptional.get();
                    if (remoteImageDetails.getUrlOrPath() != null && !remoteImageDetails.getUrlOrPath().isEmpty()) {
                        return localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(remoteImageDetails.getUrlOrPath(), bookIdForLog, provenanceData, "Longitood")
                             .thenApply(cachedDetails -> {
                                 if (isValidImageDetails(cachedDetails)) {
                                     return cachedDetails;
                                 }
                                 // Download or caching failed for a valid URL
                                 coverCacheManager.addKnownBadLongitoodIsbn(finalIsbn); 
                                 return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "longitood-dl-fail");
                             });
                    } else {
                        // Optional was present, but ImageDetails inside had no URL
                        logger.warn("LongitoodService provided ImageDetails but no URL for ISBN {}.", finalIsbn);
                        coverCacheManager.addKnownBadLongitoodIsbn(finalIsbn);
                    }
                } else {
                    // Optional was empty
                    logger.warn("LongitoodService did not provide ImageDetails for ISBN {}.", finalIsbn);
                    coverCacheManager.addKnownBadLongitoodIsbn(finalIsbn);
                }
                
                // Common failure path if not returned earlier
                ImageProvenanceData.AttemptedSourceInfo ltAttempt = new ImageProvenanceData.AttemptedSourceInfo(ImageSourceName.LONGITOOD, "isbn:" + finalIsbn, ImageAttemptStatus.FAILURE_404);
                provenanceData.getAttemptedImageSources().add(ltAttempt);
                return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "longitood-no-url-or-empty"));
            })
            .exceptionally(ex -> {
                logger.error("Exception trying Longitood for ISBN {}: {}", finalIsbn, ex.getMessage());
                coverCacheManager.addKnownBadLongitoodIsbn(finalIsbn);
                ImageProvenanceData.AttemptedSourceInfo ltAttempt = new ImageProvenanceData.AttemptedSourceInfo(ImageSourceName.LONGITOOD, "isbn:" + finalIsbn, ImageAttemptStatus.FAILURE_GENERIC);
                ltAttempt.setFailureReason(ex.getMessage());
                provenanceData.getAttemptedImageSources().add(ltAttempt);
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "longitood-exception");
            });
    }
}
