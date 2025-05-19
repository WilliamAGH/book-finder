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
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.ImageAttemptStatus;
import com.williamcallahan.book_recommendation_engine.types.ImageDetails;
import com.williamcallahan.book_recommendation_engine.types.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.types.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.types.LongitoodService;
import com.williamcallahan.book_recommendation_engine.util.ImageCacheUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

    private static final Pattern GOOGLE_PG_PATTERN =
            Pattern.compile("[?&]pg=([A-Z]+[0-9]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern GOOGLE_PRINTSEC_FRONTCOVER_PATTERN =
            Pattern.compile("[?&](printsec=frontcover|pt=frontcover)", Pattern.CASE_INSENSITIVE);
    private static final Pattern GOOGLE_EDGE_CURL_PATTERN =
            Pattern.compile("[?&]edge=curl", Pattern.CASE_INSENSITIVE);

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
            LongitoodService longitoodService,
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

    /**
     * Processes the provisional URL hint asynchronously.
     * @return A CompletableFuture yielding a list of ImageDetails candidates from the hint.
     */
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
            
            String originalHintEnhanced = enhanceGoogleImageUrl(provisionalUrlHint, null); // Enhance but keep original zoom if present or add default
            if (isLikelyGoogleCoverUrl(originalHintEnhanced, bookIdForLog, "ProvisionalHint-Original")) {
                logger.debug("Book ID {}: Processing Google hint (enhanced as-is): {}", bookIdForLog, originalHintEnhanced);
                googleHintFutures.add(
                    localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(originalHintEnhanced, bookIdForLog, provenanceData, "GoogleHint-AsIs")
                );
            } else {
                 logger.debug("Book ID {}: Google hint (enhanced as-is) {} was deemed unlikely to be a cover. Skipping.", bookIdForLog, originalHintEnhanced);
            }

            String urlZoom0 = enhanceGoogleImageUrl(provisionalUrlHint, "zoom=0");
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

    private String enhanceGoogleImageUrl(String baseUrl, String zoomParam) {
        if (baseUrl == null) return null;
        String enhancedUrl = baseUrl;

        // Ensure HTTPS
        if (enhancedUrl.startsWith("http://")) {
            enhancedUrl = "https://" + enhancedUrl.substring(7);
        }

        // Remove fife
        if (enhancedUrl.contains("&fife=")) {
            enhancedUrl = enhancedUrl.replaceAll("&fife=w\\d+(-h\\d+)?", "");
        } else if (enhancedUrl.contains("?fife=")) {
            enhancedUrl = enhancedUrl.replaceAll("\\?fife=w\\d+(-h\\d+)?", "?");
            if (enhancedUrl.endsWith("?")) {
                enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
            }
        }
        
        // Remove edge=curl if present, as we prefer flat covers.
        // This might be re-evaluated if specific edge cases need it, but generally, non-curled is better.
        enhancedUrl = enhancedUrl.replaceAll("[?&]edge=curl", "");


        if (enhancedUrl.endsWith("&")) {
            enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
        }
        if (enhancedUrl.endsWith("?")) { // If trailing '?' after removals
            enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
        }


        // Set or replace zoom if zoomParam is provided
        if (zoomParam != null && !zoomParam.isEmpty()) {
            if (enhancedUrl.contains("zoom=")) {
                enhancedUrl = enhancedUrl.replaceAll("zoom=\\d+", zoomParam);
            } else {
                enhancedUrl += (enhancedUrl.contains("?") ? "&" : "?") + zoomParam;
            }
        }
        return enhancedUrl;
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
        
        String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();
        if (isbn != null && !isbn.isEmpty()) {
            sourceFutures.add(tryGoogleBooksApiByIsbn(isbn, bookIdForLog, provenanceData));
            sourceFutures.add(tryOpenLibrary(isbn, bookIdForLog, "L", provenanceData));
            sourceFutures.add(tryOpenLibrary(isbn, bookIdForLog, "M", provenanceData));
            sourceFutures.add(tryOpenLibrary(isbn, bookIdForLog, "S", provenanceData));
            sourceFutures.add(tryLongitood(book, bookIdForLog, provenanceData));
        } else if (book.getId() != null && !book.getId().isEmpty()) { // Google Volume ID
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
    
    private ImageDetails selectBestImageDetails(List<ImageDetails> candidates, String bookIdForLog, ImageProvenanceData provenanceData) {
        if (candidates == null || candidates.isEmpty()) {
            logger.warn("Book ID {}: selectBestImageDetails called with no candidates.", bookIdForLog);
            return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "no-candidates-for-selection");
        }

        // Filter out nulls or invalid details (though isValidImageDetails should have caught most)
        List<ImageDetails> validCandidates = candidates.stream()
            .filter(this::isValidImageDetails)
            .collect(Collectors.toList());

        if (validCandidates.isEmpty()) {
            logger.warn("Book ID {}: No valid candidates after filtering for selection.", bookIdForLog);
            return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "no-valid-candidates-for-selection");
        }

        // Comparator:
        // 1. Prefer S3_CACHE if dimensions are reasonable (e.g., > 150x150 to avoid tiny S3 placeholders if any)
        // 2. Then by largest area (width * height)
        // 3. Then by a source preference (e.g. Google, OpenLibrary, Longitood)
        // 4. Fallback to first valid if all else equal
        Comparator<ImageDetails> comparator = Comparator
            .<ImageDetails>comparingInt(details -> { // Prefer S3 if decent size
                if (details.getCoverImageSource() == CoverImageSource.S3_CACHE &&
                    details.getWidth() != null && details.getWidth() > 150 &&
                    details.getHeight() != null && details.getHeight() > 150) {
                    return 0; // Highest preference
                }
                return 1; // Lower preference
            })
            .thenComparing(Comparator.comparingLong((ImageDetails details) -> { // Then by area, descending
                if (details.getWidth() == null || details.getHeight() == null) return 0L;
                return (long)details.getWidth() * details.getHeight();
            }).reversed())
            .thenComparingInt(details -> { // Then by source preference
                CoverImageSource src = details.getCoverImageSource();
                if (src == CoverImageSource.S3_CACHE) return 0; // Already handled by the first comparator if decent size
                if (src == CoverImageSource.GOOGLE_BOOKS) return 1;
                if (src == CoverImageSource.OPEN_LIBRARY) return 2;
                if (src == CoverImageSource.LONGITOOD) return 3;
                if (src == CoverImageSource.LOCAL_CACHE && details.getUrlOrPath() != null && !details.getUrlOrPath().equals(localDiskCoverCacheService.getLocalPlaceholderPath())) return 4; // Non-placeholder local cache
                return 5; // Others (like ANY, UNDEFINED, or placeholder local cache)
            });

        ImageDetails bestImage = java.util.Collections.min(validCandidates, comparator); // min because lower numbers are better in comparator

        logger.info("Book ID {}: Selected best image from {} candidates. URL/Path: {}, Source: {}, Dimensions: {}x{}",
            bookIdForLog, validCandidates.size(), bestImage.getUrlOrPath(), bestImage.getCoverImageSource(), bestImage.getWidth(), bestImage.getHeight());
        
        // Log all candidates considered for easier debugging
        if (logger.isDebugEnabled()) {
            validCandidates.forEach(candidate -> 
                logger.debug("Book ID {}: Candidate for selection - URL/Path: {}, Source: {}, Dimensions: {}x{}", 
                    bookIdForLog, candidate.getUrlOrPath(), candidate.getCoverImageSource(), candidate.getWidth(), candidate.getHeight())
            );
        }

        updateSelectedImageInfo(provenanceData, ImageCacheUtils.mapCoverImageSourceToImageSourceName(bestImage.getCoverImageSource()), bestImage, "Selected by dimension and source preference");
        return bestImage;
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
               imageDetails.getWidth() != null && imageDetails.getWidth() > 1 && // Ensure width is greater than 1 (not just >0)
               imageDetails.getHeight() != null && imageDetails.getHeight() > 1; // Ensure height is greater than 1
    }
    
    /**
     * Updates the SelectedImageInfo in ImageProvenanceData if not already set or if new one is better.
     * @param provenanceData The ImageProvenanceData object
     * @param sourceName The source from which the image was selected
     * @param imageDetails The details of the selected image
     * @param selectionReason Reason for this selection
     */
    private void updateSelectedImageInfo(ImageProvenanceData provenanceData, ImageSourceName sourceName, ImageDetails imageDetails, String selectionReason) {
        if (provenanceData == null || imageDetails == null || !isValidImageDetails(imageDetails)) {
            return;
        }
        // This method is now called only by selectBestImageDetails, which has already chosen the best.
        // So, we can directly set it.
        ImageProvenanceData.SelectedImageInfo selectedInfo = new ImageProvenanceData.SelectedImageInfo();
        selectedInfo.setSourceName(sourceName);
        selectedInfo.setFinalUrl(imageDetails.getUrlOrPath());
        selectedInfo.setResolution(imageDetails.getResolutionPreference() != null ? imageDetails.getResolutionPreference().name() : "ORIGINAL");
        // Use the new setDimensions and setSelectionReason methods
        selectedInfo.setDimensions( (imageDetails.getWidth() != null ? imageDetails.getWidth() : "N/A") + "x" + (imageDetails.getHeight() != null ? imageDetails.getHeight() : "N/A") );
        selectedInfo.setSelectionReason(selectionReason);

        if (imageDetails.getUrlOrPath().startsWith("/" + localDiskCoverCacheService.getCacheDirName())) {
            selectedInfo.setStorageLocation("LocalCache");
        } else if (imageDetails.getCoverImageSource() == CoverImageSource.S3_CACHE) {
            selectedInfo.setStorageLocation("S3");
            selectedInfo.setS3Key(imageDetails.getSourceSystemId());
        } else {
            selectedInfo.setStorageLocation("Remote");
        }
        
        provenanceData.setSelectedImageInfo(selectedInfo);
        logger.debug("Provenance updated: Selected image from {} ({}), URL: {}, Dimensions: {}x{}, Reason: {}",
                sourceName, selectedInfo.getStorageLocation(), selectedInfo.getFinalUrl(), imageDetails.getWidth(), imageDetails.getHeight(), selectionReason);
    }
    
    private void addAttemptToProvenance(ImageProvenanceData provenanceData, ImageSourceName sourceName, String urlAttempted, ImageAttemptStatus status, String failureReason, ImageDetails detailsIfSuccess) {
        if (provenanceData == null) return;
        if (provenanceData.getAttemptedImageSources() == null) {
            provenanceData.setAttemptedImageSources(
                java.util.Collections.synchronizedList(new ArrayList<>())
            );
        }
        ImageProvenanceData.AttemptedSourceInfo attemptInfo = new ImageProvenanceData.AttemptedSourceInfo(sourceName, urlAttempted, status);
        if (failureReason != null) {
            attemptInfo.setFailureReason(failureReason);
        }
        if (detailsIfSuccess != null && status == ImageAttemptStatus.SUCCESS) {
            // Use the new setFetchedUrl and setDimensions methods
            attemptInfo.setFetchedUrl(detailsIfSuccess.getUrlOrPath());
            attemptInfo.setDimensions( (detailsIfSuccess.getWidth() != null ? detailsIfSuccess.getWidth() : "N/A") + "x" + (detailsIfSuccess.getHeight() != null ? detailsIfSuccess.getHeight() : "N/A") );
        }
        provenanceData.getAttemptedImageSources().add(attemptInfo);
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
            .thenApply(s3RemoteDetailsOptional -> { // Changed from thenCompose to thenApply for direct ImageDetails or placeholder
                if (s3RemoteDetailsOptional.isPresent()) {
                    ImageDetails s3RemoteDetails = s3RemoteDetailsOptional.get();
                    if (isValidImageDetails(s3RemoteDetails) && s3RemoteDetails.getCoverImageSource() == CoverImageSource.S3_CACHE) {
                        logger.info("S3 provided valid image for Book ID {}: {}", bookIdForLog, s3RemoteDetails.getUrlOrPath());
                        addAttemptToProvenance(provenanceData, ImageSourceName.S3_CACHE, "S3 Direct Fetch", ImageAttemptStatus.SUCCESS, null, s3RemoteDetails);
                        return s3RemoteDetails;
                    }
                    String s3UrlAttempted = s3RemoteDetails.getSourceSystemId() != null ? s3RemoteDetails.getSourceSystemId() : "S3 Direct Fetch";
                    addAttemptToProvenance(provenanceData, ImageSourceName.S3_CACHE, s3UrlAttempted, ImageAttemptStatus.FAILURE_INVALID_DETAILS, "S3 details not valid or not S3_CACHE source", s3RemoteDetails);
                    logger.debug("S3 provided Optional<ImageDetails> but it was not valid or not from S3_CACHE for Book ID {}. Details: {}", bookIdForLog, s3RemoteDetails);
                } else {
                    addAttemptToProvenance(provenanceData, ImageSourceName.S3_CACHE, "S3 Direct Fetch", ImageAttemptStatus.FAILURE_NOT_FOUND, "No details from S3 fetchCover", null);
                    logger.debug("S3 did not provide Optional<ImageDetails> for Book ID {}.", bookIdForLog);
                }
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "s3-miss-or-invalid");
            })
            .exceptionally(ex -> {
                logger.error("Exception trying S3 for Book ID {}: {}", bookIdForLog, ex.getMessage());
                addAttemptToProvenance(provenanceData, ImageSourceName.S3_CACHE, "S3 Direct Fetch", ImageAttemptStatus.FAILURE_GENERIC, ex.getMessage(), null); // Use FAILURE_GENERIC
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
            .thenCompose(remoteImageDetailsOptional -> {
                String olUrlAttempted = "OpenLibrary ISBN: " + finalIsbn + ", size: " + sizeSuffix;
                if (remoteImageDetailsOptional.isPresent()) {
                    ImageDetails remoteImageDetails = remoteImageDetailsOptional.get();
                    if (remoteImageDetails.getUrlOrPath() != null && !remoteImageDetails.getUrlOrPath().isEmpty()) {
                        // Provenance for the successful fetch of URL from OL will be handled by downloadAndStoreImageLocallyAsync
                        return localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(remoteImageDetails.getUrlOrPath(), bookIdForLog, provenanceData, "OpenLibrary-" + sizeSuffix)
                            .thenApply(cachedDetails -> {
                                 if (isValidImageDetails(cachedDetails)) return cachedDetails;
                                 // If download/cache failed for a valid URL, downloadAndStoreImageLocallyAsync handles its own provenance for that specific download attempt.
                                 // We might still mark the ISBN as bad here.
                                 coverCacheManager.addKnownBadOpenLibraryIsbn(finalIsbn);
                                 return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "ol-" + sizeSuffix + "-dl-fail");
                            });
                    } else { // Optional was present, but ImageDetails inside had no URL
                         logger.warn("OpenLibraryService provided ImageDetails but no URL for {}.", olUrlAttempted);
                         addAttemptToProvenance(provenanceData, ImageSourceName.OPEN_LIBRARY, olUrlAttempted, ImageAttemptStatus.FAILURE_NO_URL_IN_RESPONSE, "OL response had no URL", null);
                    }
                } else { // Optional was empty
                    logger.warn("OpenLibraryService did not provide ImageDetails for {}.", olUrlAttempted);
                    addAttemptToProvenance(provenanceData, ImageSourceName.OPEN_LIBRARY, olUrlAttempted, ImageAttemptStatus.FAILURE_NOT_FOUND, "No ImageDetails from OL service", null);
                }
                coverCacheManager.addKnownBadOpenLibraryIsbn(finalIsbn); // Mark as bad if no URL or empty optional
                return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "ol-" + sizeSuffix + "-no-url-or-empty"));
            })
            .exceptionally(ex -> {
                String olUrlAttempted = "OpenLibrary ISBN: " + finalIsbn + ", size: " + sizeSuffix;
                logger.error("Exception trying OpenLibrary for {}: {}", olUrlAttempted, ex.getMessage());
                coverCacheManager.addKnownBadOpenLibraryIsbn(finalIsbn);
                addAttemptToProvenance(provenanceData, ImageSourceName.OPEN_LIBRARY, olUrlAttempted, ImageAttemptStatus.FAILURE_GENERIC, ex.getMessage(), null); // Use FAILURE_GENERIC
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
                    Book gBook = books.get(0);
                    if (gBook != null && gBook.getRawJsonResponse() != null && provenanceData.getGoogleBooksApiResponse() == null) {
                        provenanceData.setGoogleBooksApiResponse(gBook.getRawJsonResponse());
                    }
                    String googleUrl = gBook != null ? gBook.getCoverImageUrl() : null;
                    logger.info("Book ID {}: Google API (ISBN) returned book. Raw coverImageUrl: '{}'", bookIdForLog, googleUrl);
                    if (gBook != null && gBook.getRawJsonResponse() != null) {
                        logger.debug("Book ID {}: Google API (ISBN) raw JSON response for book: {}", bookIdForLog, gBook.getRawJsonResponse());
                    }

                    if (googleUrl != null && !googleUrl.isEmpty() &&
                        !coverCacheManager.isKnownBadImageUrl(googleUrl) &&
                        !googleUrl.contains("image-not-available.png")) {
                        
                        logger.debug("Book ID {}: Google API (ISBN) URL '{}' is not null, not empty, not known bad, and not 'image-not-available.png'. Proceeding.", bookIdForLog, googleUrl);
                        String enhancedGoogleUrl = enhanceGoogleImageUrl(googleUrl, "zoom=0"); // Always try to get best zoom
                        logger.info("Book ID {}: Google API (ISBN) original URL: '{}', enhanced URL for likelihood check: '{}'", bookIdForLog, googleUrl, enhancedGoogleUrl);
                        
                        boolean isLikely = isLikelyGoogleCoverUrl(enhancedGoogleUrl, bookIdForLog, "GoogleAPI-ISBN");
                        logger.info("Book ID {}: Google API (ISBN) isLikelyGoogleCoverUrl('{}') result: {}", bookIdForLog, enhancedGoogleUrl, isLikely);

                        if (isLikely) {
                            // Provenance for this specific download attempt will be handled by downloadAndStoreImageLocallyAsync
                            return localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(enhancedGoogleUrl, bookIdForLog, provenanceData, "GoogleBooksAPI-ISBN");
                        } else {
                            logger.warn("Book ID {}: Google Books API (by ISBN) URL {} (enhanced: {}) deemed unlikely to be a cover for {}. Original URL was: {}", bookIdForLog, googleUrl, enhancedGoogleUrl, bookIdForLog, googleUrl);
                            addAttemptToProvenance(provenanceData, ImageSourceName.GOOGLE_BOOKS, enhancedGoogleUrl, ImageAttemptStatus.FAILURE_INVALID_DETAILS, "URL deemed not a cover by isLikelyGoogleCoverUrl", null);
                        }
                    } else {
                        logger.warn("Book ID {}: Google API (ISBN) URL '{}' was rejected by pre-checks (null/empty, known bad, or contains 'image-not-available.png'). Known bad: {}, Contains 'not-available': {}",
                            bookIdForLog, googleUrl,
                            (googleUrl != null && coverCacheManager.isKnownBadImageUrl(googleUrl)),
                            (googleUrl != null && googleUrl.contains("image-not-available.png")));
                    }
                } else {
                    logger.warn("Book ID {}: Google API (ISBN) search for '{}' returned null or empty list of books.", bookIdForLog, isbn);
                }
                String urlAttempted = "Google ISBN: " + isbn;
                // This log might be redundant if the more specific logs above have fired.
                // logger.warn("Google Books API (by ISBN) did not yield a usable image for {}.", urlAttempted);
                addAttemptToProvenance(provenanceData, ImageSourceName.GOOGLE_BOOKS, urlAttempted, ImageAttemptStatus.FAILURE_NOT_FOUND, "No usable image from Google/ISBN search (either no book, no URL, or URL failed checks)", null);
                return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "google-isbn-no-image"));
            })
            .exceptionally(ex -> {
                String urlAttempted = "Google ISBN: " + isbn;
                logger.error("Exception trying Google Books API for {}: {}", urlAttempted, ex.getMessage(), ex);
                addAttemptToProvenance(provenanceData, ImageSourceName.GOOGLE_BOOKS, urlAttempted, ImageAttemptStatus.FAILURE_GENERIC, ex.getMessage(), null); // Use FAILURE_GENERIC
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
        return googleBooksService.getBookById(googleVolumeId) // This returns CompletionStage<Book>
            .thenComposeAsync(gBook -> {
                if (gBook != null && gBook.getRawJsonResponse() != null && provenanceData.getGoogleBooksApiResponse() == null) {
                    provenanceData.setGoogleBooksApiResponse(gBook.getRawJsonResponse());
                }
                String googleUrl = gBook != null ? gBook.getCoverImageUrl() : null;
                logger.info("Book ID {}: Google API (VolumeID) returned book. Raw coverImageUrl: '{}'", bookIdForLog, googleUrl);
                if (gBook != null && gBook.getRawJsonResponse() != null) {
                    logger.debug("Book ID {}: Google API (VolumeID) raw JSON response for book: {}", bookIdForLog, gBook.getRawJsonResponse());
                }

                if (googleUrl != null && !googleUrl.isEmpty() &&
                    !coverCacheManager.isKnownBadImageUrl(googleUrl) &&
                    !googleUrl.contains("image-not-available.png")) {
                    
                    logger.debug("Book ID {}: Google API (VolumeID) URL '{}' is not null, not empty, not known bad, and not 'image-not-available.png'. Proceeding.", bookIdForLog, googleUrl);
                    String enhancedGoogleUrl = enhanceGoogleImageUrl(googleUrl, "zoom=0"); // Always try to get best zoom
                    logger.info("Book ID {}: Google API (VolumeID) original URL: '{}', enhanced URL for likelihood check: '{}'", bookIdForLog, googleUrl, enhancedGoogleUrl);

                    boolean isLikely = isLikelyGoogleCoverUrl(enhancedGoogleUrl, bookIdForLog, "GoogleAPI-VolumeID");
                    logger.info("Book ID {}: Google API (VolumeID) isLikelyGoogleCoverUrl('{}') result: {}", bookIdForLog, enhancedGoogleUrl, isLikely);
                    
                    if (isLikely) {
                        // Provenance for this specific download attempt will be handled by downloadAndStoreImageLocallyAsync
                        return localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(enhancedGoogleUrl, bookIdForLog, provenanceData, "GoogleBooksAPI-VolumeID");
                    } else {
                        logger.warn("Book ID {}: Google Books API (by VolumeID) URL {} (enhanced: {}) deemed unlikely to be a cover for {}. Original URL was: {}", bookIdForLog, googleUrl, enhancedGoogleUrl, bookIdForLog, googleUrl);
                        addAttemptToProvenance(provenanceData, ImageSourceName.GOOGLE_BOOKS, enhancedGoogleUrl, ImageAttemptStatus.FAILURE_INVALID_DETAILS, "URL deemed not a cover by isLikelyGoogleCoverUrl", null);
                    }
                } else {
                     logger.warn("Book ID {}: Google API (VolumeID) URL '{}' was rejected by pre-checks (null/empty, known bad, or contains 'image-not-available.png'). Known bad: {}, Contains 'not-available': {}",
                        bookIdForLog, googleUrl,
                        (googleUrl != null && coverCacheManager.isKnownBadImageUrl(googleUrl)),
                        (googleUrl != null && googleUrl.contains("image-not-available.png")));
                }
                 String urlAttempted = "Google VolumeID: " + googleVolumeId;
                // This log might be redundant.
                // logger.warn("Google Books API (by Volume ID) did not yield a usable image for {}.", urlAttempted);
                addAttemptToProvenance(provenanceData, ImageSourceName.GOOGLE_BOOKS, urlAttempted, ImageAttemptStatus.FAILURE_NOT_FOUND, "No usable image from Google/VolumeID search (either no book, no URL, or URL failed checks)", null);
                return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "google-volid-no-image"));
            })
            .toCompletableFuture()
            .exceptionally(ex -> {
                String urlAttempted = "Google VolumeID: " + googleVolumeId;
                logger.error("Exception trying Google Books API for {}: {}", urlAttempted, ex.getMessage(), ex);
                addAttemptToProvenance(provenanceData, ImageSourceName.GOOGLE_BOOKS, urlAttempted, ImageAttemptStatus.FAILURE_GENERIC, ex.getMessage(), null); // Use FAILURE_GENERIC
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

        return longitoodService.fetchCover(book)
            .thenCompose(remoteImageDetailsOptional -> {
                String ltUrlAttempted = "Longitood ISBN: " + finalIsbn;
                if (remoteImageDetailsOptional.isPresent()) {
                    ImageDetails remoteImageDetails = remoteImageDetailsOptional.get();
                    if (remoteImageDetails.getUrlOrPath() != null && !remoteImageDetails.getUrlOrPath().isEmpty()) {
                        // Provenance for this specific download attempt will be handled by downloadAndStoreImageLocallyAsync
                        return localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(remoteImageDetails.getUrlOrPath(), bookIdForLog, provenanceData, "Longitood")
                             .thenApply(cachedDetails -> {
                                 if (isValidImageDetails(cachedDetails)) return cachedDetails;
                                 coverCacheManager.addKnownBadLongitoodIsbn(finalIsbn);
                                 return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "longitood-dl-fail");
                             });
                    } else { // Optional was present, but ImageDetails inside had no URL
                        logger.warn("LongitoodService provided ImageDetails but no URL for {}.", ltUrlAttempted);
                        addAttemptToProvenance(provenanceData, ImageSourceName.LONGITOOD, ltUrlAttempted, ImageAttemptStatus.FAILURE_NO_URL_IN_RESPONSE, "Longitood response had no URL", null);
                    }
                } else { // Optional was empty
                    logger.warn("LongitoodService did not provide ImageDetails for {}.", ltUrlAttempted);
                    addAttemptToProvenance(provenanceData, ImageSourceName.LONGITOOD, ltUrlAttempted, ImageAttemptStatus.FAILURE_NOT_FOUND, "No ImageDetails from Longitood service", null);
                }
                coverCacheManager.addKnownBadLongitoodIsbn(finalIsbn); // Mark as bad if no URL or empty optional
                return CompletableFuture.completedFuture(localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "longitood-no-url-or-empty"));
            })
            .exceptionally(ex -> {
                String ltUrlAttempted = "Longitood ISBN: " + finalIsbn;
                logger.error("Exception trying Longitood for {}: {}", ltUrlAttempted, ex.getMessage());
                coverCacheManager.addKnownBadLongitoodIsbn(finalIsbn);
                addAttemptToProvenance(provenanceData, ImageSourceName.LONGITOOD, ltUrlAttempted, ImageAttemptStatus.FAILURE_GENERIC, ex.getMessage(), null); // Use FAILURE_GENERIC
                return localDiskCoverCacheService.createPlaceholderImageDetails(bookIdForLog, "longitood-exception");
            });
    }

    /**
     * Validates if a Google Books image URL is likely to be a cover.
     * - Prefers URLs with "printsec=frontcover" or "pt=frontcover".
     * - Penalizes URLs with "edge=curl".
     * - Penalizes URLs with "pg=" followed by typical page identifiers (e.g., PA5, PT10).
     *
     * @param url The Google Books image URL to validate.
     * @param bookIdForLog Identifier for logging.
     * @param contextHint A hint for logging about where this check is being performed (e.g., "ProvisionalHint", "GoogleAPI-ISBN").
     * @return True if the URL is likely a cover, false otherwise.
     */
    private boolean isLikelyGoogleCoverUrl(String url, String bookIdForLog, String contextHint) {
        if (url == null || url.isEmpty()) {
            logger.warn("Book ID {}: isLikelyGoogleCoverUrl called with null or empty URL. Context: {}", bookIdForLog, contextHint);
            return false;
        }

        Matcher pgMatcher = GOOGLE_PG_PATTERN.matcher(url);
        boolean hasPageParam = pgMatcher.find();
        if (hasPageParam) {
            String pageId = pgMatcher.group(1);
            // Allow specific 'pg' parameters if they are known to be cover-related, e.g. "PP1" (often first page of a book, sometimes cover)
            // For now, any 'pg=' is a strong indicator it's NOT a generic cover image.
            // This could be refined if specific 'pg=' values are confirmed to be reliable for covers.
            // Example: !pageId.startsWith("PP") might be too restrictive if PP1 is sometimes the cover.
            // Current logic: if any pg= found, it's not a preferred cover.
            logger.debug("Book ID {}: URL {} has 'pg={}' parameter. Context: {}. Considered not a primary cover.", bookIdForLog, url, pageId, contextHint);
            return false; // Strict: any 'pg=' parameter makes it unlikely to be the best cover.
        }

        Matcher edgeCurlMatcher = GOOGLE_EDGE_CURL_PATTERN.matcher(url);
        if (edgeCurlMatcher.find()) {
            logger.debug("Book ID {}: URL {} has 'edge=curl' parameter. Context: {}. Considered not a primary cover.", bookIdForLog, url, contextHint);
            return false; // 'edge=curl' usually means it's a page preview with curled effect.
        }
        
        Matcher frontcoverMatcher = GOOGLE_PRINTSEC_FRONTCOVER_PATTERN.matcher(url);
        if (frontcoverMatcher.find()) {
            logger.debug("Book ID {}: URL {} has 'printsec=frontcover' or 'pt=frontcover'. Context: {}. Considered a likely cover.", bookIdForLog, url, contextHint);
            return true; // Strong positive indicator.
        }

        // If no strong negative indicators (pg, edge=curl) and no strong positive (frontcover),
        // we might accept it, but with lower confidence.
        // The existing `enhanceGoogleImageUrl` already tries to add `zoom=0` which is good.
        // For now, if it doesn't have 'pg' or 'edge=curl', let it pass,
        // as sometimes Google API might not include `printsec=frontcover` in all cover links.
        // The dimension checks later will still apply.
        logger.debug("Book ID {}: URL {} has no strong negative indicators (like 'pg=' or 'edge=curl') and no explicit positive indicators (like 'printsec=frontcover'). Context: {}. Considered a potential cover by default, relying on subsequent dimension/content checks.", bookIdForLog, url, contextHint);
        return true;
    }
}
