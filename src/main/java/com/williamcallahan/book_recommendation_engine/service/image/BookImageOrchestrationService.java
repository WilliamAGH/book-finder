package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.types.OpenLibraryService;
import com.williamcallahan.book_recommendation_engine.types.LongitoodService;

@Service
public class BookImageOrchestrationService {

    private static final Logger logger = LoggerFactory.getLogger(BookImageOrchestrationService.class);
    private static final String DEFAULT_PLACEHOLDER_IMAGE = "/images/placeholder-book-cover.svg";

    private final S3BookCoverService s3BookCoverService;
    private final BookCoverCacheService bookCoverCacheService;
    // private final S3StorageService s3StorageService; // Removed as unused
    @Autowired private OpenLibraryService openLibraryService;
    @Autowired private LongitoodService longitoodService;

    @Value("${s3.enabled:true}")
    private boolean s3Enabled;

    @Value("${app.cover-sources.prefer-s3:true}")
    private boolean preferS3;

    @Value("${app.cover-cache.dir:covers}")
    private String cacheDirName;

    @Autowired
    public BookImageOrchestrationService(S3BookCoverService s3BookCoverService,
                                         BookCoverCacheService bookCoverCacheService,
                                         // S3StorageService s3StorageService, // Removed as unused
                                         OpenLibraryService openLibraryService,
                                         LongitoodService longitoodService) {
        this.s3BookCoverService = s3BookCoverService;
        this.bookCoverCacheService = bookCoverCacheService;
        // this.s3StorageService = s3StorageService; // Removed as unused
        this.openLibraryService = openLibraryService;
        this.longitoodService = longitoodService;
    }

    /**
     * Asynchronously retrieves the best cover URL for a given book
     *
     * @param book The book to fetch the cover for
     * @return A CompletableFuture that resolves to the best cover URL
     */
    public CompletableFuture<String> getBestCoverUrlAsync(Book book) {
        return getBestCoverUrlAsync(book, CoverImageSource.ANY);
    }
    
    /**
     * Asynchronously retrieves the best cover URL for a given book from a specified source
     *
     * @param book The book to fetch the cover for
     * @param preferredSource The preferred source to fetch the cover from
     * @return A CompletableFuture that resolves to the best cover URL
     */
    public CompletableFuture<String> getBestCoverUrlAsync(Book book, CoverImageSource preferredSource) {
        return getBestCoverUrlAsync(book, preferredSource, ImageResolutionPreference.ANY);
    }
    
    /**
     * Asynchronously retrieves the best cover URL for a given book from a specified source with resolution preference
     *
     * @param book The book to fetch the cover for
     * @param preferredSource The preferred source to fetch the cover from
     * @param resolutionPreference The preferred resolution quality
     * @return A CompletableFuture that resolves to the best cover URL
     */
    public CompletableFuture<String> getBestCoverUrlAsync(Book book, CoverImageSource preferredSource, ImageResolutionPreference resolutionPreference) {
        if (book == null) {
            logger.warn("Book object is null, cannot fetch cover. Returning default placeholder.");
            // Cannot set properties on a null book object.
            return CompletableFuture.completedFuture(DEFAULT_PLACEHOLDER_IMAGE);
        }
        if (book.getId() == null) {
            logger.warn("Book ID is null for book object, cannot fetch cover effectively. Setting defaults on book and returning placeholder.");
            book.setCoverImageWidth(0);
            book.setCoverImageHeight(0);
            book.setIsCoverHighResolution(false);
            return CompletableFuture.completedFuture(DEFAULT_PLACEHOLDER_IMAGE);
        }

        String bookId = book.getId();
        String primaryExtension = getPrimaryExtensionFromBook(book);

        // Define high resolution thresholds
        final int HIGH_RES_MIN_WIDTH = 600;
        final int HIGH_RES_MIN_HEIGHT = 800;

        // S3 Pre-check (still returns String URL, dimensions are not known from HEAD request)
        if (s3Enabled && preferS3) {
            if (preferredSource != CoverImageSource.ANY) {
                String sourceStr = getSourceString(preferredSource);
                if (s3BookCoverService.coverExistsInS3(bookId, primaryExtension, sourceStr)) {
                    String s3Url = s3BookCoverService.getS3CoverUrl(bookId, primaryExtension, sourceStr);
                    logger.debug("S3 Pre-check: Found existing cover in S3 for book {} from preferred source {}: {}", bookId, preferredSource, s3Url);
                    // Dimensions unknown at this stage for S3 pre-check, will be updated if this URL is chosen and processed fully
                    book.setCoverImageWidth(null);
                    book.setCoverImageHeight(null);
                    book.setIsCoverHighResolution(null);
                    return CompletableFuture.completedFuture(s3Url);
                }
            }
            if (s3BookCoverService.coverExistsInS3(bookId, primaryExtension)) {
                String s3Url = s3BookCoverService.getS3CoverUrl(bookId, primaryExtension);
                logger.debug("S3 Pre-check: Found existing cover in S3 for book {}: {}", bookId, s3Url);
                book.setCoverImageWidth(null);
                book.setCoverImageHeight(null);
                book.setIsCoverHighResolution(null);
                return CompletableFuture.completedFuture(s3Url);
            }
        }

        logger.debug("Fetching cover for book {} with source preference {} and resolution preference {}", bookId, preferredSource, resolutionPreference);

        CompletableFuture<ImageDetails> imageDetailsFuture = bookCoverCacheService.getCachedCoverUrlAsync(book.getCoverImageUrl(), bookId)
            .thenCompose(cachedImageDetails -> {
                String cacheUrlOrPath = cachedImageDetails.getUrlOrPath();

                if (cacheUrlOrPath == null || cacheUrlOrPath.equals(DEFAULT_PLACEHOLDER_IMAGE) || knownBadUrl(cacheUrlOrPath, bookId)) {
                    logger.debug("Local cache for book {} yielded placeholder/bad/null: {}. Fetching external.", bookId, cacheUrlOrPath);
                    return fetchAndProcessExternalCandidates(book, primaryExtension, preferredSource, resolutionPreference);
                }

                // If dimensions are already known from cache, update book object
                if (cachedImageDetails.areDimensionsKnown()) {
                    updateBookWithImageDetails(book, cachedImageDetails, HIGH_RES_MIN_WIDTH, HIGH_RES_MIN_HEIGHT);
                } else {
                    // If not known, ensure defaults are set or preserved
                    if (book.getCoverImageWidth() == null) book.setCoverImageWidth(0);
                    if (book.getCoverImageHeight() == null) book.setCoverImageHeight(0);
                    if (book.getIsCoverHighResolution() == null) book.setIsCoverHighResolution(false);
                }

                if (isLocalFilePath(cacheUrlOrPath)) {
                    logger.debug("Local cache for book {} yielded local file: {}.", bookId, cacheUrlOrPath);
                    if (s3Enabled) {
                        logger.debug("Local file {} found in cache. Attempting S3 upload.", cacheUrlOrPath);
                        try {
                            // Construct the full path to the local cached file
                            // cacheUrlOrPath is like "/covers/filename.jpg"
                            // cacheDirName is "covers"
                            // We need to strip the leading "/" from cacheUrlOrPath if it exists,
                            // or ensure Paths.get handles it correctly.
                            // The actual base for static resources is "src/main/resources/static"
                            String relativePathInStatic = cacheUrlOrPath.startsWith("/") ? cacheUrlOrPath.substring(1) : cacheUrlOrPath;
                            Path localFilePath = Paths.get("src/main/resources/static", relativePathInStatic);
                            
                            logger.debug("Attempting to read local cached file from: {}", localFilePath.toString());

                            if (Files.exists(localFilePath)) {
                                byte[] fileBytes = Files.readAllBytes(localFilePath);
                                String fileExtension = s3BookCoverService.getFileExtensionFromUrl(cacheUrlOrPath);
                                String sourceString = getSourceString(preferredSource); // Or derive from cachedImageDetails if more reliable

                                return s3BookCoverService.uploadCoverFromBytesAsync(bookId, fileBytes, fileExtension, sourceString)
                                    .thenCompose(s3UploadedDetails -> {
                                        if (s3UploadedDetails != null && s3UploadedDetails.getUrlOrPath() != null && s3UploadedDetails.areDimensionsKnown()) {
                                            logger.info("Successfully uploaded local cache file {} to S3: {}", cacheUrlOrPath, s3UploadedDetails.getUrlOrPath());
                                            return CompletableFuture.completedFuture(s3UploadedDetails);
                                        } else {
                                            logger.warn("S3 upload from local file {} failed or did not return full details. Using local file details.", cacheUrlOrPath);
                                            // Fallback to using the original cachedImageDetails (which should have local path and dimensions if read correctly)
                                            return CompletableFuture.completedFuture(cachedImageDetails);
                                        }
                                    });
                            } else {
                                logger.warn("Local cached file {} not found at expected path {}. Using original cached details.", cacheUrlOrPath, localFilePath);
                                return CompletableFuture.completedFuture(cachedImageDetails);
                            }
                        } catch (IOException e) {
                            logger.error("Error reading local cached file {} for S3 upload: {}", cacheUrlOrPath, e.getMessage(), e);
                            return CompletableFuture.completedFuture(cachedImageDetails); // Fallback to original cached details on error
                        }
                    } else {
                        // S3 disabled, use local file details
                        logger.debug("S3 disabled. Using local file details for {}.", cacheUrlOrPath);
                        return CompletableFuture.completedFuture(cachedImageDetails);
                    }
                } else { // External URL from cache (e.g., original URL if cache miss initially, or an S3 URL)
                    if (s3BookCoverService.isS3Url(cacheUrlOrPath)) {
                         logger.debug("Local cache for book {} returned an S3 URL directly: {}", bookId, cacheUrlOrPath);
                         // If it's an S3 URL, dimensions might not be in cachedImageDetails if it was just a string.
                         // We trust it if it's already an S3 URL.
                         return CompletableFuture.completedFuture(cachedImageDetails); // Trust S3 URL
                    }
                    // It's an external URL not yet processed by S3 or fully cached with dimensions
                    logger.debug("Local cache for book {} returned external URL: {}. Processing as external.", bookId, cacheUrlOrPath);
                    return processExternalUrl(book, cacheUrlOrPath, primaryExtension);
                }
            });

        return imageDetailsFuture.thenApply(finalImageDetails -> {
            // Final update to the book object before returning the URL string
            return updateBookWithImageDetails(book, finalImageDetails, HIGH_RES_MIN_WIDTH, HIGH_RES_MIN_HEIGHT);
        }).exceptionally(ex -> {
            logger.error("Exception in getBestCoverUrlAsync for book {}: {}. Falling back to default.", bookId, ex.getMessage(), ex);
            book.setCoverImageWidth(0);
            book.setCoverImageHeight(0);
            book.setIsCoverHighResolution(false);
            return DEFAULT_PLACEHOLDER_IMAGE;
        });
    }

    private String updateBookWithImageDetails(Book book, ImageDetails imageDetails, int highResMinWidth, int highResMinHeight) {
        if (imageDetails != null && imageDetails.areDimensionsKnown()) {
            book.setCoverImageWidth(imageDetails.getWidth());
            book.setCoverImageHeight(imageDetails.getHeight());
            boolean isHighRes = imageDetails.getWidth() >= highResMinWidth && imageDetails.getHeight() >= highResMinHeight;
            book.setIsCoverHighResolution(isHighRes);
            logger.debug("Updated book {} with dimensions {}x{}, isHighRes: {}", book.getId(), imageDetails.getWidth(), imageDetails.getHeight(), isHighRes);
            return imageDetails.getUrlOrPath() != null ? imageDetails.getUrlOrPath() : DEFAULT_PLACEHOLDER_IMAGE;
        } else if (imageDetails != null && imageDetails.getUrlOrPath() != null) { // URL is present but dimensions are not known
            if (book.getCoverImageWidth() == null) book.setCoverImageWidth(0);
            if (book.getCoverImageHeight() == null) book.setCoverImageHeight(0);
            if (book.getIsCoverHighResolution() == null) book.setIsCoverHighResolution(false);
            logger.debug("Dimensions for book {} cover {} not known from this step. Preserving existing/default.", book.getId(), imageDetails.getUrlOrPath());
            return imageDetails.getUrlOrPath();
        } else { // imageDetails is null or has no URL
            book.setCoverImageWidth(0);
            book.setCoverImageHeight(0);
            book.setIsCoverHighResolution(false);
            logger.warn("ImageDetails were null or had no URL for book {}. Setting to default/unknown dimensions and placeholder URL.", book.getId());
            return DEFAULT_PLACEHOLDER_IMAGE;
        }
    }


    /**
     * Asynchronously fetches and processes external cover candidates
     *
     * @param book The book to fetch covers for
     * @param primaryExtension The primary file extension
     * @param preferredSource The preferred source to fetch covers from
     * @return A CompletableFuture that resolves to ImageDetails of the best external cover
     */
    // Removed unused overloaded method fetchAndProcessExternalCandidates(Book, String, CoverImageSource)
    
    /**
     * Asynchronously fetches and processes external cover candidates with resolution preference
     *
     * @param book The book to fetch covers for
     * @param primaryExtension The primary file extension
     * @param preferredSource The preferred source to fetch covers from
     * @param resolutionPreference The preferred resolution quality
     * @return A CompletableFuture that resolves to ImageDetails of the best external cover
     */
    private CompletableFuture<ImageDetails> fetchAndProcessExternalCandidates(Book book, String primaryExtension, CoverImageSource preferredSource, ImageResolutionPreference resolutionPreference) {
        List<CoverCandidate> candidates = gatherExternalCandidates(book, preferredSource, resolutionPreference);
        if (candidates.isEmpty()) {
            logger.warn("No external cover candidates found for book {}.", book.getId());
            return CompletableFuture.completedFuture(new ImageDetails(DEFAULT_PLACEHOLDER_IMAGE, 0, 0, false));
        }
        CoverCandidate bestCandidate = candidates.get(0);
        logger.debug("Best external candidate for book {} is {} from {}", book.getId(), bestCandidate.getUrl(), bestCandidate.getSource());
        // Ensure the 'book' object is passed to processExternalUrl so its dimensions can be updated
        return processExternalUrl(book, bestCandidate.getUrl(), book.getId(), primaryExtension, bestCandidate.getSource());
    }

    /**
     * Asynchronously processes an external cover URL, populating book with dimension info.
     *
     * @param book The book object to update with dimension info
     * @param externalUrl The external cover URL
     * @param bookId The book ID (already in book object, but passed for consistency for now)
     * @param fallbackExtension The fallback file extension
     * @param source The source of the image (e.g., "Google Books", "Open Library", "Longitood")
     * @return A CompletableFuture that resolves to ImageDetails (containing URL and dimensions)
     */
    private CompletableFuture<ImageDetails> processExternalUrl(Book book, String externalUrl, String bookId, String fallbackExtension, String source) {
        // This method should primarily be responsible for getting ImageDetails,
        // the updateBookWithImageDetails will be called by the main chain in getBestCoverUrlAsync.
        // However, if S3 or local cache provides dimensions, they should be in the returned ImageDetails.

        CompletableFuture<ImageDetails> processedDetailsFuture;

        if (s3Enabled) {
            String sourceStr = normalizeSourceString(source);
            // s3BookCoverService.uploadCoverFromUrlAsync now returns CompletableFuture<ImageDetails>
            processedDetailsFuture = s3BookCoverService.uploadCoverFromUrlAsync(bookId, externalUrl, sourceStr)
                .thenCompose(s3ImageDetails -> {
                    // If S3 processing was successful and gave us details (URL + dimensions)
                    if (s3ImageDetails != null && s3ImageDetails.getUrlOrPath() != null && s3ImageDetails.areDimensionsKnown()) {
                        return CompletableFuture.completedFuture(s3ImageDetails);
                    }
                    // If S3 failed or didn't return full details, try local cache for the original external URL
                    logger.debug("S3 processing for {} didn't yield full details or failed, trying local cache for original URL.", externalUrl);
                    return bookCoverCacheService.initiateLocalCacheDownloadAsync(externalUrl, bookId);
                });
        } else {
            // S3 disabled, just use local cache
            processedDetailsFuture = bookCoverCacheService.initiateLocalCacheDownloadAsync(externalUrl, bookId);
        }
        // The returned ImageDetails will be used by the main chain to update the book.
        return processedDetailsFuture;
    }
    
    /**
     * Asynchronously processes an external cover URL (backward compatibility), populating book with dimension info.
     * This overload assumes the `book` object should be updated.
     *
     * @param book The book object to update
     * @param externalUrl The external cover URL
     * @param fallbackExtension The fallback file extension (bookId is derived from book object)
     * @return A CompletableFuture that resolves to ImageDetails
     */
    private CompletableFuture<ImageDetails> processExternalUrl(Book book, String externalUrl, String fallbackExtension) {
        String source = determineSourceFromUrl(externalUrl);
        return processExternalUrl(book, externalUrl, book.getId(), fallbackExtension, source);
    }

    // Removed the overloads of processExternalUrl that don't take a Book object,
    // as the primary goal is to update the Book instance.
    // If a utility is needed to just get ImageDetails without Book context, it can be added separately.

    /**
     * Gets the primary file extension from the book's cover image URL or editions
     *
     * @param book The book to extract the extension from
     * @return The primary file extension
     */
    private String getPrimaryExtensionFromBook(Book book) {
        if (book.getCoverImageUrl() != null) {
            String ext = s3BookCoverService.getFileExtensionFromUrl(book.getCoverImageUrl());
            if (!ext.equals(".jpg")) return ext;
        }
        if (book.getOtherEditions() != null) {
            for (Book.EditionInfo edition : book.getOtherEditions()) {
                if (edition.getCoverImageUrl() != null) {
                    String ext = s3BookCoverService.getFileExtensionFromUrl(edition.getCoverImageUrl());
                     if (!ext.equals(".jpg")) return ext;
                }
            }
        }
        return ".jpg";
    }

    /**
     * Checks if a URL is a local file path
     *
     * @param url The URL to check
     * @return True if the URL is a local file path, false otherwise
     */
    private boolean isLocalFilePath(String url) {
        return url != null && url.startsWith("/" + cacheDirName + "/");
    }
    
    /**
     * Checks if a URL is a known bad URL
     *
     * @param url The URL to check
     * @param bookId The book ID
     * @return True if the URL is a known bad URL, false otherwise
     */
    private boolean knownBadUrl(String url, String bookId) {
        if (url == null) return true;
        if (url.equals(bookCoverCacheService.getLocalPlaceholderPath())) return true;
        return false; 
    }

    // This method is now private and only used internally by the overloaded version with source parameter
    
    /**
     * Converts a CoverImageSource enum to a normalized string for S3 keys
     * 
     * @param source The CoverImageSource enum value
     * @return A normalized string representation of the source
     */
    private String getSourceString(CoverImageSource source) {
        if (source == null) {
            return "unknown";
        }
        
        switch (source) {
            case GOOGLE_BOOKS:
                return "google-books";
            case OPEN_LIBRARY:
                return "open-library";
            case LONGITOOD:
                return "longitood";
            default:
                return "unknown";
        }
    }
    
    /**
     * Determines the source from a URL
     * 
     * @param url The URL to analyze
     * @return A normalized string representation of the source
     */
    private String determineSourceFromUrl(String url) {
        if (url == null) {
            return "unknown";
        }
        
        if (url.contains("books.google.com") || url.contains("googleapis.com")) {
            return "google-books";
        } else if (url.contains("openlibrary.org")) {
            return "open-library";
        } else if (url.contains("longitood.com")) {
            return "longitood";
        }
        
        return "unknown";
    }
    
    /**
     * Normalizes a source string for use in S3 keys
     * 
     * @param source The source string to normalize
     * @return A normalized string suitable for use in filenames
     */
    private String normalizeSourceString(String source) {
        if (source == null || source.isEmpty()) {
            return "unknown";
        }
        
        // Convert to lowercase and replace non-alphanumeric characters with hyphens
        return source.toLowerCase().replaceAll("[^a-z0-9_-]", "-");
    }
    
    // Removed unused overloaded method
    
    /**
     * Gathers external cover candidates from OpenLibrary and Longitood with a specified source preference and resolution preference
     *
     * @param book The book to fetch covers for
     * @param preferredSource The preferred source to fetch covers from
     * @param resolutionPreference The preferred resolution quality
     * @return A list of cover candidates
     */
    private List<CoverCandidate> gatherExternalCandidates(Book book, CoverImageSource preferredSource, ImageResolutionPreference resolutionPreference) {
        List<CoverCandidate> candidates = new ArrayList<>();
        String bookId = book.getId();
        String initialCoverUrl = book.getCoverImageUrl();
        Date publishedDate = book.getPublishedDate();

        // Only add Google Books covers if the preferred source is ANY or GOOGLE_BOOKS
        if (initialCoverUrl != null && isGoogleCoverUrlValid(initialCoverUrl) && 
            (preferredSource == CoverImageSource.ANY || preferredSource == CoverImageSource.GOOGLE_BOOKS)) {
            
            // Apply resolution preference logic
            if (resolutionPreference == ImageResolutionPreference.HIGH_ONLY) {
                // Only add high resolution version
                String highQualityUrl = enhanceGoogleCoverUrl(initialCoverUrl, "high");
                candidates.add(new CoverCandidate(
                    highQualityUrl,
                    "Google Books (High)",
                    90, // Highest priority
                    publishedDate,
                    bookId + "-high"
                ));
                return candidates;
            }
            
            // Add high quality version (zoom=5, fife=w800)
            String highQualityUrl = enhanceGoogleCoverUrl(initialCoverUrl, "high");
            candidates.add(new CoverCandidate(
                highQualityUrl,
                "Google Books (High)",
                80, // Highest priority
                publishedDate,
                bookId + "-high"
            ));
            
            // Add medium quality version (zoom=3, fife=w400)
            String mediumQualityUrl = enhanceGoogleCoverUrl(initialCoverUrl, "medium");
            candidates.add(new CoverCandidate(
                mediumQualityUrl,
                "Google Books (Medium)",
                60, // Medium priority
                publishedDate,
                bookId + "-medium"
            ));
            
            // Add original/low quality version (unmodified)
            candidates.add(new CoverCandidate(
                initialCoverUrl,
                "Google Books (Low)",
                40, // Lower priority
                publishedDate,
                bookId + "-low"
            ));
        }

        CompletableFuture<List<CoverCandidate>> openLibraryFuture = openLibraryService.fetchCovers(book);
        CompletableFuture<List<CoverCandidate>> longitoodFuture = longitoodService.fetchCovers(book);

        // Set priorities for OpenLibrary and Longitood based on image quality and resolution preference
        // Default: Google Books (80) > OpenLibrary (70) > Longitood (60)
        openLibraryFuture.thenApply(olCandidates -> {
            if (resolutionPreference == ImageResolutionPreference.HIGH_ONLY) {
                // Only keep large images for HIGH_ONLY preference
                olCandidates.removeIf(candidate -> !candidate.getSource().contains("Large"));
            }
            olCandidates.forEach(candidate -> {
                // Adjust priority based on resolution preference
                if (resolutionPreference == ImageResolutionPreference.HIGH_FIRST && candidate.getSource().contains("Large")) {
                    candidate.setPriority(75); // Boost large images for HIGH_FIRST
                } else {
                    candidate.setPriority(70); // Second highest priority (good large images)
                }
            });
            return olCandidates;
        });
        longitoodFuture.thenApply(ltCandidates -> {
            // Longitood images are generally lower resolution
            if (resolutionPreference == ImageResolutionPreference.HIGH_ONLY) {
                ltCandidates.clear(); // Remove all Longitood images for HIGH_ONLY
            } else {
                ltCandidates.forEach(candidate -> candidate.setPriority(60)); // Lower priority
            }
            return ltCandidates;
        });

        List<CoverCandidate> allCandidates = new ArrayList<>(candidates);
        try {
            List<CoverCandidate> openLibraryCandidates = openLibraryFuture.join();
            if (openLibraryCandidates != null) {
                allCandidates.addAll(openLibraryCandidates);
            }
        } catch (Exception e) {
            logger.error("Error fetching covers from OpenLibrary: {}", e.getMessage());
        }
        try {
            List<CoverCandidate> longitoodCandidates = longitoodFuture.join();
            if (longitoodCandidates != null) {
                allCandidates.addAll(longitoodCandidates);
            }
        } catch (Exception e) {
            logger.error("Error fetching covers from Longitood: {}", e.getMessage());
        }
        // Filter candidates by preferred source if specified
        List<CoverCandidate> filteredCandidates = allCandidates;
        if (preferredSource != CoverImageSource.ANY) {
            String sourceFilter = preferredSource.getDisplayName();
            filteredCandidates = allCandidates.stream()
                .filter(candidate -> candidate.getSource().contains(sourceFilter))
                .collect(Collectors.toList());
            
            // If no candidates match the preferred source, fall back to all candidates
            if (filteredCandidates.isEmpty()) {
                logger.debug("No candidates found for preferred source {}, falling back to all sources", sourceFilter);
                filteredCandidates = allCandidates;
            } else {
                logger.debug("Found {} candidates from preferred source {}", filteredCandidates.size(), sourceFilter);
            }
        }
        
        filteredCandidates.sort(Comparator.comparing(CoverCandidate::getPriority, Comparator.reverseOrder())
                .thenComparing(CoverCandidate::getPublishedDate, Comparator.nullsLast(Comparator.reverseOrder())));
        return filteredCandidates;
    }

    /**
     * Checks if a Google Cover URL is valid
     *
     * @param coverUrl The cover URL to check
     * @return True if the URL is valid, false otherwise
     */
    private boolean isGoogleCoverUrlValid(String coverUrl) {
        if (coverUrl == null || coverUrl.isEmpty()) return false;
        if (coverUrl.contains("image-not-available.png")) return false;
        
        // We've modified the Google Books URLs to use zoom=5 and fife=w800 for high quality
        // So we don't need to filter out zoom=1 URLs anymore
        return true;
    }
    
    /**
     * Enhance a Google Books cover URL to get the best quality possible
     * @param url The original URL
     * @param quality The desired quality ("high", "medium", or "low")
     * @return The enhanced URL
     */
    private String enhanceGoogleCoverUrl(String url, String quality) {
        if (url == null) return null;
        
        // Make a copy of the URL to avoid modifying the original
        String enhancedUrl = url;
        
        // Remove http protocol to use https
        if (enhancedUrl.startsWith("http://")) {
            enhancedUrl = "https://" + enhancedUrl.substring(7);
        }
        
        // Enhance based on requested quality
        switch (quality) {
            case "high":
                // Replace zoom=1 with zoom=5 for highest resolution
                if (enhancedUrl.contains("zoom=1")) {
                    enhancedUrl = enhancedUrl.replace("zoom=1", "zoom=5");
                }
                // Add fife=w800 parameter for larger width
                if (!enhancedUrl.contains("fife=")) {
                    enhancedUrl = enhancedUrl + (enhancedUrl.contains("?") ? "&" : "?") + "fife=w800";
                }
                break;
                
            case "medium":
                // Use zoom=3 for medium resolution
                if (enhancedUrl.contains("zoom=1")) {
                    enhancedUrl = enhancedUrl.replace("zoom=1", "zoom=3");
                }
                // Add fife=w400 parameter for medium width
                if (!enhancedUrl.contains("fife=")) {
                    enhancedUrl = enhancedUrl + (enhancedUrl.contains("?") ? "&" : "?") + "fife=w400";
                }
                break;
                
            case "low":
                // Use zoom=1 for low resolution (default)
                // No need to modify zoom parameter
                break;
        }
        
        return enhancedUrl;
    }

    /**
     * Represents a cover candidate with its URL, source, priority, published date, and source ID
     */
    public static class CoverCandidate {
        private final String url;
        private final String source;
        private int priority;
        private final Date publishedDate;
        private final String sourceId;

        /**
         * Constructs a CoverCandidate object
         *
         * @param url The URL of the cover candidate
         * @param source The source of the cover candidate
         * @param priority The priority of the cover candidate
         * @param publishedDate The published date of the cover candidate
         * @param sourceId The source ID of the cover candidate
         */
        public CoverCandidate(String url, String source, int priority, Date publishedDate, String sourceId) {
            this.url = url;
            this.source = source;
            this.priority = priority;
            this.publishedDate = publishedDate;
            this.sourceId = sourceId;
        }

        public String getUrl() {
            return url;
        }

        public String getSource() {
            return source;
        }

        public int getPriority() {
            return priority;
        }

        public Date getPublishedDate() {
            return publishedDate;
        }

        public String getSourceId() {
            return sourceId;
        }

        public void setPriority(int priority) {
            this.priority = priority;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CoverCandidate that = (CoverCandidate) o;
            return Objects.equals(url, that.url) && Objects.equals(sourceId, that.sourceId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(url, sourceId);
        }
    }
}
