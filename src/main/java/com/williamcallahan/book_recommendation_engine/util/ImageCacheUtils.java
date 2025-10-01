/**
 * Utility library for image caching and processing in the book recommendation engine
 * 
 * This class provides functionality for:
 * - Image hash calculation and comparison
 * - File extension detection and management
 * - Mapping between different image source enumerations
 * - Book identifier extraction and prioritization
 * - Secure filename generation for cached images
 * 
 * All methods are static and thread-safe, designed for use across the application
 * when handling book cover images and related caching operations.
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageAttemptStatus;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.model.image.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector;
import org.slf4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Base64;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class for image caching operations
 * - Provides helper methods for hashing, filename generation, and type mapping
 *
 */
public final class ImageCacheUtils {

    private static final Pattern GOOGLE_PG_PATTERN = Pattern.compile("[?&]pg=([A-Z]+[0-9]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern GOOGLE_PRINTSEC_FRONTCOVER_PATTERN = Pattern.compile("[?&](printsec=frontcover|pt=frontcover)", Pattern.CASE_INSENSITIVE);
    private static final Pattern GOOGLE_EDGE_CURL_PATTERN = Pattern.compile("[?&]edge=curl", Pattern.CASE_INSENSITIVE);

    /**
     * Private constructor to prevent instantiation of utility class
     */
    private ImageCacheUtils() {
        // Prevent instantiation
    }

    /**
     * Computes SHA-256 hash for given image data
     * 
     * @deprecated This is a generic hashing utility, not image-specific.
     * Consider creating HashUtils.computeSha256(byte[]) for broader reusability.
     * This method belongs in a general-purpose hashing utility, not image-specific utils.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * byte[] hash = ImageCacheUtils.computeImageHash(imageData);
     * 
     * // New (future):
     * byte[] hash = HashUtils.computeSha256(imageData);
     * 
     * // Or inline:
     * MessageDigest digest = MessageDigest.getInstance("SHA-256");
     * byte[] hash = digest.digest(imageData);
     * }</pre>
     * 
     * @param imageData Byte array of the image
     * @return SHA-256 hash byte array
     * @throws NoSuchAlgorithmException If SHA-256 algorithm is not available
     * 
     * @implNote Uses standard MessageDigest implementation for secure hashing
     * Useful for deduplication and content-based caching of images
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static byte[] computeImageHash(byte[] imageData) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return digest.digest(imageData);
    }

    /**
     * Compares two byte arrays for similarity (equality)
     * 
     * @deprecated Use {@link java.util.Arrays#equals(byte[], byte[])} directly.
     * This method just wraps Arrays.equals() with no added value.
     * The standard Java API is clearer and more maintainable.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * boolean similar = ImageCacheUtils.isHashSimilar(hash1, hash2);
     * 
     * // New:
     * boolean similar = java.util.Arrays.equals(hash1, hash2);
     * }</pre>
     * 
     * @param hash1 First byte array
     * @param hash2 Second byte array
     * @return True if arrays are identical, false otherwise
     * 
     * @implNote Performs byte-by-byte comparison rather than using Arrays.equals
     * for more explicit control and clarity -- returns false immediately on length
     * mismatch or when any corresponding bytes differ
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static boolean isHashSimilar(byte[] hash1, byte[] hash2) {
        if (hash1 == null || hash2 == null || hash1.length != hash2.length) {
            return false;
        }
        for (int i = 0; i < hash1.length; i++) {
            if (hash1[i] != hash2[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Extracts file extension from a URL string
     * 
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.UrlUtils#extractFileExtension(String)} instead.
     * This URL parsing logic belongs in UrlUtils, not image-specific utilities.
     * UrlUtils provides centralized URL manipulation with consistent behavior.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * String ext = ImageCacheUtils.getFileExtensionFromUrl(url);
     * 
     * // New:
     * String ext = UrlUtils.extractFileExtension(url);
     * }</pre>
     * 
     * @param url The URL string
     * @return File extension (e.g., .jpg, .png)
     * 
     * @implNote Defaults to .jpg if no valid extension is found
     * Handles URLs with query parameters by truncating them before extraction
     * Validates that the extension is a known image format to prevent invalid extensions
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static String getFileExtensionFromUrl(String url) {
        String extension = ".jpg"; // Default extension
        if (url != null && url.contains(".")) {
            int queryParamIndex = url.indexOf("?");
            String urlWithoutParams = queryParamIndex > 0 ? url.substring(0, queryParamIndex) : url;
            int lastDotIndex = urlWithoutParams.lastIndexOf(".");
            if (lastDotIndex > 0 && lastDotIndex < urlWithoutParams.length() - 1) {
                String ext = urlWithoutParams.substring(lastDotIndex).toLowerCase(Locale.ROOT);
                // Basic validation for common image extensions
                if (ext.matches("\\.(jpg|jpeg|png|gif|webp|svg|bmp|tiff)")) {
                    extension = ext;
                }
            }
        }
        return extension;
    }

    /**
     * Generates a filename from a URL using SHA-256 hash
     * 
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.S3KeyGenerator#generateFromUrl(String)} instead.
     * This method duplicates S3 key generation logic. S3KeyGenerator is the canonical implementation
     * that provides consistent filename generation for storage systems.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * String filename = ImageCacheUtils.generateFilenameFromUrl(url);
     * 
     * // New:
     * String filename = S3KeyGenerator.generateFromUrl(url);
     * }</pre>
     * 
     * @param url The URL string
     * @return Generated filename string
     * @throws NoSuchAlgorithmException If SHA-256 algorithm is not available
     * 
     * @implNote Creates a URL-safe Base64 encoded hash of the URL
     * Truncates hash to 32 characters maximum for brevity and file system compatibility
     * Appends the original file extension to maintain MIME type information
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static String generateFilenameFromUrl(String url) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(url.getBytes(StandardCharsets.UTF_8));
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        String extension = getFileExtensionFromUrl(url);
        // Use a portion of the hash to keep filename length reasonable
        return encoded.substring(0, Math.min(encoded.length(), 32)) + extension;
    }

    /**
     * Determines the best identifier to use as a cache key for a book
     * 
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.CoverIdentifierResolver#resolve(Book)} instead.
     * Will be removed in version 1.0.0.
     * 
     * @param book The book object
     * @return The most specific available identifier, or null if no valid identifiers exist
     * 
     * @implNote Prioritizes identifiers in order of specificity and standardization:
     * 1. ISBN-13 (most specific and internationally standardized)
     * 2. ISBN-10 (older but still widely used standard)
     * 3. Google Books ID (platform-specific but unique)
     * Returns null if book is null or has no valid identifiers
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static String getIdentifierKey(Book book) {
        if (book == null) {
            return null;
        }
        String preferredIsbn = ValidationUtils.BookValidator.getPreferredIsbn(book);
        if (ValidationUtils.hasText(preferredIsbn)) {
            return preferredIsbn;
        }
        return ValidationUtils.hasText(book.getId()) ? book.getId() : null;
    }

    /**
     * Maps a string representation of a source name to ImageSourceName enum
     * 
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.CoverSourceMapper#fromString(String)} instead.
     * Will be removed in version 1.0.0.
     * 
     * @param sourceNameString The string name of the source
     * @return Corresponding ImageSourceName enum, or UNKNOWN if no match found
     * 
     * @implNote Uses a multi-stage approach to mapping:
     * 1. Pattern matching for common source name variations (case-insensitive)
     * 2. Direct enum parsing for exact matches after normalization
     * 3. Falls back to UNKNOWN if no match is found or input is null
     * Designed to be flexible with different string formats from various sources
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static ImageSourceName mapStringToImageSourceName(String sourceNameString) {
        if (sourceNameString == null) {
            return ImageSourceName.UNKNOWN;
        }
        // Simplified mapping, can be expanded for more precise matching
        String lowerSourceName = sourceNameString.toLowerCase(Locale.ROOT);
        if (lowerSourceName.contains("openlibrary")) return ImageSourceName.OPEN_LIBRARY;
        if (lowerSourceName.contains("google") || lowerSourceName.contains("googlebooks")) return ImageSourceName.GOOGLE_BOOKS;
        if (lowerSourceName.contains("longitood")) return ImageSourceName.LONGITOOD;
        if (lowerSourceName.contains("s3")) return ImageSourceName.UNKNOWN;
        if (lowerSourceName.contains("local")) return ImageSourceName.UNKNOWN;
        if (lowerSourceName.equalsIgnoreCase("ProvisionalHint")) return ImageSourceName.UNKNOWN;

        try {
            // Attempt direct enum conversion for exact matches (case-insensitive for valueOf)
            return ImageSourceName.valueOf(sourceNameString.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9_]", ""));
        } catch (IllegalArgumentException e) {
            return ImageSourceName.UNKNOWN;
        }
    }

    /**
     * Maps CoverImageSource enum to ImageSourceName enum
     * 
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.CoverSourceMapper#toImageSourceName(CoverImageSource)} instead.
     * Will be removed in version 1.0.0.
     * 
     * @param coverImageSource The CoverImageSource enum value
     * @return Corresponding ImageSourceName enum, or UNKNOWN if no match found
     * 
     * @implNote Attempts direct name-based mapping first for maintainability
     * Falls back to explicit case-by-case mapping if direct conversion fails
     * Handles special cases and ensures all CoverImageSource values have a mapping
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static ImageSourceName mapCoverImageSourceToImageSourceName(CoverImageSource coverImageSource) {
        if (coverImageSource == null) {
            return ImageSourceName.UNKNOWN;
        }
        try {
            return ImageSourceName.valueOf(coverImageSource.name());
        } catch (IllegalArgumentException e) {
            // Fallback for names that might not directly map or for broader categories
            switch (coverImageSource) {
                case GOOGLE_BOOKS: return ImageSourceName.GOOGLE_BOOKS;
                case OPEN_LIBRARY: return ImageSourceName.OPEN_LIBRARY;
                case LONGITOOD: return ImageSourceName.LONGITOOD;
                case ANY: // Fall-through
                case NONE: // Fall-through
                case UNDEFINED: // Fall-through
                default: return ImageSourceName.UNKNOWN;
            }
        }
    }

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.GoogleBooksUrlEnhancer#enhanceUrl(String, int)} instead.
     * This method contains Google Books-specific logic that belongs in a dedicated utility.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * String enhanced = ImageCacheUtils.enhanceGoogleCoverUrl(baseUrl, "zoom=2");
     * 
     * // New:
     * String enhanced = GoogleBooksUrlEnhancer.enhanceUrl(baseUrl, 2);
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static String enhanceGoogleCoverUrl(String baseUrl, String zoomParam) {
        if (baseUrl == null) {
            return null;
        }

        // Use shared UrlUtils instead of inline duplication
        String enhancedUrl = UrlUtils.normalizeToHttps(baseUrl);
        if (enhancedUrl == null || enhancedUrl.isEmpty()) {
            return null;
        }

        if (enhancedUrl.contains("&fife=")) {
            enhancedUrl = enhancedUrl.replaceAll("&fife=w\\d+(-h\\d+)?", "");
        } else if (enhancedUrl.contains("?fife=")) {
            enhancedUrl = enhancedUrl.replaceAll("\\?fife=w\\d+(-h\\d+)?", "?");
            if (enhancedUrl.endsWith("?")) {
                enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
            }
        }

        enhancedUrl = enhancedUrl.replaceAll("[?&]edge=curl", "");

        if (enhancedUrl.endsWith("&")) {
            enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
        }
        if (enhancedUrl.endsWith("?")) {
            enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
        }

        if (zoomParam != null && !zoomParam.isEmpty()) {
            if (enhancedUrl.contains("zoom=")) {
                enhancedUrl = enhancedUrl.replaceAll("zoom=\\d+", zoomParam);
            } else {
                enhancedUrl += enhancedUrl.contains("?") ? "&" + zoomParam : "?" + zoomParam;
            }
        }

        return enhancedUrl;
    }

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.GoogleBooksUrlEnhancer#isGoogleBooksUrl(String)} instead.
     * This method contains Google Books-specific logic that belongs in a dedicated utility.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * boolean isGoogle = ImageCacheUtils.isLikelyGoogleCoverUrl(url);
     * 
     * // New:
     * boolean isGoogle = GoogleBooksUrlEnhancer.isGoogleBooksUrl(url);
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static boolean isLikelyGoogleCoverUrl(String url) {
        if (url == null || url.isEmpty()) {
            return false;
        }
        if (GOOGLE_PG_PATTERN.matcher(url).find()) {
            return false;
        }
        if (GOOGLE_EDGE_CURL_PATTERN.matcher(url).find()) {
            return false;
        }
        return true; // default optimistic assumption when no negative indicators are present
    }

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.GoogleBooksUrlEnhancer#hasFrontCoverHint(String)} instead.
     * This method contains Google Books-specific logic that belongs in a dedicated utility.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * boolean isFrontCover = ImageCacheUtils.hasGoogleFrontCoverHint(url);
     * 
     * // New:
     * boolean isFrontCover = GoogleBooksUrlEnhancer.hasFrontCoverHint(url);
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static boolean hasGoogleFrontCoverHint(String url) {
        if (url == null || url.isEmpty()) {
            return false;
        }
        return GOOGLE_PRINTSEC_FRONTCOVER_PATTERN.matcher(url).find();
    }

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.ImageDimensionUtils#normalize(Integer)} instead.
     * Will be removed in version 1.0.0.
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static Integer normalizeImageDimension(Integer value) {
        if (value == null || value <= 1) {
            return 512;
        }
        return value;
    }

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.StringUtils#coalesce(String...)} instead.
     * This method duplicates string coalescing logic that is now centralized in StringUtils.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * String result = ImageCacheUtils.firstNonBlank(url1, url2, url3);
     * 
     * // New:
     * String result = StringUtils.coalesce(url1, url2, url3);
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.ImageDimensionUtils#hasAcceptableDimensions(ImageDetails)} instead,
     * combined with explicit placeholder checking. This method mixes dimension validation with placeholder detection,
     * violating single responsibility principle.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * boolean valid = ImageCacheUtils.isValidImageDetails(details, placeholderPath);
     * 
     * // New:
     * boolean valid = ImageDimensionUtils.hasAcceptableDimensions(details)
     *     && details.getUrlOrPath() != null
     *     && !details.getUrlOrPath().equals(placeholderPath);
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static boolean isValidImageDetails(ImageDetails imageDetails, String placeholderPath) {
        return imageDetails != null
            && imageDetails.getUrlOrPath() != null
            && !Objects.equals(imageDetails.getUrlOrPath(), placeholderPath)
            && imageDetails.getWidth() != null && imageDetails.getWidth() > 1
            && imageDetails.getHeight() != null && imageDetails.getHeight() > 1;
    }

    /**
     * @deprecated This complex selection logic should be moved to a dedicated service.
     * Consider creating ImageSelectionService or moving to CoverSourceFetchingService.
     * This utility method has grown too complex and violates utility class principles.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Path:</b></p>
     * <pre>{@code
     * // Future: Create ImageSelectionService
     * ImageDetails best = imageSelectionService.selectBest(candidates, criteria);
     * 
     * // Or move to existing service:
     * ImageDetails best = coverSourceFetchingService.selectBestCandidate(candidates);
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static ImageSelectionResult selectBestImageDetails(
        List<ImageDetails> candidates,
        String placeholderPath,
        String cacheDirName,
        String bookIdForLog,
        Logger logger) {

        if (candidates == null || candidates.isEmpty()) {
            if (logger != null) {
                logger.warn("Book ID {}: selectBestImageDetails called with no candidates.", bookIdForLog);
            }
            return new ImageSelectionResult(null, "no-candidates-for-selection");
        }

        List<ImageDetails> validCandidates = candidates.stream()
            .filter(candidate -> isValidImageDetails(candidate, placeholderPath))
            .collect(Collectors.toList());

        if (validCandidates.isEmpty()) {
            if (logger != null) {
                logger.warn("Book ID {}: No valid candidates after filtering for selection.", bookIdForLog);
            }
            return new ImageSelectionResult(null, "no-valid-candidates-for-selection");
        }

        Comparator<ImageDetails> comparator = Comparator
            .<ImageDetails>comparingInt(details -> {
                // Priority 0: Our own cached images (local disk or S3 CDN) with reasonable dimensions
                // Check storageLocation field (new way) OR deprecated cache source enums (backward compat)
                String storage = details.getStorageLocation();
                
                boolean isCached = storage != null && !storage.isBlank();
                
                if (isCached
                    && details.getWidth() != null && details.getWidth() > 150
                    && details.getHeight() != null && details.getHeight() > 150
                    && details.getUrlOrPath() != null
                    && !details.getUrlOrPath().equals(placeholderPath)) {
                    return 0; // Highest priority for cached images
                }
                return 1; // Lower priority for external API sources
            })
            .thenComparing(Comparator.<ImageDetails>comparingLong(details -> {
                if (details.getWidth() == null || details.getHeight() == null) {
                    return 0L;
                }
                return (long) details.getWidth() * details.getHeight();
            }).reversed())
            .thenComparingInt(details -> {
                // Final tiebreaker by storage location and source quality:
                // Priority order:
                // 1. LOCAL storage (fastest - server disk)
                // 2. S3 storage (fast - CDN)
                // 3-5. External API sources (by data source quality, not cache status)
                String storage = details.getStorageLocation();
                CoverImageSource src = details.getCoverImageSource();
                
                // Check storage location first (new way)
                if (ImageDetails.STORAGE_LOCAL.equals(storage) && details.getUrlOrPath() != null 
                    && !details.getUrlOrPath().equals(placeholderPath)) {
                    return 0;
                }
                if (ImageDetails.STORAGE_S3.equals(storage)) return 1;
                
                // Actual data sources (not cache)
                if (src == CoverImageSource.GOOGLE_BOOKS) return 2;
                if (src == CoverImageSource.OPEN_LIBRARY) return 3;
                if (src == CoverImageSource.LONGITOOD) return 4;
                return 5;
            });

        ImageDetails bestImage = java.util.Collections.min(validCandidates, comparator);

        if (logger != null) {
            logger.info("Book ID {}: Selected best image from {} candidates. URL/Path: {}, Source: {}, Dimensions: {}x{}",
                bookIdForLog,
                validCandidates.size(),
                bestImage.getUrlOrPath(),
                bestImage.getCoverImageSource(),
                bestImage.getWidth(),
                bestImage.getHeight());

            if (logger.isDebugEnabled()) {
                validCandidates.forEach(candidate ->
                    logger.debug("Book ID {}: Candidate - URL/Path: {}, Source: {}, Dimensions: {}x{}",
                        bookIdForLog,
                        candidate.getUrlOrPath(),
                        candidate.getCoverImageSource(),
                        candidate.getWidth(),
                        candidate.getHeight())
                );
            }
        }

        return new ImageSelectionResult(bestImage, null);
    }

    /**
     * @deprecated Provenance data manipulation should be encapsulated in a dedicated handler.
     * Consider creating ImageProvenanceHandler.updateSelection() method.
     * This utility method violates encapsulation by directly manipulating complex domain objects.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Path:</b></p>
     * <pre>{@code
     * // Future: Create ImageProvenanceHandler
     * imageProvenanceHandler.recordSelection(provenanceData, sourceName, imageDetails, reason);
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static void updateSelectedImageInfo(
        ImageProvenanceData provenanceData,
        ImageSourceName sourceName,
        ImageDetails imageDetails,
        String selectionReason,
        String cacheDirName,
        Logger logger) {

        if (provenanceData == null || imageDetails == null || cacheDirName == null) {
            return;
        }

        ImageProvenanceData.SelectedImageInfo selectedInfo = new ImageProvenanceData.SelectedImageInfo();
        selectedInfo.setSourceName(sourceName);
        selectedInfo.setFinalUrl(imageDetails.getUrlOrPath());
        selectedInfo.setResolution(imageDetails.getResolutionPreference() != null
            ? imageDetails.getResolutionPreference().name()
            : "ORIGINAL");
        selectedInfo.setDimensions((imageDetails.getWidth() != null ? imageDetails.getWidth() : "N/A")
            + "x"
            + (imageDetails.getHeight() != null ? imageDetails.getHeight() : "N/A"));
        selectedInfo.setSelectionReason(selectionReason);

        // Determine storage location - prefer new storageLocation field, fall back to inference
        String storage = imageDetails.getStorageLocation();
        if (storage != null && !storage.isBlank()) {
            selectedInfo.setStorageLocation(storage);
            if (ImageDetails.STORAGE_S3.equals(storage)) {
                selectedInfo.setS3Key(imageDetails.getStorageKey() != null
                    ? imageDetails.getStorageKey()
                    : imageDetails.getSourceSystemId());
            }
        } else if (imageDetails.getUrlOrPath() != null) {
            UrlSourceDetector.detectStorageLocation(imageDetails.getUrlOrPath())
                .ifPresentOrElse(detected -> {
                    selectedInfo.setStorageLocation(detected);
                    if (ImageDetails.STORAGE_S3.equals(detected)) {
                        selectedInfo.setS3Key(imageDetails.getSourceSystemId());
                    }
                }, () -> selectedInfo.setStorageLocation("Remote"));
        } else {
            selectedInfo.setStorageLocation("Remote");
        }

        provenanceData.setSelectedImageInfo(selectedInfo);

        if (logger != null && logger.isDebugEnabled()) {
            logger.debug("Provenance updated: Selected image from {} ({}), URL: {}, Dimensions: {}x{}, Reason: {}",
                sourceName,
                selectedInfo.getStorageLocation(),
                selectedInfo.getFinalUrl(),
                imageDetails.getWidth(),
                imageDetails.getHeight(),
                selectionReason);
        }
    }

    /**
     * @deprecated Provenance tracking should be encapsulated in a dedicated handler.
     * Consider creating ImageProvenanceHandler.recordAttempt() method.
     * This utility method violates encapsulation by directly manipulating tracking data.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Path:</b></p>
     * <pre>{@code
     * // Future: Create ImageProvenanceHandler
     * imageProvenanceHandler.recordAttempt(provenanceData, sourceName, url, status, failureReason);
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static void addAttemptToProvenance(
        ImageProvenanceData provenanceData,
        ImageSourceName sourceName,
        String urlAttempted,
        ImageAttemptStatus status,
        String failureReason,
        ImageDetails detailsIfSuccess) {

        if (provenanceData == null) {
            return;
        }

        if (provenanceData.getAttemptedImageSources() == null) {
            provenanceData.setAttemptedImageSources(
                java.util.Collections.synchronizedList(new ArrayList<>())
            );
        }

        ImageProvenanceData.AttemptedSourceInfo attemptInfo =
            new ImageProvenanceData.AttemptedSourceInfo(sourceName, urlAttempted, status);

        if (failureReason != null) {
            attemptInfo.setFailureReason(failureReason);
        }

        if (detailsIfSuccess != null && status == ImageAttemptStatus.SUCCESS) {
            attemptInfo.setFetchedUrl(detailsIfSuccess.getUrlOrPath());
            attemptInfo.setDimensions((detailsIfSuccess.getWidth() != null ? detailsIfSuccess.getWidth() : "N/A")
                + "x"
                + (detailsIfSuccess.getHeight() != null ? detailsIfSuccess.getHeight() : "N/A"));
        }

        provenanceData.getAttemptedImageSources().add(attemptInfo);
    }

    public static final class ImageSelectionResult {
        private final ImageDetails bestImage;
        private final String fallbackReason;

        public ImageSelectionResult(ImageDetails bestImage, String fallbackReason) {
            this.bestImage = bestImage;
            this.fallbackReason = fallbackReason;
        }

        public ImageDetails bestImage() {
            return bestImage;
        }

        public String fallbackReason() {
            return fallbackReason;
        }

        public boolean hasSelection() {
            return bestImage != null;
        }
    }

    /**
     * Maps ImageSourceName enum back to CoverImageSource enum
     * 
     * @param sourceNameEnum The ImageSourceName enum value
     * @return Corresponding CoverImageSource enum, or UNDEFINED if no match found
     * 
     * @implNote Reverse mapping companion to mapCoverImageSourceToImageSourceName
     * Attempts direct name-based mapping first for maintainability
     * Falls back to explicit switch statement for handling special cases
     * Returns UNDEFINED for ImageSourceName values that have no CoverImageSource equivalent
     */
    /**
     * Maps ImageSourceName enum back to CoverImageSource enum
     *
     * @deprecated Deprecated 2025-10-01. Use {@link com.williamcallahan.book_recommendation_engine.util.cover.CoverSourceMapper#toCoverImageSource(com.williamcallahan.book_recommendation_engine.model.image.ImageSourceName)} instead.
     *             This logic is centralized in CoverSourceMapper to avoid duplication.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public static CoverImageSource mapImageSourceNameEnumToCoverImageSource(ImageSourceName sourceNameEnum) {
        if (sourceNameEnum == null) {
            return CoverImageSource.UNDEFINED;
        }
        try {
            return CoverImageSource.valueOf(sourceNameEnum.name());
        } catch (IllegalArgumentException e) {
            switch (sourceNameEnum) {
                case GOOGLE_BOOKS: return CoverImageSource.GOOGLE_BOOKS;
                case OPEN_LIBRARY: return CoverImageSource.OPEN_LIBRARY;
                case LONGITOOD: return CoverImageSource.LONGITOOD;
                // case INTERNAL_PROCESSING: // No direct CoverImageSource, map to UNDEFINED
                // case UNKNOWN: // Fall-through
                default: return CoverImageSource.UNDEFINED;
            }
        }
    }
}
