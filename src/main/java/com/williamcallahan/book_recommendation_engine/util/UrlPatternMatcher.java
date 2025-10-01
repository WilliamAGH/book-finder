package com.williamcallahan.book_recommendation_engine.util;

import java.util.regex.Pattern;

/**
 * Utility for identifying the source of URLs based on their patterns.
 * Centralizes URL pattern matching logic used across the application.
 * 
 * @deprecated This entire class is deprecated in favor of {@link com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector}.
 * UrlSourceDetector provides more comprehensive URL analysis with better integration into the cover persistence system.
 * All methods in this class have individual deprecation notices with migration examples.
 * Will be removed in version 1.0.0.
 */
@Deprecated(since = "0.9.0", forRemoval = true)
public final class UrlPatternMatcher {

    // Pattern constants for different sources
    private static final Pattern GOOGLE_BOOKS_PATTERN = Pattern.compile(
        "(?i).*(books\\.google\\.|googleusercontent\\.com|ggpht\\.com).*"
    );

    private static final Pattern OPEN_LIBRARY_PATTERN = Pattern.compile(
        "(?i).*(openlibrary\\.org|archive\\.org/services/img).*"
    );

    private static final Pattern AMAZON_PATTERN = Pattern.compile(
        "(?i).*(amazon\\.com|amazon\\.co\\.|amzn\\.to|media-amazon\\.com|ssl-images-amazon\\.com).*"
    );

    private static final Pattern GOODREADS_PATTERN = Pattern.compile(
        "(?i).*(goodreads\\.com|gr-assets\\.com).*"
    );

    private UrlPatternMatcher() {
        // Utility class
    }

    /**
     * Enum representing different URL sources.
     * 
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource} instead.
     * This enum duplicates CoverImageSource and should not be used.
     * CoverImageSource is the canonical enum for tracking image sources.
     * Will be removed in version 1.0.0.
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public enum UrlSource {
        GOOGLE_BOOKS,
        OPEN_LIBRARY,
        AMAZON,
        GOODREADS,
        UNKNOWN
    }

    /**
     * Identifies the source of a URL based on its pattern.
     * 
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector#detect(String)} instead.
     * This class duplicates URL source detection logic. UrlSourceDetector is the canonical implementation
     * that returns CoverImageSource enum and is integrated with the cover persistence system.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * UrlPatternMatcher.UrlSource source = UrlPatternMatcher.identifySource(url);
     * if (source == UrlPatternMatcher.UrlSource.GOOGLE_BOOKS) {
     *     // handle Google Books
     * }
     * 
     * // New:
     * CoverImageSource source = UrlSourceDetector.detect(url);
     * if (source == CoverImageSource.GOOGLE_BOOKS) {
     *     // handle Google Books
     * }
     * }</pre>
     *
     * @param url the URL to analyze
     * @return the identified source
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static UrlSource identifySource(String url) {
        if (url == null || url.trim().isEmpty()) {
            return UrlSource.UNKNOWN;
        }

        if (isGoogleBooksUrl(url)) {
            return UrlSource.GOOGLE_BOOKS;
        }
        if (isOpenLibraryUrl(url)) {
            return UrlSource.OPEN_LIBRARY;
        }
        if (isAmazonUrl(url)) {
            return UrlSource.AMAZON;
        }
        if (isGoodreadsUrl(url)) {
            return UrlSource.GOODREADS;
        }

        return UrlSource.UNKNOWN;
    }

    /**
     * Checks if a URL is from Google Books.
     * 
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector#detect(String)} instead.
     * Check if the result equals CoverImageSource.GOOGLE_BOOKS. This provides consistent source detection
     * across the application.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * if (UrlPatternMatcher.isGoogleBooksUrl(url)) {
     *     // handle Google Books URL
     * }
     * 
     * // New:
     * if (UrlSourceDetector.detect(url) == CoverImageSource.GOOGLE_BOOKS) {
     *     // handle Google Books URL
     * }
     * }</pre>
     *
     * @param url the URL to check
     * @return true if the URL is from Google Books
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static boolean isGoogleBooksUrl(String url) {
        if (url == null || url.trim().isEmpty()) {
            return false;
        }
        return GOOGLE_BOOKS_PATTERN.matcher(url).matches();
    }

    /**
     * Checks if a URL is from Open Library.
     * 
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector#detect(String)} instead.
     * Check if the result equals CoverImageSource.OPEN_LIBRARY. This provides consistent source detection
     * across the application.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * if (UrlPatternMatcher.isOpenLibraryUrl(url)) {
     *     // handle Open Library URL
     * }
     * 
     * // New:
     * if (UrlSourceDetector.detect(url) == CoverImageSource.OPEN_LIBRARY) {
     *     // handle Open Library URL
     * }
     * }</pre>
     *
     * @param url the URL to check
     * @return true if the URL is from Open Library
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static boolean isOpenLibraryUrl(String url) {
        if (url == null || url.trim().isEmpty()) {
            return false;
        }
        return OPEN_LIBRARY_PATTERN.matcher(url).matches();
    }

    /**
     * Checks if a URL is from Amazon.
     *
     * @param url the URL to check
     * @return true if the URL is from Amazon
     */
    /**
     * @deprecated Deprecated 2025-10-01. Use {@link com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector#detectSource(String)}
     *             and compare to non-cover sources as needed.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public static boolean isAmazonUrl(String url) {
        if (url == null || url.trim().isEmpty()) {
            return false;
        }
        return AMAZON_PATTERN.matcher(url).matches();
    }

    /**
     * Checks if a URL is from Goodreads.
     *
     * @param url the URL to check
     * @return true if the URL is from Goodreads
     */
    /**
     * @deprecated Deprecated 2025-10-01. Use {@link com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector#detectSource(String)}
     *             and compare to non-cover sources as needed.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public static boolean isGoodreadsUrl(String url) {
        if (url == null || url.trim().isEmpty()) {
            return false;
        }
        return GOODREADS_PATTERN.matcher(url).matches();
    }

    /**
     * Checks if a URL is a known book cover source.
     *
     * @param url the URL to check
     * @return true if the URL is from a known book cover source
     */
    /**
     * @deprecated Deprecated 2025-10-01. Use {@link com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector#isExternalUrl(String)}
     *             or source-specific detection methods.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public static boolean isKnownBookCoverSource(String url) {
        UrlSource source = identifySource(url);
        return source != UrlSource.UNKNOWN;
    }

    /**
     * Extracts the book ID from a Google Books URL if present.
     *
     * @param url the Google Books URL
     * @return the book ID or null if not found
     */
    /**
     * @deprecated Deprecated 2025-10-01. Inline parsing should be moved to provider-specific mappers when needed.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public static String extractGoogleBooksId(String url) {
        if (url == null || !isGoogleBooksUrl(url)) {
            return null;
        }

        // Pattern for Google Books volume ID (e.g., /books?id=XXXXX or /books/edition/_/XXXXX)
        Pattern idPattern = Pattern.compile("(?:id=|edition/_/)([A-Za-z0-9_-]+)");
        var matcher = idPattern.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }

        return null;
    }

    /**
     * Extracts the Open Library ID from an Open Library URL if present.
     *
     * @param url the Open Library URL
     * @return the Open Library ID or null if not found
     */
    /**
     * @deprecated Deprecated 2025-10-01. Inline parsing should be moved to provider-specific mappers when needed.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public static String extractOpenLibraryId(String url) {
        if (url == null || !isOpenLibraryUrl(url)) {
            return null;
        }

        // Pattern for Open Library ID (e.g., /books/OL12345M or /works/OL67890W)
        Pattern idPattern = Pattern.compile("(?:/books/|/works/)(OL[0-9]+[MW])");
        var matcher = idPattern.matcher(url);
        if (matcher.find()) {
            return matcher.group(1);
        }

        return null;
    }

    /**
     * Checks if a URL appears to be a placeholder or default image.
     *
     * @param url the URL to check
     * @return true if the URL appears to be a placeholder
     */
    /**
     * @deprecated Deprecated 2025-10-01. Use centralized placeholder handling via ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH
     *             and validation utilities.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public static boolean isPlaceholderUrl(String url) {
        if (url == null || url.trim().isEmpty()) {
            return true;
        }

        String lowerUrl = url.toLowerCase();
        return lowerUrl.contains("no-image") ||
               lowerUrl.contains("placeholder") ||
               lowerUrl.contains("default") ||
               lowerUrl.contains("missing") ||
               lowerUrl.contains("unavailable") ||
               lowerUrl.contains("blank") ||
               lowerUrl.contains("empty");
    }
}