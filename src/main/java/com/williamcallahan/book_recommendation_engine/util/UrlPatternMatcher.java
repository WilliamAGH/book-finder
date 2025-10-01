package com.williamcallahan.book_recommendation_engine.util;

import java.util.regex.Pattern;

/**
 * Utility for identifying the source of URLs based on their patterns.
 * Centralizes URL pattern matching logic used across the application.
 */
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
     */
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
     * @param url the URL to analyze
     * @return the identified source
     */
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
     * @param url the URL to check
     * @return true if the URL is from Google Books
     */
    public static boolean isGoogleBooksUrl(String url) {
        if (url == null || url.trim().isEmpty()) {
            return false;
        }
        return GOOGLE_BOOKS_PATTERN.matcher(url).matches();
    }

    /**
     * Checks if a URL is from Open Library.
     *
     * @param url the URL to check
     * @return true if the URL is from Open Library
     */
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