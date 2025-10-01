package com.williamcallahan.book_recommendation_engine.util;

import lombok.extern.slf4j.Slf4j;

import java.util.regex.Pattern;

/**
 * Utilities for enhancing Google Books cover image URLs.
 * 
 * Google Books API returns cover images with size parameters like &zoom=1.
 * This utility upgrades those URLs to use higher quality zoom levels for
 * better visual presentation.
 * 
 * @author William Callahan
 * @since 0.9.0
 */
@Slf4j
public final class GoogleBooksUrlEnhancer {
    
    private static final Pattern ZOOM_PATTERN = Pattern.compile("([&?])zoom=\\d+");
    private static final int DEFAULT_ZOOM_LEVEL = 2;
    
    private GoogleBooksUrlEnhancer() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Enhances a Google Books cover URL to use a higher quality zoom level.
     * 
     * If the URL already contains a zoom parameter, it will be replaced.
     * If the URL doesn't contain a zoom parameter, one will be added.
     * 
     * @param url Original Google Books cover URL
     * @return Enhanced URL with improved zoom level, or original URL if null/blank
     * 
     * @example
     * <pre>{@code
     * String original = "https://books.google.com/books/content?id=abc&printsec=frontcover&img=1&zoom=1";
     * String enhanced = GoogleBooksUrlEnhancer.enhanceUrl(original);
     * // Returns: "https://books.google.com/books/content?id=abc&printsec=frontcover&img=1&zoom=2"
     * }</pre>
     */
    public static String enhanceUrl(String url) {
        return enhanceUrl(url, DEFAULT_ZOOM_LEVEL);
    }
    
    /**
     * Enhances a Google Books cover URL to use a specified zoom level.
     * 
     * @param url Original Google Books cover URL
     * @param zoomLevel Desired zoom level (typically 1-5, where higher = better quality)
     * @return Enhanced URL with specified zoom level, or original URL if null/blank
     */
    public static String enhanceUrl(String url, int zoomLevel) {
        if (url == null || url.isBlank()) {
            return url;
        }
        
        if (!isGoogleBooksUrl(url)) {
            log.debug("URL is not a Google Books URL, returning unchanged: {}", url);
            return url;
        }
        
        // Check if URL already has a zoom parameter
        var matcher = ZOOM_PATTERN.matcher(url);
        if (matcher.find()) {
            // Replace existing zoom parameter
            String enhanced = matcher.replaceFirst("$1zoom=" + zoomLevel);
            log.debug("Enhanced Google Books URL from zoom level to {}: {}", zoomLevel, enhanced);
            return enhanced;
        } else {
            // Add zoom parameter
            String separator = url.contains("?") ? "&" : "?";
            String enhanced = url + separator + "zoom=" + zoomLevel;
            log.debug("Added zoom={} parameter to Google Books URL: {}", zoomLevel, enhanced);
            return enhanced;
        }
    }
    
    /**
     * Checks if the given URL is a Google Books URL.
     * 
     * @param url URL to check
     * @return true if the URL is from Google Books domain
     */
    public static boolean isGoogleBooksUrl(String url) {
        if (url == null || url.isBlank()) {
            return false;
        }
        return url.contains("books.google.com") || url.contains("books.google.");
    }
    
    /**
     * Extracts the current zoom level from a Google Books URL.
     * 
     * @param url Google Books URL
     * @return Current zoom level, or -1 if no zoom parameter found
     */
    public static int extractZoomLevel(String url) {
        if (url == null || url.isBlank()) {
            return -1;
        }
        
        var matcher = ZOOM_PATTERN.matcher(url);
        if (matcher.find()) {
            String zoomParam = matcher.group(0);
            String zoomValue = zoomParam.substring(zoomParam.indexOf('=') + 1);
            return SafeNumberParser.parseInt(zoomValue, -1);
        }
        return -1;
    }
    
    /**
     * Checks if a Google Books URL contains a front cover hint.
     * <p>
     * Google Books URLs with "printsec=frontcover" typically indicate
     * the image is a front cover rather than interior content.
     * 
     * @param url Google Books URL to check
     * @return true if URL contains front cover hint parameter
     * 
     * @example
     * <pre>{@code
     * String url = "https://books.google.com/books/content?id=abc&printsec=frontcover&img=1";
     * boolean isFrontCover = GoogleBooksUrlEnhancer.hasFrontCoverHint(url);
     * // Returns: true
     * }</pre>
     */
    public static boolean hasFrontCoverHint(String url) {
        if (url == null || url.isBlank()) {
            return false;
        }
        // Check for printsec=frontcover parameter (case-insensitive)
        return url.toLowerCase().contains("printsec=frontcover");
    }
}
