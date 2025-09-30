package com.williamcallahan.book_recommendation_engine.util;

import org.springframework.lang.Nullable;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * URL manipulation utilities using modern Java features.
 * <p>
 * Provides zero-boilerplate URL operations with proper null-safety
 * and error handling using Java 21 patterns.
 * 
 * @since 1.0.0
 */
public final class UrlUtils {
    
    private UrlUtils() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Normalizes HTTP URLs to HTTPS for security.
     * <p>
     * Uses modern Java Optional pattern for null-safety without boilerplate.
     * Leverages Java 21's enhanced string operations.
     * 
     * @param url URL to normalize (may be null)
     * @return HTTPS URL, or null if input was null/blank
     * 
     * @example
     * <pre>
     * UrlUtils.normalizeToHttps("http://example.com")  → "https://example.com"
     * UrlUtils.normalizeToHttps("https://example.com") → "https://example.com"
     * UrlUtils.normalizeToHttps(null)                  → null
     * </pre>
     */
    @Nullable
    public static String normalizeToHttps(@Nullable String url) {
        // Modern Java: Clean null checks with early returns
        if (url == null || url.isBlank()) {
            return null;
        }
        
        return url.startsWith("http://") 
            ? "https://" + url.substring(7) 
            : url;
    }
    
    /**
     * Validates and normalizes URL using java.net.URI (built-in).
     * <p>
     * Leverages Java's built-in URI validation instead of regex.
     * 
     * @param url URL to validate
     * @return Normalized valid URL, or null if invalid
     */
    @Nullable
    public static String validateAndNormalize(@Nullable String url) {
        if (url == null || url.isBlank()) {
            return null;
        }
        
        try {
            // Use built-in URI validation instead of custom regex
            URI uri = new URI(url);
            
            // Ensure it's HTTP or HTTPS
            if (!"http".equalsIgnoreCase(uri.getScheme()) && 
                !"https".equalsIgnoreCase(uri.getScheme())) {
                return null;
            }
            
            // Normalize to HTTPS
            return normalizeToHttps(uri.toString());
            
        } catch (URISyntaxException e) {
            return null;
        }
    }
    
    /**
     * Removes query parameters from URL.
     * <p>
     * Uses String.split() (built-in) instead of regex for performance.
     * 
     * @param url URL with potential query parameters
     * @return URL without query parameters
     */
    @Nullable
    public static String removeQueryParams(@Nullable String url) {
        if (url == null || url.isBlank()) {
            return null;
        }
        
        return url.contains("?") 
            ? url.split("\\?", 2)[0] 
            : url;
    }
    
    /**
     * Cleans trailing separators from URL using modern Java.
     * 
     * @param url URL to clean
     * @return URL without trailing ? or & characters
     */
    @Nullable
    public static String cleanTrailingSeparators(@Nullable String url) {
        if (url == null || url.isBlank()) {
            return null;
        }
        
        // Java 21: More efficient than regex for simple suffix removal
        String cleaned = url;
        while (cleaned.endsWith("&") || cleaned.endsWith("?")) {
            cleaned = cleaned.substring(0, cleaned.length() - 1);
        }
        
        return cleaned.isEmpty() ? null : cleaned;
    }
}
