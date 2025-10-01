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
     * Checks if the given URL is an HTTP or HTTPS URL.
     * <p>
     * Performs case-insensitive check for http:// or https:// prefixes.
     * 
     * @param url URL to check (may be null)
     * @return true if URL starts with http:// or https://, false otherwise
     * 
     * @example
     * <pre>
     * UrlUtils.isHttpUrl("https://example.com")  → true
     * UrlUtils.isHttpUrl("http://example.com")   → true
     * UrlUtils.isHttpUrl("/local/path")          → false
     * UrlUtils.isHttpUrl(null)                    → false
     * </pre>
     */
    public static boolean isHttpUrl(@Nullable String url) {
        if (url == null || url.isBlank()) {
            return false;
        }
        String lower = url.toLowerCase();
        return lower.startsWith("http://") || lower.startsWith("https://");
    }
    
    /**
     * Extracts file extension from a URL string.
     * <p>
     * Handles URLs with query parameters by truncating them before extraction.
     * Validates that the extension is a known image format.
     * 
     * @param url URL to extract extension from
     * @return File extension with leading dot (e.g., ".jpg"), defaults to ".jpg" if none found
     * 
     * @example
     * <pre>
     * UrlUtils.extractFileExtension("https://example.com/image.png?size=large")  → ".png"
     * UrlUtils.extractFileExtension("https://example.com/photo.JPEG")            → ".jpeg"
     * UrlUtils.extractFileExtension("https://example.com/doc")                   → ".jpg" (default)
     * UrlUtils.extractFileExtension(null)                                        → ".jpg" (default)
     * </pre>
     */
    @Nullable
    public static String extractFileExtension(@Nullable String url) {
        String defaultExt = ".jpg";
        
        if (url == null || !url.contains(".")) {
            return defaultExt;
        }
        
        // Remove query parameters
        int queryIndex = url.indexOf("?");
        String urlWithoutParams = queryIndex > 0 ? url.substring(0, queryIndex) : url;
        
        // Extract extension
        int lastDotIndex = urlWithoutParams.lastIndexOf(".");
        if (lastDotIndex > 0 && lastDotIndex < urlWithoutParams.length() - 1) {
            String ext = urlWithoutParams.substring(lastDotIndex).toLowerCase(java.util.Locale.ROOT);
            
            // Validate known image extensions
            if (ext.matches("\\.(jpg|jpeg|png|gif|webp|svg|bmp|tiff)")) {
                return ext;
            }
        }
        
        return defaultExt;
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
