package com.williamcallahan.book_recommendation_engine.util.cover;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;

import java.util.Locale;
import java.util.Optional;

/**
 * Single Source of Truth for detecting cover image source from URL patterns.
 * 
 * Consolidates logic from:
 * - BookCoverManagementService.inferSourceFromUrl()
 * - UrlPatternMatcher.identifySource()
 * - PostgresBookRepository.inferSourceFromUrl() (to be deleted)
 * - Inline URL detection scattered across services
 * 
 * @author William Callahan
 */
public final class UrlSourceDetector {
    
    private UrlSourceDetector() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Detects the cover image source from a URL.
     * 
     * @param url The URL to analyze
     * @return Detected CoverImageSource, or UNDEFINED if unknown
     */
    public static CoverImageSource detectSource(String url) {
        if (url == null || url.isBlank()) {
            return CoverImageSource.UNDEFINED;
        }
        
        String lower = url.toLowerCase(Locale.ROOT);
        
        // Google Books patterns
        if (lower.contains("googleapis.com/books") || lower.contains("books.google.com")) {
            return CoverImageSource.GOOGLE_BOOKS;
        }
        
        // Open Library patterns
        if (lower.contains("openlibrary.org") || lower.contains("covers.openlibrary.org")) {
            return CoverImageSource.OPEN_LIBRARY;
        }
        
        // Longitood patterns
        if (lower.contains("longitood.com")) {
            return CoverImageSource.LONGITOOD;
        }
        
        // S3/CDN patterns - Can't infer original data source from storage URL
        // Return UNDEFINED and track storage location separately
        if (lower.contains("s3.amazonaws.com") || 
            lower.contains(".digitaloceanspaces.com") ||
            lower.contains("cloudfront.net")) {
            return CoverImageSource.UNDEFINED;  // Storage ≠ Source
        }
        
        // Local paths - Can't infer original data source from local cache path
        // Return UNDEFINED and track storage location separately
        if (url.startsWith("/") && !url.startsWith("//")) {
            return CoverImageSource.UNDEFINED;  // Storage ≠ Source
        }
        
        return CoverImageSource.UNDEFINED;
    }
    
    /**
     * Detects source and returns as String (for legacy compatibility).
     * 
     * @param url The URL to analyze
     * @return Source name as String (e.g., "GOOGLE_BOOKS")
     */
    public static String detectSourceString(String url) {
        CoverImageSource source = detectSource(url);
        return source.name();
    }
    
    /**
     * Checks if URL is an external HTTP/HTTPS URL.
     * 
     * @param url The URL to check
     * @return true if URL is external (http/https), false for local paths
     */
    public static boolean isExternalUrl(String url) {
        if (url == null || url.isBlank()) {
            return false;
        }
        return (url.startsWith("http://") || url.startsWith("https://")) && !url.startsWith("/");
    }
    
    /**
     * Checks if URL is a Google Books cover URL.
     * 
     * @param url The URL to check
     * @return true if URL is from Google Books
     */
    public static boolean isGoogleBooksUrl(String url) {
        return detectSource(url) == CoverImageSource.GOOGLE_BOOKS;
    }
    
    /**
     * Checks if URL is an Open Library cover URL.
     * 
     * @param url The URL to check
     * @return true if URL is from Open Library
     */
    public static boolean isOpenLibraryUrl(String url) {
        return detectSource(url) == CoverImageSource.OPEN_LIBRARY;
    }

    /**
     * Detects storage location information from a URL or path.
     *
     * @param url URL or path to analyze
     * @return Optional storage location (e.g., {@link ImageDetails#STORAGE_S3}) when detectable
     */
    public static Optional<String> detectStorageLocation(String url) {
        if (url == null || url.isBlank()) {
            return Optional.empty();
        }

        String lower = url.toLowerCase(Locale.ROOT);

        if (lower.contains("s3.amazonaws.com") ||
            lower.contains(".digitaloceanspaces.com") ||
            lower.contains("cloudfront.net")) {
            return Optional.of(ImageDetails.STORAGE_S3);
        }

        if (url.startsWith("/") && !url.startsWith("//")) {
            return Optional.of(ImageDetails.STORAGE_LOCAL);
        }

        return Optional.empty();
    }
}
