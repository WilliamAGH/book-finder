package com.williamcallahan.book_recommendation_engine.model.image;

/**
 * Enum representing the various sources of book cover images
 *
 * @author William Callahan
 *
 * Features:
 * - Defines all possible image sources in the system
 * - Provides human-readable display names for each source
 * - Supports external APIs like Google Books and OpenLibrary
 * - Includes internal sources like cache and processing
 * - Used for tracking image provenance and attribution
 */
public enum ImageSourceName {
    GOOGLE_BOOKS("Google Books API"),
    OPEN_LIBRARY("OpenLibrary"),
    LONGITOOD("Longitood"),
    LOCAL_CACHE("LocalCache"),
    S3_CACHE("S3 Cache"),
    INTERNAL_PROCESSING("Internal Processing"),
    UNKNOWN("Unknown");

    private final String displayName;

    ImageSourceName(String displayName) {
        this.displayName = displayName;
    }

    /**
     * Get the human-readable display name for this image source
     *
     * @return The display name of the image source
     */
    public String getDisplayName() {
        return displayName;
    }
} 
