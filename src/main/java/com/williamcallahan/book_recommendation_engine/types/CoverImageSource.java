/**
 * Book cover image source providers and cache locations
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.types;

/**
 * Enum representing available external cover image sources
 * - Identifies origin of book cover images
 * - Includes external APIs and internal cache locations 
 * - Used for tracking image provenance and attribution
 * - Supports source preference selection in UI
 */
public enum CoverImageSource {
    /**
     * No specific source preference
     * - Accept cover images from any available source
     * - System will use best available image
     */
    ANY("Any source"),
    
    /**
     * Google Books API cover images
     * - Official Google Books API image repository
     * - Various sizes from thumbnail to large
     */
    GOOGLE_BOOKS("Google Books"),
    
    /**
     * Open Library cover images
     * - Open repository of book cover images
     * - Generally good quality but less comprehensive
     */
    OPEN_LIBRARY("Open Library"),
    
    /**
     * Longitood service images
     * - Third-party book cover service
     * - High-quality professional cover images
     */
    LONGITOOD("Longitood"),
    
    /**
     * Amazon S3 cached images
     * - Pre-processed and stored in S3 bucket
     * - Optimized and resized versions
     */
    S3_CACHE("S3 Cache"),
    
    /**
     * Local filesystem cached images
     * - Stored on application server
     * - Fastest access for repeated requests
     */
    LOCAL_CACHE("Local Cache"),
    
    /**
     * No cover image available
     * - Indicates book has no cover image
     * - Will use placeholder in UI
     */
    NONE("No Source"),
    
    /**
     * Mock cover source for testing
     * - Used in unit and integration tests
     * - Not used in production code
     */
    MOCK("Mock Source"),
    
    /**
     * Source not yet determined
     * - Default state before source is set
     * - Typically used during initialization
     */
    UNDEFINED("Undefined Source");

    /**
     * Human-readable name for UI display
     * - Used in source selection controls
     * - More descriptive than enum constant
     */
    private final String displayName;

    /**
     * Enum constructor with display name
     * 
     * @param displayName User-friendly source name
     */
    CoverImageSource(String displayName) {
        this.displayName = displayName;
    }

    /**
     * Get human-readable display name
     * 
     * @return User-friendly display name
     */
    public String getDisplayName() {
        return displayName;
    }
}
