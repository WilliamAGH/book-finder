/**
 * Container for book cover image URLs with source tracking
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.types;

/**
 * Immutable container for book cover image URLs
 * - Stores preferred and fallback image URLs for different resolutions
 * - Tracks the source of cover images (Google, OpenLibrary, etc)
 * - Used by image services to maintain cover image provenance
 * - Supports UI display with graceful fallback handling
 */
public class CoverImages {
    /**
     * Primary URL for cover image
     * - Typically higher resolution when available
     * - Used as main display image in UI
     */
    private String preferredUrl;
    
    /**
     * Secondary URL for cover image
     * - Used when preferred URL fails to load
     * - May be lower resolution or placeholder
     */
    private String fallbackUrl;
    
    /**
     * Source system that provided the cover image
     * - Tracks image provenance for attribution
     * - Useful for debugging image quality issues
     */
    private CoverImageSource source;

    /**
     * Default constructor
     * - Initializes with undefined image source
     * - Used when creating object before URLs are available
     */
    public CoverImages() {
        this.source = CoverImageSource.UNDEFINED;
    }

    /**
     * Two-parameter constructor with URLs
     * - Sets preferred and fallback image URLs
     * - Initializes with undefined image source
     * 
     * @param preferredUrl Primary image URL
     * @param fallbackUrl Backup image URL
     */
    public CoverImages(String preferredUrl, String fallbackUrl) {
        this.preferredUrl = preferredUrl;
        this.fallbackUrl = fallbackUrl;
        this.source = CoverImageSource.UNDEFINED; // Default, should be set explicitly by services
    }

    /**
     * Full constructor with URLs and source
     * - Sets all fields including image source
     * - Preferred for services that know the image source
     * 
     * @param preferredUrl Primary image URL
     * @param fallbackUrl Backup image URL
     * @param source Image source provider
     */
    public CoverImages(String preferredUrl, String fallbackUrl, CoverImageSource source) {
        this.preferredUrl = preferredUrl;
        this.fallbackUrl = fallbackUrl;
        this.source = source;
    }

    /**
     * Get primary image URL
     * 
     * @return Preferred image URL
     */
    public String getPreferredUrl() {
        return preferredUrl;
    }

    /**
     * Set primary image URL
     * 
     * @param preferredUrl Preferred image URL
     */
    public void setPreferredUrl(String preferredUrl) {
        this.preferredUrl = preferredUrl;
    }

    /**
     * Get backup image URL
     * 
     * @return Fallback image URL
     */
    public String getFallbackUrl() {
        return fallbackUrl;
    }

    /**
     * Set backup image URL
     * 
     * @param fallbackUrl Fallback image URL
     */
    public void setFallbackUrl(String fallbackUrl) {
        this.fallbackUrl = fallbackUrl;
    }

    /**
     * Get image source provider
     * 
     * @return Cover image source
     */
    public CoverImageSource getSource() {
        return source;
    }

    /**
     * Set image source provider
     * 
     * @param source Cover image source
     */
    public void setSource(CoverImageSource source) {
        this.source = source;
    }
}
