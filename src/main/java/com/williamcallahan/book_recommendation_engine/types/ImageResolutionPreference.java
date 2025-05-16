/**
 * Image resolution preference options for cover images
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.types;

/**
 * Enum representing available image resolution preferences
 * - Controls filtering and prioritization of cover images by resolution
 * - Used by UI to allow users to select preferred image quality
 * - Supports various resolution options from small to original size
 * - Includes special modes for mixing resolutions (ANY, HIGH_ONLY, HIGH_FIRST)
 */
public enum ImageResolutionPreference {
    /**
     * Accept any image resolution
     * - No filtering or sorting of images by resolution
     * - Typically uses whatever is most readily available
     */
    ANY("Any Resolution"),
    
    /**
     * Display only high resolution images
     * - Filters out low-resolution images completely
     * - May result in empty results if no high-res images exist
     */
    HIGH_ONLY("High Resolution Only"),
    
    /**
     * Prioritize high resolution images
     * - Shows high-res images first when available
     * - Falls back to lower resolution if needed
     * - Sorts results with high-res images first
     */
    HIGH_FIRST("High Resolution First"),
    
    /**
     * Large size images only
     * - Targets images with dimensions larger than standard thumbnails
     * - Good balance between quality and bandwidth usage
     */
    LARGE("Large Resolution"),
    
    /**
     * Medium size images only
     * - Standard web-friendly image size
     * - Balances quality and performance
     */
    MEDIUM("Medium Resolution"),
    
    /**
     * Small size images only
     * - Low bandwidth usage for mobile or slow connections
     * - Optimized for thumbnail displays
     */
    SMALL("Small Resolution"),
    
    /**
     * Original unmodified images
     * - Highest possible quality with largest file size
     * - May cause performance issues on slower connections
     */
    ORIGINAL("Original Resolution"),

    /**
     * Resolution is not known or not applicable
     * - Used for placeholders or when dimension data is unavailable
     */
    UNKNOWN("Unknown Resolution");

    /**
     * Human-readable name for UI display
     * - Provides user-friendly label for each option
     * - Used in dropdowns and preference displays
     */
    private final String displayName;

    /**
     * Enum constructor with display name parameter
     * 
     * @param displayName Human-readable name for UI display
     */
    ImageResolutionPreference(String displayName) {
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
