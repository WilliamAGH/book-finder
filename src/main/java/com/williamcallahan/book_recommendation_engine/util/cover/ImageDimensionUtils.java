package com.williamcallahan.book_recommendation_engine.util.cover;

import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;

/**
 * Single Source of Truth for image dimension validation, estimation, and normalization.
 * 
 * Consolidates logic from:
 * - CoverImageService.estimateDimensions()
 * - ImageCacheUtils.normalizeImageDimension()
 * - ImageCacheUtils.isValidImageDetails() dimension checks
 * - CoverSourceFetchingService dimension thresholds
 * 
 * @author William Callahan
 */
public final class ImageDimensionUtils {
    
    // Dimension validation thresholds
    public static final int MIN_VALID_DIMENSION = 2; // Absolute minimum for valid image
    public static final int MIN_ACCEPTABLE_NON_GOOGLE = 200; // Minimum for acceptable quality (non-Google sources)
    public static final int MIN_ACCEPTABLE_CACHED = 150; // Minimum for cached images to be considered
    
    // High-resolution threshold (total pixels)
    public static final int HIGH_RES_PIXEL_THRESHOLD = 480_000; // ~800x600 or 600x800
    
    // Default/fallback dimension for unknown images
    public static final int DEFAULT_DIMENSION = 512;
    
    private ImageDimensionUtils() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Returns priority ranking for Google Books image types.
     * Lower numbers indicate better quality.
     * 
     * @param imageType The image type string (case-insensitive)
     * @return Priority ranking (1 = best quality, 7 = worst/unknown)
     * 
     * @example
     * <pre>{@code
     * int priority = ImageDimensionUtils.getTypePriority("extraLarge");
     * // Returns: 1 (highest quality)
     * 
     * int priority = ImageDimensionUtils.getTypePriority("thumbnail");
     * // Returns: 5 (lower quality)
     * }</pre>
     */
    public static int getTypePriority(String imageType) {
        if (imageType == null) {
            return 7; // Lowest priority for null
        }
        
        return switch (imageType.toLowerCase()) {
            case "extralarge" -> 1;
            case "large" -> 2;
            case "medium" -> 3;
            case "small" -> 4;
            case "thumbnail" -> 5;
            case "smallthumbnail" -> 6;
            default -> 7;
        };
    }
    
    /**
     * Estimates dimensions based on Google Books image type.
     * 
     * @param imageType The image type from Google Books (extraLarge, large, medium, etc.)
     * @return Estimated dimensions record
     */
    public static DimensionEstimate estimateFromGoogleType(String imageType) {
        if (imageType == null) {
            return new DimensionEstimate(DEFAULT_DIMENSION, DEFAULT_DIMENSION * 3 / 2, false);
        }
        
        return switch (imageType.toLowerCase()) {
            case "extralarge" -> new DimensionEstimate(800, 1200, true);
            case "large" -> new DimensionEstimate(600, 900, true);
            case "medium" -> new DimensionEstimate(400, 600, false);
            case "small" -> new DimensionEstimate(300, 450, false);
            case "thumbnail" -> new DimensionEstimate(128, 192, false);
            case "smallthumbnail" -> new DimensionEstimate(64, 96, false);
            default -> new DimensionEstimate(DEFAULT_DIMENSION, DEFAULT_DIMENSION * 3 / 2, false);
        };
    }
    
    /**
     * Normalizes a dimension value, replacing invalid/missing values with default.
     * 
     * @param dimension The dimension value to normalize (may be null or <= 1)
     * @return Normalized dimension (returns DEFAULT_DIMENSION if input is invalid)
     */
    public static int normalize(Integer dimension) {
        if (dimension == null || dimension <= MIN_VALID_DIMENSION) {
            return DEFAULT_DIMENSION;
        }
        return dimension;
    }
    
    /**
     * Checks if dimensions represent a high-resolution image.
     * 
     * @param width Image width in pixels
     * @param height Image height in pixels
     * @return true if total pixels meet high-resolution threshold
     */
    public static boolean isHighResolution(Integer width, Integer height) {
        if (width == null || height == null) {
            return false;
        }
        
        long totalPixels = (long) width * height;
        return totalPixels >= HIGH_RES_PIXEL_THRESHOLD;
    }
    
    /**
     * Validates that dimensions meet minimum acceptable quality thresholds.
     * 
     * @param width Image width
     * @param height Image height
     * @param minThreshold Minimum acceptable dimension (width OR height must meet this)
     * @return true if dimensions meet threshold
     */
    public static boolean meetsThreshold(Integer width, Integer height, int minThreshold) {
        if (width == null || height == null) {
            return false;
        }
        
        return width >= minThreshold && height >= minThreshold;
    }
    
    /**
     * Checks if dimensions are valid (not null and greater than minimum).
     * 
     * @param width Image width
     * @param height Image height
     * @return true if both dimensions are valid
     */
    public static boolean areValid(Integer width, Integer height) {
        return width != null && width > MIN_VALID_DIMENSION
            && height != null && height > MIN_VALID_DIMENSION;
    }
    
    /**
     * Validates ImageDetails has acceptable dimensions for use.
     * 
     * @param imageDetails The image details to validate
     * @return true if imageDetails has valid, acceptable dimensions
     */
    public static boolean hasAcceptableDimensions(ImageDetails imageDetails) {
        if (imageDetails == null) {
            return false;
        }
        
        return areValid(imageDetails.getWidth(), imageDetails.getHeight());
    }
    
    /**
     * Record for dimension estimates with high-resolution flag.
     * 
     * @param width Estimated width in pixels
     * @param height Estimated height in pixels
     * @param highRes Whether dimensions qualify as high-resolution
     */
    public record DimensionEstimate(int width, int height, boolean highRes) {}
}
