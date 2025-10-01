package com.williamcallahan.book_recommendation_engine.util.cover;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageSourceName;

import java.util.Locale;

/**
 * Single Source of Truth for bidirectional cover source type conversions.
 * 
 * Consolidates logic from:
 * - ImageCacheUtils.mapStringToImageSourceName()
 * - ImageCacheUtils.mapCoverImageSourceToImageSourceName()
 * - S3BookCoverService.getSourceString()
 * 
 * Provides canonical String representations for all source types.
 * 
 * @author William Callahan
 */
public final class CoverSourceMapper {
    private static final String LEGACY_S3_CACHE_NAME = "S3_CACHE";
    private static final String LEGACY_LOCAL_CACHE_NAME = "LOCAL_CACHE";
    private static final String LEGACY_S3_ALIAS = "S3";
    private static final String LEGACY_S3_SEGMENT = "s3-cache";
    private static final String LEGACY_LOCAL_SEGMENT = "local-cache";
    
    private CoverSourceMapper() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Converts CoverImageSource enum to canonical snake_case String for S3 keys.
     * 
     * @param source The CoverImageSource enum
     * @return Canonical snake_case String representation
     */
    public static String toS3KeySegment(CoverImageSource source) {
        if (source == null) {
            return "unknown";
        }
        
        return switch (source) {
            case GOOGLE_BOOKS -> "google-books";
            case OPEN_LIBRARY -> "open-library";
            case LONGITOOD -> "longitood";
            case NONE -> "none";
            case MOCK -> "mock";
            case ANY, UNDEFINED -> "unknown";
            default -> legacySegmentFor(source);
        };
    }
    
    /**
     * Converts CoverImageSource enum to ImageSourceName enum.
     * 
     * @param source The CoverImageSource enum
     * @return Corresponding ImageSourceName enum
     */
    public static ImageSourceName toImageSourceName(CoverImageSource source) {
        if (source == null) {
            return ImageSourceName.UNKNOWN;
        }
        
        return switch (source) {
            case GOOGLE_BOOKS -> ImageSourceName.GOOGLE_BOOKS;
            case OPEN_LIBRARY -> ImageSourceName.OPEN_LIBRARY;
            case LONGITOOD -> ImageSourceName.LONGITOOD;
            case NONE, MOCK, ANY, UNDEFINED -> ImageSourceName.UNKNOWN;
            default -> legacyImageSourceName(source.name());
        };
    }
    
    /**
     * Converts String representation to ImageSourceName enum.
     * Handles multiple naming conventions (snake_case, kebab-case, etc.)
     * 
     * @param sourceString The string representation
     * @return Corresponding ImageSourceName enum, or UNKNOWN if no match
     */
    public static ImageSourceName fromString(String sourceString) {
        if (sourceString == null || sourceString.isBlank()) {
            return ImageSourceName.UNKNOWN;
        }
        
        String normalized = sourceString.toLowerCase(Locale.ROOT);
        
        // Pattern matching for common variations
        if (normalized.contains("openlibrary") || normalized.contains("open-library") || normalized.contains("open_library")) {
            return ImageSourceName.OPEN_LIBRARY;
        }
        if (normalized.contains("google") || normalized.contains("googlebooks") || normalized.contains("google-books") || normalized.contains("google_books")) {
            return ImageSourceName.GOOGLE_BOOKS;
        }
        if (normalized.contains("longitood")) {
            return ImageSourceName.LONGITOOD;
        }
        if (normalized.contains("s3") || normalized.equals("s3-cache") || normalized.equals("s3_cache")) {
            return ImageSourceName.INTERNAL_PROCESSING;
        }
        if (normalized.contains("local") || normalized.equals("local-cache") || normalized.equals("local_cache")) {
            return ImageSourceName.INTERNAL_PROCESSING;
        }
        if (normalized.equals("provisionalhint")) {
            return ImageSourceName.UNKNOWN;
        }
        
        // Try direct enum conversion as last resort
        try {
            String enumName = sourceString.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9_]", "_");
            if (LEGACY_S3_CACHE_NAME.equals(enumName) || LEGACY_S3_ALIAS.equals(enumName) || LEGACY_LOCAL_CACHE_NAME.equals(enumName)) {
                return ImageSourceName.INTERNAL_PROCESSING;
            }
            return ImageSourceName.valueOf(enumName);
        } catch (IllegalArgumentException e) {
            return ImageSourceName.UNKNOWN;
        }
    }
    
    /**
     * Converts String representation to CoverImageSource enum.
     * 
     * @param sourceString The string representation
     * @return Corresponding CoverImageSource enum, or UNDEFINED if no match
     */
    public static CoverImageSource toCoverImageSource(String sourceString) {
        if (sourceString == null || sourceString.isBlank()) {
            return CoverImageSource.UNDEFINED;
        }
        
        String normalized = sourceString.toLowerCase(Locale.ROOT);
        
        if (normalized.contains("openlibrary") || normalized.contains("open-library") || normalized.contains("open_library")) {
            return CoverImageSource.OPEN_LIBRARY;
        }
        if (normalized.contains("google") || normalized.contains("googlebooks") || normalized.contains("google-books") || normalized.contains("google_books")) {
            return CoverImageSource.GOOGLE_BOOKS;
        }
        if (normalized.contains("longitood")) {
            return CoverImageSource.LONGITOOD;
        }
        if (normalized.contains("s3") || normalized.equals("s3-cache") || normalized.equals("s3_cache")) {
            return CoverImageSource.UNDEFINED;
        }
        if (normalized.contains("local") || normalized.equals("local-cache") || normalized.equals("local_cache")) {
            return CoverImageSource.UNDEFINED;
        }
        
        // Try direct enum conversion
        try {
            String enumName = sourceString.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9_]", "_");
            return CoverImageSource.valueOf(enumName);
        } catch (IllegalArgumentException e) {
            return CoverImageSource.UNDEFINED;
        }
    }
    
    /**
     * Converts ImageSourceName to CoverImageSource.
     * 
     * @param sourceName The ImageSourceName enum
     * @return Corresponding CoverImageSource enum
     */
    public static CoverImageSource toCoverImageSource(ImageSourceName sourceName) {
        if (sourceName == null) {
            return CoverImageSource.UNDEFINED;
        }
        
        return switch (sourceName) {
            case GOOGLE_BOOKS -> CoverImageSource.GOOGLE_BOOKS;
            case OPEN_LIBRARY -> CoverImageSource.OPEN_LIBRARY;
            case LONGITOOD -> CoverImageSource.LONGITOOD;
            case INTERNAL_PROCESSING, UNKNOWN -> CoverImageSource.UNDEFINED;
            default -> CoverImageSource.UNDEFINED;
        };
    }
    
    /**
     * Sanitizes a CoverImageSource by replacing deprecated/invalid values with UNDEFINED.
     * <p>
     * Deprecated sources (S3_CACHE, LOCAL_CACHE, S3, UNKNOWN) represent storage locations
     * rather than data sources, so they are normalized to UNDEFINED.
     * 
     * @param source The CoverImageSource to sanitize
     * @return Sanitized CoverImageSource, or UNDEFINED if input is null or deprecated
     */
    public static CoverImageSource sanitize(CoverImageSource source) {
        if (source == null) {
            return CoverImageSource.UNDEFINED;
        }
        
        String name = source.name();
        
        // Replace deprecated storage-based sources with UNDEFINED
        if (LEGACY_S3_CACHE_NAME.equals(name) || 
            LEGACY_LOCAL_CACHE_NAME.equals(name) ||
            LEGACY_S3_ALIAS.equals(name) || 
            "UNKNOWN".equals(name)) {
            return CoverImageSource.UNDEFINED;
        }
        
        return source;
    }

    private static ImageSourceName legacyImageSourceName(String legacyName) {
        if (legacyName == null) {
            return ImageSourceName.UNKNOWN;
        }
        if (LEGACY_S3_CACHE_NAME.equals(legacyName) || LEGACY_S3_ALIAS.equals(legacyName)
            || LEGACY_LOCAL_CACHE_NAME.equals(legacyName)) {
            return ImageSourceName.INTERNAL_PROCESSING;
        }
        return ImageSourceName.UNKNOWN;
    }

    private static String legacySegmentFor(CoverImageSource source) {
        String name = source.name();
        if (LEGACY_LOCAL_CACHE_NAME.equals(name)) {
            return LEGACY_LOCAL_SEGMENT;
        }
        if (LEGACY_S3_CACHE_NAME.equals(name) || LEGACY_S3_ALIAS.equals(name)) {
            return LEGACY_S3_SEGMENT;
        }
        return "unknown";
    }
}
