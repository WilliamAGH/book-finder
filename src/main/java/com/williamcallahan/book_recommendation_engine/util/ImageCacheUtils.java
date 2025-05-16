package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.ImageSourceName;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Utility class for image caching operations
 * - Provides helper methods for hashing, filename generation, and type mapping
 *
 * @author William Callahan
 */
public final class ImageCacheUtils {

    private ImageCacheUtils() {
        // Prevent instantiation
    }

    /**
     * Computes SHA-256 hash for given image data
     * @param imageData Byte array of the image
     * @return SHA-256 hash byte array
     * @throws NoSuchAlgorithmException If SHA-256 algorithm is not available
     */
    public static byte[] computeImageHash(byte[] imageData) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return digest.digest(imageData);
    }

    /**
     * Compares two byte arrays for similarity (equality)
     * @param hash1 First byte array
     * @param hash2 Second byte array
     * @return True if arrays are identical, false otherwise
     */
    public static boolean isHashSimilar(byte[] hash1, byte[] hash2) {
        if (hash1 == null || hash2 == null || hash1.length != hash2.length) {
            return false;
        }
        for (int i = 0; i < hash1.length; i++) {
            if (hash1[i] != hash2[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Extracts file extension from a URL string
     * - Defaults to .jpg if no valid extension is found
     * @param url The URL string
     * @return File extension (e.g., .jpg, .png)
     */
    public static String getFileExtensionFromUrl(String url) {
        String extension = ".jpg"; // Default extension
        if (url != null && url.contains(".")) {
            int queryParamIndex = url.indexOf("?");
            String urlWithoutParams = queryParamIndex > 0 ? url.substring(0, queryParamIndex) : url;
            int lastDotIndex = urlWithoutParams.lastIndexOf(".");
            if (lastDotIndex > 0 && lastDotIndex < urlWithoutParams.length() - 1) {
                String ext = urlWithoutParams.substring(lastDotIndex).toLowerCase();
                // Basic validation for common image extensions
                if (ext.matches("\\.(jpg|jpeg|png|gif|webp|svg|bmp|tiff)")) {
                    extension = ext;
                }
            }
        }
        return extension;
    }

    /**
     * Generates a filename from a URL using SHA-256 hash
     * - Truncates hash for brevity and appends original extension
     * @param url The URL string
     * @return Generated filename string
     * @throws NoSuchAlgorithmException If SHA-256 algorithm is not available
     */
    public static String generateFilenameFromUrl(String url) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(url.getBytes());
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        String extension = getFileExtensionFromUrl(url);
        // Use a portion of the hash to keep filename length reasonable
        return encoded.substring(0, Math.min(encoded.length(), 32)) + extension;
    }

    /**
     * Determines the best identifier to use as a cache key for a book
     * - Prioritizes ISBN-13, then ISBN-10, then Google Books ID
     * @param book The book object
     * @return The most specific available identifier, or null
     */
    public static String getIdentifierKey(Book book) {
        if (book == null) {
            return null;
        }
        if (book.getIsbn13() != null && !book.getIsbn13().isEmpty()) {
            return book.getIsbn13();
        }
        if (book.getIsbn10() != null && !book.getIsbn10().isEmpty()) {
            return book.getIsbn10();
        }
        // Assuming 'id' field in Book maps to Google Volume ID if ISBNs are not present
        if (book.getId() != null && !book.getId().isEmpty()) {
            return book.getId();
        }
        return null;
    }

    /**
     * Maps a string representation of a source name to ImageSourceName enum
     * @param sourceNameString The string name of the source
     * @return Corresponding ImageSourceName enum, or UNKNOWN
     */
    public static ImageSourceName mapStringToImageSourceName(String sourceNameString) {
        if (sourceNameString == null) {
            return ImageSourceName.UNKNOWN;
        }
        // Simplified mapping, can be expanded for more precise matching
        String lowerSourceName = sourceNameString.toLowerCase();
        if (lowerSourceName.contains("openlibrary")) return ImageSourceName.OPEN_LIBRARY;
        if (lowerSourceName.contains("google") || lowerSourceName.contains("googlebooks")) return ImageSourceName.GOOGLE_BOOKS;
        if (lowerSourceName.contains("longitood")) return ImageSourceName.LONGITOOD;
        if (lowerSourceName.contains("s3")) return ImageSourceName.S3_CACHE;
        if (lowerSourceName.contains("local")) return ImageSourceName.LOCAL_CACHE;
        if (lowerSourceName.equalsIgnoreCase("ProvisionalHint")) return ImageSourceName.UNKNOWN;

        try {
            // Attempt direct enum conversion for exact matches (case-insensitive for valueOf)
            return ImageSourceName.valueOf(sourceNameString.toUpperCase().replaceAll("[^A-Z0-9_]", ""));
        } catch (IllegalArgumentException e) {
            return ImageSourceName.UNKNOWN;
        }
    }

    /**
     * Maps CoverImageSource enum to ImageSourceName enum
     * @param coverImageSource The CoverImageSource enum value
     * @return Corresponding ImageSourceName enum, or UNKNOWN
     */
    public static ImageSourceName mapCoverImageSourceToImageSourceName(CoverImageSource coverImageSource) {
        if (coverImageSource == null) {
            return ImageSourceName.UNKNOWN;
        }
        try {
            return ImageSourceName.valueOf(coverImageSource.name());
        } catch (IllegalArgumentException e) {
            // Fallback for names that might not directly map or for broader categories
            switch (coverImageSource) {
                case GOOGLE_BOOKS: return ImageSourceName.GOOGLE_BOOKS;
                case OPEN_LIBRARY: return ImageSourceName.OPEN_LIBRARY;
                case LONGITOOD: return ImageSourceName.LONGITOOD;
                case S3_CACHE: return ImageSourceName.S3_CACHE;
                case LOCAL_CACHE: return ImageSourceName.LOCAL_CACHE;
                case ANY: // Fall-through
                case NONE: // Fall-through
                case UNDEFINED: // Fall-through
                default: return ImageSourceName.UNKNOWN;
            }
        }
    }

    /**
     * Maps ImageSourceName enum back to CoverImageSource enum
     * @param sourceNameEnum The ImageSourceName enum value
     * @return Corresponding CoverImageSource enum, or UNDEFINED
     */
    public static CoverImageSource mapImageSourceNameEnumToCoverImageSource(ImageSourceName sourceNameEnum) {
        if (sourceNameEnum == null) {
            return CoverImageSource.UNDEFINED;
        }
        try {
            return CoverImageSource.valueOf(sourceNameEnum.name());
        } catch (IllegalArgumentException e) {
            switch (sourceNameEnum) {
                case GOOGLE_BOOKS: return CoverImageSource.GOOGLE_BOOKS;
                case OPEN_LIBRARY: return CoverImageSource.OPEN_LIBRARY;
                case LONGITOOD: return CoverImageSource.LONGITOOD;
                case S3_CACHE: return CoverImageSource.S3_CACHE;
                case LOCAL_CACHE: return CoverImageSource.LOCAL_CACHE;
                // case INTERNAL_PROCESSING: // No direct CoverImageSource, map to UNDEFINED
                // case UNKNOWN: // Fall-through
                default: return CoverImageSource.UNDEFINED;
            }
        }
    }
}
