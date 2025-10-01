package com.williamcallahan.book_recommendation_engine.util.cover;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;

/**
 * Factory for creating CoverImages objects with consistent defaults.
 * 
 * <p>Eliminates duplicate placeholder and construction logic scattered across:
 * <ul>
 *   <li>BookImageOrchestrationService (4 locations)</li>
 *   <li>BookCoverManagementService.createPlaceholderCoverImages()</li>
 *   <li>Other services with inline CoverImages construction</li>
 * </ul>
 * 
 * <p>This factory ensures all CoverImages objects are created with consistent
 * defaults and proper initialization, following the Single Responsibility Principle.
 * 
 * @author William Callahan
 * @since 0.9.0
 */
public final class CoverImagesFactory {
    
    private CoverImagesFactory() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Creates a placeholder CoverImages with both preferred and fallback URLs set to the placeholder path.
     * Used when no actual cover image is available.
     * 
     * @param placeholderPath Path to the placeholder image (typically a local asset)
     * @return CoverImages with placeholder URLs and NONE source
     * 
     * @example
     * <pre>{@code
     * String placeholder = "/images/book-cover-placeholder.svg";
     * CoverImages coverImages = CoverImagesFactory.createPlaceholder(placeholder);
     * // coverImages.getPreferredUrl() == placeholder
     * // coverImages.getFallbackUrl() == placeholder
     * // coverImages.getSource() == CoverImageSource.NONE
     * }</pre>
     */
    public static CoverImages createPlaceholder(String placeholderPath) {
        if (placeholderPath == null || placeholderPath.isEmpty()) {
            throw new IllegalArgumentException("Placeholder path cannot be null or empty");
        }
        
        CoverImages coverImages = new CoverImages();
        coverImages.setPreferredUrl(placeholderPath);
        coverImages.setFallbackUrl(placeholderPath);
        coverImages.setSource(CoverImageSource.NONE);
        return coverImages;
    }
    
    /**
     * Creates a CoverImages from ImageDetails with a fallback URL.
     * Extracts the URL and source from ImageDetails and sets appropriate defaults.
     * 
     * @param imageDetails ImageDetails containing the preferred URL and source
     * @param fallbackUrl Fallback URL to use if the preferred URL fails
     * @return CoverImages with URLs and source from ImageDetails
     * @throws IllegalArgumentException if imageDetails is null
     * 
     * @example
     * <pre>{@code
     * ImageDetails details = new ImageDetails("https://cdn.example.com/cover.jpg", 
     *                                          "google-books", "key123", 
     *                                          CoverImageSource.GOOGLE_BOOKS);
     * String fallback = "https://example.com/default.jpg";
     * CoverImages coverImages = CoverImagesFactory.createFromImageDetails(details, fallback);
     * // coverImages.getPreferredUrl() == details.getUrlOrPath()
     * // coverImages.getFallbackUrl() == fallback
     * // coverImages.getSource() == CoverImageSource.GOOGLE_BOOKS
     * }</pre>
     */
    public static CoverImages createFromImageDetails(ImageDetails imageDetails, String fallbackUrl) {
        if (imageDetails == null) {
            throw new IllegalArgumentException("ImageDetails cannot be null");
        }
        
        String preferredUrl = imageDetails.getUrlOrPath();
        CoverImageSource source = imageDetails.getCoverImageSource() != null 
            ? imageDetails.getCoverImageSource() 
            : CoverImageSource.UNDEFINED;
        
        return createDefault(preferredUrl, fallbackUrl, source);
    }
    
    /**
     * Creates a CoverImages with explicit preferred URL, fallback URL, and source.
     * Provides maximum control over CoverImages construction.
     * 
     * @param preferredUrl Primary URL to use for the cover image
     * @param fallbackUrl Fallback URL if preferred URL fails
     * @param source Source of the cover image
     * @return CoverImages with specified values
     * 
     * @example
     * <pre>{@code
     * CoverImages coverImages = CoverImagesFactory.createDefault(
     *     "https://s3.amazonaws.com/covers/book123.jpg",
     *     "https://example.com/default.jpg",
     *     CoverImageSource.UNDEFINED
     * );
     * }</pre>
     */
    public static CoverImages createDefault(String preferredUrl, String fallbackUrl, CoverImageSource source) {
        CoverImages coverImages = new CoverImages();
        coverImages.setPreferredUrl(preferredUrl);
        coverImages.setFallbackUrl(fallbackUrl);
        coverImages.setSource(source != null ? source : CoverImageSource.UNDEFINED);
        return coverImages;
    }
    
    /**
     * Creates a CoverImages using the two-argument constructor.
     * Convenience method that wraps the CoverImages(String, String) constructor.
     * 
     * @param preferredUrl Primary URL to use for the cover image
     * @param fallbackUrl Fallback URL if preferred URL fails
     * @return CoverImages with specified URLs and UNDEFINED source
     * 
     * @example
     * <pre>{@code
     * CoverImages coverImages = CoverImagesFactory.create(
     *     "https://covers.example.com/book.jpg",
     *     "https://example.com/default.jpg"
     * );
     * // Equivalent to: new CoverImages(preferredUrl, fallbackUrl)
     * }</pre>
     */
    public static CoverImages create(String preferredUrl, String fallbackUrl) {
        return new CoverImages(preferredUrl, fallbackUrl);
    }
    
    /**
     * Creates a CoverImages using the three-argument constructor.
     * Convenience method that wraps the CoverImages(String, String, CoverImageSource) constructor.
     * 
     * @param preferredUrl Primary URL to use for the cover image
     * @param fallbackUrl Fallback URL if preferred URL fails
     * @param source Source of the cover image
     * @return CoverImages with specified values
     * 
     * @example
     * <pre>{@code
     * CoverImages coverImages = CoverImagesFactory.create(
     *     "https://books.google.com/cover.jpg",
     *     "https://example.com/default.jpg",
     *     CoverImageSource.GOOGLE_BOOKS
     * );
     * // Equivalent to: new CoverImages(preferredUrl, fallbackUrl, source)
     * }</pre>
     */
    public static CoverImages create(String preferredUrl, String fallbackUrl, CoverImageSource source) {
        return new CoverImages(preferredUrl, fallbackUrl, source);
    }
}
