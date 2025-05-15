package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;

import java.util.Objects;

/**
 * Represents details about an image, particularly book cover images
 * Contains information about the image's URL or path, source, dimensions, and other metadata
 */
public class ImageDetails {
    private final String urlOrPath;
    private final String sourceName;          // Source name (e.g., "Google Books", "OpenLibrary")
    private final String sourceSystemId;      // Source-specific identifier
    private final CoverImageSource coverImageSource; // Enum representing the cover source
    private final ImageResolutionPreference resolutionPreference; // Preferred resolution
    private final int width;
    private final int height;
    private final boolean dimensionsKnown;

    /**
     * Creates a new ImageDetails instance with unknown dimensions
     *
     * @param urlOrPath The URL or file path to the image
     * @param sourceName The name of the source (e.g., "Google Books", "OpenLibrary")
     * @param sourceSystemId A source-specific identifier for the image
     * @param coverImageSource The enum representing the cover source
     * @param resolutionPreference The preferred resolution for the image
     */
    public ImageDetails(String urlOrPath, String sourceName, String sourceSystemId, 
                        CoverImageSource coverImageSource, ImageResolutionPreference resolutionPreference) {
        this.urlOrPath = urlOrPath;
        this.sourceName = sourceName;
        this.sourceSystemId = sourceSystemId;
        this.coverImageSource = coverImageSource;
        this.resolutionPreference = resolutionPreference;
        this.width = 0;
        this.height = 0;
        this.dimensionsKnown = false;
    }

    /**
     * Creates a new ImageDetails instance with known dimensions
     *
     * @param urlOrPath The URL or file path to the image
     * @param sourceName The name of the source (e.g., "Google Books", "OpenLibrary")
     * @param sourceSystemId A source-specific identifier for the image
     * @param coverImageSource The enum representing the cover source
     * @param resolutionPreference The preferred resolution for the image
     * @param width The width of the image in pixels
     * @param height The height of the image in pixels
     */
    public ImageDetails(String urlOrPath, String sourceName, String sourceSystemId, 
                        CoverImageSource coverImageSource, ImageResolutionPreference resolutionPreference,
                        int width, int height) {
        this.urlOrPath = urlOrPath;
        this.sourceName = sourceName;
        this.sourceSystemId = sourceSystemId;
        this.coverImageSource = coverImageSource;
        this.resolutionPreference = resolutionPreference;
        this.width = width;
        this.height = height;
        this.dimensionsKnown = true;
    }

    /**
     * Gets the URL or file path to the image
     *
     * @return The URL or file path to the image
     */
    public String getUrlOrPath() {
        return urlOrPath;
    }

    /**
     * Gets the name of the source (e.g., "Google Books", "OpenLibrary")
     *
     * @return The name of the source
     */
    public String getSourceName() {
        return sourceName;
    }

    /**
     * Gets the source-specific identifier for the image
     *
     * @return The source-specific identifier
     */
    public String getSourceSystemId() {
        return sourceSystemId;
    }

    /**
     * Gets the enum representing the cover source
     *
     * @return The cover image source enum
     */
    public CoverImageSource getCoverImageSource() {
        return coverImageSource;
    }

    /**
     * Gets the preferred resolution for the image
     *
     * @return The image resolution preference enum
     */
    public ImageResolutionPreference getResolutionPreference() {
        return resolutionPreference;
    }

    /**
     * Gets the width of the image in pixels
     * Returns 0 if the dimensions are not known
     *
     * @return The width of the image in pixels
     */
    public int getWidth() {
        return width;
    }

    /**
     * Gets the height of the image in pixels
     * Returns 0 if the dimensions are not known
     *
     * @return The height of the image in pixels
     */
    public int getHeight() {
        return height;
    }

    /**
     * Checks if the dimensions of the image are known
     *
     * @return true if the dimensions are known, false otherwise
     */
    public boolean areDimensionsKnown() {
        return dimensionsKnown;
    }

    /**
     * Creates a new ImageDetails instance with the same properties as this one but with updated dimensions.
     * This is useful when dimensions become known after the initial creation of the ImageDetails object.
     *
     * @param width The width of the image in pixels
     * @param height The height of the image in pixels
     * @return A new ImageDetails instance with updated dimensions
     */
    public ImageDetails withDimensions(int width, int height) {
        return new ImageDetails(this.urlOrPath, this.sourceName, this.sourceSystemId, 
                                this.coverImageSource, this.resolutionPreference, 
                                width, height);
    }

    /**
     * Compares this ImageDetails with another object for equality
     * Two ImageDetails are considered equal if they have the same URL/path, source name,
     * source system ID, cover image source, resolution preference, width, height, and dimensions known status
     *
     * @param o the object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ImageDetails that = (ImageDetails) o;

        return width == that.width &&
               height == that.height &&
               dimensionsKnown == that.dimensionsKnown &&
               Objects.equals(urlOrPath, that.urlOrPath) &&
               Objects.equals(sourceName, that.sourceName) &&
               Objects.equals(sourceSystemId, that.sourceSystemId) &&
               coverImageSource == that.coverImageSource &&
               resolutionPreference == that.resolutionPreference;
    }

    /**
     * Returns a hash code value for this ImageDetails
     *
     * @return a hash code value for this object
     */
    @Override
    public int hashCode() {
        return Objects.hash(urlOrPath, sourceName, sourceSystemId, coverImageSource, 
                           resolutionPreference, width, height, dimensionsKnown);
    }

    /**
     * Returns a string representation of this ImageDetails
     *
     * @return a string representation of this object
     */
    @Override
    public String toString() {
        return "ImageDetails{" +
               "urlOrPath='" + urlOrPath + '\'' +
               ", sourceName='" + sourceName + '\'' +
               ", sourceSystemId='" + sourceSystemId + '\'' +
               ", coverImageSource=" + coverImageSource +
               ", resolutionPreference=" + resolutionPreference +
               ", width=" + width +
               ", height=" + height +
               ", dimensionsKnown=" + dimensionsKnown +
               '}';
    }
}
