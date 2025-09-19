package com.williamcallahan.book_recommendation_engine.model.image;

/**
 * Book cover image details including source and dimension information
 *
 * @author William Callahan
 *
 * Features:
 * - Stores metadata for book cover images
 * - Tracks image dimensions and source attribution
 * - Supports mutable properties through setters
 * - Maintains resolution preference information
 * - Enables source tracking across different image providers
 */
public class ImageDetails {

    private String urlOrPath;
    private String sourceName; // e.g., "GOOGLE_BOOKS_API", "OPEN_LIBRARY", "LOCAL_CACHE", "S3_CACHE"
    private String sourceSystemId; // e.g., Google Volume ID, ISBN, local filename, S3 key
    private CoverImageSource coverImageSource;
    private ImageResolutionPreference resolutionPreference;
    private Integer width;
    private Integer height;

    /**
     * Constructor for placeholder/failed images (no dimensions)
     *
     * @param urlOrPath The URL or file path to the image
     * @param sourceName The name of the image source system
     * @param sourceSystemId Source-specific identifier
     * @param coverImageSource The enum representing the cover source
     * @param resolutionPreference The preferred resolution for the image
     */
    public ImageDetails(String urlOrPath, String sourceName, String sourceSystemId, CoverImageSource coverImageSource, ImageResolutionPreference resolutionPreference) {
        this.urlOrPath = urlOrPath;
        this.sourceName = sourceName;
        this.sourceSystemId = sourceSystemId;
        this.coverImageSource = coverImageSource;
        this.resolutionPreference = resolutionPreference;
        this.width = 0;
        this.height = 0;
    }

    /**
     * Full constructor with all image properties including dimensions
     *
     * @param urlOrPath The URL or file path to the image
     * @param sourceName The name of the image source system
     * @param sourceSystemId Source-specific identifier
     * @param coverImageSource The enum representing the cover source
     * @param resolutionPreference The preferred resolution for the image
     * @param width The width of the image in pixels
     * @param height The height of the image in pixels
     */
    public ImageDetails(String urlOrPath, String sourceName, String sourceSystemId, CoverImageSource coverImageSource, ImageResolutionPreference resolutionPreference, Integer width, Integer height) {
        this.urlOrPath = urlOrPath;
        this.sourceName = sourceName;
        this.sourceSystemId = sourceSystemId;
        this.coverImageSource = coverImageSource;
        this.resolutionPreference = resolutionPreference;
        this.width = width;
        this.height = height;
    }

    /**
     * Gets the URL or file path to the image
     *
     * @return The URL or file path
     */
    public String getUrlOrPath() {
        return urlOrPath;
    }

    /**
     * Gets the source name for the image
     *
     * @return The source name
     */
    public String getSourceName() {
        return sourceName;
    }

    /**
     * Gets the source-specific identifier
     *
     * @return The source system identifier
     */
    public String getSourceSystemId() {
        return sourceSystemId;
    }

    /**
     * Gets the cover image source enum
     *
     * @return The cover image source
     */
    public CoverImageSource getCoverImageSource() {
        return coverImageSource;
    }

    /**
     * Gets the resolution preference for this image
     *
     * @return The resolution preference
     */
    public ImageResolutionPreference getResolutionPreference() {
        return resolutionPreference;
    }

    /**
     * Gets the image width in pixels
     *
     * @return The width or 0 if unknown
     */
    public Integer getWidth() {
        return width;
    }

    /**
     * Gets the image height in pixels
     *
     * @return The height or 0 if unknown
     */
    public Integer getHeight() {
        return height;
    }

    /**
     * Sets the URL or file path to the image
     *
     * @param urlOrPath The new URL or file path
     */
    public void setUrlOrPath(String urlOrPath) {
        this.urlOrPath = urlOrPath;
    }

    /**
     * Sets the source name for the image
     *
     * @param sourceName The new source name
     */
    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    /**
     * Sets the source-specific identifier
     *
     * @param sourceSystemId The new source system identifier
     */
    public void setSourceSystemId(String sourceSystemId) {
        this.sourceSystemId = sourceSystemId;
    }

    /**
     * Sets the cover image source enum
     *
     * @param coverImageSource The new cover image source
     */
    public void setCoverImageSource(CoverImageSource coverImageSource) {
        this.coverImageSource = coverImageSource;
    }

    /**
     * Sets the resolution preference for this image
     *
     * @param resolutionPreference The new resolution preference
     */
    public void setResolutionPreference(ImageResolutionPreference resolutionPreference) {
        this.resolutionPreference = resolutionPreference;
    }

    /**
     * Sets the image width in pixels
     *
     * @param width The new width
     */
    public void setWidth(Integer width) {
        this.width = width;
    }

    /**
     * Sets the image height in pixels
     *
     * @param height The new height
     */
    public void setHeight(Integer height) {
        this.height = height;
    }

    /**
     * Returns a string representation of this ImageDetails
     *
     * @return A string representation of the object
     */
    @Override
    public String toString() {
        return "ImageDetails{" +
               "urlOrPath='" + urlOrPath + "'" +
               ", sourceName='" + sourceName + "'" +
               ", sourceSystemId='" + sourceSystemId + "'" +
               ", coverImageSource=" + coverImageSource +
               ", resolutionPreference=" + resolutionPreference +
               ", width=" + width +
               ", height=" + height +
               '}';
    }
} 
