package com.williamcallahan.book_recommendation_engine.types;

public class ImageDetails {

    private String urlOrPath;
    private String sourceName; // e.g., "GOOGLE_BOOKS_API", "OPEN_LIBRARY", "LOCAL_CACHE", "S3_CACHE"
    private String sourceSystemId; // e.g., Google Volume ID, ISBN, local filename, S3 key
    private CoverImageSource coverImageSource;
    private ImageResolutionPreference resolutionPreference;
    private Integer width;
    private Integer height;

    // Constructor for placeholder/failed images (no dimensions)
    public ImageDetails(String urlOrPath, String sourceName, String sourceSystemId, CoverImageSource coverImageSource, ImageResolutionPreference resolutionPreference) {
        this.urlOrPath = urlOrPath;
        this.sourceName = sourceName;
        this.sourceSystemId = sourceSystemId;
        this.coverImageSource = coverImageSource;
        this.resolutionPreference = resolutionPreference;
        this.width = 0;
        this.height = 0;
    }

    // Full constructor
    public ImageDetails(String urlOrPath, String sourceName, String sourceSystemId, CoverImageSource coverImageSource, ImageResolutionPreference resolutionPreference, Integer width, Integer height) {
        this.urlOrPath = urlOrPath;
        this.sourceName = sourceName;
        this.sourceSystemId = sourceSystemId;
        this.coverImageSource = coverImageSource;
        this.resolutionPreference = resolutionPreference;
        this.width = width;
        this.height = height;
    }

    // Getters
    public String getUrlOrPath() {
        return urlOrPath;
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getSourceSystemId() {
        return sourceSystemId;
    }

    public CoverImageSource getCoverImageSource() {
        return coverImageSource;
    }

    public ImageResolutionPreference getResolutionPreference() {
        return resolutionPreference;
    }

    public Integer getWidth() {
        return width;
    }

    public Integer getHeight() {
        return height;
    }

    // Setters (optional, but can be useful)
    public void setUrlOrPath(String urlOrPath) {
        this.urlOrPath = urlOrPath;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public void setSourceSystemId(String sourceSystemId) {
        this.sourceSystemId = sourceSystemId;
    }

    public void setCoverImageSource(CoverImageSource coverImageSource) {
        this.coverImageSource = coverImageSource;
    }

    public void setResolutionPreference(ImageResolutionPreference resolutionPreference) {
        this.resolutionPreference = resolutionPreference;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public void setHeight(Integer height) {
        this.height = height;
    }

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