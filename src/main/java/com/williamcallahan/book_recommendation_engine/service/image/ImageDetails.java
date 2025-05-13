package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService.CoverImageSource;

public class ImageDetails {
    private final String urlOrPath;
    private final String sourceName;          // New field
    private final String sourceSystemId;      // New field
    private final CoverImageSource coverImageSource; // New field
    private final ImageResolutionPreference resolutionPreference; // New field
    private final int width;
    private final int height;
    private final boolean dimensionsKnown;

    // New primary constructor
    public ImageDetails(String urlOrPath, String sourceName, String sourceSystemId, 
                        CoverImageSource coverImageSource, ImageResolutionPreference resolutionPreference) {
        this.urlOrPath = urlOrPath;
        this.sourceName = sourceName;
        this.sourceSystemId = sourceSystemId;
        this.coverImageSource = coverImageSource;
        this.resolutionPreference = resolutionPreference;
        this.width = 0; // Default, to be updated after download/processing
        this.height = 0; // Default, to be updated after download/processing
        this.dimensionsKnown = false; // Default
    }

    // Constructor for when dimensions are known (e.g., after processing or from S3 metadata)
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

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public boolean areDimensionsKnown() {
        return dimensionsKnown;
    }

    // Consider adding a method to update dimensions, returning a new ImageDetails instance
    public ImageDetails withDimensions(int width, int height) {
        return new ImageDetails(this.urlOrPath, this.sourceName, this.sourceSystemId, 
                                this.coverImageSource, this.resolutionPreference, 
                                width, height);
    }
}
