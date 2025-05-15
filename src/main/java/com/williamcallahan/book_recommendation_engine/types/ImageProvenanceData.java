package com.williamcallahan.book_recommendation_engine.types;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Data class for tracking the provenance (origin and processing history) of book cover images.
 * Used to record which image sources were attempted, their results, and which image was ultimately selected.
 * This data helps with debugging, auditing, and improving image quality over time.
 */
public class ImageProvenanceData {

    private String bookId; // e.g., Google Books ID
    private Object googleBooksApiResponse; // Can be String (raw JSON) or a Map/Object representation
    private List<AttemptedSourceInfo> attemptedImageSources;
    private SelectedImageInfo selectedImageInfo;
    private Instant timestamp;

    public ImageProvenanceData() {
        this.timestamp = Instant.now();
    }

    // Getters and Setters
    public String getBookId() {
        return bookId;
    }

    public void setBookId(String bookId) {
        this.bookId = bookId;
    }

    public Object getGoogleBooksApiResponse() {
        return googleBooksApiResponse;
    }

    public void setGoogleBooksApiResponse(Object googleBooksApiResponse) {
        this.googleBooksApiResponse = googleBooksApiResponse;
    }

    public List<AttemptedSourceInfo> getAttemptedImageSources() {
        return attemptedImageSources;
    }

    public void setAttemptedImageSources(List<AttemptedSourceInfo> attemptedImageSources) {
        this.attemptedImageSources = attemptedImageSources;
    }

    public SelectedImageInfo getSelectedImageInfo() {
        return selectedImageInfo;
    }

    public void setSelectedImageInfo(SelectedImageInfo selectedImageInfo) {
        this.selectedImageInfo = selectedImageInfo;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Inner class representing an attempt to fetch an image from a specific source.
     * Tracks the source name, URL attempted, status of the attempt, and any additional details.
     */
    public static class AttemptedSourceInfo {
        private ImageSourceName sourceName;        // The source of the image (e.g., GOOGLE_BOOKS, OPEN_LIBRARY)
        private String urlAttempted;               // The URL that was attempted to fetch
        private ImageAttemptStatus status;         // The outcome of the attempt (success, failure, etc.)
        private String failureReason;              // Optional: details if status is a failure type
        private Map<String, String> metadata;      // Optional: e.g. resolution tried

        public AttemptedSourceInfo(ImageSourceName sourceName, String urlAttempted, ImageAttemptStatus status) {
            this.sourceName = sourceName;
            this.urlAttempted = urlAttempted;
            this.status = status;
        }

        // Getters and Setters
        public ImageSourceName getSourceName() {
            return sourceName;
        }

        public void setSourceName(ImageSourceName sourceName) {
            this.sourceName = sourceName;
        }

        public String getUrlAttempted() {
            return urlAttempted;
        }

        public void setUrlAttempted(String urlAttempted) {
            this.urlAttempted = urlAttempted;
        }

        public ImageAttemptStatus getStatus() {
            return status;
        }

        public void setStatus(ImageAttemptStatus status) {
            this.status = status;
        }

        public String getFailureReason() {
            return failureReason;
        }

        public void setFailureReason(String failureReason) {
            this.failureReason = failureReason;
        }

        public Map<String, String> getMetadata() {
            return metadata;
        }

        public void setMetadata(Map<String, String> metadata) {
            this.metadata = metadata;
        }
    }

    /**
     * Inner class representing the final selected image and its details.
     * Records which source provided the image, its URL, resolution, storage location, and any S3-specific data.
     */
    public static class SelectedImageInfo {
        private ImageSourceName sourceName;      // The source that provided the final selected image
        private String finalUrl;                 // The URL of the selected image
        private String resolution;               // e.g., "large", "medium", "thumbnail", "ORIGINAL"
        private String storageLocation;          // e.g., "S3", "LocalCache" 
        private String s3Key;                    // If stored in S3, the S3 key

        // Getters and Setters
        public ImageSourceName getSourceName() {
            return sourceName;
        }

        public void setSourceName(ImageSourceName sourceName) {
            this.sourceName = sourceName;
        }

        public String getFinalUrl() {
            return finalUrl;
        }

        public void setFinalUrl(String finalUrl) {
            this.finalUrl = finalUrl;
        }

        public String getResolution() {
            return resolution;
        }

        public void setResolution(String resolution) {
            this.resolution = resolution;
        }

        public String getStorageLocation() {
            return storageLocation;
        }

        public void setStorageLocation(String storageLocation) {
            this.storageLocation = storageLocation;
        }

        public String getS3Key() {
            return s3Key;
        }

        public void setS3Key(String s3Key) {
            this.s3Key = s3Key;
        }
    }
}
