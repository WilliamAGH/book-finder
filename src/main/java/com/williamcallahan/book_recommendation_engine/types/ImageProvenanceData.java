package com.williamcallahan.book_recommendation_engine.types;

import java.time.Instant;
import java.util.List;
import java.util.Map;

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

    // Inner class for AttemptedSourceInfo
    public static class AttemptedSourceInfo {
        private String sourceName; // e.g., "Google Books API", "OpenLibrary", "LocalCache"
        private String urlAttempted;
        private String status; // e.g., "SUCCESS", "FAILURE_404", "FAILURE_TIMEOUT", "SKIPPED"
        private String failureReason; // Optional: details if status is a failure type
        private Map<String, String> metadata; // Optional: e.g. resolution tried

        public AttemptedSourceInfo(String sourceName, String urlAttempted, String status) {
            this.sourceName = sourceName;
            this.urlAttempted = urlAttempted;
            this.status = status;
        }

        // Getters and Setters
        public String getSourceName() {
            return sourceName;
        }

        public void setSourceName(String sourceName) {
            this.sourceName = sourceName;
        }

        public String getUrlAttempted() {
            return urlAttempted;
        }

        public void setUrlAttempted(String urlAttempted) {
            this.urlAttempted = urlAttempted;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
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

    // Inner class for SelectedImageInfo
    public static class SelectedImageInfo {
        private String sourceName;
        private String finalUrl;
        private String resolution; // e.g., "large", "medium", "thumbnail"
        private String storageLocation; // e.g., "S3", "LocalCache"
        private String s3Key; // if stored in S3

        // Getters and Setters
        public String getSourceName() {
            return sourceName;
        }

        public void setSourceName(String sourceName) {
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
