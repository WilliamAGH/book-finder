/**
 * Data class for tracking the provenance of book cover images
 *
 * @author William Callahan
 *
 * Features:
 * - Records which image sources were attempted and their results
 * - Tracks the selected image and its attributes
 * - Preserves original API responses for debugging
 * - Includes timestamps for auditing image selection history
 * - Supports detailed failure tracking for each source
 */
package com.williamcallahan.book_recommendation_engine.types;

import java.time.Instant;
import java.util.List;
import java.util.Map;
public class ImageProvenanceData {

    private String bookId;
    private Object googleBooksApiResponse;
    private List<AttemptedSourceInfo> attemptedImageSources;
    private SelectedImageInfo selectedImageInfo;
    private Instant timestamp;

    public ImageProvenanceData() {
        this.timestamp = Instant.now();
    }

    /**
     * Get the book identifier
     * 
     * @return Book identifier from external API
     */
    public String getBookId() {
        return bookId;
    }

    /**
     * Set the book identifier
     * 
     * @param bookId Book identifier from external API
     */
    public void setBookId(String bookId) {
        this.bookId = bookId;
    }

    /**
     * Get the raw Google Books API response
     * 
     * @return Original API response for debugging
     */
    public Object getGoogleBooksApiResponse() {
        return googleBooksApiResponse;
    }

    /**
     * Set the raw Google Books API response
     * 
     * @param googleBooksApiResponse Original API response for debugging
     */
    public void setGoogleBooksApiResponse(Object googleBooksApiResponse) {
        this.googleBooksApiResponse = googleBooksApiResponse;
    }

    /**
     * Get list of attempted image source operations
     * 
     * @return List of attempted image source retrieval operations
     */
    public List<AttemptedSourceInfo> getAttemptedImageSources() {
        return attemptedImageSources;
    }

    /**
     * Set list of attempted image source operations
     * 
     * @param attemptedImageSources List of attempted image source retrieval operations
     */
    public void setAttemptedImageSources(List<AttemptedSourceInfo> attemptedImageSources) {
        this.attemptedImageSources = attemptedImageSources;
    }

    /**
     * Get information about the selected image
     * 
     * @return Details about the finally selected image
     */
    public SelectedImageInfo getSelectedImageInfo() {
        return selectedImageInfo;
    }

    /**
     * Set information about the selected image
     * 
     * @param selectedImageInfo Details about the finally selected image
     */
    public void setSelectedImageInfo(SelectedImageInfo selectedImageInfo) {
        this.selectedImageInfo = selectedImageInfo;
    }

    /**
     * Get timestamp when this data was created
     * 
     * @return Creation timestamp
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    /**
     * Set timestamp when this data was created
     * 
     * @param timestamp Creation timestamp
     */
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Records details about an attempt to fetch an image from a specific source
     * 
     * Features:
     * - Tracks source name and target URL
     * - Stores result status (success, failure, etc)
     * - Records detailed failure information when applicable
     * - Preserves metadata about the attempt for troubleshooting
     */
    public static class AttemptedSourceInfo {
        private ImageSourceName sourceName;
        private String urlAttempted;
        private ImageAttemptStatus status;
        private String failureReason;
        private Map<String, String> metadata;
        private String fetchedUrl;
        private String dimensions;

        public AttemptedSourceInfo(ImageSourceName sourceName, String urlAttempted, ImageAttemptStatus status) {
            this.sourceName = sourceName;
            this.urlAttempted = urlAttempted;
            this.status = status;
        }

        /**
         * Get the source name for this attempt
         * 
         * @return Image source identifier
         */
        public ImageSourceName getSourceName() {
            return sourceName;
        }

        /**
         * Set the source name for this attempt
         * 
         * @param sourceName Image source identifier
         */
        public void setSourceName(ImageSourceName sourceName) {
            this.sourceName = sourceName;
        }

        /**
         * Get the URL that was attempted to fetch
         * 
         * @return URL string that was tried
         */
        public String getUrlAttempted() {
            return urlAttempted;
        }

        /**
         * Set the URL that was attempted to fetch
         * 
         * @param urlAttempted URL string that was tried
         */
        public void setUrlAttempted(String urlAttempted) {
            this.urlAttempted = urlAttempted;
        }

        /**
         * Get the status of this fetch attempt
         * 
         * @return Status indicating success or failure
         */
        public ImageAttemptStatus getStatus() {
            return status;
        }

        /**
         * Set the status of this fetch attempt
         * 
         * @param status Status indicating success or failure
         */
        public void setStatus(ImageAttemptStatus status) {
            this.status = status;
        }

        /**
         * Get the reason for failure if applicable
         * 
         * @return Failure reason description
         */
        public String getFailureReason() {
            return failureReason;
        }

        /**
         * Set the reason for failure if applicable
         * 
         * @param failureReason Failure reason description
         */
        public void setFailureReason(String failureReason) {
            this.failureReason = failureReason;
        }

        /**
         * Get additional metadata about this attempt
         * 
         * @return Map of additional metadata key-value pairs
         */
        public Map<String, String> getMetadata() {
            return metadata;
        }

        /**
         * Set additional metadata about this attempt
         * 
         * @param metadata Map of additional metadata key-value pairs
         */
        public void setMetadata(Map<String, String> metadata) {
            this.metadata = metadata;
        }

        /**
         * Get the actual URL of the fetched image
         * 
         * @return URL of the fetched image which may differ from attempted URL
         */
        public String getFetchedUrl() {
            return fetchedUrl;
        }

        /**
         * Set the actual URL of the fetched image
         * 
         * @param fetchedUrl URL of the fetched image which may differ from attempted URL
         */
        public void setFetchedUrl(String fetchedUrl) {
            this.fetchedUrl = fetchedUrl;
        }

        /**
         * Get the dimensions of the image in WxH format
         * 
         * @return Image dimensions in WxH format
         */
        public String getDimensions() {
            return dimensions;
        }

        /**
         * Set the dimensions of the image in WxH format
         * 
         * @param dimensions Image dimensions in WxH format
         */
        public void setDimensions(String dimensions) {
            this.dimensions = dimensions;
        }
    }

    /**
     * Records details about the final selected image
     * 
     * Features:
     * - Identifies which source provided the image
     * - Tracks final URL and resolution
     * - Stores storage location information
     * - Preserves S3-specific metadata when applicable
     */
    public static class SelectedImageInfo {
        private ImageSourceName sourceName;
        private String finalUrl;
        private String resolution;
        private String dimensions;
        private String selectionReason;
        private String storageLocation;
        private String s3Key;

        /**
         * Get the source of the selected image
         * 
         * @return Image source identifier
         */
        public ImageSourceName getSourceName() {
            return sourceName;
        }

        /**
         * Set the source of the selected image
         * 
         * @param sourceName Image source identifier
         */
        public void setSourceName(ImageSourceName sourceName) {
            this.sourceName = sourceName;
        }

        /**
         * Get the final URL for the selected image
         * 
         * @return Final image URL
         */
        public String getFinalUrl() {
            return finalUrl;
        }

        /**
         * Set the final URL for the selected image
         * 
         * @param finalUrl Final image URL
         */
        public void setFinalUrl(String finalUrl) {
            this.finalUrl = finalUrl;
        }

        /**
         * Get the resolution description of the image
         * 
         * @return Resolution description (e.g., "large", "medium")
         */
        public String getResolution() {
            return resolution;
        }

        /**
         * Set the resolution description of the image
         * 
         * @param resolution Resolution description (e.g., "large", "medium")
         */
        public void setResolution(String resolution) {
            this.resolution = resolution;
        }

        /**
         * Get the storage location type
         * 
         * @return Storage type identifier (e.g., "S3", "LocalCache")
         */
        public String getStorageLocation() {
            return storageLocation;
        }

        /**
         * Set the storage location type
         * 
         * @param storageLocation Storage type identifier (e.g., "S3", "LocalCache")
         */
        public void setStorageLocation(String storageLocation) {
            this.storageLocation = storageLocation;
        }

        /**
         * Get the S3 key if applicable
         * 
         * @return S3 key for this image
         */
        public String getS3Key() {
            return s3Key;
        }

        /**
         * Set the S3 key if applicable
         * 
         * @param s3Key S3 key for this image
         */
        public void setS3Key(String s3Key) {
            this.s3Key = s3Key;
        }

        /**
         * Get the dimensions of the image in WxH format
         * 
         * @return Image dimensions in WxH format
         */
        public String getDimensions() {
            return dimensions;
        }

        /**
         * Set the dimensions of the image in WxH format
         * 
         * @param dimensions Image dimensions in WxH format
         */
        public void setDimensions(String dimensions) {
            this.dimensions = dimensions;
        }

        /**
         * Get the reason why this image was selected
         * 
         * @return Brief description of selection criteria
         */
        public String getSelectionReason() {
            return selectionReason;
        }

        /**
         * Set the reason why this image was selected
         * 
         * @param selectionReason Brief description of selection criteria
         */
        public void setSelectionReason(String selectionReason) {
            this.selectionReason = selectionReason;
        }
    }
}
