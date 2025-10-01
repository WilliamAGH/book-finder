/**
 * Status codes for image fetch and processing attempts
 * - Categorizes image retrieval results
 * - Distinguishes between different error types
 * - Enables targeted error handling and recovery
 * - Supports detailed logging of image processing workflow
 * - Used for determining retry strategies
 * - Tracks outcomes of cover image retrieval operations
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.model.image;
public enum ImageAttemptStatus {
    /**
     * Image successfully retrieved and processed
     */
    SUCCESS,
    
    /**
     * Image not found at source (HTTP 404)
     */
    FAILURE_404,

    /**
     * Generic "not found" status, e.g. when a service indicates an item doesn't exist without a specific HTTP 404.
     * Could be S3 NoSuchKeyException, or an API response indicating no data.
     */
    FAILURE_NOT_FOUND,
    
    /**
     * Request timed out while fetching image
     */
    FAILURE_TIMEOUT,
    
    /**
     * Generic failure during image retrieval
     */
    FAILURE_GENERIC,
    
    /**
     * Image fetch attempt deliberately skipped
     */
    SKIPPED,

    /**
     * Image fetch attempt deliberately skipped due to URL being on a known bad list
     */
    SKIPPED_BAD_URL,

    /**
     * Image fetched but processing failed (e.g. hashing error, metadata extraction error)
     */
    FAILURE_PROCESSING,

    /**
     * Download resulted in empty or null content
     */
    FAILURE_EMPTY_CONTENT,

    /**
     * Downloaded image matched a known placeholder hash
     */
    FAILURE_PLACEHOLDER_DETECTED,

    /**
     * Generic IO failure during download or local caching
     */
    FAILURE_IO,

    /**
     * Generic failure specifically during the download phase from an external source
     */
    FAILURE_GENERIC_DOWNLOAD,
    
    /**
     * Image successfully retrieved and saved, but metadata (like dimensions) could not be read
     */
    SUCCESS_NO_METADATA,

    /**
     * Image fetch attempt is currently pending or in progress
     */
    PENDING,

    /**
     * Service responded, but the details returned were not valid or usable (e.g. S3 object metadata not matching expectations)
     */
    FAILURE_INVALID_DETAILS,

    /**
     * Service responded, but the expected data (e.g. a URL) was missing from the response.
     */
    FAILURE_NO_URL_IN_RESPONSE,

    /**
     * Image fetched but rejected due to content analysis (e.g., predominantly white, too simple).
     */
    FAILURE_CONTENT_REJECTED,

}
