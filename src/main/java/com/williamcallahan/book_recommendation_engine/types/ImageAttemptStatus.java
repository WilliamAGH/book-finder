/**
 * Status codes for image fetch and processing attempts
 *
 * @author William Callahan
 *
 * Features:
 * - Categorizes image retrieval results
 * - Distinguishes between different error types
 * - Enables targeted error handling and recovery
 * - Supports detailed logging of image processing workflow
 * - Used for determining retry strategies
 */
package com.williamcallahan.book_recommendation_engine.types;

/**
 * Status codes for image fetch and processing attempts
 * - Tracks outcomes of cover image retrieval operations
 * - Differentiates between error types for proper handling
 * - Used throughout image processing pipeline
 */
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
    FAILURE_PROCESSING, // Renamed from PROCESSING_FAILED for consistency, or add as new if distinct

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
    PENDING
}
