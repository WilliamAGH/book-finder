/**
 * Exception thrown when a connection pool has been shut down and cannot process requests
 *
 * @author William Callahan
 *
 * Features:
 * - Indicates connection pool shutdown state
 * - Designed to be retryable as the pool may recover
 * - Used by S3 and Redis retry templates
 */

package com.williamcallahan.book_recommendation_engine.service;

public class PoolShutdownException extends RuntimeException {
    
    public PoolShutdownException(String message) {
        super(message);
    }
    
    public PoolShutdownException(String message, Throwable cause) {
        super(message, cause);
    }
}