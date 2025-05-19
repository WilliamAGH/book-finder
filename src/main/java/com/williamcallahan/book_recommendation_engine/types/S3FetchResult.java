/**
 * Represents the result of attempting to fetch an item from S3 storage
 * - Provides clear distinction between different fetch outcomes
 * - Allows distinguishing not found (cache miss) from service errors
 * - Enables more intelligent cache fallback decisions
 *
 * @param <T> The type of data contained in a successful result
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.types;

import java.util.Optional;

public final class S3FetchResult<T> {
    
    public enum Status {
        SUCCESS,        // Item was successfully retrieved
        NOT_FOUND,      // Item was not found in S3 (normal cache miss)
        SERVICE_ERROR,  // S3 service encountered an error (temporary failure)
        DISABLED        // S3 is disabled or misconfigured
    }
    
    private final Status status;
    private final T data;
    private final String errorMessage;
    
    private S3FetchResult(Status status, T data, String errorMessage) {
        if (status == Status.SUCCESS && data == null) {
            throw new IllegalArgumentException("SUCCESS result requires non-null data");
        }
        if (status != Status.SUCCESS && errorMessage == null) {
            throw new IllegalArgumentException(status + " result requires non-null errorMessage");
        }
        this.status = status;
        this.data = data;
        this.errorMessage = errorMessage;
    }
    
    /**
     * Create a successful result with data
     *
     * @param data The data retrieved from S3
     * @return A successful S3FetchResult containing the data
     */
    public static <T> S3FetchResult<T> success(T data) {
        return new S3FetchResult<>(Status.SUCCESS, data, null);
    }
    
    /**
     * Create a not found result (normal cache miss)
     *
     * @return A not found S3FetchResult
     */
    public static <T> S3FetchResult<T> notFound() {
        return new S3FetchResult<>(Status.NOT_FOUND, null, "Item not found in S3");
    }
    
    /**
     * Create a service error result
     *
     * @param errorMessage Description of the error
     * @return A service error S3FetchResult with the error message
     */
    public static <T> S3FetchResult<T> serviceError(String errorMessage) {
        return new S3FetchResult<>(Status.SERVICE_ERROR, null, errorMessage);
    }
    
    /**
     * Create a disabled result for when S3 is not configured
     *
     * @return A disabled S3FetchResult
     */
    public static <T> S3FetchResult<T> disabled() {
        return new S3FetchResult<>(Status.DISABLED, null, "S3 is disabled or misconfigured");
    }
    
    /**
     * Check if this result indicates success
     *
     * @return true if the status is SUCCESS
     */
    public boolean isSuccess() {
        return status == Status.SUCCESS;
    }
    
    /**
     * Check if this result indicates a normal not found (cache miss)
     *
     * @return true if the status is NOT_FOUND
     */
    public boolean isNotFound() {
        return status == Status.NOT_FOUND;
    }
    
    /**
     * Check if this result indicates a temporary service error
     *
     * @return true if the status is SERVICE_ERROR
     */
    public boolean isServiceError() {
        return status == Status.SERVICE_ERROR;
    }
    
    /**
     * Check if this result indicates S3 is disabled
     *
     * @return true if the status is DISABLED
     */
    public boolean isDisabled() {
        return status == Status.DISABLED;
    }
    
    /**
     * Get the data if this result was successful
     *
     * @return An Optional containing the data if successful, empty otherwise
     */
    public Optional<T> getData() {
        return Optional.ofNullable(data);
    }
    
    /**
     * Get the error message if this result was not successful
     *
     * @return An Optional containing the error message if there was an error, empty otherwise
     */
    public Optional<String> getErrorMessage() {
        return Optional.ofNullable(errorMessage);
    }
    
    /**
     * Get the status of this fetch result
     *
     * @return The Status enum value
     */
    public Status getStatus() {
        return status;
    }
}