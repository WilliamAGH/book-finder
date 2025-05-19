/**
 * Service for handling S3 operations with retry capabilities
 * - Provides retry logic for S3 operations to minimize fallbacks to API
 * - Implements exponential backoff for transient S3 errors
 * - Tracks retry metrics for monitoring and alerting
 * - Separates retry logic from storage service implementation
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.types.S3FetchResult;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class S3RetryService {
    private static final Logger logger = LoggerFactory.getLogger(S3RetryService.class);

    private final S3StorageService s3StorageService;
    private final ApiRequestMonitor apiRequestMonitor;
    private final int maxRetries;
    private final int initialBackoffMs;
    private final double backoffMultiplier;

    /**
     * Constructs an S3RetryService with required dependencies
     *
     * @param s3StorageService The S3 storage service to use for operations
     * @param apiRequestMonitor The API request monitor for tracking metrics
     * @param maxRetries Maximum number of retry attempts for S3 operations
     * @param initialBackoffMs Initial backoff time in milliseconds
     * @param backoffMultiplier Multiplier for exponential backoff
     */
    @Autowired
    public S3RetryService(
            S3StorageService s3StorageService,
            ApiRequestMonitor apiRequestMonitor,
            @Value("${s3.retry.max-attempts:3}") int maxRetries,
            @Value("${s3.retry.initial-backoff-ms:200}") int initialBackoffMs,
            @Value("${s3.retry.backoff-multiplier:2.0}") double backoffMultiplier) {
        this.s3StorageService = s3StorageService;
        this.apiRequestMonitor = apiRequestMonitor;
        this.maxRetries = maxRetries;
        this.initialBackoffMs = initialBackoffMs;
        this.backoffMultiplier = backoffMultiplier;
    }

    /**
     * Fetch JSON from S3 with retry logic for transient errors
     *
     * @param volumeId The Google Books volume ID to fetch
     * @return A CompletableFuture that completes with the S3 fetch result
     */
    public CompletableFuture<S3FetchResult<String>> fetchJsonWithRetry(String volumeId) {
        return fetchJsonWithRetryInternal(volumeId, 0, initialBackoffMs);
    }

    private CompletableFuture<S3FetchResult<String>> fetchJsonWithRetryInternal(
            String volumeId, int attempt, long backoffMs) {
        
        // First attempt or retry
        return s3StorageService.fetchJsonAsync(volumeId)
            .exceptionally(ex -> {
                // Assuming logger is available and S3FetchResult.serviceError is correctly typed.
                // The type T of S3FetchResult<T> is determined by fetchJsonAsync.
                logger.warn("S3 fetch threw exception for volumeId {}: {}", volumeId, ex.getMessage());
                return S3FetchResult.serviceError(ex.getMessage());
            })
            .thenComposeAsync(result -> {
                // Successfully fetched or confirmed not found - no retry needed
                if (result.isSuccess() || result.isNotFound() || result.isDisabled()) {
                    return CompletableFuture.completedFuture(result);
                }
                
                // Service error - potential retry candidate
                if (result.isServiceError() && attempt < maxRetries) {
                    int nextAttempt = attempt + 1;
                    long nextBackoffMs = (long) (backoffMs * backoffMultiplier);
                    
                    // Record retry metric
                    apiRequestMonitor.recordMetric("s3/retry", 
                        String.format("Attempt %d for volumeId %s: %s", 
                            nextAttempt, volumeId, result.getErrorMessage().orElse("Unknown error")));
                    
                    logger.warn("S3 service error for volumeId {}. Retry attempt {}/{} after {}ms: {}", 
                        volumeId, nextAttempt, maxRetries, backoffMs, 
                        result.getErrorMessage().orElse("Unknown error"));
                    
                    // Implement backoff delay
                    return CompletableFuture.supplyAsync(() -> {
                        // This supplier is just for the delay, it doesn't produce a meaningful value for the next stage.
                        return null; 
                    }, CompletableFuture.delayedExecutor(backoffMs, TimeUnit.MILLISECONDS))
                        .thenComposeAsync(ignored -> fetchJsonWithRetryInternal(volumeId, nextAttempt, nextBackoffMs));
                }
                
                // Exhausted all retries or not a retriable error
                if (attempt > 0) {
                    logger.error("S3 operation failed after {} retries for volumeId {}: {}", 
                        attempt, volumeId, result.getErrorMessage().orElse("Unknown error"));
                    apiRequestMonitor.recordMetric("s3/retry_exhausted", 
                        String.format("volumeId %s after %d attempts: %s", 
                            volumeId, attempt, result.getErrorMessage().orElse("Unknown error")));
                }
                
                return CompletableFuture.completedFuture(result);
            });
    }
    
    /**
     * Upload JSON to S3 with retry logic for transient errors
     *
     * @param volumeId The Google Books volume ID
     * @param jsonContent The JSON content to upload
     * @return A CompletableFuture that completes when the upload is done
     */
    public CompletableFuture<Void> uploadJsonWithRetry(String volumeId, String jsonContent) {
        return uploadJsonWithRetryInternal(volumeId, jsonContent, 0, initialBackoffMs);
    }

    private CompletableFuture<Void> uploadJsonWithRetryInternal(
            String volumeId, String jsonContent, int attempt, long backoffMs) {

        return s3StorageService.uploadJsonAsync(volumeId, jsonContent)
            .thenAccept(result -> { // Changed from thenApply to thenAccept for Void
                // Success - no retry needed
                apiRequestMonitor.recordMetric("s3/upload_success", "volumeId " + volumeId);
            })
            .exceptionallyCompose(e -> { // Changed to exceptionallyCompose for async retry
                if (attempt < maxRetries) {
                    int nextAttempt = attempt + 1;
                    long nextBackoffMs = (long) (backoffMs * backoffMultiplier);

                    apiRequestMonitor.recordMetric("s3/upload_retry",
                        String.format("Attempt %d for volumeId %s: %s",
                            nextAttempt, volumeId, e.getMessage()));

                    logger.warn("S3 upload error for volumeId {}. Retry attempt {}/{} after {}ms: {}",
                        volumeId, nextAttempt, maxRetries, backoffMs, e.getMessage());

                    // Implement backoff delay before retry
                    return CompletableFuture.runAsync(() -> {
                        // Delay operation
                    }, CompletableFuture.delayedExecutor(backoffMs, TimeUnit.MILLISECONDS))
                        .thenCompose(ignored -> uploadJsonWithRetryInternal(volumeId, jsonContent, nextAttempt, nextBackoffMs));
                } else {
                    // Exhausted all retries
                    logger.error("S3 upload failed after {} retries for volumeId {}: {}",
                        attempt, volumeId, e.getMessage());
                    apiRequestMonitor.recordMetric("s3/upload_retry_exhausted",
                        String.format("volumeId %s after %d attempts: %s",
                            volumeId, attempt, e.getMessage()));
                    // Propagate the exception if retries are exhausted or not applicable
                    return CompletableFuture.failedFuture(e); 
                }
            });
    }

    /**
     * Update an existing book JSON in S3 with new data, specifically for adding qualifiers
     * First fetches existing data, merges with new book data, then uploads the combined result
     * 
     * @param book The book with updated qualifiers to persist
     * @return A CompletableFuture that completes when the update is done
     */
    public CompletableFuture<Void> updateBookJsonWithRetry(Book book) {
        if (book == null || book.getId() == null) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Cannot update S3 book data: Book or Book ID is null"));
        }
        
        String volumeId = book.getId();
        
        // First fetch the existing JSON
        return fetchJsonWithRetry(volumeId)
            .thenCompose(fetchResult -> {
                try {
                    String updatedJson;
                    
                    // If we successfully fetched existing data
                    if (fetchResult.isSuccess() && fetchResult.getData().isPresent()) {
                        String existingJson = fetchResult.getData().get();
                        ObjectMapper objectMapper = new ObjectMapper();
                        
                        // Parse existing JSON to JsonNode
                        JsonNode existingNode = objectMapper.readTree(existingJson);
                        
                        // Convert current book to JsonNode
                        JsonNode bookNode = objectMapper.valueToTree(book);
                        
                        // Create a merged node - this will be a deep merge
                        ObjectNode mergedNode;
                        if (existingNode instanceof ObjectNode) {
                            mergedNode = (ObjectNode) existingNode;
                            
                            // If the existing node has qualifiers, we need to merge them
                            if (bookNode.has("qualifiers")) {
                                // If existing node already has qualifiers, merge them
                                if (mergedNode.has("qualifiers")) {
                                    JsonNode existingQualifiers = mergedNode.get("qualifiers");
                                    JsonNode newQualifiers = bookNode.get("qualifiers");
                                    
                                    // Deep merge the qualifier nodes
                                    if (existingQualifiers instanceof ObjectNode && newQualifiers instanceof ObjectNode) {
                                        ObjectNode mergedQualifiers = (ObjectNode) existingQualifiers;
                                        
                                        // Copy all fields from new qualifiers to existing qualifiers
                                        newQualifiers.fields().forEachRemaining(entry -> {
                                            mergedQualifiers.set(entry.getKey(), entry.getValue());
                                        });
                                        
                                        // Update the merged node with merged qualifiers
                                        mergedNode.set("qualifiers", mergedQualifiers);
                                    } else {
                                        // If not both are ObjectNodes, prefer the new one
                                        mergedNode.set("qualifiers", newQualifiers);
                                    }
                                } else {
                                    // No existing qualifiers, just add the new ones
                                    mergedNode.set("qualifiers", bookNode.get("qualifiers"));
                                }
                            }
                        } else {
                            // If existing node is not an ObjectNode, use the new book node as is
                            mergedNode = (ObjectNode) bookNode;
                        }
                        
                        // Convert merged node back to JSON string
                        updatedJson = objectMapper.writeValueAsString(mergedNode);
                    } else {
                        // If we couldn't fetch existing data, just use the current book data
                        ObjectMapper objectMapper = new ObjectMapper();
                        updatedJson = objectMapper.writeValueAsString(book);
                    }
                    
                    // Upload the updated JSON
                    return uploadJsonWithRetry(volumeId, updatedJson);
                } catch (Exception e) {
                    logger.error("Error merging or updating book JSON for volumeId {}: {}", volumeId, e.getMessage());
                    return CompletableFuture.failedFuture(e);
                }
            });
    }
}
