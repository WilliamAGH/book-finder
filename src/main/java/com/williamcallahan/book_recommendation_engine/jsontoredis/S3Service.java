/**
 * Service for interacting with Amazon S3 storage specifically for JSON migration
 *
 * @author William Callahan
 *
 * Features:
 * - Provides access to book data stored as JSON files in S3
 * - Lists objects under specified prefixes with pagination support
 * - Retrieves object content as input streams for processing
 * - Configured with specific paths for Google Books and NYT bestsellers data
 * - Uses AWS SDK v2 for S3 operations
 */

package com.williamcallahan.book_recommendation_engine.jsontoredis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import jakarta.annotation.PreDestroy;

@Service("jsonS3ToRedis_S3Service")
@Profile("jsontoredis")
public class S3Service {

    private static final Logger log = LoggerFactory.getLogger(S3Service.class);

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final S3Client s3Client;
    private final AsyncTaskExecutor mvcTaskExecutor;
    private final String bucketName;
    private final String googleBooksPrefix;
    private final String nytBestsellersKey;

    /**
     * Constructs the S3Service with necessary S3 client and configuration values
     *
     * @param s3Client The AWS S3 client
     * @param bucketName The name of the S3 bucket
     * @param googleBooksPrefix The S3 prefix for Google Books data
     * @param nytBestsellersKey The S3 key for NYT Bestsellers data
     * @param mvcTaskExecutor The asynchronous task executor
     * @throws IllegalStateException if the s3Client is null
     */
    public S3Service(S3Client s3Client,
                     @Value("${s3.bucket-name}") String bucketName,
                     @Value("${jsontoredis.s3.google-books-prefix}") String googleBooksPrefix,
                     @Value("${jsontoredis.s3.nyt-bestsellers-key}") String nytBestsellersKey,
                     @Qualifier("mvcTaskExecutor") AsyncTaskExecutor mvcTaskExecutor) {
        if (s3Client == null) {
            throw new IllegalStateException("S3Client is not available. Ensure S3 is enabled and configured correctly in S3Config.");
        }
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.googleBooksPrefix = googleBooksPrefix;
        this.nytBestsellersKey = nytBestsellersKey;
        this.mvcTaskExecutor = mvcTaskExecutor;
        log.info("jsonS3ToRedis S3Service initialized with bucket: {}, googleBooksPrefix: {}, nytBestsellersKey: {}", bucketName, googleBooksPrefix, nytBestsellersKey);
    }

    /**
     * Graceful shutdown of S3 service operations for jsontoredis profile
     * Prevents new operations from starting during shutdown
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down jsontoredis S3Service - disabling new operations");
        shuttingDown.set(true);
    }

    /**
     * Lists object keys under a given prefix in the configured bucket asynchronously
     * Handles pagination using AWS SDK v2
     *
     * @param prefix The S3 key prefix to list objects under
     * @return A CompletableFuture containing a list of S3 object keys matching the prefix
     */
    public CompletableFuture<List<String>> listObjectKeys(String prefix) {
        if (shuttingDown.get()) {
            log.warn("S3Service is shutting down, rejecting listObjectKeys request for prefix: {}", prefix);
            return CompletableFuture.completedFuture(new ArrayList<>());
        }
        
        // Check if executor is shutdown to avoid RejectedExecutionException
        boolean executorShutdown = false;
        if (mvcTaskExecutor instanceof ThreadPoolTaskExecutor) {
            executorShutdown = ((ThreadPoolTaskExecutor) mvcTaskExecutor).getThreadPoolExecutor().isShutdown();
        } else {
            log.warn("mvcTaskExecutor is not an instance of ThreadPoolTaskExecutor in listObjectKeys. Actual type: {}. Cannot reliably check for shutdown status via getThreadPoolExecutor().", mvcTaskExecutor.getClass().getName());
            // Assuming not shutdown if type is unknown or doesn't support this check directly
        }

        if (executorShutdown) {
            log.warn("Executor is shutdown, executing S3 list operation synchronously for prefix: {}", prefix);
            return CompletableFuture.completedFuture(listObjectKeysSync(prefix));
        }
        
        try {
            return CompletableFuture.supplyAsync(() -> listObjectKeysSync(prefix), mvcTaskExecutor);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            log.warn("Executor rejected task, falling back to synchronous execution for prefix: {}", prefix);
            return CompletableFuture.completedFuture(listObjectKeysSync(prefix));
        }
    }
    
    /**
     * Synchronously lists object keys under a given prefix in the configured bucket
     * This is used as a fallback when the async executor is unavailable
     */
    private List<String> listObjectKeysSync(String prefix) {
        log.info("Listing objects in bucket {} with prefix {}", bucketName, prefix);
        List<String> keys = new ArrayList<>();
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();

        ListObjectsV2Response listRes;
        do {
            listRes = s3Client.listObjectsV2(listReq);
            for (S3Object s3ObjectSummary : listRes.contents()) {
                keys.add(s3ObjectSummary.key());
            }
            listReq = listReq.toBuilder()
                    .continuationToken(listRes.nextContinuationToken())
                    .build();
        } while (Boolean.TRUE.equals(listRes.isTruncated()));

        log.info("Found {} objects matching prefix {}", keys.size(), prefix);
        return keys;
    }

    /**
     * Gets the content of an S3 object as an InputStream asynchronously
     *
     * @param key The S3 object key to retrieve
     * @return A CompletableFuture containing an InputStream of the object's content
     * @throws NoSuchKeyException if the specified key does not exist
     * @throws RuntimeException if any other error occurs during S3 interaction
     */
    public CompletableFuture<InputStream> getObjectContent(String key) {
        // Check if executor is shutdown to avoid RejectedExecutionException
        boolean executorShutdown = false;
        if (mvcTaskExecutor instanceof ThreadPoolTaskExecutor) {
            executorShutdown = ((ThreadPoolTaskExecutor) mvcTaskExecutor).getThreadPoolExecutor().isShutdown();
        } else {
            log.warn("mvcTaskExecutor is not an instance of ThreadPoolTaskExecutor. Actual type: {}. Cannot reliably check for shutdown status via getThreadPoolExecutor().", mvcTaskExecutor.getClass().getName());
            // Assuming not shutdown if type is unknown or doesn't support this check directly
        }

        if (executorShutdown) {
            log.warn("Executor is shutdown, executing S3 operation synchronously for key: {}", key);
            return CompletableFuture.completedFuture(getObjectContentSync(key));
        }
        
        try {
            return CompletableFuture.supplyAsync(() -> getObjectContentSync(key), mvcTaskExecutor);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            log.warn("Executor rejected task, falling back to synchronous execution for key: {}", key);
            return CompletableFuture.completedFuture(getObjectContentSync(key));
        }
    }
    
    /**
     * Synchronously gets the content of an S3 object as an InputStream
     * This is used as a fallback when the async executor is unavailable
     */
    private InputStream getObjectContentSync(String key) {
        log.debug("Getting object content for S3 key: {} from bucket: {}", key, bucketName);
        GetObjectRequest getReq = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        try {
            ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(getReq);
            log.info("Successfully retrieved S3 object stream for key: {}", key);
            return stream;
        } catch (IllegalStateException ise) {
            log.error("IllegalStateException while getting S3 object for key {}: {}", key, ise.getMessage(), ise);
            throw new RuntimeException("Failed to get S3 object content for key " + key, ise);
        } catch (NoSuchKeyException e) {
            log.warn("S3 key not found: {} (bucket {})", key, bucketName);
            throw e;
        } catch (Exception e) {
            log.error("Generic error getting S3 object content for key {}: {}", key, e.getMessage(), e);
            throw new RuntimeException("Failed to get S3 object content for key " + key, e);
        }
    }

    /**
     * Gets the configured Google Books S3 prefix
     *
     * @return The Google Books S3 prefix string
     */
    public String getGoogleBooksPrefix() {
        return googleBooksPrefix;
    }

    /**
     * Gets the configured NYT Bestsellers S3 key
     *
     * @return The NYT Bestsellers S3 key string
     */
    public String getNytBestsellersKey() {
        return nytBestsellersKey;
    }

    /**
     * Moves an S3 object to a new key within the same bucket asynchronously
     * This operation first copies the object to the new key and then deletes the original
     *
     * @param sourceKey The current key of the object
     * @param destinationKey The new key for the object
     * @return A CompletableFuture<Void> that completes when the move operation (copy and delete) is finished
     * @throws NoSuchKeyException if the sourceKey does not exist
     * @throws RuntimeException if any other error occurs during S3 interaction
     */
    public CompletableFuture<Void> moveObject(String sourceKey, String destinationKey) {
        return CompletableFuture.runAsync(() -> {
            log.debug("Moving S3 object from {} to {} in bucket {}", sourceKey, destinationKey, bucketName);
            try {
                CopyObjectRequest copyReq = CopyObjectRequest.builder()
                        .sourceBucket(bucketName)
                        .sourceKey(sourceKey)
                        .destinationBucket(bucketName)
                        .destinationKey(destinationKey)
                        .build();
                s3Client.copyObject(copyReq);
                log.info("Successfully copied S3 object from {} to {}", sourceKey, destinationKey);

                DeleteObjectRequest deleteReq = DeleteObjectRequest.builder()
                        .bucket(bucketName)
                        .key(sourceKey)
                        .build();
                s3Client.deleteObject(deleteReq);
                log.info("Successfully deleted original S3 object {}", sourceKey);

            } catch (NoSuchKeyException e) {
                log.error("Cannot move S3 object: Source key {} not found in bucket {}.", sourceKey, bucketName, e);
                throw new RuntimeException("Source key not found: " + sourceKey, e);
            } catch (Exception e) {
                log.error("Error moving S3 object from {} to {}: {}", sourceKey, destinationKey, e.getMessage(), e);
                throw new RuntimeException("Failed to move S3 object from " + sourceKey + " to " + destinationKey, e);
            }
        }, mvcTaskExecutor);
    }

}
