/**
 * Service for handling file storage operations in S3
 * - Provides asynchronous file upload capabilities
 * - Handles S3 bucket operations using AWS SDK
 * - Generates public URLs for uploaded content
 * - Supports CDN integration through configuration
 * - Implements error handling and logging for storage operations
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import com.williamcallahan.book_recommendation_engine.config.S3EnvironmentCondition;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import jakarta.annotation.PreDestroy;
import com.williamcallahan.book_recommendation_engine.types.S3FetchResult;
import com.williamcallahan.book_recommendation_engine.monitoring.MetricsService;
import com.williamcallahan.book_recommendation_engine.util.ErrorHandlingUtils;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import org.springframework.beans.factory.annotation.Autowired;

@Service
@Conditional(S3EnvironmentCondition.class)
public class S3StorageService {
    private static final Logger logger = LoggerFactory.getLogger(S3StorageService.class);
    private static final String GOOGLE_BOOKS_API_CACHE_DIRECTORY = "books/v1/";

    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final S3Client s3Client;
    private final String bucketName;
    private final String publicCdnUrl;
    private final String serverUrl;
    private final MetricsService metricsService;

    /**
     * Constructs an S3StorageService with required dependencies
     * - Initializes AWS S3 client for bucket operations
     * - Configures bucket name from application properties
     * - Sets up optional CDN URL for public access
     * - Sets up optional server URL for DigitalOcean Spaces
     * - Integrates metrics service for monitoring
     *
     * @param s3Client AWS S3 client for interacting with buckets
     * @param bucketName Name of the S3 bucket to use for storage
     * @param publicCdnUrl Optional CDN URL for public access to files
     * @param serverUrl Optional server URL for DigitalOcean Spaces
     * @param metricsService Optional metrics service for tracking operations
     */
    public S3StorageService(S3Client s3Client, 
                            @Value("${s3.bucket-name:${S3_BUCKET}}") String bucketName,
                            @Value("${s3.cdn-url:${S3_CDN_URL:#{null}}}") String publicCdnUrl,
                            @Value("${s3.server-url:${S3_SERVER_URL:#{null}}}") String serverUrl,
                            @Autowired(required = false) MetricsService metricsService) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.publicCdnUrl = publicCdnUrl;
        this.serverUrl = serverUrl;
        this.metricsService = metricsService;
    }

    /**
     * Graceful shutdown of S3 service operations
     * Prevents new operations from starting during shutdown
     */
    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down S3StorageService - disabling new operations");
        shuttingDown.set(true);
    }

    /**
     * Checks if the S3Client is available and not shut down.
     * This is especially important during application restarts when the client may be closed.
     *
     * @return true if the client is available and operational, false otherwise
     */
    private boolean isS3ClientAvailable() {
        if (s3Client == null || shuttingDown.get()) {
            return false;
        }
        try {
            // Lightweight health check: verify bucket accessibility
            s3Client.headBucket(HeadBucketRequest.builder()
                .bucket(bucketName)
                .build());
            return true;
        } catch (NoSuchBucketException e) {
            // Bucket doesn't exist but client is operational
            logger.info("S3Client is operational, but bucket '{}' does not exist or is not accessible.", bucketName);
            return true; // Client is working, bucket might be an issue for operations but not client health itself.
        } catch (S3Exception e) {
            // AWS S3 error (e.g. permission or connectivity)
            logger.warn("S3 health check failed (AWS S3 Error Code: {}): {}", e.awsErrorDetails().errorCode(), e.awsErrorDetails().errorMessage());
            return false;
        } catch (IllegalStateException e) {
            if (e.getMessage() != null && e.getMessage().contains("Connection pool shut down")) {
                logger.warn("S3Client connection pool has been shut down. This typically occurs during application restart.");
                throw new PoolShutdownException("S3Client connection pool has been shut down", e);
            }
            // Re-throw other IllegalStateExceptions as they might indicate a different issue
            throw e; 
        } catch (Exception e) {
            // Catch any other unexpected exceptions during the health check
            logger.warn("Unexpected error during S3Client health check: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Asynchronously uploads a file to the S3 bucket with standardized error handling and metrics
     *
     * @param keyName The key (path/filename) under which to store the file in the bucket
     * @param inputStream The InputStream of the file to upload
     * @param contentLength The length of the content to be uploaded
     * @param contentType The MIME type of the file
     * @return A CompletableFuture<String> with the public URL of the uploaded file, or null if upload failed
     */
    public CompletableFuture<String> uploadFileAsync(String keyName, InputStream inputStream, long contentLength, String contentType) {
        if (!isS3ClientAvailable()) {
            logger.warn("S3Client is not available. Cannot upload file: {}. S3 may be disabled, misconfigured, or shut down.", keyName);
            return CompletableFuture.failedFuture(new IllegalStateException("S3Client is not available or has been shut down."));
        }
        
        // Track timing if metrics available
        io.micrometer.core.instrument.Timer.Sample sample = null;
        if (metricsService != null) {
            sample = metricsService.startS3Timer();
        }
        final io.micrometer.core.instrument.Timer.Sample finalSample = sample;
        
        return Mono.fromCallable(() -> {
            try {
                // Double-check client availability inside the async operation
                if (!isS3ClientAvailable()) {
                    throw new IllegalStateException("S3Client became unavailable during async operation.");
                }
                
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .contentType(contentType)
                        .build();

                s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, contentLength));
                
                logger.info("Successfully uploaded {} to S3 bucket {}", keyName, bucketName);

                // Construct the public URL - and ensure no double slashes
                if (publicCdnUrl == null || publicCdnUrl.isEmpty()) {
                    // Fall back to configured S3 server URL (e.g., DigitalOcean Spaces)
                    if (serverUrl != null && !serverUrl.isEmpty()) {
                        String server = serverUrl.endsWith("/") ? serverUrl.substring(0, serverUrl.length() - 1) : serverUrl;
                        String key = keyName.startsWith("/") ? keyName.substring(1) : keyName;
                        return server + "/" + bucketName + "/" + key;
                    }
                    // Final fallback: AWS S3 URL
                    return String.format("https://%s.s3.amazonaws.com/%s", bucketName,
                        keyName.startsWith("/") ? keyName.substring(1) : keyName);
                }
                
                String cdn = publicCdnUrl.endsWith("/") ? publicCdnUrl : publicCdnUrl + "/";
                String key = keyName.startsWith("/") ? keyName.substring(1) : keyName;
                return cdn + key;
            } finally {
                if (finalSample != null && metricsService != null) {
                    metricsService.stopS3Timer(finalSample);
                }
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(throwable -> {
            // Use standardized error handling
            ErrorHandlingUtils.ErrorCategory category = ErrorHandlingUtils.categorizeError(throwable);
            
            switch (category) {
                case S3_CONNECTION:
                    logger.error("S3 connection error uploading {}: {}", keyName, throwable.getMessage());
                    if (metricsService != null) {
                        metricsService.incrementS3Error();
                    }
                    break;
                case TIMEOUT:
                    logger.error("Timeout uploading {} to S3", keyName);
                    if (metricsService != null) {
                        metricsService.incrementS3Timeout();
                    }
                    break;
                default:
                    logger.error("Error uploading {} to S3: {}", keyName, throwable.getMessage(), throwable);
                    if (metricsService != null) {
                        metricsService.incrementS3Error();
                    }
            }
            
            return Mono.error(throwable);
        })
        .toFuture();
    }

    /**
     * Asynchronously uploads a JSON string as a GZIP-compressed file to S3.
     *
     * @param volumeId The Google Books volume ID, used to construct the S3 key.
     * @param jsonContent The JSON string to upload.
     * @return A CompletableFuture<Void> that completes when the upload is finished or fails.
     */
    public CompletableFuture<Void> uploadJsonAsync(String volumeId, String jsonContent) {
        if (!isS3ClientAvailable()) {
            logger.warn("S3Client is not available. Cannot upload JSON for volumeId: {}. S3 may be disabled, misconfigured, or shut down.", volumeId);
            return CompletableFuture.failedFuture(new IllegalStateException("S3Client is not available or has been shut down."));
        }
        return Mono.<Void>fromRunnable(() -> {
            String keyName = GOOGLE_BOOKS_API_CACHE_DIRECTORY + volumeId + ".json";
            try {
                // Double-check client availability inside the async operation
                if (!isS3ClientAvailable()) {
                    throw new IllegalStateException("S3Client became unavailable during async operation.");
                }
                byte[] compressedJson;
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                     GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bos)) {
                    gzipOutputStream.write(jsonContent.getBytes(StandardCharsets.UTF_8));
                    gzipOutputStream.finish();
                    compressedJson = bos.toByteArray();
                }

                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .contentType("application/json")
                        .contentEncoding("gzip")
                        .build();

                s3Client.putObject(putObjectRequest, RequestBody.fromBytes(compressedJson));
                logger.info("Successfully uploaded JSON for volumeId {} to S3 key {}", volumeId, keyName);
            } catch (Exception e) {
                logger.error("Error uploading JSON for volumeId {} to S3: {}", volumeId, e.getMessage(), e);
                throw new RuntimeException("Failed to upload JSON to S3 for volumeId " + volumeId, e);
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .toFuture();
    }
    
    /**
     * Asynchronously fetches and GZIP-decompresses a JSON file from S3.
     *
     * @param volumeId The Google Books volume ID, used to construct the S3 key.
     * @return A CompletableFuture<S3FetchResult<String>> containing the result status and optionally the JSON string if found
     */
    public CompletableFuture<S3FetchResult<String>> fetchJsonAsync(String volumeId) {
        if (!isS3ClientAvailable()) {
            logger.warn("S3Client is not available. Cannot fetch JSON for volumeId: {}. S3 may be disabled, misconfigured, or shut down.", volumeId);
            return CompletableFuture.completedFuture(S3FetchResult.disabled());
        }
        String keyName = GOOGLE_BOOKS_API_CACHE_DIRECTORY + volumeId + ".json";
        
        return Mono.<S3FetchResult<String>>fromCallable(() -> {
            try {
                // Double-check client availability inside the async operation
                if (!isS3ClientAvailable()) {
                    return S3FetchResult.serviceError("S3Client became unavailable during fetch operation.");
                }
                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .build();

                ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getObjectRequest);
                String jsonString;
                String contentEncoding = objectBytes.response().contentEncoding();
                if (contentEncoding != null && contentEncoding.equalsIgnoreCase("gzip")) {
                    try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(objectBytes.asByteArray()));
                         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = gis.read(buffer)) > 0) {
                            baos.write(buffer, 0, len);
                        }
                        jsonString = baos.toString(StandardCharsets.UTF_8.name());
                    } catch (IOException e) { 
                        logger.error("IOException during GZIP decompression for key {}: {}", keyName, e.getMessage(), e);
                        return S3FetchResult.serviceError("Failed to decompress GZIP content for key " + keyName);
                    }
                } else {
                    jsonString = objectBytes.asUtf8String();
                }
                logger.info("Successfully fetched JSON from S3 key {}", keyName);
                return S3FetchResult.success(jsonString);
            } catch (NoSuchKeyException e) {
                logger.debug("JSON not found in S3 for key {}: {}", keyName, e.getMessage());
                return S3FetchResult.notFound();
            } catch (Exception e) {
                logger.error("Error fetching JSON from S3 for key {}: {}", keyName, e.getMessage(), e);
                return S3FetchResult.serviceError(e.getMessage());
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorReturn(S3FetchResult.serviceError("Failed to execute S3 fetch operation for key " + keyName))
        .toFuture();
    }

    /**
     * Asynchronously uploads a generic JSON string to a specified S3 key
     *
     * @param keyName The full S3 key (path/filename) under which to store the JSON
     * @param jsonContent The JSON string to upload
     * @param gzipCompress true to GZIP compress the content before uploading, false otherwise
     * @return A CompletableFuture<Void> that completes when the upload is finished or fails
     */
    public CompletableFuture<Void> uploadGenericJsonAsync(String keyName, String jsonContent, boolean gzipCompress) {
        if (!isS3ClientAvailable()) {
            logger.warn("S3Client is not available. Cannot upload generic JSON to key: {}. S3 may be disabled, misconfigured, or shut down.", keyName);
            return CompletableFuture.failedFuture(new IllegalStateException("S3Client is not available or has been shut down."));
        }
        return Mono.<Void>fromRunnable(() -> {
            try {
                // Double-check client availability inside the async operation
                if (!isS3ClientAvailable()) {
                    throw new IllegalStateException("S3Client became unavailable during async operation.");
                }
                
                byte[] contentBytes = jsonContent.getBytes(StandardCharsets.UTF_8);
                String contentEncodingHeader = null;

                if (gzipCompress) {
                    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                         GZIPOutputStream gzipOutputStream = new GZIPOutputStream(bos)) {
                        gzipOutputStream.write(contentBytes);
                        gzipOutputStream.finish();
                        contentBytes = bos.toByteArray();
                        contentEncodingHeader = "gzip";
                    } catch (IOException e) { 
                        logger.error("IOException during GZIP compression for key {}: {}", keyName, e.getMessage(), e);
                        throw new RuntimeException("Failed to GZIP compress content for key " + keyName, e);
                    }
                }

                PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .contentType("application/json");

                if (contentEncodingHeader != null) {
                    requestBuilder.contentEncoding(contentEncodingHeader);
                }
                
                PutObjectRequest putObjectRequest = requestBuilder.build();

                s3Client.putObject(putObjectRequest, RequestBody.fromBytes(contentBytes));
                logger.info("Successfully uploaded generic JSON to S3 key {}{}", keyName, gzipCompress ? " (GZIP compressed)" : "");
            } catch (IllegalStateException e) {
                if (e.getMessage() != null && e.getMessage().contains("Connection pool shut down")) {
                    logger.error("S3 connection pool was shut down during upload to key {}: {}", keyName, e.getMessage());
                    throw new PoolShutdownException("S3 connection pool shut down during operation for key " + keyName, e);
                }
                throw e;
            } catch (Exception e) { 
                logger.error("Error uploading generic JSON to S3 key {}: {}", keyName, e.getMessage(), e);
                throw new RuntimeException("Failed to upload generic JSON to S3 for key " + keyName, e);
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .toFuture();
    }

    /**
     * Asynchronously fetches a generic JSON file from a specified S3 key.
     * Automatically handles GZIP decompression if Content-Encoding header is 'gzip'.
     *
     * @param keyName The full S3 key (path/filename) from which to fetch the JSON.
     * @return A CompletableFuture<S3FetchResult<String>> containing the result status and optionally the JSON string if found.
     */
    public CompletableFuture<S3FetchResult<String>> fetchGenericJsonAsync(String keyName) {
        if (!isS3ClientAvailable()) {
            logger.warn("S3Client is not available. Cannot fetch generic JSON from key: {}. S3 may be disabled, misconfigured, or shut down.", keyName);
            return CompletableFuture.completedFuture(S3FetchResult.disabled());
        }
        
        return Mono.<S3FetchResult<String>>fromCallable(() -> {
            try {
                // Double-check client availability inside the async operation
                if (!isS3ClientAvailable()) {
                    return S3FetchResult.serviceError("S3Client became unavailable during fetch operation.");
                }
                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .build();

                ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getObjectRequest);
                String jsonString;
                
                String contentEncoding = objectBytes.response().contentEncoding();
                if (contentEncoding != null && contentEncoding.equalsIgnoreCase("gzip")) {
                    logger.debug("Attempting GZIP decompression for S3 key {}", keyName);
                    try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(objectBytes.asByteArray()));
                         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = gis.read(buffer)) > 0) {
                            baos.write(buffer, 0, len);
                        }
                        jsonString = baos.toString(StandardCharsets.UTF_8.name());
                    } catch (IOException e) { 
                        logger.error("IOException during GZIP decompression for generic key {}: {}", keyName, e.getMessage(), e);
                        return S3FetchResult.serviceError("Failed to decompress GZIP content for generic key " + keyName);
                    }
                } else {
                    logger.debug("Content for S3 key {} is not GZIP encoded or encoding not specified, reading as plain string.", keyName);
                    jsonString = objectBytes.asUtf8String();
                }
                logger.info("Successfully fetched generic JSON from S3 key {}", keyName);
                return S3FetchResult.success(jsonString);
            } catch (NoSuchKeyException e) {
                logger.debug("Generic JSON not found in S3 for key {}: {}", keyName, e.getMessage());
                return S3FetchResult.notFound();
            } catch (Exception e) { 
                logger.error("Error fetching generic JSON from S3 for key {}: {}", keyName, e.getMessage(), e);
                return S3FetchResult.serviceError(e.getMessage());
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorReturn(S3FetchResult.serviceError("Failed to execute S3 fetch operation for generic JSON key " + keyName))
        .toFuture();
    }

    /**
     * Gets the configured S3 bucket name.
     * @return The S3 bucket name.
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * Lists objects in the S3 bucket asynchronously, handling pagination.
     *
     * @param prefix The prefix to filter objects by (e.g., "covers/"). Can be empty or null.
     * @return A CompletableFuture<List<S3Object>> containing the list of S3Object summaries.
     */
    public CompletableFuture<List<S3Object>> listObjectsAsync(String prefix) {
        if (!isS3ClientAvailable()) {
            logger.warn("S3Client is not available. Cannot list objects. S3 may be disabled, misconfigured, or shut down.");
            return CompletableFuture.completedFuture(new ArrayList<>());
        }
        
        return Mono.<List<S3Object>>fromCallable(() -> {
            // Double-check client availability inside the async operation
            if (!isS3ClientAvailable()) {
                logger.warn("S3Client became unavailable during list operation.");
                return new ArrayList<>();
            }
            
            logger.info("Listing objects in bucket {} with prefix '{}'", bucketName, prefix);
            List<S3Object> allObjects = new ArrayList<>();
            String continuationToken = null;

            try {
                do {
                    ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                            .bucket(bucketName)
                            .continuationToken(continuationToken);

                    if (prefix != null && !prefix.isEmpty()) {
                        requestBuilder.prefix(prefix);
                    }

                    ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
                    int fetchedThisPage = response.contents().size();
                    allObjects.addAll(response.contents());
                    continuationToken = response.nextContinuationToken();
                    logger.debug("Fetched a page of {} S3 object(s). More pages to fetch: {}", fetchedThisPage, (continuationToken != null));
                } while (continuationToken != null);

                logger.info("Finished listing all S3 objects for prefix. Total objects found: {}", allObjects.size());
            } catch (S3Exception e) {
                logger.error("Error listing objects in S3 bucket {}: {}", bucketName, e.awsErrorDetails().errorMessage(), e);
                throw e; // Re-throw to be handled by onErrorResume
            } catch (Exception e) {
                logger.error("Unexpected error listing objects in S3 bucket {}: {}", bucketName, e.getMessage(), e);
                throw e; // Re-throw to be handled by onErrorResume
            }
            return allObjects;
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorReturn(new ArrayList<>())
        .toFuture();
    }

    /**
     * Downloads a file from S3 as a byte array asynchronously
     *
     * @param key The key of the object to download
     * @return A CompletableFuture<byte[]> containing the file data, or null if an error occurs or file not found
     */
    public CompletableFuture<byte[]> downloadFileAsBytesAsync(String key) {
        if (!isS3ClientAvailable()) {
            logger.warn("S3Client is not available. Cannot download file {}. S3 may be disabled, misconfigured, or shut down.", key);
            return CompletableFuture.completedFuture(null);
        }
        
        return Mono.<byte[]>fromCallable(() -> {
            // Double-check client availability inside the async operation
            if (!isS3ClientAvailable()) {
                logger.warn("S3Client became unavailable during download operation for key {}.", key);
                return null;
            }
            
            logger.debug("Attempting to download file {} from bucket {}", key, bucketName);
            try {
                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();
                ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getObjectRequest);
                logger.info("Successfully downloaded file {} from bucket {}", key, bucketName);
                return objectBytes.asByteArray();
            } catch (NoSuchKeyException e) {
                logger.warn("File not found in S3: bucket={}, key={}", bucketName, key);
                return null;
            } catch (S3Exception e) {
                logger.error("S3 error downloading file {} from bucket {}: {}", key, bucketName, e.awsErrorDetails().errorMessage(), e);
                return null;
            } catch (Exception e) {
                logger.error("Unexpected error downloading file {} from bucket {}: {}", key, bucketName, e.getMessage(), e);
                return null;
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorReturn(null)
        .toFuture();
    }

    /**
     * Copies an object within the S3 bucket asynchronously.
     *
     * @param sourceKey      The key of the source object.
     * @param destinationKey The key of the destination object.
     * @return A CompletableFuture<Boolean> indicating if the operation was successful.
     */
    public CompletableFuture<Boolean> copyObjectAsync(String sourceKey, String destinationKey) {
        if (!isS3ClientAvailable()) {
            logger.warn("S3Client is not available. Cannot copy object from {} to {}. S3 may be disabled, misconfigured, or shut down.", sourceKey, destinationKey);
            return CompletableFuture.completedFuture(false);
        }
        
        return Mono.<Boolean>fromCallable(() -> {
            // Double-check client availability inside the async operation
            if (!isS3ClientAvailable()) {
                logger.warn("S3Client became unavailable during copy operation from {} to {}.", sourceKey, destinationKey);
                return false;
            }
            
            logger.info("Attempting to copy object in bucket {} from key {} to key {}", bucketName, sourceKey, destinationKey);
            try {
                CopyObjectRequest copyReq = CopyObjectRequest.builder()
                        .sourceBucket(bucketName)
                        .sourceKey(sourceKey)
                        .destinationBucket(bucketName)
                        .destinationKey(destinationKey)
                        .build();

                s3Client.copyObject(copyReq);
                logger.info("Successfully copied object from {} to {}", sourceKey, destinationKey);
                return true;
            } catch (S3Exception e) {
                logger.error("S3 error copying object from {} to {}: {}", sourceKey, destinationKey, e.awsErrorDetails().errorMessage(), e);
                return false;
            } catch (Exception e) {
                logger.error("Unexpected error copying object from {} to {}: {}", sourceKey, destinationKey, e.getMessage(), e);
                return false;
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorReturn(false)
        .toFuture();
    }

    /**
     * Deletes an object from the S3 bucket asynchronously
     *
     * @param key The key of the object to delete
     * @return A CompletableFuture<Boolean> indicating if the operation was successful
     */
    public CompletableFuture<Boolean> deleteObjectAsync(String key) {
        if (!isS3ClientAvailable()) {
            logger.warn("S3Client is not available. Cannot delete object {}. S3 may be disabled, misconfigured, or shut down.", key);
            return CompletableFuture.completedFuture(false);
        }
        
        return Mono.<Boolean>fromCallable(() -> {
            // Double-check client availability inside the async operation
            if (!isS3ClientAvailable()) {
                logger.warn("S3Client became unavailable during delete operation for key {}.", key);
                return false;
            }
            
            logger.info("Attempting to delete object {} from bucket {}", key, bucketName);
            try {
                DeleteObjectRequest deleteReq = DeleteObjectRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build();
                logger.trace("Executing S3 deleteObject for key: {}", key);
                s3Client.deleteObject(deleteReq);
                logger.info("Successfully deleted object {}", key);
                return true;
            } catch (IllegalStateException ise) {
                if (ise.getMessage() != null && ise.getMessage().toLowerCase().contains("connection pool shut down")) {
                    logger.error("CRITICAL: HTTP Connection pool shutdown during S3 deleteObject for key {}: {}",key, ise.getMessage(), ise);
                    throw new PoolShutdownException("HTTP Connection pool shutdown during S3 deleteObject for key " + key, ise);
                } else {
                    logger.error("IllegalStateException during S3 deleteObject for key {}: {}",key, ise.getMessage(), ise);
                }
                return false;
            } catch (S3Exception e) {
                logger.error("S3 error deleting object {}: {}", key, e.awsErrorDetails().errorMessage(), e);
                return false;
            } catch (Exception e) {
                logger.error("Unexpected error deleting object {}: {}", key, e.getMessage(), e);
                return false;
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorReturn(false)
        .toFuture();
    }

    // Backward compatibility synchronous methods - deprecated in favor of async versions

    /**
     * @deprecated Use listObjectsAsync() instead for better performance and non-blocking behavior
     */
    @Deprecated
    public List<S3Object> listObjects(String prefix) {
        logger.warn("Deprecated synchronous listObjects called; use listObjectsAsync instead.");
        return listObjectsAsync(prefix).join();
    }

    /**
     * @deprecated Use downloadFileAsBytesAsync() instead for better performance and non-blocking behavior
     */
    @Deprecated
    public byte[] downloadFileAsBytes(String key) {
        logger.warn("Deprecated synchronous downloadFileAsBytes called; use downloadFileAsBytesAsync instead.");
        return downloadFileAsBytesAsync(key).join();
    }

    /**
     * @deprecated Use copyObjectAsync() instead for better performance and non-blocking behavior
     */
    @Deprecated
    public boolean copyObject(String sourceKey, String destinationKey) {
        logger.warn("Deprecated synchronous copyObject called; use copyObjectAsync instead.");
        return copyObjectAsync(sourceKey, destinationKey).join();
    }

    /**
     * @deprecated Use deleteObjectAsync() instead for better performance and non-blocking behavior
     */
    @Deprecated
    public boolean deleteObject(String key) {
        logger.warn("Deprecated synchronous deleteObject called; use deleteObjectAsync instead.");
        return deleteObjectAsync(key).join();
    }
}
