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

import jakarta.annotation.PostConstruct;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import com.williamcallahan.book_recommendation_engine.service.s3.S3FetchResult;
import com.williamcallahan.book_recommendation_engine.util.CompressionUtils;
import com.williamcallahan.book_recommendation_engine.util.S3Paths;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

@Service
@Conditional(S3EnvironmentCondition.class)
public class S3StorageService {
    private static final Logger logger = LoggerFactory.getLogger(S3StorageService.class);


    private final S3Client s3Client;
    private final String bucketName;
    private final String publicCdnUrl;
    private final String serverUrl;

    /**
     * Constructs an S3StorageService with required dependencies
     * - Initializes AWS S3 client for bucket operations
     * - Configures bucket name from application properties
     * - Sets up optional CDN URL for public access
     * - Sets up optional server URL for DigitalOcean Spaces
     *
     * @param s3Client AWS S3 client for interacting with buckets
     * @param bucketName Name of the S3 bucket to use for storage
     * @param publicCdnUrl Optional CDN URL for public access to files
     * @param serverUrl Optional server URL for DigitalOcean Spaces
     */
    public S3StorageService(S3Client s3Client, 
                            @Value("${s3.bucket-name:${S3_BUCKET}}") String bucketName,
                            @Value("${s3.cdn-url:${S3_CDN_URL:#{null}}}") String publicCdnUrl,
                            @Value("${s3.server-url:${S3_SERVER_URL:#{null}}}") String serverUrl) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.publicCdnUrl = publicCdnUrl;
        this.serverUrl = serverUrl;
    }

    @PostConstruct
    void validateConfiguration() {
        if (s3Client == null) {
            logger.warn("S3StorageService initialized without an S3 client. All S3 operations will be disabled.");
            return;
        }
        if (bucketName == null || bucketName.isBlank()) {
            throw new IllegalStateException("S3 bucket name must be configured when S3StorageService is active.");
        }
    }

    /**
     * Asynchronously uploads a file to the S3 bucket
     *
     * @param keyName The key (path/filename) under which to store the file in the bucket
     * @param inputStream The InputStream of the file to upload
     * @param contentLength The length of the content to be uploaded
     * @param contentType The MIME type of the file
     * @return A CompletableFuture<String> with the public URL of the uploaded file, or null if upload failed
     */
    public CompletableFuture<String> uploadFileAsync(String keyName, InputStream inputStream, long contentLength, String contentType) {
        return Mono.fromCallable(() -> {
            try {
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
            } catch (S3Exception e) {
                logger.error("Error uploading file {} to S3: {}", keyName, e.awsErrorDetails().errorMessage(), e);
                throw e; // Re-throw to be handled by onErrorResume
            } catch (Exception e) {
                logger.error("Unexpected error uploading file {} to S3: {}", keyName, e.getMessage(), e);
                throw e; // Re-throw to be handled by onErrorResume
            }
        })
        .subscribeOn(Schedulers.boundedElastic()) // Execute the blocking call on an I/O-optimized scheduler
        .onErrorResume(e -> {
            // Log already happened in the callable, rethrow the exception
            return Mono.error(e); 
        })
        .toFuture(); // Convert the Mono to CompletableFuture
    }

    /**
     * Asynchronously uploads a JSON string as a GZIP-compressed file to S3.
     *
     * @param volumeId The Google Books volume ID, used to construct the S3 key.
     * @param jsonContent The JSON string to upload.
     * @return A CompletableFuture<Void> that completes when the upload is finished or fails.
     */
    public CompletableFuture<Void> uploadJsonAsync(String volumeId, String jsonContent) {
        if (s3Client == null) {
            logger.warn("S3Client is null. Cannot upload JSON for volumeId: {}. S3 may be disabled or misconfigured.", volumeId);
            return CompletableFuture.failedFuture(new IllegalStateException("S3Client is not available."));
        }
        String keyName = S3Paths.GOOGLE_BOOK_CACHE_PREFIX + volumeId + ".json";
        return uploadGenericJsonAsync(keyName, jsonContent, true);
    }
    
    /**
     * Asynchronously fetches and GZIP-decompresses a JSON file from S3.
     *
     * @param volumeId The Google Books volume ID, used to construct the S3 key.
     * @return A CompletableFuture<S3FetchResult<String>> containing the result status and optionally the JSON string if found
     */
    public CompletableFuture<S3FetchResult<String>> fetchJsonAsync(String volumeId) {
        if (s3Client == null) {
            logger.warn("S3Client is null. Cannot fetch JSON for volumeId: {}. S3 may be disabled or misconfigured.", volumeId);
            return CompletableFuture.completedFuture(S3FetchResult.disabled());
        }
        String keyName = S3Paths.GOOGLE_BOOK_CACHE_PREFIX + volumeId + ".json";
        return fetchGenericJsonAsync(keyName);
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
        if (s3Client == null) {
            logger.warn("S3Client is null. Cannot upload generic JSON to key: {}. S3 may be disabled or misconfigured.", keyName);
            return CompletableFuture.failedFuture(new IllegalStateException("S3Client is not available."));
        }
        return Mono.<Void>fromRunnable(() -> {
            try {
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
        if (s3Client == null) {
            logger.warn("S3Client is null. Cannot fetch generic JSON from key: {}. S3 may be disabled or misconfigured.", keyName);
            return CompletableFuture.completedFuture(S3FetchResult.disabled());
        }
        
        return Mono.<S3FetchResult<String>>fromCallable(() -> {
            try {
                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .build();

                ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getObjectRequest);
                String jsonString;

                String contentEncoding = objectBytes.response().contentEncoding();
                byte[] payload = objectBytes.asByteArray();
                if (contentEncoding != null && contentEncoding.equalsIgnoreCase("gzip")) {
                    logger.debug("Attempting GZIP decompression for S3 key {}", keyName);
                    try {
                        jsonString = CompressionUtils.decodeUtf8ExpectingGzip(payload);
                    } catch (IOException e) {
                        logger.error("IOException during GZIP decompression for generic key {}: {}", keyName, e.getMessage(), e);
                        return S3FetchResult.serviceError("Failed to decompress GZIP content for generic key " + keyName);
                    }
                } else {
                    logger.debug("Content for S3 key {} is not GZIP encoded or encoding not specified, attempting direct UTF-8 decode.", keyName);
                    jsonString = CompressionUtils.decodeUtf8WithOptionalGzip(payload);
                    if (jsonString == null) {
                        return S3FetchResult.serviceError("Failed to decode content for key " + keyName);
                    }
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
     * Lists objects in the S3 bucket, handling pagination.
     *
     * @param prefix The prefix to filter objects by (e.g., "covers/"). Can be empty or null.
     * @return A list of S3Object summaries.
     */
    public List<S3Object> listObjects(String prefix) {
        if (s3Client == null) {
            logger.warn("S3Client is null. Cannot list objects. S3 may be disabled or misconfigured.");
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
            // Depending on requirements, might rethrow or return empty/partial list
        } catch (Exception e) {
            logger.error("Unexpected error listing objects in S3 bucket {}: {}", bucketName, e.getMessage(), e);
        }
        return allObjects;
    }

    /**
     * Downloads a file from S3 as a byte array
     *
     * @param key The key of the object to download
     * @return A byte array containing the file data, or null if an error occurs or file not found
     */
    public byte[] downloadFileAsBytes(String key) {
        if (s3Client == null) {
            logger.warn("S3Client is null. Cannot download file {}. S3 may be disabled or misconfigured.", key);
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
    }

    /**
     * Copies an object within the S3 bucket.
     *
     * @param sourceKey      The key of the source object.
     * @param destinationKey The key of the destination object.
     * @return true if successful, false otherwise.
     */
    public boolean copyObject(String sourceKey, String destinationKey) {
        if (s3Client == null) {
            logger.warn("S3Client is null. Cannot copy object from {} to {}. S3 may be disabled or misconfigured.", sourceKey, destinationKey);
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
    }

    /**
     * Deletes an object from the S3 bucket
     *
     * @param key The key of the object to delete
     * @return true if successful, false otherwise
     */
    public boolean deleteObject(String key) {
        if (s3Client == null) {
            logger.warn("S3Client is null. Cannot delete object {}. S3 may be disabled or misconfigured.", key);
            return false;
        }
        logger.info("Attempting to delete object {} from bucket {}", key, bucketName);
        try {
            DeleteObjectRequest deleteReq = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            s3Client.deleteObject(deleteReq);
            logger.info("Successfully deleted object {}", key);
            return true;
        } catch (S3Exception e) {
            logger.error("S3 error deleting object {}: {}", key, e.awsErrorDetails().errorMessage(), e);
            return false;
        } catch (Exception e) {
            logger.error("Unexpected error deleting object {}: {}", key, e.getMessage(), e);
            return false;
        }
    }
}
