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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
@Service
public class S3StorageService {
    private static final Logger logger = LoggerFactory.getLogger(S3StorageService.class);
    private static final String GOOGLE_BOOKS_API_CACHE_DIRECTORY = "books/v1/";


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
    @Autowired
    public S3StorageService(S3Client s3Client, 
                            @Value("${s3.bucket-name:${S3_BUCKET}}") String bucketName,
                            @Value("${s3.cdn-url:${S3_CDN_URL:#{null}}}") String publicCdnUrl,
                            @Value("${s3.server-url:${S3_SERVER_URL:#{null}}}") String serverUrl) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.publicCdnUrl = publicCdnUrl;
        this.serverUrl = serverUrl;
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
        return Mono.<Void>fromRunnable(() -> {
            String keyName = GOOGLE_BOOKS_API_CACHE_DIRECTORY + volumeId + ".json";
            try {
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .contentType("application/json")
                        .build();

                s3Client.putObject(putObjectRequest, RequestBody.fromString(jsonContent, StandardCharsets.UTF_8));
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
     * @return A CompletableFuture<Optional<String>> containing the decompressed JSON string if found,
     *         or an empty Optional if not found or an error occurs.
     */
    public CompletableFuture<Optional<String>> fetchJsonAsync(String volumeId) {
        if (s3Client == null) {
            logger.warn("S3Client is null. Cannot fetch JSON for volumeId: {}. S3 may be disabled or misconfigured.", volumeId);
            return CompletableFuture.completedFuture(Optional.empty());
        }
        String keyName = GOOGLE_BOOKS_API_CACHE_DIRECTORY + volumeId + ".json";
        
        return Mono.<Optional<String>>fromCallable(() -> {
            try {
                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .build();

                ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getObjectRequest);
                String jsonString = objectBytes.asUtf8String();

                logger.debug("Successfully fetched JSON for volumeId {} from S3 key {}", volumeId, keyName);
                return Optional.of(jsonString);
            } catch (NoSuchKeyException e) {
                logger.debug("JSON for volumeId {} not found in S3 at key {}.", volumeId, keyName);
                return Optional.empty();
            } catch (Exception e) {
                logger.error("Error fetching JSON for volumeId {} from S3 key {}: {}", volumeId, keyName, e.getMessage(), e);
                return Optional.empty(); // Return empty on other errors as well
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .toFuture();
    }
}
