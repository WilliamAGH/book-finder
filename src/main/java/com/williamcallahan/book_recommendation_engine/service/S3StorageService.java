package com.williamcallahan.book_recommendation_engine.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

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
@Service
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
                // Create a byte array from the input stream to avoid closed channel issues
                // This readAllBytes itself is blocking, but it's part of the callable on boundedElastic.
                byte[] bytes = inputStream.readAllBytes();
                
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .contentType(contentType)
                        .build();

                s3Client.putObject(putObjectRequest, RequestBody.fromBytes(bytes));
                
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
            // Log already happened in the callable, just return null for the CompletableFuture
            return Mono.justOrEmpty(null); 
        })
        .toFuture(); // Convert the Mono to CompletableFuture
    }
}
