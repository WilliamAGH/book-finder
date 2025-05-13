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

import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

@Service
public class S3StorageService {
    private static final Logger logger = LoggerFactory.getLogger(S3StorageService.class);

    private final S3Client s3Client;
    private final String bucketName;
    private final String publicCdnUrl;

    @Autowired
    public S3StorageService(S3Client s3Client, 
                            @Value("${s3.bucket}") String bucketName,
                            @Value("${s3.public-cdn-url:#{null}}") String publicCdnUrl) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.publicCdnUrl = publicCdnUrl;
    }

    /**
     * Asynchronously uploads a file to the S3 bucket.
     *
     * @param keyName The key (path/filename) under which to store the file in the bucket.
     * @param inputStream The InputStream of the file to upload.
     * @param contentLength The length of the content to be uploaded.
     * @param contentType The MIME type of the file.
     * @return A CompletableFuture<String> with the public URL of the uploaded file, or null if upload failed.
     */
    public CompletableFuture<String> uploadFileAsync(String keyName, InputStream inputStream, long contentLength, String contentType) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Create a byte array from the input stream to avoid closed channel issues
                byte[] bytes = inputStream.readAllBytes();
                
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(keyName)
                        .contentType(contentType)
                        .build();

                s3Client.putObject(putObjectRequest, RequestBody.fromBytes(bytes));
                
                logger.info("Successfully uploaded {} to S3 bucket {}", keyName, bucketName);

                // Construct the public URL. Ensure no double slashes.
                if (publicCdnUrl == null || publicCdnUrl.isEmpty()) {
                    // Fall back to bucket URL if no CDN URL is provided
                    return String.format("https://%s.s3.amazonaws.com/%s", bucketName, 
                        keyName.startsWith("/") ? keyName.substring(1) : keyName);
                }
                
                String cdn = publicCdnUrl.endsWith("/") ? publicCdnUrl : publicCdnUrl + "/";
                String key = keyName.startsWith("/") ? keyName.substring(1) : keyName;
                return cdn + key;
            } catch (S3Exception e) {
                logger.error("Error uploading file {} to S3: {}", keyName, e.awsErrorDetails().errorMessage(), e);
                return null;
            } catch (Exception e) {
                logger.error("Unexpected error uploading file {} to S3: {}", keyName, e.getMessage(), e);
                return null;
            }
        });
    }
} 