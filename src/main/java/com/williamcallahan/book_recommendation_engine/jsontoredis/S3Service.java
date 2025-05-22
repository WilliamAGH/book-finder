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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Service("jsonS3ToRedis_S3Service")
@Profile("jsontoredis")
public class S3Service {

    private static final Logger log = LoggerFactory.getLogger(S3Service.class);

    private final S3Client s3Client;
    private final String bucketName;
    private final String googleBooksPrefix;
    private final String nytBestsellersKey;

    public S3Service(S3Client s3Client, // Injects the existing S3Client bean from S3Config.java
                     @Value("${s3.bucket-name}") String bucketName, // Uses existing bucket name config
                     @Value("${jsontoredis.s3.google-books-prefix}") String googleBooksPrefix,
                     @Value("${jsontoredis.s3.nyt-bestsellers-key}") String nytBestsellersKey) {
        if (s3Client == null) {
            throw new IllegalStateException("S3Client is not available. Ensure S3 is enabled and configured correctly in S3Config.");
        }
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.googleBooksPrefix = googleBooksPrefix;
        this.nytBestsellersKey = nytBestsellersKey;
        log.info("jsonS3ToRedis S3Service initialized with bucket: {}, googleBooksPrefix: {}, nytBestsellersKey: {}", bucketName, googleBooksPrefix, nytBestsellersKey);
    }

    /**
     * Lists object keys under a given prefix in the configured bucket
     * Handles pagination using AWS SDK v2
     * 
     * @param prefix The S3 key prefix to list objects under
     * @return List of S3 object keys matching the prefix
     */
    public List<String> listObjectKeys(String prefix) {
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
            listReq = listReq.toBuilder().continuationToken(listRes.nextContinuationToken()).build();
        } while (Boolean.TRUE.equals(listRes.isTruncated())); // Handle null for isTruncated

        log.info("Found {} objects matching prefix {}", keys.size(), prefix);
        return keys;
    }

    /**
     * Gets the content of an S3 object as an InputStream
     * Caller is responsible for closing the stream
     * Uses AWS SDK v2
     * 
     * @param key The S3 object key to retrieve
     * @return InputStream containing the object's content
     * @throws RuntimeException if retrieving the object fails
     */
    public InputStream getObjectContent(String key) {
        log.debug("Getting object content for S3 key: {} from bucket: {}", key, bucketName);
        GetObjectRequest getReq = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        try {
            ResponseInputStream<GetObjectResponse> s3ObjectStream = s3Client.getObject(getReq);
            log.info("Successfully retrieved S3 object stream for key: {}", key);
            return s3ObjectStream;
        } catch (IllegalStateException ise) {
            log.error("IllegalStateException (e.g., connection pool shut down) while getting S3 object for key {}: {}", key, ise.getMessage(), ise);
            throw new RuntimeException("Failed to get S3 object content for key " + key + " due to IllegalStateException.", ise);
        } catch (NoSuchKeyException e) {
            log.warn("S3 key not found: {} (bucket {})", key, bucketName);
            throw e;
        } catch (Exception e) {
            log.error("Generic error getting S3 object content for key {}: {}", key, e.getMessage(), e);
            throw new RuntimeException("Failed to get S3 object content for key " + key, e);
        }
    }

    /**
     * Get the configured Google Books S3 prefix
     * 
     * @return The Google Books prefix string
     */
    public String getGoogleBooksPrefix() {
        return googleBooksPrefix;
    }

    /**
     * Get the configured NYT Bestsellers S3 key
     * 
     * @return The NYT Bestsellers key string
     */
    public String getNytBestsellersKey() {
        return nytBestsellersKey;
    }

    /**
     * Moves an S3 object to a new key within the same bucket
     * This is typically a copy then delete operation
     *
     * @param sourceKey      The current key of the object
     * @param destinationKey The new key for the object
     */
    public void moveObject(String sourceKey, String destinationKey) {
        log.debug("Moving S3 object from {} to {} in bucket {}", sourceKey, destinationKey, bucketName);
        try {
            // Copy object
            CopyObjectRequest copyReq = CopyObjectRequest.builder()
                    .sourceBucket(bucketName)
                    .sourceKey(sourceKey)
                    .destinationBucket(bucketName)
                    .destinationKey(destinationKey)
                    .build();
            s3Client.copyObject(copyReq);
            log.info("Successfully copied S3 object from {} to {}", sourceKey, destinationKey);

            // Delete original object
            DeleteObjectRequest deleteReq = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(sourceKey)
                    .build();
            s3Client.deleteObject(deleteReq);
            log.info("Successfully deleted original S3 object {}", sourceKey);

        } catch (NoSuchKeyException e) {
            log.error("Cannot move S3 object: Source key {} not found in bucket {}.", sourceKey, bucketName, e);
            // Depending on requirements, you might rethrow or handle this (e.g., if already moved)
        } catch (Exception e) {
            log.error("Error moving S3 object from {} to {}: {}", sourceKey, destinationKey, e.getMessage(), e);
            // Consider how to handle partial failures (e.g., copy succeeded but delete failed)
            throw new RuntimeException("Failed to move S3 object from " + sourceKey + " to " + destinationKey, e);
        }
    }
}
