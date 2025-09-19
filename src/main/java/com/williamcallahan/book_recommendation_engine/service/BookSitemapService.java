/**
 * Service for managing book IDs in S3 for sitemap generation
 *
 * @author William Callahan
 *
 * Key responsibilities:
 * - Fetch accumulated book IDs from S3
 * - Manage existing accumulated book IDs in S3
 * - Handle errors gracefully for sitemap generation
 * Note: Cache functionality has been removed
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.core.ResponseInputStream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class BookSitemapService {

    private static final Logger logger = LoggerFactory.getLogger(BookSitemapService.class);

    private final S3Client s3Client;
    private final ObjectMapper objectMapper;
    private final String s3BucketName;
    private final String accumulatedIdsS3Key;

    /**
     * Constructs BookSitemapService with required dependencies
     * - Initializes S3 client for storage operations
     * - Configures JSON processing capabilities
     * - Sets S3 bucket and key parameters from configuration
     * Note: Cache service functionality has been removed
     *
     * @param s3Client AWS S3 client for bucket operations
     * @param objectMapper JSON object mapper for serialization
     * @param s3BucketName Name of S3 bucket from configuration
     * @param accumulatedIdsS3Key S3 key for accumulated IDs file
     */
    public BookSitemapService(S3Client s3Client,
                              ObjectMapper objectMapper,
                              @Value("${s3.bucket-name}") String s3BucketName,
                              @Value("${sitemap.s3.accumulated-ids-key}") String accumulatedIdsS3Key) {
        this.s3Client = s3Client;
        this.objectMapper = objectMapper;
        this.s3BucketName = s3BucketName;
        this.accumulatedIdsS3Key = accumulatedIdsS3Key;
    }

    /**
     * Internal method to fetch and parse book IDs from S3
     * Handles empty streams and parsing errors with appropriate fallbacks
     * 
     * @return Set of book IDs from S3
     * @throws IOException if fetch or parse fails
     */
    private Set<String> fetchAndParseBookIdsFromS3Internal() throws IOException {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3BucketName)
                .key(accumulatedIdsS3Key)
                .build();
        try (ResponseInputStream<GetObjectResponse> s3ObjectStream = s3Client.getObject(getObjectRequest)) {
            // Attempt to parse the JSON stream directly to a Set of Strings
            // Empty or malformed content is handled in the catch blocks
            Set<String> bookIds = objectMapper.readValue(s3ObjectStream, new TypeReference<Set<String>>() {});
            if (bookIds == null) {
                logger.info("S3 file '{}' parsed to null. Returning new HashSet to be safe.", accumulatedIdsS3Key);
                return new HashSet<>();
            }
            return bookIds;
        } catch (NoSuchKeyException e) {
            logger.info("Accumulated book IDs file not found in S3 ('{}'). Returning new HashSet for initial creation.", accumulatedIdsS3Key);
            return new HashSet<>();
        } catch (IOException e) {
            // Handle empty content case gracefully
            if (e instanceof com.fasterxml.jackson.databind.exc.MismatchedInputException && e.getMessage().toLowerCase().contains("no content to map")) {
                logger.info("S3 file '{}' is empty or has no content to map. Returning new HashSet.", accumulatedIdsS3Key);
                return new HashSet<>();
            }
            logger.error("IOException during S3 getObject or parsing for key '{}': {}", accumulatedIdsS3Key, e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error fetching/parsing '{}' from S3: {}", accumulatedIdsS3Key, e.getMessage());
            throw new IOException("Failed to fetch or parse S3 object: " + accumulatedIdsS3Key, e);
        }
    }

    /**
     * Get accumulated book IDs from S3 for sitemap generation
     * Provides a non-throwing facade for the internal fetch method
     * 
     * @return Set of book IDs, empty set on error
     */
    public Set<String> getAccumulatedBookIdsFromS3() {
        try {
            return fetchAndParseBookIdsFromS3Internal();
        } catch (IOException e) {
            logger.warn("Failed to get accumulated book IDs from S3 for sitemap generation (key: {}). Returning empty set. Error: {}", accumulatedIdsS3Key, e.getMessage());
            return Collections.emptySet();
        }
    }

    /**
     * Update accumulated book IDs in S3 with new IDs from cache
     * Fetches existing IDs from S3, merges with current IDs from cache,
     * and uploads the combined set only if changes are detected
     * 
     * Process flow:
     * 1. Fetch existing accumulated book IDs from S3
     * 2. Retrieve current book IDs from cache 
     * 3. Merge both sets and detect changes
     * 4. If changes found, sort and upload to S3
     */
    public void updateAccumulatedBookIdsInS3() {
        logger.info("Starting update of accumulated book IDs in S3 (key: {}).", accumulatedIdsS3Key);
        
        // Step 1: Fetch existing IDs from S3
        Set<String> accumulatedIds;
        try {
            accumulatedIds = fetchAndParseBookIdsFromS3Internal();
        } catch (IOException e) {
            logger.error("CRITICAL: Failed to read existing accumulated book IDs from S3 (key: {}). Aborting update cycle. Error: {}", accumulatedIdsS3Key, e.getMessage());
            return;
        }
        
        int initialSize = accumulatedIds.size();
        logger.info("Fetched {} existing book IDs from S3.", initialSize);

        // Step 2: Note - Cache functionality has been removed
        // No new IDs to fetch from cache, so we just work with existing accumulated IDs
        logger.info("Cache functionality has been removed. Working with existing {} accumulated IDs.", initialSize);

        // Step 3: Since no new IDs are being added, check if we should still upload
        // (for example, if the format needs to be updated or file doesn't exist)
        if (initialSize > 0) {
            logger.info("Updating S3 file format. Total accumulated IDs: {}. Sorting and uploading to S3.", accumulatedIds.size());
            try {
                // Convert Set to List and sort alphabetically for consistent storage
                List<String> sortedIds = new ArrayList<>(accumulatedIds);
                Collections.sort(sortedIds);

                String updatedJsonContent = objectMapper.writeValueAsString(sortedIds);
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(s3BucketName)
                        .key(accumulatedIdsS3Key)
                        .contentType("application/json")
                        .build();
                s3Client.putObject(putObjectRequest, RequestBody.fromString(updatedJsonContent, StandardCharsets.UTF_8));
                logger.info("Successfully uploaded {} sorted accumulated book IDs to S3 (key: {}).", sortedIds.size(), accumulatedIdsS3Key);
            } catch (IOException e) {
                logger.error("Error serializing or uploading sorted accumulated book IDs to S3 (key: {}): {}", accumulatedIdsS3Key, e.getMessage());
            } catch (Exception e) {
                logger.error("Unexpected error writing accumulated book IDs to S3 (key: {}): {}", accumulatedIdsS3Key, e.getMessage());
            }
        } else {
            logger.info("No accumulated IDs to process. S3 file (key: {}) remains unchanged.", accumulatedIdsS3Key);
        }
    }
}
