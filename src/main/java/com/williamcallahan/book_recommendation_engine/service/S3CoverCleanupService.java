/**
 * Service for cleaning up problematic book cover images in S3
 *
 * @author William Callahan
 *
 * Features:
 * - Scans S3 bucket for potentially bad cover images
 * - Uses ImageProcessingService for content analysis
 * - Identifies covers with predominantly white backgrounds
 * - Supports dry run mode for safe evaluation
 * - Provides detailed logging of scan results
 * - Handles S3 object retrieval and analysis efficiently
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.service.image.ImageProcessingService;
import com.williamcallahan.book_recommendation_engine.types.DryRunSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class S3CoverCleanupService {

    private static final Logger logger = LoggerFactory.getLogger(S3CoverCleanupService.class);

    private final S3StorageService s3StorageService;
    private final ImageProcessingService imageProcessingService;

    /**
     * Constructs the S3CoverCleanupService with required dependencies
     *
     * @param s3StorageService service for S3 operations
     * @param imageProcessingService service for analyzing image content
     */
    public S3CoverCleanupService(S3StorageService s3StorageService, ImageProcessingService imageProcessingService) {
        this.s3StorageService = s3StorageService;
        this.imageProcessingService = imageProcessingService;
    }

    /**
     * Performs a dry run of the S3 cover cleanup process
     * It lists objects in the S3 bucket, downloads each, analyzes them,
     * and logs those identified as potentially "bad" (e.g., dominantly white)
     * No actual delete or move operations are performed in this dry run
     * @param s3Prefix The S3 prefix to scan - can be empty to scan the whole bucket.
     * @param batchLimit The maximum number of records to process in this run.
     * @return DryRunSummary containing the counts of scanned and flagged items.
     */
    public DryRunSummary performDryRun(String s3Prefix, int batchLimit) {
        logger.info("Starting S3 Cover Cleanup DRY RUN for prefix '{}', batch limit {}...", s3Prefix, batchLimit);

        String bucketName = s3StorageService.getBucketName();
        if (bucketName == null || bucketName.isEmpty()) {
            logger.error("S3 bucket name is not configured. Aborting dry run.");
            return new DryRunSummary(0, 0, new ArrayList<>());
        }
        logger.info("Target S3 Bucket: {}", bucketName);
        
        String prefix = s3Prefix;

        AtomicInteger totalScanned = new AtomicInteger(0);
        AtomicInteger totalFlagged = new AtomicInteger(0);
        List<String> flaggedKeysList = new ArrayList<>();

        try {
            List<S3Object> allS3Objects = s3StorageService.listObjects(prefix);
            logger.info("Found {} total objects in bucket {} with prefix '{}' to scan.", allS3Objects.size(), bucketName, prefix);

            List<S3Object> objectsToProcess = allS3Objects;
            if (batchLimit > 0 && allS3Objects.size() > batchLimit) {
                objectsToProcess = allS3Objects.subList(0, batchLimit);
                logger.info("Processing a batch of {} objects due to limit {}.", objectsToProcess.size(), batchLimit);
            } else {
                logger.info("Processing all {} found objects (batch limit {} not exceeded or not set).", objectsToProcess.size(), batchLimit);
            }

            for (S3Object s3Object : objectsToProcess) {
                String key = s3Object.key();
                totalScanned.incrementAndGet();
                logger.debug("Processing S3 object: {} (Size: {} bytes)", key, s3Object.size());

                if (s3Object.size() == null || s3Object.size() == 0) {
                    logger.warn("S3 object {} is empty or size is unknown. Skipping.", key);
                    continue;
                }

                try {
                    byte[] imageData = s3StorageService.downloadFileAsBytes(key);
                    if (imageData == null || imageData.length == 0) {
                        logger.warn("Failed to download or received empty data for S3 object: {}. Skipping.", key);
                        continue;
                    }

                    // Analyze the image
                    boolean isBadCover = imageProcessingService.isDominantlyWhite(imageData, key);

                    if (isBadCover) {
                        totalFlagged.incrementAndGet();
                        flaggedKeysList.add(key); // Add key to the list
                        logger.info("[FLAGGED] S3 object: {} - Identified as potentially bad cover.", key);
                    } else {
                        logger.debug("S3 object: {} - Analysis: OK.", key);
                    }

                } catch (Exception e) {
                    logger.error("Error processing S3 object: {}. Error: {}", key, e.getMessage(), e);
                }
            }

        } catch (Exception e) {
            logger.error("Failed to list or process objects from S3 bucket: {}. Error: {}", bucketName, e.getMessage(), e);
        }

        logger.info("S3 Cover Cleanup DRY RUN Finished.");
        logger.info("Summary: Total Objects Scanned: {}, Total Objects Flagged: {}", totalScanned.get(), totalFlagged.get());
        return new DryRunSummary(totalScanned.get(), totalFlagged.get(), flaggedKeysList);
    }

    /**
     * Performs the action of moving flagged S3 cover images to a quarantine prefix.
     *
     * @param s3Prefix The S3 prefix to scan for original images.
     * @param batchLimit The maximum number of records to process in this run.
     * @param quarantinePrefix The S3 prefix to move flagged images to.
     * @return MoveActionSummary containing counts and lists of processed, moved, and failed items.
     */
    public com.williamcallahan.book_recommendation_engine.types.MoveActionSummary performMoveAction(String s3Prefix, int batchLimit, String quarantinePrefix) {
        logger.info("Starting S3 Cover Cleanup MOVE ACTION for prefix '{}', batch limit {}, target quarantine prefix '{}'...",
                s3Prefix, batchLimit, quarantinePrefix);

        String bucketName = s3StorageService.getBucketName();
        if (bucketName == null || bucketName.isEmpty()) {
            logger.error("S3 bucket name is not configured. Aborting move action.");
            return new com.williamcallahan.book_recommendation_engine.types.MoveActionSummary(0, 0, 0, 0, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        }
        if (quarantinePrefix == null || quarantinePrefix.isEmpty() || quarantinePrefix.equals(s3Prefix)) {
            logger.error("Quarantine prefix is invalid (null, empty, or same as source prefix: '{}'). Aborting move action.", quarantinePrefix);
            return new com.williamcallahan.book_recommendation_engine.types.MoveActionSummary(0, 0, 0, 0, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        }
        
        // Ensure quarantinePrefix ends with a slash if it's not empty
        String normalizedQuarantinePrefix = quarantinePrefix;
        if (!normalizedQuarantinePrefix.isEmpty() && !normalizedQuarantinePrefix.endsWith("/")) {
            normalizedQuarantinePrefix += "/";
        }


        AtomicInteger totalScanned = new AtomicInteger(0);
        AtomicInteger totalFlagged = new AtomicInteger(0);
        AtomicInteger successfullyMoved = new AtomicInteger(0);
        AtomicInteger failedToMove = new AtomicInteger(0);
        List<String> flaggedKeysList = new ArrayList<>();
        List<String> movedFileKeys = new ArrayList<>(); // Stores original keys of moved files
        List<String> failedMoveFileKeys = new ArrayList<>(); // Stores original keys of files that failed to move

        try {
            List<S3Object> allS3Objects = s3StorageService.listObjects(s3Prefix);
            logger.info("Found {} total objects in bucket {} with prefix '{}' to scan for move action.", allS3Objects.size(), bucketName, s3Prefix);

            List<S3Object> objectsToProcess = allS3Objects;
            if (batchLimit > 0 && allS3Objects.size() > batchLimit) {
                objectsToProcess = allS3Objects.subList(0, batchLimit);
                logger.info("Processing a batch of {} objects for move action due to limit {}.", objectsToProcess.size(), batchLimit);
            } else {
                logger.info("Processing all {} found objects for move action (batch limit {} not exceeded or not set).", objectsToProcess.size(), batchLimit);
            }

            for (S3Object s3Object : objectsToProcess) {
                String sourceKey = s3Object.key();
                totalScanned.incrementAndGet();
                logger.debug("Processing S3 object for move: {} (Size: {} bytes)", sourceKey, s3Object.size());

                if (s3Object.size() == null || s3Object.size() == 0) {
                    logger.warn("S3 object {} is empty or size is unknown. Skipping.", sourceKey);
                    continue;
                }

                try {
                    byte[] imageData = s3StorageService.downloadFileAsBytes(sourceKey);
                    if (imageData == null || imageData.length == 0) {
                        logger.warn("Failed to download or received empty data for S3 object: {}. Skipping.", sourceKey);
                        continue;
                    }

                    boolean isBadCover = imageProcessingService.isDominantlyWhite(imageData, sourceKey);

                    if (isBadCover) {
                        totalFlagged.incrementAndGet();
                        flaggedKeysList.add(sourceKey);
                        logger.info("[FLAGGED FOR MOVE] S3 object: {}", sourceKey);

                        String originalFileName;
                        if (sourceKey.startsWith(s3Prefix)) {
                            originalFileName = sourceKey.substring(s3Prefix.length());
                        } else {
                            originalFileName = sourceKey;
                        }
                        if (originalFileName.startsWith("/")) {
                            originalFileName = originalFileName.substring(1);
                        }
                        String destinationKey = normalizedQuarantinePrefix + originalFileName;
                        
                        logger.info("Attempting to move {} to {}", sourceKey, destinationKey);
                        boolean copySuccess = s3StorageService.copyObject(sourceKey, destinationKey);
                        if (copySuccess) {
                            logger.info("Successfully copied {} to {}. Attempting to delete original.", sourceKey, destinationKey);
                            boolean deleteSuccess = s3StorageService.deleteObject(sourceKey);
                            if (deleteSuccess) {
                                successfullyMoved.incrementAndGet();
                                movedFileKeys.add(sourceKey + " -> " + destinationKey);
                                logger.info("Successfully moved {} to {}", sourceKey, destinationKey);
                            } else {
                                failedToMove.incrementAndGet();
                                failedMoveFileKeys.add(sourceKey);
                                logger.error("Failed to delete original object {} after copying to {}. Manual cleanup of original might be needed.", sourceKey, destinationKey);
                            }
                        } else {
                            failedToMove.incrementAndGet();
                            failedMoveFileKeys.add(sourceKey);
                            logger.error("Failed to copy object {} to {}. Object not moved.", sourceKey, destinationKey);
                        }
                    } else {
                        logger.debug("S3 object: {} - Analysis: OK. No move action needed.", sourceKey);
                    }

                } catch (Exception e) {
                    logger.error("Error processing S3 object {} for move action: {}", sourceKey, e.getMessage(), e);
                    failedToMove.incrementAndGet(); // Count as failed if processing itself fails
                    failedMoveFileKeys.add(sourceKey);
                }
            }

        } catch (Exception e) {
            logger.error("Failed to list or process objects from S3 bucket {} for move action: {}", bucketName, e.getMessage(), e);
        }

        logger.info("S3 Cover Cleanup MOVE ACTION Finished.");
        logger.info("Summary: Total Scanned: {}, Total Flagged: {}, Successfully Moved: {}, Failed to Move: {}",
                totalScanned.get(), totalFlagged.get(), successfullyMoved.get(), failedToMove.get());
        
        return new com.williamcallahan.book_recommendation_engine.types.MoveActionSummary(
                totalScanned.get(), totalFlagged.get(), successfullyMoved.get(), failedToMove.get(),
                flaggedKeysList, movedFileKeys, failedMoveFileKeys);
    }
}
