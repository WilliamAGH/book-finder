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

import com.williamcallahan.book_recommendation_engine.config.S3EnvironmentCondition;
import com.williamcallahan.book_recommendation_engine.service.image.ImageProcessingService;
import com.williamcallahan.book_recommendation_engine.types.DryRunSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.scheduling.annotation.Async;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.Optional;
import java.util.Collections;
import org.springframework.beans.factory.annotation.Autowired;

@Service
@Conditional(S3EnvironmentCondition.class)
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
    public S3CoverCleanupService(@Autowired(required = false) S3StorageService s3StorageService, ImageProcessingService imageProcessingService) {
        this.s3StorageService = s3StorageService;
        this.imageProcessingService = imageProcessingService;
    }

    /**
     * Asynchronously performs a dry run of the S3 cover cleanup process.
     * Uses non-blocking S3 calls and image processing, returning a CompletableFuture.
     */
    @Async
    public CompletableFuture<DryRunSummary> performDryRun(String s3Prefix, int batchLimit) {
        if (s3StorageService == null) {
            logger.warn("S3StorageService is not available. S3 cleanup functionality is disabled.");
            return CompletableFuture.completedFuture(new DryRunSummary(0, 0, Collections.emptyList()));
        }
        String bucketName = s3StorageService.getBucketName();
        if (bucketName == null || bucketName.isEmpty()) {
            logger.error("S3 bucket name is not configured. Aborting dry run.");
            return CompletableFuture.completedFuture(new DryRunSummary(0, 0, Collections.emptyList()));
        }
        // List objects asynchronously
        return s3StorageService.listObjectsAsync(s3Prefix)
            .thenCompose(allS3Objects -> {
                int total = allS3Objects.size();
                // Determine the list to process, respecting batchLimit
                List<S3Object> objectsToProcess = (batchLimit > 0 && total > batchLimit)
                    ? allS3Objects.subList(0, batchLimit)
                    : allS3Objects;
                logger.info("Processing {} objects for prefix '{}'.", objectsToProcess.size(), s3Prefix);
                // For each object, download and analyze
                List<CompletableFuture<Optional<String>>> futures = objectsToProcess.stream()
                    .map(obj -> s3StorageService.downloadFileAsBytesAsync(obj.key())
                        .thenCompose(data -> {
                            if (data != null && data.length > 0) {
                                return imageProcessingService.isDominantlyWhiteAsync(data, obj.key())
                                    .thenApply(isDominantlyWhite -> {
                                        if (isDominantlyWhite) {
                                            return Optional.of(obj.key());
                                        }
                                        return Optional.<String>empty();
                                    });
                            }
                            return CompletableFuture.completedFuture(Optional.<String>empty());
                        })
                        .exceptionally(e -> {
                            logger.error("Error downloading or analyzing {}: {}", obj.key(), e.getMessage());
                            return Optional.empty();
                        })
                    )
                    .collect(Collectors.toList());
                // Wait for all analysis
                return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join)
                        .flatMap(Optional::stream)
                        .collect(Collectors.toList())
                    )
                    .thenApply(flaggedKeys -> new DryRunSummary(objectsToProcess.size(), flaggedKeys.size(), flaggedKeys));
            })
            .exceptionally(e -> {
                logger.error("Error during async dry run: {}", e.getMessage());
                return new DryRunSummary(0, 0, Collections.emptyList());
            });
    }
    
    /**
     * Performs the action of moving flagged S3 cover images to a quarantine prefix.
     *
     * @param s3Prefix The S3 prefix to scan for original images.
     * @param batchLimit The maximum number of records to process in this run.
     * @param quarantinePrefix The S3 prefix to move flagged images to.
     * @return MoveActionSummary containing counts and lists of processed, moved, and failed items.
     */
    @Async
    public CompletableFuture<com.williamcallahan.book_recommendation_engine.types.MoveActionSummary> performMoveAction(String s3Prefix, int batchLimit, String quarantinePrefix) {
        logger.info("Starting S3 Cover Cleanup MOVE ACTION for prefix '{}', batch limit {}, target quarantine prefix '{}'...", s3Prefix, batchLimit, quarantinePrefix);
        if (s3StorageService == null) {
            logger.warn("S3StorageService is not available. S3 cleanup functionality is disabled.");
            return CompletableFuture.completedFuture(
                new com.williamcallahan.book_recommendation_engine.types.MoveActionSummary(0, 0, 0, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList())
            );
        }
        String bucketName = s3StorageService.getBucketName();
        if (bucketName == null || bucketName.isEmpty()) {
            logger.error("S3 bucket name is not configured. Aborting move action.");
            return CompletableFuture.completedFuture(
                new com.williamcallahan.book_recommendation_engine.types.MoveActionSummary(0, 0, 0, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList())
            );
        }
        if (quarantinePrefix == null || quarantinePrefix.isEmpty() || quarantinePrefix.equals(s3Prefix)) {
            logger.error("Quarantine prefix is invalid (null, empty, or same as source prefix: '{}'). Aborting move action.", quarantinePrefix);
            return CompletableFuture.completedFuture(
                new com.williamcallahan.book_recommendation_engine.types.MoveActionSummary(0, 0, 0, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList())
            );
        }
        final String normalizedQuarantinePrefix;
        if (quarantinePrefix != null && !quarantinePrefix.isEmpty() && !quarantinePrefix.endsWith("/")) {
            normalizedQuarantinePrefix = quarantinePrefix + "/";
        } else {
            normalizedQuarantinePrefix = quarantinePrefix;
        }
        AtomicInteger totalScanned = new AtomicInteger();
        AtomicInteger totalFlagged = new AtomicInteger();
        AtomicInteger successfullyMoved = new AtomicInteger();
        AtomicInteger failedToMove = new AtomicInteger();
        List<String> flaggedKeysList = Collections.synchronizedList(new ArrayList<>());
        List<String> movedFileKeys = Collections.synchronizedList(new ArrayList<>());
        List<String> failedMoveFileKeys = Collections.synchronizedList(new ArrayList<>());

        return s3StorageService.listObjectsAsync(s3Prefix)
            .<com.williamcallahan.book_recommendation_engine.types.MoveActionSummary>thenCompose(allS3Objects -> {
                List<S3Object> objectsToMove = (batchLimit > 0 && allS3Objects.size() > batchLimit)
                    ? allS3Objects.subList(0, batchLimit)
                    : allS3Objects;
                logger.info("Processing {} objects for move action.", objectsToMove.size());
                totalScanned.set(objectsToMove.size());
                List<CompletableFuture<Void>> futures = objectsToMove.stream()
                    .map(obj -> {
                        String sourceKey = obj.key();
                        if (obj.size() == null || obj.size() == 0) {
                            logger.warn("S3 object {} is empty or size is unknown. Skipping.", sourceKey);
                            return CompletableFuture.<Void>completedFuture(null);
                        }
                        return s3StorageService.downloadFileAsBytesAsync(sourceKey)
                            .thenCompose(data -> {
                                if (data == null || data.length == 0) {
                                    logger.debug("S3 object: {} - Analysis OK. No move.", sourceKey);
                                    return CompletableFuture.<Void>completedFuture(null);
                                }
                                return imageProcessingService.isDominantlyWhiteAsync(data, sourceKey)
                                    .thenCompose(isDominantlyWhite -> {
                                        if (!isDominantlyWhite) {
                                            logger.debug("S3 object: {} - Analysis OK. No move.", sourceKey);
                                            return CompletableFuture.<Void>completedFuture(null);
                                        }
                                        totalFlagged.incrementAndGet();
                                        flaggedKeysList.add(sourceKey);
                                        logger.info("[FLAGGED FOR MOVE] S3 object: {}", sourceKey);
                                        String originalName = sourceKey.startsWith(s3Prefix) ? sourceKey.substring(s3Prefix.length()) : sourceKey;
                                        if (originalName.startsWith("/")) originalName = originalName.substring(1);
                                        String destinationKey = normalizedQuarantinePrefix + originalName;
                                        return s3StorageService.copyObjectAsync(sourceKey, destinationKey)
                                            .thenCompose(copySuccess -> {
                                                if (!copySuccess) {
                                                    failedToMove.incrementAndGet();
                                                    failedMoveFileKeys.add(sourceKey);
                                                    logger.error("Failed to copy {} to {}. Skipping.", sourceKey, destinationKey);
                                                    return CompletableFuture.<Void>completedFuture(null);
                                                }
                                                logger.info("Copied {} to {}. Deleting original.", sourceKey, destinationKey);
                                                return s3StorageService.deleteObjectAsync(sourceKey)
                                                    .thenAccept(delSuccess -> {
                                                        if (delSuccess) {
                                                            successfullyMoved.incrementAndGet();
                                                            movedFileKeys.add(sourceKey + " -> " + destinationKey);
                                                            logger.info("Moved {} to {}", sourceKey, destinationKey);
                                                        } else {
                                                            failedToMove.incrementAndGet();
                                                            failedMoveFileKeys.add(sourceKey);
                                                            logger.error("Failed to delete {} after copy.", sourceKey);
                                                        }
                                                    });
                                            });
                                    });
                            })
                            .exceptionally(e -> {
                                logger.error("Error processing {}: {}", sourceKey, e.getMessage(), e);
                                failedToMove.incrementAndGet();
                                failedMoveFileKeys.add(sourceKey);
                                return null;
                            });
                    }).collect(Collectors.toList());
                return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> {
                        logger.info("Move action complete. Scanned: {}, Flagged: {}, Moved: {}, Failed: {}", totalScanned.get(), totalFlagged.get(), successfullyMoved.get(), failedToMove.get());
                        return new com.williamcallahan.book_recommendation_engine.types.MoveActionSummary(
                            totalScanned.get(), totalFlagged.get(), successfullyMoved.get(), failedToMove.get(),
                            flaggedKeysList, movedFileKeys, failedMoveFileKeys
                        );
                    });
            })
            .exceptionally(e -> {
                logger.error("Move action failed for prefix {}: {}", s3Prefix, e.getMessage(), e);
                return new com.williamcallahan.book_recommendation_engine.types.MoveActionSummary(0, 0, 0, 0, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
            });
    }
}
