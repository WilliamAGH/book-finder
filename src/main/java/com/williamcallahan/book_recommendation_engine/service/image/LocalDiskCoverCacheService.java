/**
 * Service for managing the local disk cache of book cover images
 *
 * @author William Callahan
 *
 * Features:
 * - Handles downloading images from URLs
 * - Stores images on the local filesystem
 * - Performs hash checks against known placeholder images
 * - Manages cache directory creation and cleanup of old files
 * - Detects and handles placeholder or invalid images
 * - Tracks image provenance for debugging and quality auditing
 */
package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.ImageAttemptStatus;
import com.williamcallahan.book_recommendation_engine.types.ImageDetails;
import com.williamcallahan.book_recommendation_engine.types.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.types.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.util.ImageCacheUtils;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
@Service
public class LocalDiskCoverCacheService {

    private static final Logger logger = LoggerFactory.getLogger(LocalDiskCoverCacheService.class);
    private static final String LOCAL_PLACEHOLDER_PATH = "/images/placeholder-book-cover.svg";
    private static final String GOOGLE_PLACEHOLDER_CLASSPATH_PATH = "/images/image-not-available.png";

    @Value("${app.cover-cache.enabled:true}")
    private boolean cacheEnabled;

    @Value("${app.cover-cache.dir:/tmp/book-covers}")
    private String cacheDirString;

    @Value("${app.cover-cache.max-age-days:30}")
    private int maxCacheAgeDays;

    private Path cacheDir;
    private String cacheDirName;
    private byte[] googlePlaceholderHash;

    private final WebClient webClient;
    private final CoverCacheManager coverCacheManager;
    private final ImageProcessingService imageProcessingService;
    private final AsyncTaskExecutor mvcTaskExecutor;
    private final ScheduledExecutorService scheduler;

    /**
     * Constructs the LocalDiskCoverCacheService
     * @param webClientBuilder WebClient Builder
     * @param coverCacheManager Manager for in-memory caches
     * @param imageProcessingService Service for image processing
     * @param mvcTaskExecutor Executor for async tasks
     */
    public LocalDiskCoverCacheService(WebClient.Builder webClientBuilder,
                                      CoverCacheManager coverCacheManager,
                                      ImageProcessingService imageProcessingService,
                                      @Qualifier("mvcTaskExecutor") AsyncTaskExecutor mvcTaskExecutor) {
        this.webClient = webClientBuilder.build();
        this.coverCacheManager = coverCacheManager;
        this.imageProcessingService = imageProcessingService;
        this.mvcTaskExecutor = mvcTaskExecutor;
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            t.setName("LocalDiskCoverCache-CleanupScheduler");
            return t;
        });
    }

    /**
     * Initializes the service
     * - Creates cache directory if it doesn't exist
     * - Loads placeholder image hashes for comparison
     * - Schedules cleanup of old cached covers
     * - Disables caching if initialization fails
     */
    @PostConstruct
    public void init() {
        if (!cacheEnabled) {
            logger.info("Local disk cover caching is disabled by configuration");
            return;
        }
        try {
            this.cacheDir = Paths.get(cacheDirString);
            this.cacheDirName = this.cacheDir.getFileName().toString();

            if (!Files.exists(cacheDir)) {
                Files.createDirectories(cacheDir);
                logger.info("Created book cover cache directory: {}", cacheDir);
            } else {
                logger.info("Using existing book cover cache directory: {}", cacheDir);
            }

            // Load Google placeholder image for hash comparison
            try (InputStream placeholderStream = getClass().getResourceAsStream(GOOGLE_PLACEHOLDER_CLASSPATH_PATH)) {
                if (placeholderStream != null) {
                    byte[] placeholderBytes = placeholderStream.readAllBytes();
                    if (placeholderBytes.length > 0) {
                        googlePlaceholderHash = ImageCacheUtils.computeImageHash(placeholderBytes);
                        logger.info("Loaded Google Books placeholder image hash from classpath: {}", GOOGLE_PLACEHOLDER_CLASSPATH_PATH);
                    } else {
                        logger.warn("Google Books placeholder image from classpath {} was empty, hash-based detection disabled", GOOGLE_PLACEHOLDER_CLASSPATH_PATH);
                    }
                } else {
                    logger.warn("Google Books placeholder image not found in classpath at {}, hash-based detection disabled", GOOGLE_PLACEHOLDER_CLASSPATH_PATH);
                }
            } catch (NoSuchAlgorithmException e) {
                logger.error("Failed to compute Google Books placeholder hash (SHA-256 not available). Hash-based detection disabled.", e);
            } catch (IOException e) { // Catch IO exceptions from stream operations
                logger.warn("IOException while loading Google Books placeholder image from classpath {}: {}", GOOGLE_PLACEHOLDER_CLASSPATH_PATH, e.getMessage(), e);
            } catch (Exception e) { // Catch any other unexpected exceptions
                logger.warn("Unexpected error loading Google Books placeholder image for hash comparison from classpath {}: {}", GOOGLE_PLACEHOLDER_CLASSPATH_PATH, e.getMessage(), e);
            }

            // Schedule cleanup task
            long initialDelayDays = Math.max(0, Math.min(1, maxCacheAgeDays)); // Run quickly on startup, but not less than 0
            long periodDays = Math.max(1, maxCacheAgeDays); // Ensure period is at least 1 day

            scheduler.scheduleAtFixedRate(this::safeCleanupOldCachedCovers, initialDelayDays, periodDays, TimeUnit.DAYS);
            logger.info("Scheduled cleanup of old cached covers (older than {} days) to run with initial delay of {} day(s) and then every {} days", maxCacheAgeDays, initialDelayDays, periodDays);

        } catch (IOException e) { // For Files.createDirectories
            logger.error("Failed to create or access book cover cache directory: {}. Disabling local disk caching.", cacheDirString, e);
            cacheEnabled = false;
        }
        // The NoSuchAlgorithmException from computeImageHash inside the try-with-resources is handled locally.
        // If cacheDir creation fails, the service is disabled, so subsequent operations won't run.
    }

    /**
     * Downloads an image from a URL and caches it locally
     * - Tracks download attempts and outcomes in provenanceData
     * - Skips known bad URLs
     * - Compares downloaded image hash against known placeholder hashes
     * - Saves valid images to the local disk cache
     * - Updates in-memory URL-to-path cache
     * @param imageUrl URL of the image to download
     * @param bookIdForLog Identifier for logging (e.g., book ID or ISBN)
     * @param provenanceData Container for tracking image source attempts
     * @param sourceNameString String representation of the image source for provenance
     * @return CompletableFuture containing ImageDetails of the downloaded image, or a placeholder
     */
    public CompletableFuture<ImageDetails> downloadAndStoreImageLocallyAsync(
            String imageUrl, String bookIdForLog,
            ImageProvenanceData provenanceData, String sourceNameString) {

        final String logContext = String.format("BookID: %s, URL: %s, Source: %s", bookIdForLog, imageUrl, sourceNameString);
        logger.info("downloadAndStoreImageLocallyAsync started. Context: {}", logContext);

        if (!cacheEnabled) {
            logger.warn("Local disk cache is disabled. Returning placeholder. Context: {}", logContext);
            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "diskcache-disabled"));
        }

        Path destinationPath;
        try {
            destinationPath = cacheDir.resolve(ImageCacheUtils.generateFilenameFromUrl(imageUrl));
            logger.debug("Generated destination path: {}. Context: {}", destinationPath, logContext);
        } catch (NoSuchAlgorithmException e) {
            logger.error("CRITICAL: SHA-256 algorithm not found for generating filename. Context: {}", logContext, e);
            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "hash-algo-filename"));
        }

        ImageSourceName sourceNameEnum = ImageCacheUtils.mapStringToImageSourceName(sourceNameString);
        ImageProvenanceData.AttemptedSourceInfo attemptInfo = null;

        if (provenanceData != null) {
            attemptInfo = new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, imageUrl, ImageAttemptStatus.PENDING);
            if (provenanceData.getAttemptedImageSources() == null) {
                provenanceData.setAttemptedImageSources(new ArrayList<>());
            }
            provenanceData.getAttemptedImageSources().add(attemptInfo);
            logger.debug("Added PENDING attempt to provenance. Context: {}", logContext);
        }

        if (coverCacheManager.isKnownBadImageUrl(imageUrl)) {
            logger.info("Skipping download for known bad URL. Context: {}", logContext);
            if (attemptInfo != null) attemptInfo.setStatus(ImageAttemptStatus.SKIPPED_BAD_URL);
            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "badurl-skip"));
        }
        
        // Effectively final variables for use in lambdas
        final String effectivelyFinalLogContext = logContext;
        final ImageProvenanceData.AttemptedSourceInfo effectivelyFinalAttemptInfo = attemptInfo;

        // Asynchronously check if the image already exists locally
        return CompletableFuture.supplyAsync(() -> {
            if (Files.exists(destinationPath)) {
                try {
                    logger.info("Image already exists locally at {}. Attempting to read dimensions. Context: {}", destinationPath, effectivelyFinalLogContext);
                    byte[] existingBytes = Files.readAllBytes(destinationPath);
                    BufferedImage existingImage = ImageIO.read(new ByteArrayInputStream(existingBytes));
                    if (existingImage != null) {
                        int width = existingImage.getWidth();
                        int height = existingImage.getHeight();
                        logger.info("Successfully read dimensions from existing local image: {}x{}. Context: {}", width, height, effectivelyFinalLogContext);
                        if (effectivelyFinalAttemptInfo != null) {
                            effectivelyFinalAttemptInfo.setStatus(ImageAttemptStatus.SUCCESS);
                            String webSafePathForExisting = "/" + this.cacheDirName + "/" + destinationPath.getFileName().toString();
                            effectivelyFinalAttemptInfo.setFetchedUrl(webSafePathForExisting);
                            effectivelyFinalAttemptInfo.setDimensions(width + "x" + height);
                        }
                        String webSafePath = "/" + this.cacheDirName + "/" + destinationPath.getFileName().toString();
                        return Optional.of(
                            new ImageDetails(webSafePath, sourceNameEnum.getDisplayName(), destinationPath.getFileName().toString(),
                                           CoverImageSource.LOCAL_CACHE, ImageResolutionPreference.ORIGINAL, width, height)
                        );
                    } else {
                        logger.info("Existing file {} could not be read as an image (ImageIO.read returned null). Will attempt re-download. Context: {}", destinationPath, effectivelyFinalLogContext);
                    }
                } catch (IOException e) {
                    logger.info("IOException reading existing file {} or its dimensions. Will attempt re-download. Error: {}. Context: {}", destinationPath, e.getMessage(), effectivelyFinalLogContext, e);
                }
            }
            return Optional.<ImageDetails>empty();
        }, mvcTaskExecutor)
        .thenComposeAsync(localCheckResultOpt -> {
            if (localCheckResultOpt.isPresent()) {
                return CompletableFuture.completedFuture(localCheckResultOpt.get());
            } else {
                // File not found locally or unreadable, proceed to download
                String webSafeCachedPathOnDownload = "/" + this.cacheDirName + "/" + destinationPath.getFileName().toString();
                logger.debug("Attempting to download image. Context: {}", effectivelyFinalLogContext);
                return webClient.get().uri(imageUrl).retrieve().bodyToMono(byte[].class)
                        .timeout(Duration.ofSeconds(10))
                        .toFuture()
                        .thenComposeAsync(imageBytes -> {
                            logger.info("Successfully downloaded {} bytes. Context: {}", imageBytes.length, effectivelyFinalLogContext);
                            try {
                                if (googlePlaceholderHash != null) {
                                    byte[] downloadedHash = ImageCacheUtils.computeImageHash(imageBytes);
                                    if (java.util.Arrays.equals(downloadedHash, googlePlaceholderHash)) {
                                        logger.info("Downloaded image is a known Google placeholder. Marking as bad URL and returning placeholder. Context: {}", effectivelyFinalLogContext);
                                        coverCacheManager.addKnownBadImageUrl(imageUrl);
                                        if (effectivelyFinalAttemptInfo != null) effectivelyFinalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_PLACEHOLDER_DETECTED);
                                        return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "google-placeholder-match"));
                                    }
                                    logger.debug("Downloaded image is not the Google placeholder. Context: {}", effectivelyFinalLogContext);
                                }
                            } catch (NoSuchAlgorithmException e) {
                                logger.error("NoSuchAlgorithmException (SHA-256) during hash computation for placeholder check. Context: {}", effectivelyFinalLogContext, e);
                                if (effectivelyFinalAttemptInfo != null) effectivelyFinalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_PROCESSING);
                                return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "hash-algo-placeholder-check"));
                            }

                            // Chain the asynchronous image processing
                            return imageProcessingService.processImageForS3(imageBytes, bookIdForLog)
                                .thenApplyAsync(processedImage -> { // Inner thenApplyAsync to handle ProcessedImage
                                    try {
                                        if (!processedImage.processingSuccessful()) {
                                            logger.warn("Book ID {}: Image processing failed for URL: {}. Error: {}. Context: {}", bookIdForLog, imageUrl, processedImage.processingError(), effectivelyFinalLogContext);
                                            if (effectivelyFinalAttemptInfo != null) {
                                                if ("LikelyNotACover_DominantColor".equals(processedImage.processingError())) {
                                                    effectivelyFinalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_CONTENT_REJECTED);
                                                } else {
                                                    effectivelyFinalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_PROCESSING);
                                                }
                                                effectivelyFinalAttemptInfo.setFailureReason(processedImage.processingError());
                                            }
                                            coverCacheManager.addKnownBadImageUrl(imageUrl);
                                            return createPlaceholderImageDetails(bookIdForLog, "processing-failed-" + processedImage.processingError());
                                        }
                                        
                                        Files.write(destinationPath, processedImage.processedBytes());
                                        logger.info("Successfully processed, content-checked, and saved image to {}. Dimensions: {}x{}. Context: {}", 
                                                    destinationPath, processedImage.width(), processedImage.height(), effectivelyFinalLogContext);
                                        coverCacheManager.putPathToUrlCache(imageUrl, webSafeCachedPathOnDownload);

                                        if (effectivelyFinalAttemptInfo != null) {
                                            effectivelyFinalAttemptInfo.setStatus(ImageAttemptStatus.SUCCESS);
                                            effectivelyFinalAttemptInfo.setFetchedUrl(webSafeCachedPathOnDownload);
                                            effectivelyFinalAttemptInfo.setDimensions(processedImage.width() + "x" + processedImage.height());
                                        }
                                        return new ImageDetails(webSafeCachedPathOnDownload, sourceNameEnum.getDisplayName(), destinationPath.getFileName().toString(),
                                                CoverImageSource.LOCAL_CACHE, ImageResolutionPreference.ORIGINAL,
                                                processedImage.width(), processedImage.height());
                                    } catch (IOException e) { // Handles IOException from Files.write
                                        logger.error("IOException during saving processed image. Context: {}", effectivelyFinalLogContext, e);
                                        if (effectivelyFinalAttemptInfo != null) effectivelyFinalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_IO);
                                        return createPlaceholderImageDetails(bookIdForLog, "io-exception-saving");
                                    } catch (Exception e) { // Catch any other unexpected error from this inner block
                                        logger.error("Unexpected exception during post-processing or saving. Context: {}", effectivelyFinalLogContext, e);
                                        if (effectivelyFinalAttemptInfo != null) effectivelyFinalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_GENERIC);
                                        return createPlaceholderImageDetails(bookIdForLog, "generic-post-processing-ex");
                                    }
                                }, mvcTaskExecutor);
                        }, mvcTaskExecutor) // Execute the composition on mvcTaskExecutor
                        .exceptionally(ex -> {
                            // Check if the cause is WebClientResponseException.NotFound
                            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                            if (cause instanceof org.springframework.web.reactive.function.client.WebClientResponseException.NotFound) {
                                logger.info("Failed to download image (404 Not Found). URL: {}. Context: {}", imageUrl, effectivelyFinalLogContext);
                            } else {
                                logger.warn("Failed to download or process image. Error: {}. Context: {}", cause.getMessage(), effectivelyFinalLogContext, cause);
                            }
                            coverCacheManager.addKnownBadImageUrl(imageUrl);
                            if (effectivelyFinalAttemptInfo != null) {
                                effectivelyFinalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_GENERIC_DOWNLOAD);
                                effectivelyFinalAttemptInfo.setFailureReason(cause.getMessage());
                            }
                            return createPlaceholderImageDetails(bookIdForLog, "download-or-process-ex");
                        });
            }
        }, mvcTaskExecutor); // Execute the outer composition on mvcTaskExecutor
    }

    /**
     * Creates standardized ImageDetails for a placeholder image
     * @param bookIdForLog Identifier for logging
     * @param reasonSuffix Suffix for the placeholder reason, used in sourceSystemId
     * @return ImageDetails object representing the local placeholder
     */
    public ImageDetails createPlaceholderImageDetails(String bookIdForLog, String reasonSuffix) {
        String cleanReasonSuffix = reasonSuffix != null ? reasonSuffix.replaceAll("[^a-zA-Z0-9-]", "_") : "unknown";
        return new ImageDetails(
                LOCAL_PLACEHOLDER_PATH,
                "SYSTEM_PLACEHOLDER",
                "placeholder-" + cleanReasonSuffix + "-" + bookIdForLog,
                CoverImageSource.LOCAL_CACHE,
                ImageResolutionPreference.UNKNOWN
        );
    }
    
    /**
     * Gets the web path to the local placeholder image
     * @return Web-accessible path to the placeholder image
     */
    public String getLocalPlaceholderPath() {
        return LOCAL_PLACEHOLDER_PATH;
    }

    /**
     * Gets the configured cache directory name
     * @return The name of the cache directory
     */
    public String getCacheDirName() {
        return cacheDirName;
    }

    /**
     * Gets the configured cache directory string (full path)
     * @return The string representation of the cache directory path
     */
    public String getCacheDirString() {
        return cacheDirString;
    }

    /**
     * Wrapper for cleanupOldCachedCovers to ensure exceptions are caught.
     */
    private void safeCleanupOldCachedCovers() {
        try {
            cleanupOldCachedCovers();
        } catch (Throwable t) {
            logger.error("Uncaught exception in LocalDiskCoverCacheService cleanup task. Scheduler thread might have died if not for this catch.", t);
        }
    }

    /**
     * Periodically cleans up old cached cover files from the local disk
     * - Files older than maxCacheAgeDays are deleted
     * - Runs on a schedule defined by scheduler
     */
    private void cleanupOldCachedCovers() {
        if (!cacheEnabled || cacheDir == null) {
            logger.debug("Cleanup: Local disk cache disabled or directory not set, skipping cleanup");
            return;
        }
        logger.info("Starting cleanup of old cached book covers in {} older than {} days", cacheDir, maxCacheAgeDays);
        
        // Execute cleanup asynchronously to avoid blocking scheduler thread
        CompletableFuture.runAsync(() -> {
            try {
                long cutoffTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(maxCacheAgeDays);
                final AtomicInteger deleteCount = new AtomicInteger(0);

                Files.list(cacheDir)
                        .filter(Files::isRegularFile)
                        .filter(p -> {
                            try {
                                return Files.getLastModifiedTime(p).toMillis() < cutoffTime;
                            } catch (IOException e) {
                                logger.warn("Could not get last modified time for {}, skipping in cleanup", p, e);
                                return false;
                            }
                        })
                        .forEach(p -> {
                            try {
                                Files.delete(p);
                                deleteCount.incrementAndGet();
                                logger.debug("Deleted old cached cover: {}", p.getFileName());
                            } catch (IOException e) {
                                logger.warn("Failed to delete old cached cover: {}", p.getFileName(), e);
                            }
                        });
                logger.info("Completed cleanup of old cached book covers. Deleted {} files", deleteCount.get());
            } catch (IOException e) {
                logger.error("Error during cleanup of cached book covers in {}", cacheDir, e);
            }
        }, mvcTaskExecutor).exceptionally(ex -> {
            logger.error("Unexpected error in async cleanup task", ex);
            return null;
        });
    }

    /**
     * Shuts down the scheduled executor service on bean destruction.
     */
    @PreDestroy
    public void destroy() {
        if (scheduler != null && !scheduler.isShutdown()) {
            logger.info("Shutting down LocalDiskCoverCacheService scheduler...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                    if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                        logger.error("LocalDiskCoverCacheService scheduler did not terminate.");
                    }
                }
            } catch (InterruptedException ie) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("LocalDiskCoverCacheService scheduler shut down.");
        }
    }
}
