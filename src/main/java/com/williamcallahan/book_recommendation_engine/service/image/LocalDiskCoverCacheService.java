package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageAttemptStatus;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData.AttemptedSourceInfo;
import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.model.image.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import com.williamcallahan.book_recommendation_engine.util.cover.CoverSourceMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

/**
 * Legacy compatibility shim that used to cache covers on local disk.
 *
 * <p>The modern pipeline persists downloaded covers straight to S3 via
 * {@link S3BookCoverService}. This class now delegates every legacy entry point
 * to that pipeline while still providing placeholder helpers and cache naming
 * utilities needed by existing callers.</p>
 */
@Service
public class LocalDiskCoverCacheService {

    private static final Logger logger = LoggerFactory.getLogger(LocalDiskCoverCacheService.class);
    private static final String DEFAULT_CACHE_DIR = "book-covers";

    @Value("${app.cover-cache.dir:/tmp/book-covers}")
    private String cacheDirString;

    private final S3BookCoverService s3BookCoverService;

    public LocalDiskCoverCacheService(S3BookCoverService s3BookCoverService) {
        this.s3BookCoverService = s3BookCoverService;
    }

    public CompletableFuture<ImageDetails> cacheRemoteImageAsync(String imageUrl,
                                                                 String bookIdForLog,
                                                                 ImageProvenanceData provenanceData,
                                                                 String sourceNameString) {
        return cacheRemoteImageInternal(imageUrl, bookIdForLog, provenanceData, sourceNameString);
    }

    /**
     * @deprecated Local disk caching has been retired. Delegates to the S3 pipeline.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public CompletableFuture<ImageDetails> downloadAndStoreImageLocallyAsync(String imageUrl,
                                                                             String bookIdForLog,
                                                                             ImageProvenanceData provenanceData,
                                                                             String sourceNameString) {
        return cacheRemoteImageInternal(imageUrl, bookIdForLog, provenanceData, sourceNameString);
    }

    private CompletableFuture<ImageDetails> cacheRemoteImageInternal(String imageUrl,
                                                                     String bookIdForLog,
                                                                     ImageProvenanceData provenanceData,
                                                                     String sourceNameString) {
        String logContext = String.format("BookID: %s, URL: %s, Source: %s", bookIdForLog, imageUrl, sourceNameString);
        logger.debug("Delegating legacy disk cache call to S3 pipeline. Context: {}", logContext);

        ImageSourceName sourceNameEnum = CoverSourceMapper.fromString(sourceNameString);
        AttemptedSourceInfo attemptInfo = registerAttempt(provenanceData, sourceNameEnum, imageUrl);

        if (!ValidationUtils.hasText(imageUrl)) {
            markAttemptFailure(attemptInfo, ImageAttemptStatus.FAILURE_INVALID_DETAILS, "Blank image URL");
            return CompletableFuture.completedFuture(buildPlaceholderImageDetails(bookIdForLog, "missing-url"));
        }

        String normalizedSource = ValidationUtils.hasText(sourceNameString)
            ? sourceNameString
            : sourceNameEnum.name();

        Mono<ImageDetails> upload = s3BookCoverService
            .uploadCoverToS3Async(imageUrl, bookIdForLog, normalizedSource, provenanceData)
            .doOnSuccess(details -> handleSuccessfulUpload(details, attemptInfo, sourceNameEnum))
            .onErrorResume(ex -> {
                LoggingUtils.error(logger, ex, "Failed to persist cover via S3 pipeline. Context: {}", logContext);
                markAttemptFailure(attemptInfo, ImageAttemptStatus.FAILURE_GENERIC_DOWNLOAD, ex.getMessage());
                return Mono.just(buildPlaceholderImageDetails(bookIdForLog, "s3-upload-failed"));
            })
            .switchIfEmpty(Mono.fromCallable(() -> {
                markAttemptFailure(attemptInfo, ImageAttemptStatus.FAILURE_NOT_FOUND, "S3 pipeline returned empty result");
                return buildPlaceholderImageDetails(bookIdForLog, "s3-upload-empty");
            }));

        return upload.toFuture();
    }

    private AttemptedSourceInfo registerAttempt(ImageProvenanceData provenanceData,
                                                ImageSourceName sourceName,
                                                String attemptedUrl) {
        if (provenanceData == null) {
            return null;
        }
        if (provenanceData.getAttemptedImageSources() == null) {
            provenanceData.setAttemptedImageSources(new ArrayList<>());
        }
        AttemptedSourceInfo attemptInfo = new AttemptedSourceInfo(sourceName, attemptedUrl, ImageAttemptStatus.PENDING);
        provenanceData.getAttemptedImageSources().add(attemptInfo);
        return attemptInfo;
    }

    private void handleSuccessfulUpload(ImageDetails details,
                                        AttemptedSourceInfo attemptInfo,
                                        ImageSourceName sourceNameEnum) {
        if (attemptInfo != null) {
            attemptInfo.setStatus(ImageAttemptStatus.SUCCESS);
            attemptInfo.setFetchedUrl(details.getUrlOrPath());
            if (details.getWidth() != null && details.getHeight() != null
                && details.getWidth() > 0 && details.getHeight() > 0) {
                attemptInfo.setDimensions(details.getWidth() + "x" + details.getHeight());
            }
        }

        // Normalize cover source metadata so downstream callers do not see legacy storage enums.
        CoverImageSource sanitizedSource = CoverSourceMapper.sanitize(
            CoverSourceMapper.toCoverImageSource(sourceNameEnum)
        );
        details.setCoverImageSource(sanitizedSource);
        details.setSourceName(sourceNameEnum.name());
        if (details.getStorageLocation() == null) {
            details.setStorageLocation(ImageDetails.STORAGE_S3);
        }
    }

    private void markAttemptFailure(AttemptedSourceInfo attemptInfo,
                                    ImageAttemptStatus status,
                                    String reason) {
        if (attemptInfo == null) {
            return;
        }
        attemptInfo.setStatus(status);
        attemptInfo.setFailureReason(reason);
    }

    public ImageDetails placeholderImageDetails(String bookIdForLog, String reasonSuffix) {
        String cleanReasonSuffix = reasonSuffix != null
            ? reasonSuffix.replaceAll("[^a-zA-Z0-9-]", "_")
            : "unknown";
        String placeholderPath = ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH;

        ImageDetails details = new ImageDetails(
            placeholderPath,
            "SYSTEM_PLACEHOLDER",
            "placeholder-" + cleanReasonSuffix + "-" + bookIdForLog,
            CoverImageSource.NONE,
            ImageResolutionPreference.UNKNOWN
        );
        details.setStorageLocation(ImageDetails.STORAGE_LOCAL);
        details.setStorageKey(placeholderPath);
        return details;
    }

    public ImageDetails buildPlaceholderImageDetails(String bookIdForLog, String reasonSuffix) {
        return placeholderImageDetails(bookIdForLog, reasonSuffix);
    }

    /**
     * @deprecated Use {@link #placeholderImageDetails(String, String)}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public ImageDetails createPlaceholderImageDetails(String bookIdForLog, String reasonSuffix) {
        return placeholderImageDetails(bookIdForLog, reasonSuffix);
    }

    public String getLocalPlaceholderPath() {
        return ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH;
    }

    public String getCacheDirName() {
        if (!ValidationUtils.hasText(cacheDirString)) {
            return DEFAULT_CACHE_DIR;
        }
        Path path = Paths.get(cacheDirString);
        Path fileName = path.getFileName();
        return fileName != null ? fileName.toString() : DEFAULT_CACHE_DIR;
    }

    public String getCacheDirString() {
        return ValidationUtils.hasText(cacheDirString)
            ? cacheDirString
            : "/tmp/" + DEFAULT_CACHE_DIR;
    }
}
