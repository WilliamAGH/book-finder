package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.image.ImageAttemptStatus;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.model.image.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import com.williamcallahan.book_recommendation_engine.util.cover.ImageDimensionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Shared helper for downloading external cover images, recording provenance, and marking "known bad" identifiers.
 */
@Component
public class ExternalCoverFetchHelper {

    private static final Logger log = LoggerFactory.getLogger(ExternalCoverFetchHelper.class);

    private final LocalDiskCoverCacheService localDiskCoverCacheService;
    private final ImageProvenanceHandler imageProvenanceHandler;
    private final S3BookCoverService s3BookCoverService;

    public ExternalCoverFetchHelper(LocalDiskCoverCacheService localDiskCoverCacheService,
                                    ImageProvenanceHandler imageProvenanceHandler,
                                    S3BookCoverService s3BookCoverService) {
        this.localDiskCoverCacheService = localDiskCoverCacheService;
        this.imageProvenanceHandler = imageProvenanceHandler;
        this.s3BookCoverService = s3BookCoverService;
    }

    /**
     * @deprecated The cover orchestration now flows through
     * {@link com.williamcallahan.book_recommendation_engine.service.image.CoverSourceFetchingService}
     * and {@link com.williamcallahan.book_recommendation_engine.service.image.CoverPersistenceService}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public CompletableFuture<ImageDetails> fetchAndCache(
            String cacheKey,
            Predicate<String> isKnownBad,
            Consumer<String> markKnownBad,
            Supplier<CompletableFuture<Optional<ImageDetails>>> remoteSupplier,
            String attemptDescriptor,
            ImageSourceName sourceName,
            String downloadLabel,
            String placeholderReasonPrefix,
            ImageProvenanceData provenanceData,
            String bookIdForLog) {
        return fetchAndCache(cacheKey, isKnownBad, markKnownBad, remoteSupplier, attemptDescriptor,
            sourceName, downloadLabel, placeholderReasonPrefix, provenanceData, bookIdForLog, null);
    }

    public CompletableFuture<ImageDetails> fetchAndCache(
            String cacheKey,
            Predicate<String> isKnownBad,
            Consumer<String> markKnownBad,
            Supplier<CompletableFuture<Optional<ImageDetails>>> remoteSupplier,
            String attemptDescriptor,
            ImageSourceName sourceName,
            String downloadLabel,
            String placeholderReasonPrefix,
            ImageProvenanceData provenanceData,
            String bookIdForLog,
            ValidationHooks validationHooks) {

        if (cacheKey != null && isKnownBad != null && isKnownBad.test(cacheKey)) {
            imageProvenanceHandler.recordAttempt(
                provenanceData,
                sourceName,
                attemptDescriptor,
                ImageAttemptStatus.SKIPPED_BAD_URL,
                "Known bad cache key",
                null
            );
            return CompletableFuture.completedFuture(createPlaceholder(bookIdForLog, placeholderReasonPrefix + "-known-bad"));
        }

        return remoteSupplier.get()
            .thenCompose(optionalDetails -> handleRemoteResponse(
                cacheKey,
                markKnownBad,
                optionalDetails,
                attemptDescriptor,
                sourceName,
                downloadLabel,
                placeholderReasonPrefix,
                provenanceData,
                bookIdForLog,
                validationHooks
            ))
            .exceptionally(ex -> {
                log.error("Exception retrieving cover for {} ({}): {}", attemptDescriptor, bookIdForLog, ex.getMessage());
                if (cacheKey != null && markKnownBad != null) {
                    markKnownBad.accept(cacheKey);
                }
                imageProvenanceHandler.recordAttempt(
                    provenanceData,
                    sourceName,
                    attemptDescriptor,
                    ImageAttemptStatus.FAILURE_GENERIC,
                    ex.getMessage(),
                    null
                );
                return createPlaceholder(bookIdForLog, placeholderReasonPrefix + "-exception");
            });
    }

    /**
     * @deprecated Legacy response handling retained only for backwards compatibility with
     * {@link #fetchAndCache}. Prefer the CoverSourceFetchingService pipeline.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private CompletableFuture<ImageDetails> handleRemoteResponse(
            String cacheKey,
            Consumer<String> markKnownBad,
            Optional<ImageDetails> optionalDetails,
            String attemptDescriptor,
            ImageSourceName sourceName,
            String downloadLabel,
            String placeholderReasonPrefix,
            ImageProvenanceData provenanceData,
            String bookIdForLog,
            ValidationHooks hooks) {

        if (optionalDetails.isEmpty()) {
            if (cacheKey != null && markKnownBad != null) {
                markKnownBad.accept(cacheKey);
            }
            imageProvenanceHandler.recordAttempt(
                provenanceData,
                sourceName,
                attemptDescriptor,
                ImageAttemptStatus.FAILURE_NOT_FOUND,
                "No ImageDetails returned from remote service",
                null
            );
            return CompletableFuture.completedFuture(createPlaceholder(bookIdForLog, placeholderReasonPrefix + "-no-image"));
        }

        ImageDetails remoteDetails = optionalDetails.get();
        if (!ValidationUtils.hasText(remoteDetails.getUrlOrPath())) {
            if (cacheKey != null && markKnownBad != null) {
                markKnownBad.accept(cacheKey);
            }
            imageProvenanceHandler.recordAttempt(
                provenanceData,
                sourceName,
                attemptDescriptor,
                ImageAttemptStatus.FAILURE_NO_URL_IN_RESPONSE,
                "Remote response lacked URL",
                null
            );
            return CompletableFuture.completedFuture(createPlaceholder(bookIdForLog, placeholderReasonPrefix + "-no-url"));
        }

        String url = remoteDetails.getUrlOrPath();
        if (hooks != null && hooks.urlValidator() != null && !hooks.urlValidator().test(url)) {
            if (cacheKey != null && markKnownBad != null) {
                markKnownBad.accept(cacheKey);
            }
            imageProvenanceHandler.recordAttempt(
                provenanceData,
                sourceName,
                attemptDescriptor,
                ImageAttemptStatus.FAILURE_INVALID_DETAILS,
                "URL rejected by validator",
                remoteDetails
            );
            return CompletableFuture.completedFuture(createPlaceholder(bookIdForLog, placeholderReasonPrefix + "-invalid-url"));
        }

        return cacheImageLocally(url, bookIdForLog, provenanceData, downloadLabel)
            .thenApply(cachedDetails -> {
                if (cachedDetails != null
                    && ImageDimensionUtils.hasAcceptableDimensions(cachedDetails)
                    && cachedDetails.getUrlOrPath() != null
                    && !localDiskCoverCacheService.getLocalPlaceholderPath().equals(cachedDetails.getUrlOrPath())) {
                    return cachedDetails;
                }

                if (cacheKey != null && markKnownBad != null) {
                    markKnownBad.accept(cacheKey);
                }
                if (hooks != null && hooks.postDownloadValidator() != null
                        && !hooks.postDownloadValidator().test(cachedDetails)) {
                    imageProvenanceHandler.recordAttempt(
                        provenanceData,
                        sourceName,
                        attemptDescriptor,
                        ImageAttemptStatus.FAILURE_INVALID_DETAILS,
                        "Downloaded image failed custom validator",
                        cachedDetails
                    );
                    return createPlaceholder(bookIdForLog, placeholderReasonPrefix + "-custom-invalid");
                }
                imageProvenanceHandler.recordAttempt(
                    provenanceData,
                    sourceName,
                    attemptDescriptor,
                    ImageAttemptStatus.FAILURE_INVALID_DETAILS,
                    "Downloaded image failed validation",
                    cachedDetails
                );
                return createPlaceholder(bookIdForLog, placeholderReasonPrefix + "-dl-fail");
            });
    }

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.CoverImagesFactory#createPlaceholder(String)}
     * and persist via {@link com.williamcallahan.book_recommendation_engine.service.image.CoverPersistenceService}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private ImageDetails createPlaceholder(String bookIdForLog, String reason) {
        // Use unified placeholder path; avoid local disk cache dependency for placeholders
        return localDiskCoverCacheService.placeholderImageDetails(bookIdForLog, reason);
    }

    private CompletableFuture<ImageDetails> cacheImageLocally(String url,
                                                               String bookIdForLog,
                                                               ImageProvenanceData provenanceData,
                                                               String downloadLabel) {
        return s3BookCoverService.uploadCoverToS3Async(url, bookIdForLog, downloadLabel, provenanceData).toFuture();
    }

    public static final class ValidationHooks {
        private final Predicate<String> urlValidator;
        private final Predicate<ImageDetails> postDownloadValidator;

        public ValidationHooks(Predicate<String> urlValidator, Predicate<ImageDetails> postDownloadValidator) {
            this.urlValidator = urlValidator;
            this.postDownloadValidator = postDownloadValidator;
        }

        public Predicate<String> urlValidator() {
            return urlValidator;
        }

        public Predicate<ImageDetails> postDownloadValidator() {
            return postDownloadValidator;
        }
    }
}
