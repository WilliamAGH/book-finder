package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.image.ImageAttemptStatus;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.model.image.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Handles image provenance tracking for audit and debugging purposes.
 * <p>
 * Consolidates provenance manipulation logic that was previously scattered
 * in ImageCacheUtils static methods. Provides encapsulated, testable
 * provenance operations.
 * 
 * @author William Callahan
 * @since 0.9.0
 */
@Slf4j
@Component
public class ImageProvenanceHandler {
    
    /**
     * Records the final selected image in provenance data.
     * 
     * @param provenanceData Provenance data to update
     * @param sourceName Source of the selected image
     * @param imageDetails Details of the selected image
     * @param selectionReason Reason for selection
     */
    public void recordSelection(
        ImageProvenanceData provenanceData,
        ImageSourceName sourceName,
        ImageDetails imageDetails,
        String selectionReason
    ) {
        if (provenanceData == null || imageDetails == null) {
            log.warn("Cannot record selection: provenanceData or imageDetails is null");
            return;
        }
        
        ImageProvenanceData.SelectedImageInfo selectedInfo = new ImageProvenanceData.SelectedImageInfo();
        selectedInfo.setSourceName(sourceName != null ? sourceName : ImageSourceName.UNKNOWN);
        selectedInfo.setFinalUrl(imageDetails.getUrlOrPath());
        selectedInfo.setResolution(imageDetails.getResolutionPreference() != null
            ? imageDetails.getResolutionPreference().name()
            : "ORIGINAL");
        selectedInfo.setDimensions(formatDimensions(imageDetails.getWidth(), imageDetails.getHeight()));
        selectedInfo.setSelectionReason(selectionReason);
        
        // Determine and set storage location
        String storage = imageDetails.getStorageLocation();
        if (storage != null && !storage.isBlank()) {
            selectedInfo.setStorageLocation(storage);
            if (ImageDetails.STORAGE_S3.equals(storage)) {
                selectedInfo.setS3Key(imageDetails.getStorageKey() != null
                    ? imageDetails.getStorageKey()
                    : imageDetails.getSourceSystemId());
            }
        } else if (imageDetails.getUrlOrPath() != null) {
            UrlSourceDetector.detectStorageLocation(imageDetails.getUrlOrPath())
                .ifPresentOrElse(detected -> {
                    selectedInfo.setStorageLocation(detected);
                    if (ImageDetails.STORAGE_S3.equals(detected)) {
                        selectedInfo.setS3Key(imageDetails.getSourceSystemId());
                    }
                }, () -> selectedInfo.setStorageLocation("Remote"));
        } else {
            selectedInfo.setStorageLocation("Remote");
        }
        
        provenanceData.setSelectedImageInfo(selectedInfo);
        
        log.debug("Provenance updated: Selected image from {} ({}), URL: {}, Dimensions: {}, Reason: {}",
            sourceName,
            selectedInfo.getStorageLocation(),
            selectedInfo.getFinalUrl(),
            selectedInfo.getDimensions(),
            selectionReason);
    }
    
    /**
     * Records an attempted image fetch in provenance data.
     * 
     * @param provenanceData Provenance data to update
     * @param sourceName Source that was attempted
     * @param urlAttempted URL that was attempted
     * @param status Result status of the attempt
     * @param failureReason Reason for failure (if applicable)
     * @param detailsIfSuccess Details if successful (if applicable)
     */
    public void recordAttempt(
        ImageProvenanceData provenanceData,
        ImageSourceName sourceName,
        String urlAttempted,
        ImageAttemptStatus status,
        String failureReason,
        ImageDetails detailsIfSuccess
    ) {
        if (provenanceData == null) {
            log.warn("Cannot record attempt: provenanceData is null");
            return;
        }
        
        if (provenanceData.getAttemptedImageSources() == null) {
            provenanceData.setAttemptedImageSources(
                Collections.synchronizedList(new ArrayList<>())
            );
        }
        
        ImageProvenanceData.AttemptedSourceInfo attemptInfo =
            new ImageProvenanceData.AttemptedSourceInfo(
                sourceName != null ? sourceName : ImageSourceName.UNKNOWN,
                urlAttempted,
                status != null ? status : ImageAttemptStatus.FAILURE_GENERIC
            );
        
        if (failureReason != null) {
            attemptInfo.setFailureReason(failureReason);
        }
        
        if (detailsIfSuccess != null && status == ImageAttemptStatus.SUCCESS) {
            attemptInfo.setFetchedUrl(detailsIfSuccess.getUrlOrPath());
            attemptInfo.setDimensions(formatDimensions(
                detailsIfSuccess.getWidth(),
                detailsIfSuccess.getHeight()
            ));
        }
        
        provenanceData.getAttemptedImageSources().add(attemptInfo);
        
        log.debug("Recorded provenance attempt: {} - {} ({})",
            sourceName, urlAttempted, status);
    }
    
    /**
     * Formats dimensions as a string.
     * 
     * @param width Width in pixels (may be null)
     * @param height Height in pixels (may be null)
     * @return Formatted dimension string (e.g., "800x600" or "N/A")
     */
    private String formatDimensions(Integer width, Integer height) {
        String widthStr = width != null ? String.valueOf(width) : "N/A";
        String heightStr = height != null ? String.valueOf(height) : "N/A";
        return widthStr + "x" + heightStr;
    }
}
