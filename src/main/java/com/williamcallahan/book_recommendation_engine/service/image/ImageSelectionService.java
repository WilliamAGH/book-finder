package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.util.cover.ImageDimensionUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Service for selecting the best image from multiple candidates.
 * <p>
 * Encapsulates complex image selection logic that was previously in
 * ImageCacheUtils. Provides prioritization based on storage location,
 * dimensions, and data source quality.
 * 
 * @author William Callahan
 * @since 0.9.0
 */
@Slf4j
@Service
public class ImageSelectionService {
    
    /**
     * Result of image selection with optional fallback reason.
     */
    public record SelectionResult(ImageDetails bestImage, String fallbackReason) {
        public boolean hasSelection() {
            return bestImage != null;
        }
    }
    
    /**
     * Selects the best image from a list of candidates.
     * 
     * @param candidates List of image candidates
     * @param placeholderPath Path to placeholder image (to exclude)
     * @param bookIdForLog Book ID for logging purposes
     * @return Selection result with best image or fallback reason
     */
    public SelectionResult selectBest(
        List<ImageDetails> candidates,
        String placeholderPath,
        String bookIdForLog
    ) {
        if (candidates == null || candidates.isEmpty()) {
            log.warn("Book ID {}: selectBest called with no candidates", bookIdForLog);
            return new SelectionResult(null, "no-candidates-for-selection");
        }
        
        List<ImageDetails> validCandidates = candidates.stream()
            .filter(candidate -> isValid(candidate, placeholderPath))
            .collect(Collectors.toList());
        
        if (validCandidates.isEmpty()) {
            log.warn("Book ID {}: No valid candidates after filtering", bookIdForLog);
            return new SelectionResult(null, "no-valid-candidates-for-selection");
        }
        
        Comparator<ImageDetails> comparator = buildSelectionComparator(placeholderPath);
        ImageDetails bestImage = validCandidates.stream()
            .min(comparator)
            .orElse(null);
        
        if (bestImage != null) {
            log.info("Book ID {}: Selected best image from {} candidates. URL: {}, Source: {}, Dimensions: {}x{}",
                bookIdForLog,
                validCandidates.size(),
                bestImage.getUrlOrPath(),
                bestImage.getCoverImageSource(),
                bestImage.getWidth(),
                bestImage.getHeight());
            
            if (log.isDebugEnabled()) {
                validCandidates.forEach(candidate ->
                    log.debug("Book ID {}: Candidate - URL: {}, Source: {}, Dimensions: {}x{}",
                        bookIdForLog,
                        candidate.getUrlOrPath(),
                        candidate.getCoverImageSource(),
                        candidate.getWidth(),
                        candidate.getHeight())
                );
            }
        }
        
        return new SelectionResult(bestImage, null);
    }
    
    /**
     * Checks if an image candidate is valid for selection.
     * 
     * @param imageDetails Image to validate
     * @param placeholderPath Placeholder path to exclude
     * @return true if valid, false otherwise
     */
    private boolean isValid(ImageDetails imageDetails, String placeholderPath) {
        return imageDetails != null
            && imageDetails.getUrlOrPath() != null
            && !Objects.equals(imageDetails.getUrlOrPath(), placeholderPath)
            && ImageDimensionUtils.areValid(imageDetails.getWidth(), imageDetails.getHeight());
    }
    
    /**
     * Builds a comparator for image selection prioritization.
     * <p>
     * Priority order:
     * 1. Cached images (local/S3) with good dimensions
     * 2. Total pixel count (larger is better)
     * 3. Storage location and source quality
     * 
     * @param placeholderPath Placeholder path for validation
     * @return Comparator for image selection
     */
    private Comparator<ImageDetails> buildSelectionComparator(String placeholderPath) {
        return Comparator
            // Priority 1: Cached images with reasonable dimensions
            .<ImageDetails>comparingInt(details -> {
                String storage = details.getStorageLocation();
                boolean isCached = storage != null && !storage.isBlank();
                
                if (isCached
                    && ImageDimensionUtils.meetsThreshold(
                        details.getWidth(), 
                        details.getHeight(), 
                        ImageDimensionUtils.MIN_ACCEPTABLE_CACHED)
                    && details.getUrlOrPath() != null
                    && !details.getUrlOrPath().equals(placeholderPath)) {
                    return 0; // Highest priority for cached images
                }
                return 1; // Lower priority for external API sources
            })
            // Priority 2: Total pixel count (higher is better)
            .thenComparing(Comparator.<ImageDetails>comparingLong(details -> {
                if (details.getWidth() == null || details.getHeight() == null) {
                    return 0L;
                }
                return (long) details.getWidth() * details.getHeight();
            }).reversed())
            // Priority 3: Storage location and source quality
            .thenComparingInt(details -> {
                String storage = details.getStorageLocation();
                CoverImageSource src = details.getCoverImageSource();
                
                // Check storage location (new way)
                if (ImageDetails.STORAGE_LOCAL.equals(storage) 
                    && details.getUrlOrPath() != null
                    && !details.getUrlOrPath().equals(placeholderPath)) {
                    return 0; // Fastest - server disk
                }
                if (ImageDetails.STORAGE_S3.equals(storage)) {
                    return 1; // Fast - CDN
                }
                
                // External API sources by data source quality
                if (src == CoverImageSource.GOOGLE_BOOKS) return 2;
                if (src == CoverImageSource.OPEN_LIBRARY) return 3;
                if (src == CoverImageSource.LONGITOOD) return 4;
                return 5; // Unknown sources
            });
    }
}
