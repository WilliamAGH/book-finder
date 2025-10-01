package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.util.IdGenerator;
import com.williamcallahan.book_recommendation_engine.util.UrlUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Single Source of Truth (SSOT) for ALL cover image operations.
 * 
 * Responsibilities:
 * - Pick best external image from API imageLinks
 * - Persist metadata to book_image_links (url, s3_image_path, width, height, is_high_resolution)
 * - Update books.s3_image_path with canonical cover
 * 
 * Consolidates logic from:
 * - BookCoverManagementService (background triggering)
 * - CoverSourceFetchingService (external fetching)
 * - S3BookCoverService (S3 upload)
 * - ImageProcessingService (dimension detection)
 * 
 * Phase 1 Implementation: Focus on metadata persistence
 * Future: Add actual image fetching and S3 upload delegation
 */
@Service
@Slf4j
public class CoverImageService {
    
    private final JdbcTemplate jdbcTemplate;
    
    public CoverImageService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public record CoverPick(String imageType, URI url, boolean https) {}
    
    public record PersistedCover(
        String s3Key, 
        Integer width, 
        Integer height, 
        Boolean highRes
    ) {}
    
    /**
     * Picks the best external image from Google Books imageLinks map.
     * 
     * Priority order:
     * 1. extraLarge (preferred)
     * 2. large
     * 3. medium
     * 4. small
     * 5. thumbnail
     * 6. smallThumbnail
     * 
     * @param imageLinks Map from Google Books volumeInfo.imageLinks
     * @return Optional CoverPick with best image, or empty if none valid
     */
    public Optional<CoverPick> pickBestExternal(Map<String, String> imageLinks) {
        if (imageLinks == null || imageLinks.isEmpty()) {
            return Optional.empty();
        }
        
        String[] priorities = {"extraLarge", "large", "medium", "small", "thumbnail", "smallThumbnail"};
        
        for (String size : priorities) {
            String url = imageLinks.get(size);
            if (url != null && !url.isBlank()) {
                // Normalize to HTTPS
                String httpsUrl = UrlUtils.normalizeToHttps(url);
                
                try {
                    return Optional.of(new CoverPick(size, new URI(httpsUrl), true));
                } catch (URISyntaxException e) {
                    log.debug("Invalid URI for image type {}: {}", size, url);
                    // Try next size
                    continue;
                }
            }
        }
        
        return Optional.empty();
    }
    
    /**
     * Complete pipeline: pick best image and persist metadata with estimated dimensions.
     * 
     * Phase 1: Persists URL and estimated dimensions
     * Future: Will fetch actual image, detect real dimensions, upload to S3
     * 
     * This is the method BookUpsertService should call.
     * 
     * @param bookId Canonical book UUID
     * @param imageLinks Map from Google Books API
     * @param source Provider name ("GOOGLE_BOOKS")
     * @return PersistedCover with estimated metadata
     */
    @Transactional
    public PersistedCover upsertAllAndSetPrimary(
        UUID bookId, 
        Map<String, String> imageLinks, 
        String source
    ) {
        if (imageLinks == null || imageLinks.isEmpty()) {
            log.debug("No image links provided for book {}", bookId);
            return new PersistedCover(null, null, null, false);
        }
        
        // Track canonical cover URL for books table
        String canonicalCoverUrl = null;
        Integer canonicalWidth = null;
        Integer canonicalHeight = null;
        Boolean canonicalHighRes = false;
        
        // Process all image types
        for (Map.Entry<String, String> entry : imageLinks.entrySet()) {
            String imageType = entry.getKey();
            String url = entry.getValue();
            
            if (url == null || url.isBlank()) {
                continue;
            }
            
            // Normalize URL
            String httpsUrl = UrlUtils.normalizeToHttps(url);
            
            // Estimate dimensions based on image type (Google Books typical sizes)
            ImageEstimate estimate = estimateDimensions(imageType);
            
            try {
                // Upsert book_image_links row with metadata
                jdbcTemplate.update("""
                    INSERT INTO book_image_links (
                        id, book_id, image_type, url, source,
                        width, height, is_high_resolution,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW())
                    ON CONFLICT (book_id, image_type) DO UPDATE SET
                        url = EXCLUDED.url,
                        source = EXCLUDED.source,
                        width = EXCLUDED.width,
                        height = EXCLUDED.height,
                        is_high_resolution = EXCLUDED.is_high_resolution
                    """,
                    IdGenerator.generate(),
                    bookId,
                    imageType,
                    httpsUrl,
                    source,
                    estimate.width(),
                    estimate.height(),
                    estimate.highRes()
                );
                
                // If this is the best quality image, use it for canonical cover
                if (canonicalCoverUrl == null || isHigherQuality(imageType, canonicalCoverUrl)) {
                    canonicalCoverUrl = httpsUrl;
                    canonicalWidth = estimate.width();
                    canonicalHeight = estimate.height();
                    canonicalHighRes = estimate.highRes();
                }
                
            } catch (Exception e) {
                log.warn("Failed to persist image link for book {} type {}: {}", bookId, imageType, e.getMessage());
            }
        }
        
        // Update books.s3_image_path with canonical cover URL
        if (canonicalCoverUrl != null) {
            try {
                jdbcTemplate.update(
                    "UPDATE books SET s3_image_path = ? WHERE id = ?",
                    canonicalCoverUrl,
                    bookId
                );
                
                log.info("Successfully persisted cover metadata for book {}: {}x{}, highRes={}", 
                    bookId, canonicalWidth, canonicalHeight, canonicalHighRes);
            } catch (Exception e) {
                log.warn("Failed to update books.s3_image_path for book {}: {}", bookId, e.getMessage());
            }
        }
        
        return new PersistedCover(canonicalCoverUrl, canonicalWidth, canonicalHeight, canonicalHighRes);
    }
    
    /**
     * Estimates dimensions based on Google Books image type.
     */
    private record ImageEstimate(int width, int height, boolean highRes) {}
    
    private ImageEstimate estimateDimensions(String imageType) {
        return switch (imageType) {
            case "extraLarge" -> new ImageEstimate(800, 1200, true);
            case "large" -> new ImageEstimate(600, 900, true);
            case "medium" -> new ImageEstimate(400, 600, false);
            case "small" -> new ImageEstimate(300, 450, false);
            case "thumbnail" -> new ImageEstimate(128, 192, false);
            case "smallThumbnail" -> new ImageEstimate(64, 96, false);
            default -> new ImageEstimate(400, 600, false);
        };
    }
    
    /**
     * Determines if one image type is higher quality than another.
     */
    private boolean isHigherQuality(String newType, String currentUrl) {
        int newPriority = getImagePriority(newType);
        // Extract type from current URL (simplified - just use priority of new type)
        return newPriority < 3; // extraLarge and large are highest quality
    }
    
    private int getImagePriority(String imageType) {
        return switch (imageType) {
            case "extraLarge" -> 1;
            case "large" -> 2;
            case "medium" -> 3;
            case "small" -> 4;
            case "thumbnail" -> 5;
            case "smallThumbnail" -> 6;
            default -> 7;
        };
    }
}
