package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.util.IdGenerator;
import com.williamcallahan.book_recommendation_engine.util.UrlUtils;
import com.williamcallahan.book_recommendation_engine.util.cover.ImageDimensionUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;

/**
 * Single Source of Truth for ALL book cover image persistence operations.
 * 
 * Consolidates and replaces logic from:
 * - CoverImageService (metadata persistence)
 * - BookCoverManagementService (background S3 upload coordination)
 * - S3BookCoverService (direct S3 upload handling)
 * - Scattered book_image_links INSERT/UPDATE statements
 * 
 * Responsibilities:
 * 1. Persist cover metadata to book_image_links table
 * 2. Update books.s3_image_path with canonical cover URL
 * 3. Handle both initial estimates and post-upload actual dimensions
 * 4. Provide idempotent upsert operations
 * 
 * Phase 1: Metadata persistence (this implementation)
 * Phase 2: Integration with S3 upload workflows (next iteration)
 * 
 * @author William Callahan
 */
@Service
@Slf4j
public class CoverPersistenceService {
    
    private final JdbcTemplate jdbcTemplate;
    
    public CoverPersistenceService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    /**
     * Result record for cover persistence operations.
     * 
     * @param success Whether the operation succeeded
     * @param s3Key S3 object key if applicable, or external URL
     * @param width Image width in pixels
     * @param height Image height in pixels
     * @param highRes Whether the image is high-resolution
     */
    public record PersistenceResult(
        boolean success,
        String s3Key,
        Integer width,
        Integer height,
        Boolean highRes
    ) {}
    
    /**
     * Persists all image variants from Google Books imageLinks and sets the best as primary.
     * 
     * This method handles the initial persistence when a book is first fetched from the API,
     * using estimated dimensions based on Google's image type classifications.
     * 
     * @param bookId Canonical book UUID
     * @param imageLinks Map from Google Books volumeInfo.imageLinks
     * @param source Provider name (e.g., "GOOGLE_BOOKS")
     * @return PersistenceResult with the canonical cover metadata
     */
    @Transactional
    public PersistenceResult persistFromGoogleImageLinks(UUID bookId, Map<String, String> imageLinks, String source) {
        if (imageLinks == null || imageLinks.isEmpty()) {
            log.debug("No image links provided for book {}", bookId);
            return new PersistenceResult(false, null, null, null, false);
        }
        
        String canonicalCoverUrl = null;
        Integer canonicalWidth = null;
        Integer canonicalHeight = null;
        Boolean canonicalHighRes = false;
        int bestPriority = Integer.MAX_VALUE;
        
        // Process all image types
        for (Map.Entry<String, String> entry : imageLinks.entrySet()) {
            String imageType = entry.getKey();
            String url = entry.getValue();
            
            if (url == null || url.isBlank()) {
                continue;
            }
            
            // Normalize to HTTPS
            String httpsUrl = UrlUtils.normalizeToHttps(url);
            
            // Estimate dimensions based on Google's image type
            ImageDimensionUtils.DimensionEstimate estimate = 
                ImageDimensionUtils.estimateFromGoogleType(imageType);
            
            try {
                // Upsert book_image_links row
                upsertImageLink(bookId, imageType, httpsUrl, source, 
                    estimate.width(), estimate.height(), estimate.highRes());
                
                // Track best quality image for canonical cover
                int priority = getImageTypePriority(imageType);
                if (priority < bestPriority) {
                    bestPriority = priority;
                    canonicalCoverUrl = httpsUrl;
                    canonicalWidth = estimate.width();
                    canonicalHeight = estimate.height();
                    canonicalHighRes = estimate.highRes();
                }
                
            } catch (Exception e) {
                log.warn("Failed to persist image link for book {} type {}: {}", 
                    bookId, imageType, e.getMessage());
            }
        }
        
        // Update books.s3_image_path with canonical cover
        if (canonicalCoverUrl != null) {
            updateBookCoverPath(bookId, canonicalCoverUrl);
            log.info("Persisted cover metadata for book {}: {}x{}, highRes={}", 
                bookId, canonicalWidth, canonicalHeight, canonicalHighRes);
            return new PersistenceResult(true, canonicalCoverUrl, canonicalWidth, canonicalHeight, canonicalHighRes);
        }
        
        return new PersistenceResult(false, null, null, null, false);
    }
    
    /**
     * Updates cover metadata after successful S3 upload with actual dimensions.
     * 
     * This method is called after an image has been downloaded, processed, and uploaded to S3,
     * replacing estimated dimensions with actual detected dimensions.
     * 
     * @param bookId Canonical book UUID
     * @param s3Key S3 object key
     * @param s3CdnUrl Full CDN URL for the image
     * @param width Actual detected width
     * @param height Actual detected height
     * @param source Original source that provided the image
     * @return PersistenceResult indicating success
     */
    @Transactional
    public PersistenceResult updateAfterS3Upload(
        UUID bookId,
        String s3Key,
        String s3CdnUrl,
        Integer width,
        Integer height,
        CoverImageSource source
    ) {
        if (s3Key == null || s3CdnUrl == null) {
            log.warn("Cannot update cover for book {}: S3 key or URL is null", bookId);
            return new PersistenceResult(false, null, width, height, false);
        }
        
        boolean highRes = ImageDimensionUtils.isHighResolution(width, height);

        try {
            // Upsert book_image_links with actual S3 URL and dimensions, including S3 path
            upsertImageLink(bookId, "large", s3CdnUrl, source.name(), width, height, highRes, s3Key);

            // Update books table with S3 CDN URL as primary cover
            updateBookCoverPath(bookId, s3CdnUrl);
            
            log.info("Updated cover metadata for book {} after S3 upload: {} ({}x{}, highRes={})",
                bookId, s3Key, width, height, highRes);
            
            return new PersistenceResult(true, s3Key, width, height, highRes);
            
        } catch (Exception e) {
            log.error("Failed to update cover metadata after S3 upload for book {}: {}", 
                bookId, e.getMessage(), e);
            return new PersistenceResult(false, s3Key, width, height, highRes);
        }
    }
    
    /**
     * Persists external cover URL with estimated or actual dimensions.
     * Used for non-Google sources or when directly persisting external URLs.
     * 
     * @param bookId Canonical book UUID
     * @param externalUrl External image URL
     * @param source Source identifier
     * @param width Image width (null for unknown)
     * @param height Image height (null for unknown)
     * @return PersistenceResult indicating success
     */
    @Transactional
    public PersistenceResult persistExternalCover(
        UUID bookId,
        String externalUrl,
        String source,
        Integer width,
        Integer height
    ) {
        if (externalUrl == null || externalUrl.isBlank()) {
            log.warn("Cannot persist external cover for book {}: URL is null/blank", bookId);
            return new PersistenceResult(false, null, null, null, false);
        }
        
        String httpsUrl = UrlUtils.normalizeToHttps(externalUrl);
        int finalWidth = width != null ? width : ImageDimensionUtils.DEFAULT_DIMENSION;
        int finalHeight = height != null ? height : ImageDimensionUtils.DEFAULT_DIMENSION;
        boolean highRes = ImageDimensionUtils.isHighResolution(finalWidth, finalHeight);
        
        try {
            upsertImageLink(bookId, "medium", httpsUrl, source, finalWidth, finalHeight, highRes);
            updateBookCoverPath(bookId, httpsUrl);
            
            log.info("Persisted external cover for book {} from {}: {}x{}", 
                bookId, source, finalWidth, finalHeight);
            
            return new PersistenceResult(true, httpsUrl, finalWidth, finalHeight, highRes);
            
        } catch (Exception e) {
            log.error("Failed to persist external cover for book {}: {}", bookId, e.getMessage(), e);
            return new PersistenceResult(false, httpsUrl, finalWidth, finalHeight, highRes);
        }
    }
    
    /**
     * Internal method to upsert a row in book_image_links.
     * Handles conflict resolution with ON CONFLICT DO UPDATE.
     */
    private void upsertImageLink(
        UUID bookId,
        String imageType,
        String url,
        String source,
        Integer width,
        Integer height,
        Boolean highRes
    ) {
        upsertImageLink(bookId, imageType, url, source, width, height, highRes, null);
    }

    /**
     * Internal method to upsert a row in book_image_links with optional S3 path.
     * Handles conflict resolution with ON CONFLICT DO UPDATE.
     */
    private void upsertImageLink(
        UUID bookId,
        String imageType,
        String url,
        String source,
        Integer width,
        Integer height,
        Boolean highRes,
        String s3ImagePath
    ) {
        jdbcTemplate.update("""
            INSERT INTO book_image_links (
                id, book_id, image_type, url, source,
                width, height, is_high_resolution, s3_image_path, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
            ON CONFLICT (book_id, image_type) DO UPDATE SET
                url = EXCLUDED.url,
                source = EXCLUDED.source,
                width = EXCLUDED.width,
                height = EXCLUDED.height,
                is_high_resolution = EXCLUDED.is_high_resolution,
                s3_image_path = EXCLUDED.s3_image_path,
                created_at = NOW()
            """,
            IdGenerator.generate(),
            bookId,
            imageType,
            url,
            source,
            width,
            height,
            highRes,
            s3ImagePath
        );
    }
    
    /**
     * Updates the books.s3_image_path column with the canonical cover URL.
     * This column serves as the primary cover reference for the book.
     */
    private void updateBookCoverPath(UUID bookId, String coverUrl) {
        try {
            jdbcTemplate.update(
                "UPDATE books SET s3_image_path = ? WHERE id = ?",
                coverUrl,
                bookId
            );
        } catch (Exception e) {
            log.warn("Failed to update books.s3_image_path for book {}: {}", bookId, e.getMessage());
        }
    }
    
    /**
     * Returns priority ranking for Google Books image types (lower = better quality).
     * 
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.ImageDimensionUtils#getTypePriority(String)} instead.
     * This method duplicates image type ranking logic that is now centralized in ImageDimensionUtils.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * int priority = getImageTypePriority(imageType);
     * 
     * // New:
     * int priority = ImageDimensionUtils.getTypePriority(imageType);
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    private int getImageTypePriority(String imageType) {
        return switch (imageType.toLowerCase()) {
            case "extralarge" -> 1;
            case "large" -> 2;
            case "medium" -> 3;
            case "small" -> 4;
            case "thumbnail" -> 5;
            case "smallthumbnail" -> 6;
            default -> 7;
        };
    }
}
