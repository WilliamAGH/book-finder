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
 * @deprecated This class has been replaced by {@link CoverPersistenceService}.
 * 
 * All methods in this class are deprecated and will be removed in version 1.0.0.
 * 
 * Migration guide:
 * - {@link #pickBestExternal(Map)} → No direct replacement (internal logic in CoverPersistenceService)
 * - {@link #upsertAllAndSetPrimary(UUID, Map, String)} → {@link CoverPersistenceService#persistFromGoogleImageLinks(UUID, Map, String)}
 * - {@link #setPrimaryS3(UUID, String, Integer, Integer, Boolean)} → {@link CoverPersistenceService#updateAfterS3Upload(UUID, String, String, Integer, Integer, com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource)}
 * 
 * Reason for deprecation:
 * This class mixed persistence logic with URL selection logic. The new CoverPersistenceService
 * provides a cleaner separation of concerns and handles both initial persistence and post-S3-upload
 * updates atomically.
 * 
 * @author William Callahan
 * @see CoverPersistenceService
 */
@Deprecated(since = "0.9.0", forRemoval = true)
@Service
@Slf4j
public class CoverImageService {
    
    private final JdbcTemplate jdbcTemplate;
    
    @org.springframework.beans.factory.annotation.Value("${s3.cdn-url:${S3_CDN_URL:}}")
    private String s3CdnUrl;

    @org.springframework.beans.factory.annotation.Value("${s3.public-cdn-url:${S3_PUBLIC_CDN_URL:}}")
    private String s3PublicCdnUrl;

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
     * @deprecated This method is now internal logic in {@link CoverPersistenceService#persistFromGoogleImageLinks(UUID, Map, String)}.
     * There is no direct replacement as this logic is now encapsulated. Will be removed in version 1.0.0.
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
    @Deprecated(since = "0.9.0", forRemoval = true)
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
     * @deprecated Use {@link CoverPersistenceService#persistFromGoogleImageLinks(UUID, Map, String)} instead.
     * Will be removed in version 1.0.0.
     * 
     * The new method provides the same functionality with better atomicity guarantees and cleaner
     * separation between initial persistence and post-upload updates.
     * 
     * Migration example:
     * <pre>{@code
     * // OLD:
     * coverImageService.upsertAllAndSetPrimary(bookId, imageLinks, "GOOGLE_BOOKS");
     * 
     * // NEW:
     * coverPersistenceService.persistFromGoogleImageLinks(bookId, imageLinks, "GOOGLE_BOOKS");
     * }</pre>
     * 
     * @param bookId Canonical book UUID
     * @param imageLinks Map from Google Books API
     * @param source Provider name ("GOOGLE_BOOKS")
     * @return PersistedCover with estimated metadata
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
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
        
        // Update books.s3_image_path ONLY if the canonical URL is an S3/Spaces URL
        if (canonicalCoverUrl != null && isS3Url(canonicalCoverUrl)) {
            try {
                jdbcTemplate.update(
                    "UPDATE books SET s3_image_path = ? WHERE id = ?",
                    canonicalCoverUrl,
                    bookId
                );
                
                log.info("Successfully persisted S3 cover metadata for book {}: {}x{}, highRes={}", 
                    bookId, canonicalWidth, canonicalHeight, canonicalHighRes);
            } catch (Exception e) {
                log.warn("Failed to update books.s3_image_path for book {}: {}", bookId, e.getMessage());
            }
        } else if (canonicalCoverUrl != null) {
            log.debug("Skipping books.s3_image_path update for book {} because URL is not S3: {}", bookId, canonicalCoverUrl);
        }
        
        return new PersistedCover(canonicalCoverUrl, canonicalWidth, canonicalHeight, canonicalHighRes);
    }
    
    /**
     * Estimates dimensions based on Google Books image type.
     * 
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.ImageDimensionUtils#estimateFromGoogleType(String)} instead.
     * Will be removed in version 1.0.0.
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    private record ImageEstimate(int width, int height, boolean highRes) {}
    
    @Deprecated(since = "0.9.0", forRemoval = true)
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

    private boolean isS3Url(String url) {
        if (url == null) return false;
        String lower = url.toLowerCase();
        if (s3CdnUrl != null && !s3CdnUrl.isBlank() && lower.contains(s3CdnUrl.toLowerCase())) return true;
        if (s3PublicCdnUrl != null && !s3PublicCdnUrl.isBlank() && lower.contains(s3PublicCdnUrl.toLowerCase())) return true;
        return lower.contains("digitaloceanspaces.com") || lower.contains("s3.amazonaws.com");
    }

    /**
     * Assigns a given S3 URL as the primary cover for the book and persists basic metadata.
     * 
     * @deprecated Use {@link CoverPersistenceService#updateAfterS3Upload(UUID, String, String, Integer, Integer, com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource)} instead.
     * Will be removed in version 1.0.0.
     * 
     * Migration example:
     * <pre>{@code
     * // OLD:
     * coverImageService.setPrimaryS3(bookId, s3Url, width, height, highRes);
     * 
     * // NEW:
     * coverPersistenceService.updateAfterS3Upload(
     *     bookId, 
     *     s3Key,  // S3 object key
     *     s3Url,  // Full CDN URL
     *     width, 
     *     height, 
     *     CoverImageSource.S3_CACHE
     * );
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    @Transactional
    public void setPrimaryS3(UUID bookId, String s3Url, Integer width, Integer height, Boolean highRes) {
        if (bookId == null || s3Url == null || s3Url.isBlank()) {
            return;
        }
        if (!isS3Url(s3Url)) {
            log.debug("Refusing to set non-S3 URL as primary for {}: {}", bookId, s3Url);
            return;
        }
        try {
            jdbcTemplate.update("UPDATE books SET s3_image_path = ? WHERE id = ?", s3Url, bookId);
            // Upsert a generic 's3' image link row for traceability
            jdbcTemplate.update(
                "INSERT INTO book_image_links (id, book_id, image_type, url, source, width, height, is_high_resolution, created_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW()) " +
                "ON CONFLICT (book_id, image_type) DO UPDATE SET url = EXCLUDED.url, source = EXCLUDED.source, width = EXCLUDED.width, height = EXCLUDED.height, is_high_resolution = EXCLUDED.is_high_resolution",
                IdGenerator.generate(),
                bookId,
                "s3",
                s3Url,
                "S3",
                width,
                height,
                highRes != null ? highRes : Boolean.FALSE
            );
        } catch (Exception e) {
            log.warn("Failed to set primary S3 cover for book {}: {}", bookId, e.getMessage());
        }
    }
}
