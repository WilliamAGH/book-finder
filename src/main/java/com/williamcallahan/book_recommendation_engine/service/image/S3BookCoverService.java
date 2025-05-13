package com.williamcallahan.book_recommendation_engine.service.image;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Service to handle storing and retrieving book cover images in S3 compatible storage.
 * This service provides high-resolution persistent storage for book covers.
 */
@Service
public class S3BookCoverService {
    private static final Logger logger = LoggerFactory.getLogger(S3BookCoverService.class);
    private static final String BOOKS_DIRECTORY = "books/";
    private static final String LARGE_SUFFIX = "-lg";
    private static final int MIN_DIMENSION_FOR_LG_SUFFIX = 150;
    
    @Value("${s3.bucket:book-finder}")
    private String s3Bucket;
    
    @Value("${s3.cdn-url:${S3_CDN_URL:https://book-finder.sfo3.digitaloceanspaces.com}}")
    private String s3CdnUrl;
    
    @Value("${s3.public-cdn-url:${S3_PUBLIC_CDN_URL:}}")
    private String s3PublicCdnUrl;
    
    @Value("${s3.server-url:https://sfo3.digitaloceanspaces.com}")
    private String s3ServerUrl;
    
    @Value("${s3.access-key-id:}")
    private String s3AccessKeyId;
    
    @Value("${s3.secret-access-key:}")
    private String s3SecretAccessKey;
    
    @Value("${s3.enabled:true}")
    private boolean s3Enabled;
    
    private S3Client s3Client;
    private final WebClient webClient;
    
    // Cache to avoid repeated S3 HEAD requests for objects we know exist
    private final Map<String, Boolean> objectExistsCache = new ConcurrentHashMap<>();

    // Using the shared ImageDetails class now
    // private static class ImageValidationDetails { ... } // Removed

    public S3BookCoverService(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }
    
    @PostConstruct
    public void init() {
        if (!s3Enabled) {
            logger.info("S3 book cover storage is disabled");
            return;
        }
        
        if (s3AccessKeyId.isEmpty() || s3SecretAccessKey.isEmpty()) {
            logger.warn("S3 credentials not provided. S3 book cover storage will be disabled.");
            s3Enabled = false;
            return;
        }
        
        try {
            AwsBasicCredentials credentials = AwsBasicCredentials.create(s3AccessKeyId, s3SecretAccessKey);
            
            s3Client = S3Client.builder()
                    .region(Region.US_WEST_2) // This is ignored for custom endpoints
                    .endpointOverride(URI.create(s3ServerUrl))
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .build();
            
            logger.info("S3 book cover storage initialized with bucket: {}, CDN URL: {}", s3Bucket, s3CdnUrl);
        } catch (Exception e) {
            logger.error("Failed to initialize S3 client", e);
            s3Enabled = false;
        }
    }
    
    @PreDestroy
    public void destroy() {
        if (s3Client != null) {
            s3Client.close();
            logger.info("S3 client closed");
        }
    }
    
    /**
     * Generate a consistent S3 key for a book cover based on book ID and source
     * @param bookId The Google Books ID
     * @param fileExtension The file extension (e.g., jpg, png)
     * @param source The source of the image (e.g., GOOGLE_BOOKS, OPEN_LIBRARY, LONGITOOD)
     * @return The S3 key (path) for the book cover
     */
    public String generateS3Key(String bookId, String fileExtension, String source) {
        if (bookId == null || bookId.isEmpty()) {
            throw new IllegalArgumentException("Book ID cannot be null or empty");
        }
        if (!bookId.matches("[a-zA-Z0-9_-]+")) {
            throw new IllegalArgumentException("Book ID contains invalid characters. Only alphanumeric characters, hyphens, and underscores are allowed.");
        }
        // Ensure the file extension starts with a dot
        if (!fileExtension.startsWith(".")) {
            fileExtension = "." + fileExtension;
        }
        
        // Normalize source to a valid filename component
        String normalizedSource = source != null ? source.toLowerCase().replaceAll("[^a-z0-9_-]", "-") : "unknown";
        
        return BOOKS_DIRECTORY + bookId + LARGE_SUFFIX + "-" + normalizedSource + fileExtension;
    }
    
    /**
     * Generate a consistent S3 key for a book cover based on book ID (backward compatibility)
     * @param bookId The Google Books ID
     * @param fileExtension The file extension (e.g., jpg, png)
     * @return The S3 key (path) for the book cover
     */
    public String generateS3Key(String bookId, String fileExtension) {
        return generateS3Key(bookId, fileExtension, "google-books");
    }
    /**
     * Check if a book cover already exists in S3
     * @param bookId The Google Books ID
     * @param fileExtension The file extension (e.g., jpg, png)
     * @param source The source of the image (e.g., GOOGLE_BOOKS, OPEN_LIBRARY, LONGITOOD)
     * @return true if the cover exists in S3, false otherwise
     */
    public boolean coverExistsInS3(String bookId, String fileExtension, String source) {
        if (!s3Enabled) {
            return false;
        }
        
        String s3Key = generateS3Key(bookId, fileExtension, source);
        
        // Check cache first
        if (objectExistsCache.containsKey(s3Key)) {
            return objectExistsCache.get(s3Key);
        }
        
        try {
            HeadObjectResponse response = s3Client.headObject(HeadObjectRequest.builder()
                    .bucket(s3Bucket)
                    .key(s3Key)
                    .build());
            
            boolean exists = response != null;
            objectExistsCache.put(s3Key, exists);
            return exists;
        } catch (NoSuchKeyException e) {
            objectExistsCache.put(s3Key, false);
            return false;
        } catch (Exception e) {
            logger.warn("Error checking if cover exists in S3 for book {}: {}", bookId, e.getMessage());
            return false;
        }
    }
    
    /**
     * Check if a book cover already exists in S3 with any source
     * @param bookId The Google Books ID
     * @param fileExtension The file extension (e.g., jpg, png)
     * @return true if the cover exists in S3 with any source, false otherwise
     */
    public boolean coverExistsInS3(String bookId, String fileExtension) {
        // Check for common sources, including "unknown" for images processed from local cache without a specific original source.
        String[] sourcesToTry = {"google-books", "open-library", "longitood", "unknown"};
        for (String source : sourcesToTry) {
            if (coverExistsInS3(bookId, fileExtension, source)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Get the CDN URL for a book cover in S3
     * @param bookId The Google Books ID
     * @param fileExtension The file extension (e.g., jpg, png)
     * @param source The source of the image (e.g., GOOGLE_BOOKS, OPEN_LIBRARY, LONGITOOD)
     * @return The CDN URL for the book cover
     */
    public String getS3CoverUrl(String bookId, String fileExtension, String source) {
        if (!s3Enabled) {
            return null;
        }
        
        String s3Key = generateS3Key(bookId, fileExtension, source);
        
        // Use public CDN URL if available, otherwise fall back to direct S3 CDN URL
        if (s3PublicCdnUrl != null && !s3PublicCdnUrl.isEmpty()) {
            return s3PublicCdnUrl + "/" + s3Key;
        }
        
        return s3CdnUrl + "/" + s3Key;
    }
    
    /**
     * Get the CDN URL for a book cover in S3 with the best available source
     * @param bookId The Google Books ID
     * @param fileExtension The file extension (e.g., jpg, png)
     * @return The CDN URL for the book cover from the best available source
     */
    public String getS3CoverUrl(String bookId, String fileExtension) {
        // Try to find the cover in the preferred order based on image quality:
        // 1. Google Books (highest quality with zoom=5 and fife=w800)
        // 2. Open Library (good quality with -L size parameter)
        // 3. Longitood (variable quality)
        // 4. Unknown (covers processed from local cache without a specific original source)
        String[] sourcesToTry = {"google-books", "open-library", "longitood", "unknown"};
        
        for (String source : sourcesToTry) {
            if (coverExistsInS3(bookId, fileExtension, source)) {
                logger.debug("Found S3 cover for book {} from source {}", bookId, source);
                return getS3CoverUrl(bookId, fileExtension, source);
            }
        }
        
        // If no specific source is found, this implies it doesn't exist under common/expected keys.
        // Returning a URL for a non-existent "google-books" default might lead to 404s if it truly isn't there.
        // However, the current behavior is to return this default.
        // For consistency, we'll keep it, but ideally, this might return null if no version is found.
        logger.warn("No S3 cover found for book {} under any checked sources. Falling back to 'google-books' default URL construction, which may not exist.", bookId);
        return getS3CoverUrl(bookId, fileExtension, "google-books");
    }
    
    /**
     * Upload a book cover to S3 from a remote URL asynchronously
     * @param bookId The Google Books ID
     * @param remoteUrl The remote URL of the book cover
     * @param source The source of the image (e.g., GOOGLE_BOOKS, OPEN_LIBRARY, LONGITOOD)
     * @return CompletableFuture with ImageDetails containing CDN URL and dimensions, or ImageDetails with null/original URL if upload failed
     */
    public CompletableFuture<ImageDetails> uploadCoverFromUrlAsync(String bookId, String remoteUrl, String source) {
        // Perform initial synchronous checks first
        if (!s3Enabled) {
            logger.debug("S3 storage is disabled, skipping upload for book {}", bookId);
            return CompletableFuture.completedFuture(new ImageDetails(remoteUrl)); // Return original URL, no S3 attempt
        }
        
        if (remoteUrl == null || remoteUrl.isEmpty()) {
            logger.debug("Remote URL is empty, skipping upload for book {}", bookId);
            return CompletableFuture.completedFuture(new ImageDetails(null));
        }
        
        // If it's already an S3 URL from this service, try to get its dimensions if we don't have them
        // This scenario is less likely here as BookImageOrchestrationService usually handles S3 pre-checks
        if (isS3Url(remoteUrl)) {
            logger.debug("URL is already from our S3 bucket for book {}: {}. Attempting to ensure dimensions are known.", bookId, remoteUrl);
            // Potentially, I could add a method to fetch dimensions for an existing S3 object if needed,
            // but for now, assume if it's an S3 URL, its dimensions were set when it was first uploaded.
            // If this method is called with an S3 URL, it implies a direct request to "process" it.
            // it'll return it with unknown dimensions, assuming a higher layer might know them or it's a re-check.
            return CompletableFuture.completedFuture(new ImageDetails(remoteUrl)); 
        }
        
        if (remoteUrl.startsWith("/") && !remoteUrl.startsWith("//")) {
            logger.debug("URL is local, skipping S3 upload for book {}. This method is for remote URLs.", bookId);
            return CompletableFuture.completedFuture(new ImageDetails(remoteUrl)); 
        }
        
        if (remoteUrl.contains("image-not-available.png") || remoteUrl.contains("placeholder")) {
            logger.debug("URL contains placeholder indicators, skipping upload for book {}", bookId);
            return CompletableFuture.completedFuture(new ImageDetails(remoteUrl));
        }

        // If initial checks pass, proceed with potential S3 interaction and download
        String fileExtension = getFileExtensionFromUrl(remoteUrl);
        if (coverExistsInS3(bookId, fileExtension, source)) {
            logger.debug("Cover already exists in S3 for book {} from source {}, skipping upload", bookId, source);
            String existingS3Url = getS3CoverUrl(bookId, fileExtension, source);
            // We don't know dimensions from just a HEAD request
            // A more advanced cache for S3 objects could store dimensions
            // For now, return existing URL with unknown dimensions
            return CompletableFuture.completedFuture(new ImageDetails(existingS3Url));
        }

        // Now start the asynchronous download and upload process
        logger.debug("Downloading image from {} for book {}", remoteUrl, bookId);
        logger.debug("Downloading image from {} for book {}", remoteUrl, bookId);
        return webClient.get()
                .uri(remoteUrl)
                .retrieve()
                .bodyToMono(byte[].class)
                .timeout(Duration.ofSeconds(10))
                .toFuture()
                .thenCompose(imageBytes -> {
                    if (imageBytes == null) {
                        logger.warn("Null image data received from URL {} for book {}", remoteUrl, bookId);
                        return CompletableFuture.completedFuture(new ImageDetails(remoteUrl));
                    }
                    if (imageBytes.length < 2048) { // Minimum 2KB
                        logger.warn("Image too small ({}B) from URL {} for book {}, likely a placeholder",
                                   imageBytes.length, remoteUrl, bookId);
                        return CompletableFuture.completedFuture(new ImageDetails(remoteUrl));
                    }

                    String s3Key = generateS3Key(bookId, fileExtension, source);
                    ImageDetails imageDetails = getImageDetailsIfValid(imageBytes);

                    if (!imageDetails.areDimensionsKnown() || !isImageFormatValid(imageBytes)) {
                         logger.warn("Invalid image format or unreadable image from URL {} for book {}", remoteUrl, bookId);
                         return CompletableFuture.completedFuture(new ImageDetails(remoteUrl));
                    }

                    if (imageDetails.getWidth() < MIN_DIMENSION_FOR_LG_SUFFIX || imageDetails.getHeight() < MIN_DIMENSION_FOR_LG_SUFFIX) {
                        logger.warn("Image for book {} from {} is too small ({}x{}) to be stored with '-lg' suffix. "+
                                    "Skipping S3 upload for this version, as it may be low quality.",
                                    bookId, remoteUrl, imageDetails.getWidth(), imageDetails.getHeight());
                        return CompletableFuture.completedFuture(new ImageDetails(remoteUrl, imageDetails.getWidth(), imageDetails.getHeight(), true)); // Valid, but too small for S3 -lg
                    }

                    try {
                        byte[] imageBytesToUpload = Arrays.copyOf(imageBytes, imageBytes.length);
                        s3Client.putObject(PutObjectRequest.builder()
                                .bucket(s3Bucket)
                                .key(s3Key)
                                .contentType(getContentTypeFromExtension(fileExtension))
                                .build(), RequestBody.fromBytes(imageBytesToUpload));
                        
                        String s3Url = getS3CoverUrl(bookId, fileExtension, source);
                        logger.info("Successfully uploaded cover to S3 for book {}: {}", bookId, s3Url);
                        objectExistsCache.put(s3Key, true);
                        return CompletableFuture.completedFuture(new ImageDetails(s3Url, imageDetails.getWidth(), imageDetails.getHeight(), true));
                    } catch (Exception e) {
                        logger.error("S3 putObject failed for book {} from URL {}: {}", bookId, remoteUrl, e.getMessage());
                        return CompletableFuture.completedFuture(new ImageDetails(remoteUrl, imageDetails.getWidth(), imageDetails.getHeight(), true)); // Upload failed, return original with known dims
                    }
                })
                .exceptionally(ex -> {
                    logger.error("Error downloading or processing image for book {} from URL {}: {}",
                                 bookId, remoteUrl, ex.getMessage());
                    return new ImageDetails(remoteUrl);
                });
    }

    /**
     * Upload a book cover to S3 from a byte array asynchronously
     * This method is useful for uploading locally cached files
     * @param bookId The Book ID
     * @param imageBytes The image data as a byte array
     * @param fileExtension The file extension for the S3 key (e.g., ".jpg")
     * @param source The source of the image (e.g., "google-books")
     * @return CompletableFuture with ImageDetails containing S3 URL and dimensions, or ImageDetails with null URL if upload failed/skipped.
     */
    public CompletableFuture<ImageDetails> uploadCoverFromBytesAsync(String bookId, byte[] imageBytes, String fileExtension, String source) {
        if (!s3Enabled) {
            logger.debug("S3 storage is disabled, skipping upload from bytes for book {}", bookId);
            // Cannot return original URL as we only have bytes. Return null ImageDetails or with placeholder.
            return CompletableFuture.completedFuture(new ImageDetails(null, 0,0, false));
        }

        if (imageBytes == null || imageBytes.length == 0) {
            logger.warn("Empty image data provided for book {}, skipping upload from bytes.", bookId);
            return CompletableFuture.completedFuture(new ImageDetails(null, 0,0, false));
        }

        // Validate image and get dimensions
        ImageDetails imageDetails = getImageDetailsIfValid(imageBytes);
        if (!imageDetails.areDimensionsKnown() || !isImageFormatValid(imageBytes)) {
            logger.warn("Invalid image format or unreadable image from bytes for book {}", bookId);
            return CompletableFuture.completedFuture(new ImageDetails(null, imageDetails.getWidth(), imageDetails.getHeight(), false)); // Dimensions might be partially known
        }

        if (imageDetails.getWidth() < MIN_DIMENSION_FOR_LG_SUFFIX || imageDetails.getHeight() < MIN_DIMENSION_FOR_LG_SUFFIX) {
            logger.warn("Image for book {} from bytes is too small ({}x{}) to be stored with '-lg' suffix. Skipping S3 upload.",
                        bookId, imageDetails.getWidth(), imageDetails.getHeight());
            // Return details of the image, but indicate it wasn't uploaded to S3 by not providing an S3 URL
            // The caller (BookImageOrchestrationService) will then use the local cache path
            return CompletableFuture.completedFuture(new ImageDetails(null, imageDetails.getWidth(), imageDetails.getHeight(), true));
        }

        String s3Key = generateS3Key(bookId, fileExtension, source);

        // Check if it already exists (though less likely if we are uploading from a fresh local cache miss)
        if (coverExistsInS3(bookId, fileExtension, source)) {
            logger.debug("Cover from bytes already exists in S3 for book {} from source {}, skipping upload", bookId, source);
            String existingS3Url = getS3CoverUrl(bookId, fileExtension, source);
            return CompletableFuture.completedFuture(new ImageDetails(existingS3Url, imageDetails.getWidth(), imageDetails.getHeight(), true));
        }
        
        try {
            // As S3Client.putObject is synchronous, we wrap it in a CompletableFuture if needed,
            // or ensure this method is called from an async context.
            // For simplicity here, assuming it's okay to be blocking within this thenCompose/thenApply chain.
            s3Client.putObject(PutObjectRequest.builder()
                    .bucket(s3Bucket)
                    .key(s3Key)
                    .contentType(getContentTypeFromExtension(fileExtension))
                    .build(), RequestBody.fromBytes(imageBytes));
            
            String s3Url = getS3CoverUrl(bookId, fileExtension, source);
            logger.info("Successfully uploaded cover from bytes to S3 for book {}: {}", bookId, s3Url);
            objectExistsCache.put(s3Key, true);
            return CompletableFuture.completedFuture(new ImageDetails(s3Url, imageDetails.getWidth(), imageDetails.getHeight(), true));
        } catch (Exception e) {
            logger.error("S3 putObject failed for book {} from bytes: {}", bookId, e.getMessage(), e);
            // Return details with original dimensions but no S3 URL, indicating failure
            return CompletableFuture.completedFuture(new ImageDetails(null, imageDetails.getWidth(), imageDetails.getHeight(), true));
        }
    }

    // Renamed and modified to use ImageDetails
    private ImageDetails getImageDetailsIfValid(byte[] imageBytes) {
        if (imageBytes == null) return new ImageDetails(null, 0, 0, false);

        // Basic format validation (magic bytes) can remain here or be part of a more robust validation
        if (!isImageFormatValid(imageBytes)) {
             logger.warn("Image magic bytes do not match JPEG, PNG, or GIF.");
            return new ImageDetails(null, 0, 0, false);
        }

        try {
            BufferedImage bufferedImage = ImageIO.read(new ByteArrayInputStream(imageBytes));
            if (bufferedImage == null) {
                logger.warn("Could not read image dimensions - ImageIO.read returned null despite magic bytes match.");
                return new ImageDetails(null, 0, 0, false);
            }
            int width = bufferedImage.getWidth();
            int height = bufferedImage.getHeight();
            if (width <= 0 || height <= 0 || width > 5000 || height > 5000) {
                logger.warn("Image dimensions invalid ({}x{}) after ImageIO.read, likely not a valid image or too large.", width, height);
                return new ImageDetails(null, width, height, false);
            }
            return new ImageDetails(null, width, height, true);
        } catch (IOException e) {
            logger.warn("Error reading image dimensions with ImageIO: {}", e.getMessage());
            return new ImageDetails(null, 0, 0, false);
        }
    }

    // Helper for basic magic byte checking
    private boolean isImageFormatValid(byte[] imageBytes) {
        if (imageBytes == null || imageBytes.length < 8) return false; // Min length for common headers

        // Check for JPEG magic bytes (FF D8 FF)
        if ((imageBytes[0] & 0xFF) == 0xFF && (imageBytes[1] & 0xFF) == 0xD8 && (imageBytes[2] & 0xFF) == 0xFF) {
            return true; // JPEG
        }
        // Check for PNG magic bytes (89 50 4E 47 0D 0A 1A 0A)
        if ((imageBytes[0] & 0xFF) == 0x89 && (imageBytes[1] & 0xFF) == 0x50 && (imageBytes[2] & 0xFF) == 0x4E &&
            (imageBytes[3] & 0xFF) == 0x47 && (imageBytes[4] & 0xFF) == 0x0D && (imageBytes[5] & 0xFF) == 0x0A &&
            (imageBytes[6] & 0xFF) == 0x1A && (imageBytes[7] & 0xFF) == 0x0A) {
            return true; // PNG
        }
        // Check for GIF magic bytes (GIF8)
        if ((imageBytes[0] & 0xFF) == 'G' && (imageBytes[1] & 0xFF) == 'I' && (imageBytes[2] & 0xFF) == 'F' &&
            (imageBytes[3] & 0xFF) == '8') {
            return true; // GIF
        }
        return false;
    }

    /**
     * Upload a book cover to S3 from a remote URL asynchronously (backward compatibility)
     * @param bookId The Google Books ID
     * @param remoteUrl The remote URL of the book cover
     * @return CompletableFuture with ImageDetails containing CDN URL and dimensions, or ImageDetails with original URL if upload failed
     */
    public CompletableFuture<ImageDetails> uploadCoverFromUrlAsync(String bookId, String remoteUrl) {
        String source = "google-books"; // Default source
        if (remoteUrl != null) {
            if (remoteUrl.contains("openlibrary.org")) {
                source = "open-library";
            } else if (remoteUrl.contains("longitood.com")) {
                source = "longitood";
            }
        }
        return uploadCoverFromUrlAsync(bookId, remoteUrl, source);
    }
    
    /**
     * Upload a book cover to S3 from a remote URL (synchronous version)
     * @param bookId The Google Books ID
     * @param remoteUrl The remote URL of the book cover
     * @param source The source of the image (e.g., GOOGLE_BOOKS, OPEN_LIBRARY, LONGITOOD)
     * @return ImageDetails containing CDN URL and dimensions, or ImageDetails with original URL if upload failed
     */
    public ImageDetails uploadCoverFromUrl(String bookId, String remoteUrl, String source) {
        try {
            // Ensure the CompletableFuture completes and return its result.
            return uploadCoverFromUrlAsync(bookId, remoteUrl, source).get(15, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("Error or timeout uploading cover to S3 for book {} from URL {}: {}", bookId, remoteUrl, e.getMessage());
            return new ImageDetails(remoteUrl); // Fallback to original URL, dimensions unknown
        }
    }
    
    /**
     * Upload a book cover to S3 from a remote URL (synchronous version, backward compatibility)
     * @param bookId The Google Books ID
     * @param remoteUrl The remote URL of the book cover
     * @return ImageDetails containing CDN URL and dimensions, or ImageDetails with original URL if upload failed
     */
    public ImageDetails uploadCoverFromUrl(String bookId, String remoteUrl) {
        String source = "google-books"; // Default source
        if (remoteUrl != null) {
            if (remoteUrl.contains("openlibrary.org")) {
                source = "open-library";
            } else if (remoteUrl.contains("longitood.com")) {
                source = "longitood";
            }
        }
        return uploadCoverFromUrl(bookId, remoteUrl, source);
    }
    
    /**
     * Extract file extension from URL
     * @param url The URL
     * @return The file extension (with dot)
     */
    public String getFileExtensionFromUrl(String url) {
        String extension = ".jpg"; // Default extension
        
        if (url != null && url.contains(".")) {
            int queryParamIndex = url.indexOf("?");
            String urlWithoutParams = queryParamIndex > 0 ? url.substring(0, queryParamIndex) : url;
            
            int lastDotIndex = urlWithoutParams.lastIndexOf(".");
            if (lastDotIndex > 0 && lastDotIndex < urlWithoutParams.length() - 1) {
                String ext = urlWithoutParams.substring(lastDotIndex).toLowerCase();
                if (ext.matches("\\.(jpg|jpeg|png|gif|webp|svg)")) {
                    extension = ext;
                }
            }
        }
        
        return extension;
    }
    
    /**
     * Get content type from file extension
     * @param extension The file extension (with or without dot)
     * @return The content type
     */
    private String getContentTypeFromExtension(String extension) {
        if (extension.startsWith(".")) {
            extension = extension.substring(1);
        }
        
        switch (extension.toLowerCase()) {
            case "jpg":
            case "jpeg":
                return "image/jpeg";
            case "png":
                return "image/png";
            case "gif":
                return "image/gif";
            case "webp":
                return "image/webp";
            case "svg":
                return "image/svg+xml";
            default:
                return "image/jpeg";
        }
    }
    
    /**
     * Check if S3 client is configured and available
     * @return true if S3 client is available, false otherwise
     */
    public boolean isS3Enabled() {
        return this.s3Client != null;
    }

    /**
     * Checks if the given URL is an S3 URL managed by this service
     * @param url The URL to check.
     * @return true if the URL is an S3 URL, false otherwise
     */
    public boolean isS3Url(String url) {
        if (url == null || url.isEmpty()) {
            return false;
        }
        boolean isMainCdn = s3CdnUrl != null && !s3CdnUrl.isEmpty() && url.startsWith(s3CdnUrl);
        boolean isPublicCdn = s3PublicCdnUrl != null && !s3PublicCdnUrl.isEmpty() && url.startsWith(s3PublicCdnUrl);
        return isMainCdn || isPublicCdn;
    }
}
