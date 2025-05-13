package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Mono;

import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService.CoverImageSource; 
import com.williamcallahan.book_recommendation_engine.types.ExternalCoverService;

/**
 * Service for interacting with S3-compatible storage for book cover images.
 * Handles uploading images to S3, checking for existence, and generating S3 URLs.
 * Implements ExternalCoverService to provide a consistent interface for fetching covers from S3.
 */
@Service
public class S3BookCoverService implements ExternalCoverService {
    private static final Logger logger = LoggerFactory.getLogger(S3BookCoverService.class);
    private static final String BOOKS_DIRECTORY = "books/";
    private static final String LARGE_SUFFIX = "-lg";
    
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

    @Value("${app.cover-cache.max-file-size-bytes:5242880}") 
    private long maxFileSizeBytes; 
    
    private S3Client s3Client;
    private final WebClient webClient;
    
    private final Map<String, Boolean> objectExistsCache = new ConcurrentHashMap<>();

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
                    .region(Region.US_WEST_2) 
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
    
    public String generateS3Key(String bookId, String fileExtension, String source) {
        if (bookId == null || bookId.isEmpty()) {
            throw new IllegalArgumentException("Book ID cannot be null or empty");
        }
        if (!bookId.matches("[a-zA-Z0-9_-]+")) {
            throw new IllegalArgumentException("Book ID contains invalid characters. Only alphanumeric characters, hyphens, and underscores are allowed.");
        }
        if (fileExtension == null || fileExtension.isEmpty() || !fileExtension.startsWith(".")) {
             fileExtension = ".jpg"; 
        }
        String normalizedSource = source != null ? source.toLowerCase().replaceAll("[^a-z0-9_-]", "-") : "unknown";
        return BOOKS_DIRECTORY + bookId + LARGE_SUFFIX + "-" + normalizedSource + fileExtension;
    }

    @Override
    public Mono<ImageDetails> fetchCover(Book book) {
        if (!s3Enabled || book == null) {
            return Mono.empty();
        }
        String bookKey = book.getId();
        if (bookKey == null || bookKey.trim().isEmpty()) bookKey = book.getIsbn13();
        if (bookKey == null || bookKey.trim().isEmpty()) bookKey = book.getIsbn10();
        
        if (bookKey == null || bookKey.trim().isEmpty()) {
            logger.warn("Cannot fetch S3 cover for book without a valid ID or ISBN. Title: {}", book.getTitle());
            return Mono.empty();
        }

        final String finalBookKey = bookKey;
        String fileExtension = ".jpg"; // Default, could be improved if book object has reliable extension info
        
        // Using CoverImageSource enum to generate source strings for S3 keys
        String[] s3SourceStrings = {
            getSourceString(CoverImageSource.GOOGLE_BOOKS_API),
            getSourceString(CoverImageSource.OPEN_LIBRARY_API),
            getSourceString(CoverImageSource.LONGITOOD_API),
            getSourceString(CoverImageSource.LOCAL_CACHE),
            "unknown" // Fallback for generic/unknown source
        };

        for (String sourceForS3Key : s3SourceStrings) {
            if (coverExistsInS3(finalBookKey, fileExtension, sourceForS3Key)) {
                String s3Key = generateS3Key(finalBookKey, fileExtension, sourceForS3Key);
                String cdnUrl = getS3CoverUrl(finalBookKey, fileExtension, sourceForS3Key);
                logger.debug("Found existing S3 cover for book {} from source key '{}': {}", finalBookKey, sourceForS3Key, cdnUrl);
                return Mono.just(new ImageDetails(cdnUrl, "S3_CACHE", s3Key, CoverImageSource.S3_CACHE, ImageResolutionPreference.ORIGINAL));
            }
        }
        logger.debug("No S3 cover found for book {} after checking common source keys.", finalBookKey);
        return Mono.empty(); 
    }

    public boolean coverExistsInS3(String bookId, String fileExtension, String source) {
        if (!s3Enabled) return false;
        String s3Key = generateS3Key(bookId, fileExtension, source);
        if (objectExistsCache.containsKey(s3Key)) return objectExistsCache.get(s3Key);
        try {
            HeadObjectResponse response = s3Client.headObject(HeadObjectRequest.builder().bucket(s3Bucket).key(s3Key).build());
            boolean exists = response != null;
            objectExistsCache.put(s3Key, exists);
            return exists;
        } catch (NoSuchKeyException e) {
            objectExistsCache.put(s3Key, false);
            return false;
        } catch (Exception e) {
            logger.error("Error checking S3 object existence for key {}: {}", s3Key, e.getMessage());
            objectExistsCache.put(s3Key, false); 
            return false;
        }
    }

    public boolean coverExistsInS3(String bookId, String fileExtension) {
        String[] sourcesToTry = {"google-books", "open-library", "longitood", "local-cache", "unknown"};
        for (String source : sourcesToTry) {
            if (coverExistsInS3(bookId, fileExtension, source)) {
                return true;
            }
        }
        return false;
    }
    
    public Mono<ImageDetails> uploadCoverToS3Async(String imageUrl, String bookId, String source) {
        if (!s3Enabled || imageUrl == null || imageUrl.isEmpty() || bookId == null || bookId.isEmpty()) {
            logger.debug("S3 upload skipped: S3 disabled, or imageUrl/bookId is null/empty. ImageUrl: {}, BookId: {}", imageUrl, bookId);
            return Mono.just(new ImageDetails(imageUrl, source, imageUrl, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL)); 
        }
        final String s3Source = (source != null && !source.isEmpty()) ? source : "unknown";

        return webClient.get().uri(imageUrl).retrieve().bodyToMono(byte[].class)
            .timeout(Duration.ofSeconds(10))
            .flatMap(imageBytes -> {
                try {
                    ImageValidationResult validationResult = validateAndGetImageExtension(imageBytes, bookId);
                    if (!validationResult.isValid()) {
                        logger.warn("Image validation failed for book {}: {}. URL: {}", bookId, validationResult.getReason(), imageUrl);
                        return Mono.just(new ImageDetails(imageUrl, source, imageUrl, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL)); 
                    }

                    String fileExtension = validationResult.getExtension();
                    String s3Key = generateS3Key(bookId, fileExtension, s3Source);

                    try {
                        HeadObjectResponse headResponse = s3Client.headObject(HeadObjectRequest.builder().bucket(s3Bucket).key(s3Key).build());
                        if (headResponse.contentLength() == imageBytes.length) {
                            logger.info("Cover for book {} already exists in S3 with same size, skipping upload. Key: {}", bookId, s3Key);
                            String cdnUrl = getS3CoverUrl(bookId, fileExtension, s3Source);
                            objectExistsCache.put(s3Key, true);
                            return Mono.just(new ImageDetails(cdnUrl, "S3_CACHE", s3Key, CoverImageSource.S3_CACHE, ImageResolutionPreference.ORIGINAL));
                        }
                    } catch (NoSuchKeyException e) { /* Proceed to upload */ }
                    catch (Exception e) { logger.warn("Error checking existing S3 object for book {}: {}. Proceeding with upload.", bookId, e.getMessage()); }

                    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                            .bucket(s3Bucket).key(s3Key).contentType(validationResult.getMimeType()).build();
                    s3Client.putObject(putObjectRequest, RequestBody.fromBytes(imageBytes));
                    
                    String cdnUrl = getS3CoverUrl(bookId, fileExtension, s3Source);
                    objectExistsCache.put(s3Key, true);
                    logger.info("Successfully uploaded cover for book {} to S3. Key: {}", bookId, s3Key);
                    
                    BufferedImage img = ImageIO.read(new ByteArrayInputStream(imageBytes));
                    if (img != null) {
                        return Mono.just(new ImageDetails(cdnUrl, "S3_UPLOAD", s3Key, CoverImageSource.S3_CACHE, ImageResolutionPreference.ORIGINAL, img.getWidth(), img.getHeight()));
                    } else { 
                        return Mono.just(new ImageDetails(cdnUrl, "S3_UPLOAD", s3Key, CoverImageSource.S3_CACHE, ImageResolutionPreference.ORIGINAL));
                    }
                } catch (IOException e) {
                    logger.error("IOException during S3 upload process for book {}: {}. URL: {}", bookId, e.getMessage(), imageUrl, e);
                    return Mono.just(new ImageDetails(imageUrl, source, imageUrl, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL)); 
                } catch (Exception e) {
                    logger.error("Unexpected exception during S3 upload process for book {}: {}. URL: {}", bookId, e.getMessage(), imageUrl, e);
                    return Mono.just(new ImageDetails(imageUrl, source, imageUrl, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL));
                }
            })
            .onErrorResume(e -> {
                logger.error("Error downloading image for S3 upload for book {}: {}. URL: {}", bookId, e.getMessage(), imageUrl, e);
                return Mono.just(new ImageDetails(imageUrl, source, imageUrl, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL));
            });
    }
 
    public ImageDetails uploadCoverToS3(String imageUrl, String bookId, String source) {
        try {
            return uploadCoverToS3Async(imageUrl, bookId, source).block(Duration.ofSeconds(15));
        } catch (Exception e) {
            logger.error("Error or timeout uploading cover to S3 for book {} from URL {}: {}", bookId, imageUrl, e.getMessage());
            return new ImageDetails(imageUrl, source, imageUrl, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL);
        }
    }
    
    public ImageDetails uploadCoverToS3(String imageUrl, String bookId) {
        String source = "google-books"; 
        if (imageUrl != null) {
            if (imageUrl.contains("openlibrary.org")) source = "open-library";
            else if (imageUrl.contains("longitood.com")) source = "longitood";
        }
        return uploadCoverToS3(imageUrl, bookId, source);
    }

    public String getS3CoverUrl(String bookId, String fileExtension, String source) {
        String s3Key = generateS3Key(bookId, fileExtension, source);
        return (s3PublicCdnUrl != null && !s3PublicCdnUrl.isEmpty() ? s3PublicCdnUrl : s3CdnUrl) + "/" + s3Key;
    }

    public String getS3CoverUrl(String bookId, String fileExtension) {
        String[] sourcesToTry = {"google-books", "open-library", "longitood", "local-cache", "unknown"};
        for (String source : sourcesToTry) {
            if (coverExistsInS3(bookId, fileExtension, source)) {
                return getS3CoverUrl(bookId, fileExtension, source);
            }
        }
        logger.warn("getS3CoverUrl(bookId, fileExtension) called for book {} with extension {}, but no cover found from any known source.", bookId, fileExtension);
        return null; 
    }

    public String getFileExtensionFromUrl(String url) {
        String extension = ".jpg"; 
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

    private String getFileExtensionFromMagicBytes(byte[] imageBytes) {
        if (imageBytes.length >=3 && (imageBytes[0] & 0xFF) == 0xFF && (imageBytes[1] & 0xFF) == 0xD8 && (imageBytes[2] & 0xFF) == 0xFF) return ".jpg";
        if (imageBytes.length >= 8 && (imageBytes[0] & 0xFF) == 0x89 && (imageBytes[1] & 0xFF) == 0x50 && (imageBytes[2] & 0xFF) == 0x4E &&
            (imageBytes[3] & 0xFF) == 0x47 && (imageBytes[4] & 0xFF) == 0x0D && (imageBytes[5] & 0xFF) == 0x0A &&
            (imageBytes[6] & 0xFF) == 0x1A && (imageBytes[7] & 0xFF) == 0x0A) return ".png";
        if (imageBytes.length >= 4 && (imageBytes[0] & 0xFF) == 'G' && (imageBytes[1] & 0xFF) == 'I' && (imageBytes[2] & 0xFF) == 'F' &&
            (imageBytes[3] & 0xFF) == '8') return ".gif";
        logger.warn("Could not determine image type from magic bytes, defaulting to .jpg");
        return ".jpg";
    }

    private String getContentTypeFromExtension(String extension) {
        if (extension.startsWith(".")) extension = extension.substring(1);
        switch (extension.toLowerCase()) {
            case "jpg": case "jpeg": return "image/jpeg";
            case "png": return "image/png";
            case "gif": return "image/gif";
            case "webp": return "image/webp";
            case "svg": return "image/svg+xml";
            default: return "image/jpeg";
        }
    }

    private boolean isImageFormatValid(byte[] imageBytes) {
        if (imageBytes == null || imageBytes.length < 8) {
            logger.warn("Image format validation: imageBytes is null or too short (length: {})", imageBytes != null ? imageBytes.length : "null");
            return false; 
        }
        if (((imageBytes[0] & 0xFF) == 0xFF && (imageBytes[1] & 0xFF) == 0xD8 && (imageBytes[2] & 0xFF) == 0xFF) || // JPG
            ((imageBytes[0] & 0xFF) == 0x89 && (imageBytes[1] & 0xFF) == 0x50 && (imageBytes[2] & 0xFF) == 0x4E && (imageBytes[3] & 0xFF) == 0x47)) { // PNG
            return true;
        }
        if (((imageBytes[0] & 0xFF) == 'G' && (imageBytes[1] & 0xFF) == 'I' && (imageBytes[2] & 0xFF) == 'F' && (imageBytes[3] & 0xFF) == '8')) { // GIF
            return true;
        }
        logger.warn("Image format validation: Unknown magic bytes.");
        return false;
    }

    private ImageValidationResult validateAndGetImageExtension(byte[] imageBytes, String bookId) {
        if (imageBytes == null) return new ImageValidationResult(false, null, null, "Null image bytes");
        try {
            BufferedImage bufferedImage = ImageIO.read(new ByteArrayInputStream(imageBytes));
            if (bufferedImage == null) return new ImageValidationResult(false, null, null, "Could not read image dimensions (ImageIO.read returned null)");
            
            int width = bufferedImage.getWidth();
            int height = bufferedImage.getHeight();
            if (width <= 0 || height <= 0 || width > 5000 || height > 5000) {
                return new ImageValidationResult(false, null, null, "Invalid image dimensions: " + width + "x" + height);
            }
            if (!isImageFormatValid(imageBytes)) {
                return new ImageValidationResult(false, null, null, "Invalid image format (magic bytes mismatch)");
            }
            
            if (imageBytes.length > this.maxFileSizeBytes) { 
                return new ImageValidationResult(false, null, null, "Image too large (size: " + imageBytes.length + " bytes, max: " + this.maxFileSizeBytes + " bytes)");
            }
            String extension = getFileExtensionFromMagicBytes(imageBytes);
            String mimeType = getContentTypeFromExtension(extension);
            return new ImageValidationResult(true, extension, mimeType, null);
        } catch (IOException e) {
            logger.error("IOException while validating image for book {}: {}", bookId, e.getMessage());
            return new ImageValidationResult(false, null, null, "IOException during validation: " + e.getMessage());
        }
    }

    private static class ImageValidationResult {
        private final boolean isValid;
        private final String extension;
        private final String mimeType; 
        private final String reason;

        public ImageValidationResult(boolean isValid, String extension, String mimeType, String reason) {
            this.isValid = isValid;
            this.extension = extension;
            this.mimeType = mimeType;
            this.reason = reason;
        }

        public boolean isValid() { return isValid; }
        public String getExtension() { return extension; }
        public String getMimeType() { return mimeType; }        
        public String getReason() { return reason; }
    }

    // Helper method to convert CoverImageSource enum to string for S3 keys
    private String getSourceString(CoverImageSource source) {
        if (source == null) {
            return "unknown";
        }
        switch (source) {
            case GOOGLE_BOOKS_API: return "google-books";
            case OPEN_LIBRARY_API: return "open-library";
            case LONGITOOD_API: return "longitood";
            case LOCAL_CACHE: return "local-cache";
            case S3_CACHE: return "s3-cache";
            case SYSTEM_PLACEHOLDER: return "system-placeholder";
            case ANY: // 'ANY' itself isn't a storage source, defaults to 'unknown' for key generation
            default: return "unknown";
        }
    }
}
