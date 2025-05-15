package com.williamcallahan.book_recommendation_engine.service.image;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.EnvironmentService;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.types.ProcessedImage;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.ExternalCoverService;
import com.williamcallahan.book_recommendation_engine.types.ImageProvenanceData;

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
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Mono;

@Service
public class S3BookCoverService implements ExternalCoverService {
    private static final Logger logger = LoggerFactory.getLogger(S3BookCoverService.class);
    private static final String BOOKS_DIRECTORY = "books/";
    private static final String LARGE_SUFFIX = "-lg";
    
    @Value("${s3.bucket-name}")
    private String s3BucketName;
    
    @Value("${s3.cdn-url}")
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
    private final ImageProcessingService imageProcessingService;
    private final EnvironmentService environmentService;
    private final ObjectMapper objectMapper;
    
    private final Map<String, Boolean> objectExistsCache = new ConcurrentHashMap<>();

    public S3BookCoverService(WebClient.Builder webClientBuilder, 
                              ImageProcessingService imageProcessingService,
                              EnvironmentService environmentService) {
        this.webClient = webClientBuilder.build();
        this.imageProcessingService = imageProcessingService;
        this.environmentService = environmentService;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
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
            logger.info("S3 book cover storage initialized with bucket: {}, CDN URL: {}", s3BucketName, s3CdnUrl);
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
            getSourceString(CoverImageSource.GOOGLE_BOOKS),
            getSourceString(CoverImageSource.OPEN_LIBRARY),
            getSourceString(CoverImageSource.LONGITOOD),
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
            HeadObjectResponse response = s3Client.headObject(HeadObjectRequest.builder().bucket(s3BucketName).key(s3Key).build());
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
    
    // Existing method, can be kept for calls not needing provenance or refactored
    public Mono<ImageDetails> uploadCoverToS3Async(String imageUrl, String bookId, String source) {
        // For now, let's assume calls to this specific signature don't have detailed provenance
        // Or, we can decide to construct a minimal ImageProvenanceData here if needed.
        // For this iteration, it will not log provenance unless called by a method that supplies it.
        return uploadCoverToS3Async(imageUrl, bookId, source, null);
    }

    public Mono<ImageDetails> uploadCoverToS3Async(String imageUrl, String bookId, String source, ImageProvenanceData provenanceData) {
        if (!s3Enabled || imageUrl == null || imageUrl.isEmpty() || bookId == null || bookId.isEmpty()) {
            logger.debug("S3 upload skipped: S3 disabled, or imageUrl/bookId is null/empty. ImageUrl: {}, BookId: {}", imageUrl, bookId);
            return Mono.just(new ImageDetails(imageUrl, source, imageUrl, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL)); 
        }
        final String s3Source = (source != null && !source.isEmpty()) ? source : "unknown";

        return webClient.get().uri(imageUrl).retrieve().bodyToMono(byte[].class)
            .timeout(Duration.ofSeconds(10))
            .flatMap(rawImageBytes -> {
                try {
                    logger.debug("Book ID {}: Downloaded {} bytes from {}. Starting image processing.", bookId, rawImageBytes.length, imageUrl);
                    ProcessedImage processedImage = imageProcessingService.processImageForS3(rawImageBytes, bookId);

                    if (!processedImage.isProcessingSuccessful()) {
                        logger.warn("Book ID {}: Image processing failed. Reason: {}. Will not upload to S3.", bookId, processedImage.getProcessingError());
                        return Mono.just(new ImageDetails(imageUrl, source, "processing-failed-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL)); 
                    }
                    logger.debug("Book ID {}: Image processing successful. New size: {}x{}, Extension: {}, MimeType: {}.", 
                                 bookId, processedImage.getWidth(), processedImage.getHeight(), processedImage.getNewFileExtension(), processedImage.getNewMimeType());

                    byte[] imageBytesForS3 = processedImage.getProcessedBytes(); 
                    String fileExtensionForS3 = processedImage.getNewFileExtension();
                    String mimeTypeForS3 = processedImage.getNewMimeType();

                    if (imageBytesForS3.length > this.maxFileSizeBytes) {
                        logger.warn("Book ID {}: Processed image too large (size: {} bytes, max: {} bytes). URL: {}. Will not upload to S3.", 
                                    bookId, imageBytesForS3.length, this.maxFileSizeBytes, imageUrl);
                        return Mono.just(new ImageDetails(imageUrl, source, "processed-image-too-large-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL));
                    }

                    String s3Key = generateS3Key(bookId, fileExtensionForS3, s3Source);

                    try {
                        HeadObjectResponse headResponse = s3Client.headObject(HeadObjectRequest.builder().bucket(s3BucketName).key(s3Key).build());
                        if (headResponse.contentLength() == imageBytesForS3.length) {
                            logger.info("Processed cover for book {} already exists in S3 with same size, skipping upload. Key: {}", bookId, s3Key);
                            String cdnUrl = getS3CoverUrl(bookId, fileExtensionForS3, s3Source);
                            objectExistsCache.put(s3Key, true);
                            return Mono.just(new ImageDetails(cdnUrl, "S3_CACHE", s3Key, CoverImageSource.S3_CACHE, ImageResolutionPreference.ORIGINAL, processedImage.getWidth(), processedImage.getHeight()));
                        }
                    } catch (NoSuchKeyException e) { /* Proceed to upload */ }
                    catch (Exception e) { logger.warn("Error checking existing S3 object for book {}: {}. Proceeding with upload.", bookId, e.getMessage()); }

                    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                            .bucket(s3BucketName)
                            .key(s3Key)
                            .contentType(mimeTypeForS3)
                            .acl(ObjectCannedACL.PUBLIC_READ)
                            .build();
                    s3Client.putObject(putObjectRequest, RequestBody.fromBytes(imageBytesForS3));
                    
                    String cdnUrl = getS3CoverUrl(bookId, fileExtensionForS3, s3Source);
                    objectExistsCache.put(s3Key, true);
                    logger.info("Successfully uploaded processed cover for book {} to S3. Key: {}", bookId, s3Key);

                    // Log provenance data if debug mode is enabled and data is provided
                    if (provenanceData != null && environmentService.isBookCoverDebugMode()) {
                        // Update selected image info in provenance data
                        ImageProvenanceData.SelectedImageInfo selectedInfo = provenanceData.getSelectedImageInfo();
                        if (selectedInfo == null) selectedInfo = new ImageProvenanceData.SelectedImageInfo();
                        selectedInfo.setS3Key(s3Key);
                        selectedInfo.setStorageLocation("S3");
                        selectedInfo.setFinalUrl(cdnUrl);
                        // Assuming source and resolution are set earlier in orchestration service
                        provenanceData.setSelectedImageInfo(selectedInfo);
                        
                        uploadProvenanceData(s3Key, provenanceData);
                    }
                    
                    return Mono.just(new ImageDetails(cdnUrl, "S3_UPLOAD", s3Key, CoverImageSource.S3_CACHE, ImageResolutionPreference.ORIGINAL, processedImage.getWidth(), processedImage.getHeight()));
                
                } catch (Exception e) {
                    logger.error("Unexpected exception during S3 upload (including processing) for book {}: {}. URL: {}", bookId, e.getMessage(), imageUrl, e);
                    return Mono.just(new ImageDetails(imageUrl, source, "upload-process-exception-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL));
                }
            })
            .onErrorResume(e -> {
                logger.error("Error downloading image for S3 upload for book {}: {}. URL: {}", bookId, e.getMessage(), imageUrl, e);
                return Mono.just(new ImageDetails(imageUrl, source, "download-failed-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL));
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

    // Helper method to convert CoverImageSource enum to string for S3 keys
    private String getSourceString(CoverImageSource source) {
        if (source == null) {
            return "unknown";
        }
        switch (source) {
            case GOOGLE_BOOKS: return "google-books";
            case OPEN_LIBRARY: return "open-library";
            case LONGITOOD: return "longitood";
            case LOCAL_CACHE: return "local-cache";
            case S3_CACHE: return "s3-cache";
            // ANY, NONE, UNDEFINED will fall through to default
            default: return "unknown";
        }
    }

    
    // Existing method, can be kept for calls not needing provenance or refactored
    public Mono<ImageDetails> uploadProcessedCoverToS3Async(byte[] processedImageBytes, String fileExtension, String mimeType, int width, int height, String bookId, String originalSourceForS3Key) {
        return uploadProcessedCoverToS3Async(processedImageBytes, fileExtension, mimeType, width, height, bookId, originalSourceForS3Key, null);
    }

    public Mono<ImageDetails> uploadProcessedCoverToS3Async(byte[] processedImageBytes, String fileExtension, String mimeType, int width, int height, String bookId, String originalSourceForS3Key, ImageProvenanceData provenanceData) {
        if (!s3Enabled || processedImageBytes == null || processedImageBytes.length == 0 || bookId == null || bookId.isEmpty()) {
            logger.debug("S3 upload of processed cover skipped: S3 disabled, or image bytes/bookId is null/empty. BookId: {}", bookId);
            return Mono.just(new ImageDetails(null, originalSourceForS3Key, "processed-upload-skipped-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL, width, height));
        }
        final String s3Source = (originalSourceForS3Key != null && !originalSourceForS3Key.isEmpty()) ? originalSourceForS3Key : "unknown";

        try {
            if (processedImageBytes.length > this.maxFileSizeBytes) {
                logger.warn("Book ID {}: Processed image too large for S3 (size: {} bytes, max: {} bytes). Will not upload.", 
                            bookId, processedImageBytes.length, this.maxFileSizeBytes);
                return Mono.just(new ImageDetails(null, originalSourceForS3Key, "processed-image-too-large-for-s3-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL, width, height));
            }

            String s3Key = generateS3Key(bookId, fileExtension, s3Source);

            try {
                HeadObjectResponse headResponse = s3Client.headObject(HeadObjectRequest.builder().bucket(s3BucketName).key(s3Key).build());
                if (headResponse.contentLength() == processedImageBytes.length) {
                    logger.info("Processed cover for book {} (from source {}) already exists in S3 with same size, skipping upload. Key: {}", bookId, s3Source, s3Key);
                    String cdnUrl = getS3CoverUrl(bookId, fileExtension, s3Source);
                    objectExistsCache.put(s3Key, true);
                    return Mono.just(new ImageDetails(cdnUrl, "S3_CACHE", s3Key, CoverImageSource.S3_CACHE, ImageResolutionPreference.ORIGINAL, width, height));
                }
            } catch (NoSuchKeyException e) { /* Proceed to upload */ }
            catch (Exception e) { logger.warn("Error checking existing S3 object for book {}: {}. Proceeding with upload.", bookId, e.getMessage()); }
            
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(s3BucketName)
                    .key(s3Key)
                    .contentType(mimeType)
                    .acl(ObjectCannedACL.PUBLIC_READ)
                    .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(processedImageBytes));
            
            String cdnUrl = getS3CoverUrl(bookId, fileExtension, s3Source);
            objectExistsCache.put(s3Key, true);
            logger.info("Successfully uploaded processed cover for book {} (from source {}) to S3. Key: {}", bookId, s3Source, s3Key);

            // Log provenance data if debug mode is enabled and data is provided
            if (provenanceData != null && environmentService.isBookCoverDebugMode()) {
                 // Update selected image info in provenance data
                ImageProvenanceData.SelectedImageInfo selectedInfo = provenanceData.getSelectedImageInfo();
                if (selectedInfo == null) selectedInfo = new ImageProvenanceData.SelectedImageInfo();
                selectedInfo.setS3Key(s3Key);
                selectedInfo.setStorageLocation("S3");
                selectedInfo.setFinalUrl(cdnUrl);
                // Assuming source and resolution are set earlier in orchestration service
                provenanceData.setSelectedImageInfo(selectedInfo);

                uploadProvenanceData(s3Key, provenanceData);
            }
            
            return Mono.just(new ImageDetails(cdnUrl, "S3_UPLOAD", s3Key, CoverImageSource.S3_CACHE, ImageResolutionPreference.ORIGINAL, width, height));
        
        } catch (Exception e) {
            logger.error("Unexpected exception during S3 upload of processed cover for book {}: {}.", bookId, e.getMessage(), e);
            return Mono.just(new ImageDetails(null, originalSourceForS3Key, "processed-upload-exception-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL, width, height));
        }
    }

    private void uploadProvenanceData(String imageS3Key, ImageProvenanceData provenanceData) {
        if (s3Client == null || !s3Enabled) {
            logger.warn("S3 client not available or S3 disabled. Skipping provenance data upload for image key: {}", imageS3Key);
            return;
        }
        if (provenanceData == null) {
            logger.debug("Provenance data is null. Skipping upload for image key: {}", imageS3Key);
            return;
        }

        String provenanceS3Key = imageS3Key.replaceAll("\\.(jpg|jpeg|png|gif|webp|svg)$", ".txt");
        if (provenanceS3Key.equals(imageS3Key)) { // Should not happen if imageS3Key has a valid extension
            provenanceS3Key = imageS3Key + ".txt";
            logger.warn("Image S3 key {} did not have a recognized image extension. Appending .txt for provenance: {}", imageS3Key, provenanceS3Key);
        }
        
        try {
            String jsonProvenance = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(provenanceData);
            byte[] jsonDataBytes = jsonProvenance.getBytes(StandardCharsets.UTF_8);

            PutObjectRequest putProvenanceRequest = PutObjectRequest.builder()
                    .bucket(s3BucketName)
                    .key(provenanceS3Key)
                    .contentType("application/json; charset=utf-8")
                    .acl(ObjectCannedACL.PUBLIC_READ) // Or private, depending on requirements
                    .build();
            s3Client.putObject(putProvenanceRequest, RequestBody.fromBytes(jsonDataBytes));
            logger.info("Successfully uploaded provenance data for image {} to S3. Key: {}", imageS3Key, provenanceS3Key);
        } catch (Exception e) {
            logger.error("Failed to upload provenance data for image {} to S3 key {}: {}", imageS3Key, provenanceS3Key, e.getMessage(), e);
        }
    }
}
