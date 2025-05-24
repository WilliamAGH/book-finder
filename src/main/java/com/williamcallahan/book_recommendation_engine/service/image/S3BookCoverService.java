/**
 * Service for managing book cover images in S3 object storage
 *
 * @author William Callahan
 *
 * Features:
 * - Provides durable object storage for book cover images
 * - Manages image uploading and URL generation
 * - Implements in-memory caching for optimized performance
 * - Supports multiple resolution variants of cover images
 * - Handles image metadata and resolution preferences
 * - Integrates with content delivery networks for fast global access
 */
package com.williamcallahan.book_recommendation_engine.service.image;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.EnvironmentService;
import com.williamcallahan.book_recommendation_engine.types.ImageDetails;
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
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;

import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import java.util.concurrent.CompletableFuture;
import java.util.Optional;
@Service
public class S3BookCoverService implements ExternalCoverService {
    private static final Logger logger = LoggerFactory.getLogger(S3BookCoverService.class);
    private static final String COVER_IMAGES_DIRECTORY = "images/book-covers/";
    private static final String PROVENANCE_DATA_DIRECTORY = "images/provenance-data/";
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
    private boolean s3EnabledCheck;

    @Value("${app.cover-cache.max-file-size-bytes:5242880}") 
    private long maxFileSizeBytes; 
    
    private final S3Client s3Client;
    private final WebClient webClient;
    private final ImageProcessingService imageProcessingService;
    private final EnvironmentService environmentService;
    private final ObjectMapper objectMapper;

    private final Cache<String, Boolean> objectExistsCache;

    public S3BookCoverService(WebClient.Builder webClientBuilder,
                               ImageProcessingService imageProcessingService,
                               EnvironmentService environmentService,
                               S3Client s3Client) {
        this.webClient = webClientBuilder.build();
        this.imageProcessingService = imageProcessingService;
        this.environmentService = environmentService;
        this.s3Client = s3Client;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        this.objectExistsCache = Caffeine.newBuilder()
            .maximumSize(2000)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();
        
        if (this.s3Client == null && this.s3EnabledCheck) {
            logger.warn("S3 is configured as enabled, but S3Client bean was not injected (likely due to missing credentials/config). S3 functionality will be disabled.");
            this.s3EnabledCheck = false;
        } else if (this.s3Client != null && this.s3EnabledCheck) {
             logger.info("S3BookCoverService initialized with injected S3Client. Bucket: {}, CDN URL: {}", s3BucketName, s3CdnUrl);
        } else {
            logger.info("S3BookCoverService: S3 is disabled by configuration.");
        }
    }

    /**
     * Cleanup method called during bean destruction
     * - S3Client lifecycle managed by Spring
     */
    @PreDestroy
    public void destroy() {
        logger.info("S3BookCoverService @PreDestroy called. S3Client lifecycle managed by Spring config.");
    }

    /**
     * Checks if S3 functionality is enabled and the client is available
     * @return true if S3 is enabled and usable, false otherwise
     */
    public boolean isS3Enabled() {
        return this.s3EnabledCheck && this.s3Client != null;
    }
    
    /**
     * Generates the S3 object key for a book cover image
     * - Creates standardized path structure for book cover images
     * - Validates book ID for valid characters
     * - Normalizes source identifier for S3 compatibility
     * - Defaults to JPG extension if none provided
     * 
     * @param bookId Unique identifier for the book
     * @param fileExtension File extension including the dot (e.g. ".jpg")
     * @param source Origin source identifier (e.g. "google-books", "open-library")
     * @return Constructed S3 key for the image
     * @throws IllegalArgumentException if bookId is null, empty or contains invalid characters
     */
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
        return COVER_IMAGES_DIRECTORY + bookId + LARGE_SUFFIX + "-" + normalizedSource + fileExtension;
    }

    /**
     * Fetches cover image from S3 storage for a given book
     * - Checks S3 storage for existing cover image
     * - Tries multiple source identifiers to find any available cover
     * - Returns image details with CDN URL if found
     * - Returns empty result if book has no valid identifier or S3 is disabled
     * 
     * @param book Book object containing identifiers to search with
     * @return CompletableFuture with Optional<ImageDetails> if cover exists in S3, empty Optional otherwise
     */
    @Override
    public CompletableFuture<Optional<ImageDetails>> fetchCover(Book book) {
        if (!s3EnabledCheck || s3Client == null || book == null) {
            if (!s3EnabledCheck || s3Client == null) logger.debug("S3 fetchCover skipped: S3 disabled or S3Client not available.");
            return CompletableFuture.completedFuture(Optional.empty());
        }
        String bookKey = book.getId();
        if (bookKey == null || bookKey.trim().isEmpty()) bookKey = book.getIsbn13();
        if (bookKey == null || bookKey.trim().isEmpty()) bookKey = book.getIsbn10();
        
        if (bookKey == null || bookKey.trim().isEmpty()) {
            logger.warn("Cannot fetch S3 cover for book without a valid ID or ISBN. Title: {}", book.getTitle());
            return CompletableFuture.completedFuture(Optional.empty());
        }

        final String finalBookKey = bookKey;
        String fileExtension = ".jpg"; 
        
        String[] s3SourceStrings = {
            getSourceString(CoverImageSource.GOOGLE_BOOKS),
            getSourceString(CoverImageSource.OPEN_LIBRARY),
            getSourceString(CoverImageSource.LONGITOOD),
            getSourceString(CoverImageSource.LOCAL_CACHE),
            "unknown"
        };

        return Flux.fromArray(s3SourceStrings)
            .concatMap(sourceForS3Key -> 
                coverExistsInS3Async(finalBookKey, fileExtension, sourceForS3Key)
                    .filter(Boolean::booleanValue)
                    .map(exists -> {
                        String s3Key = generateS3Key(finalBookKey, fileExtension, sourceForS3Key);
                        String cdnUrl = getS3CoverUrl(finalBookKey, fileExtension, sourceForS3Key);
                        logger.debug("Found existing S3 cover for book {} from source key '{}': {}", finalBookKey, sourceForS3Key, cdnUrl);
                        // Assuming width/height are not known from S3 HEAD request alone, use constructor without them
                        return Optional.of(new com.williamcallahan.book_recommendation_engine.types.ImageDetails(cdnUrl, "S3_CACHE", s3Key, CoverImageSource.S3_CACHE, ImageResolutionPreference.ORIGINAL));
                    })
                    .switchIfEmpty(Mono.just(Optional.empty()))
            )
            .filter(Optional::isPresent)
            .next()
            .defaultIfEmpty(Optional.empty())
            .doOnTerminate(() -> logger.debug("S3 cover check completed for book {}.", finalBookKey))
            .toFuture();
    }
    
    /**
     * Synchronous version for checking if a cover exists in S3
     * - Used for internal operations where blocking is acceptable
     * - Checks cache and makes direct S3 HEAD request
     * 
     * @param bookId Book identifier for the S3 key
     * @param fileExtension File extension to append to the key
     * @param source Source identifier string for the key
     * @return Boolean indicating if the object exists in S3
     */
    public boolean coverExistsInS3(String bookId, String fileExtension, String source) {
        if (!s3EnabledCheck || s3Client == null) return false;
        String s3Key = generateS3Key(bookId, fileExtension, source);
        
        Boolean cachedExists = objectExistsCache.getIfPresent(s3Key);
        if (cachedExists != null) {
            return cachedExists;
        }
        
        try {
            HeadObjectResponse response = s3Client.headObject(HeadObjectRequest.builder().bucket(s3BucketName).key(s3Key).build());
            boolean exists = response != null; 
            objectExistsCache.put(s3Key, exists);
            return exists;
        } catch (NoSuchKeyException e) { // Specifically for object not found
            objectExistsCache.put(s3Key, false);
            return false;
        } catch (software.amazon.awssdk.services.s3.model.S3Exception s3e) { // Catch broader S3 exceptions
            if (s3e.statusCode() == 404) { // Not Found
                objectExistsCache.put(s3Key, false);
                return false;
            }
            // For other S3 errors, log it and return false
            logger.error("S3Exception checking S3 object existence for key {}: Status={}, Message={}", s3Key, s3e.statusCode(), s3e.getMessage());
            objectExistsCache.put(s3Key, false);
            return false;
        } catch (Exception e) {
            logger.error("Unexpected error checking S3 object existence for key {}: {}", s3Key, e.getMessage(), e);
            objectExistsCache.put(s3Key, false);
            return false; 
        }
    }

    /**
     * Asynchronously checks if a cover image exists in S3 for specific parameters
     * - Checks in-memory cache first to avoid redundant S3 calls
     * - Makes non-blocking HEAD request to S3 if not found in cache
     * - Updates cache with results to improve future lookup performance
     * - Returns false for any errors without propagating exceptions
     * 
     * @param bookId Book identifier for the S3 key
     * @param fileExtension File extension to append to the key
     * @param source Source identifier string for the key
     * @return Mono containing boolean indicating if the object exists in S3
     */
    public Mono<Boolean> coverExistsInS3Async(String bookId, String fileExtension, String source) {
        if (!s3EnabledCheck || s3Client == null) {
            return Mono.just(false);
        }
        String s3Key = generateS3Key(bookId, fileExtension, source);
        
        Boolean cachedExists = objectExistsCache.getIfPresent(s3Key);
        if (cachedExists != null) {
            return Mono.just(cachedExists);
        }
        
        return Mono.fromCallable(() -> {
            try {
                s3Client.headObject(HeadObjectRequest.builder().bucket(s3BucketName).key(s3Key).build());
                return true; // If no exception, object exists
            } catch (NoSuchKeyException e) {
                return false; // Object does not exist specifically
            } catch (software.amazon.awssdk.services.s3.model.S3Exception s3e) {
                if (s3e.statusCode() == 404) {
                    return false; // Object not found via S3Exception
                }
                // For other S3 errors, log it and re-throw to be handled by onErrorResume
                logger.error("S3Exception (async) checking S3 object existence for key {}: Status={}, Message={}", s3Key, s3e.statusCode(), s3e.getMessage());
                throw s3e; 
            } catch (Exception e) { // Catch any other unexpected exceptions
                logger.error("Unexpected error (async) checking S3 object existence for key {}: {}", s3Key, e.getMessage(), e);
                throw e; // Re-throw to be handled by onErrorResume
            }
        })
        .subscribeOn(Schedulers.boundedElastic()) // Offload blocking call
        .doOnSuccess(exists -> objectExistsCache.put(s3Key, exists)) // Cache success (true or false from try-catch)
        .onErrorResume(e -> {
            // This will catch exceptions re-thrown from the try-catch block (e.g., non-404 S3Exception, other unexpected errors)
            // It will cache 'false' in these error cases to prevent repeated failed attempts for a while
            // Depending on the error, one might choose not to cache or cache for a shorter duration
            objectExistsCache.put(s3Key, false); 
            logger.warn("Async S3 check failed for key {} due to {}. Caching as non-existent.", s3Key, e.getClass().getSimpleName());
            return Mono.just(false); 
        });
    }

    public Mono<Boolean> coverExistsInS3Async(String bookId, String fileExtension) {
        String[] sourcesToTry = {"google-books", "open-library", "longitood", "local-cache", "unknown"};
        return Flux.fromArray(sourcesToTry)
            .concatMap(source -> coverExistsInS3Async(bookId, fileExtension, source))
            .any(exists -> exists); // Returns true if any source exists
    }
    
    public Mono<ImageDetails> uploadCoverToS3Async(String imageUrl, String bookId, String source) {
        return uploadCoverToS3Async(imageUrl, bookId, source, null);
    }

    public Mono<ImageDetails> uploadCoverToS3Async(String imageUrl, String bookId, String source, ImageProvenanceData provenanceData) {
        if (!s3EnabledCheck || s3Client == null || imageUrl == null || imageUrl.isEmpty() || bookId == null || bookId.isEmpty()) {
            logger.debug("S3 upload skipped: S3 disabled/S3Client not available, or imageUrl/bookId is null/empty. ImageUrl: {}, BookId: {}", imageUrl, bookId);
            return Mono.just(new com.williamcallahan.book_recommendation_engine.types.ImageDetails(imageUrl, source, imageUrl, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL)); 
        }
        final String s3Source = (source != null && !source.isEmpty()) ? source : "unknown";

        return webClient.get().uri(imageUrl).retrieve().bodyToMono(byte[].class)
            .timeout(Duration.ofSeconds(10))
            .flatMap(rawImageBytes -> {
                logger.debug("Book ID {}: Downloaded {} bytes from {}. Starting image processing.", bookId, rawImageBytes.length, imageUrl);
                // Convert CompletableFuture to Mono and continue reactive chain
                return Mono.fromFuture(imageProcessingService.processImageForS3(rawImageBytes, bookId))
                    .subscribeOn(Schedulers.boundedElastic())
                    .flatMap(processedImage -> {
                        if (!processedImage.isProcessingSuccessful()) {
                            logger.warn("Book ID {}: Image processing failed. Reason: {}. Will not upload to S3.", bookId, processedImage.getProcessingError());
                            return Mono.just(new com.williamcallahan.book_recommendation_engine.types.ImageDetails(imageUrl, source, "processing-failed-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL));
                        }
                        logger.debug("Book ID {}: Image processing successful. New size: {}x{}, Extension: {}, MimeType: {}.",
                                     bookId, processedImage.getWidth(), processedImage.getHeight(), processedImage.getNewFileExtension(), processedImage.getNewMimeType());

                        byte[] imageBytesForS3 = processedImage.getProcessedBytes();
                        String fileExtensionForS3 = processedImage.getNewFileExtension();
                        String mimeTypeForS3 = processedImage.getNewMimeType();

                        if (imageBytesForS3.length > this.maxFileSizeBytes) {
                            logger.warn("Book ID {}: Processed image too large (size: {} bytes, max: {} bytes). URL: {}. Will not upload to S3.",
                                        bookId, imageBytesForS3.length, this.maxFileSizeBytes, imageUrl);
                            return Mono.just(new com.williamcallahan.book_recommendation_engine.types.ImageDetails(imageUrl, source, "processed-image-too-large-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL));
                        }

                        String s3Key = generateS3Key(bookId, fileExtensionForS3, s3Source);

                        // Asynchronous check for existing object
                        return coverExistsInS3Async(bookId, fileExtensionForS3, s3Source)
                            .flatMap(exists -> {
                                if (exists) {
                                    // Check content length if it exists
                                    return Mono.fromCallable(() -> s3Client.headObject(HeadObjectRequest.builder().bucket(s3BucketName).key(s3Key).build()))
                                        .subscribeOn(Schedulers.boundedElastic())
                                        .flatMap(headResponse -> {
                                            if (headResponse.contentLength() == imageBytesForS3.length) {
                                                logger.info("Processed cover for book {} already exists in S3 with same size, skipping upload. Key: {}", bookId, s3Key);
                                                String cdnUrl = getS3CoverUrl(bookId, fileExtensionForS3, s3Source);
                                                return Mono.just(new com.williamcallahan.book_recommendation_engine.types.ImageDetails(cdnUrl, "S3_CACHE", s3Key, CoverImageSource.S3_CACHE, ImageResolutionPreference.ORIGINAL, processedImage.getWidth(), processedImage.getHeight()));
                                            }
                                            return uploadToS3Internal(s3Key, imageBytesForS3, mimeTypeForS3, bookId, fileExtensionForS3, s3Source, processedImage, provenanceData);
                                        })
                                        .onErrorResume(NoSuchKeyException.class, e -> uploadToS3Internal(s3Key, imageBytesForS3, mimeTypeForS3, bookId, fileExtensionForS3, s3Source, processedImage, provenanceData))
                                        .onErrorResume(e -> {
                                             logger.warn("Error checking existing S3 object for book {}: {}. Proceeding with upload.", bookId, e.getMessage());
                                             return uploadToS3Internal(s3Key, imageBytesForS3, mimeTypeForS3, bookId, fileExtensionForS3, s3Source, processedImage, provenanceData);
                                        });
                                } else {
                                    return uploadToS3Internal(s3Key, imageBytesForS3, mimeTypeForS3, bookId, fileExtensionForS3, s3Source, processedImage, provenanceData);
                                }
                            });
                    })
                    .onErrorResume(e -> { // Catches exceptions from imageProcessingService.processImageForS3 or subsequent reactive chain
                        logger.error("Unexpected exception during S3 upload (image processing or subsequent steps) for book {}: {}. URL: {}", bookId, e.getMessage(), imageUrl, e);
                        return Mono.just(new com.williamcallahan.book_recommendation_engine.types.ImageDetails(imageUrl, source, "upload-process-exception-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL));
                    });
            })
            .onErrorResume(e -> {
                logger.error("Error downloading image for S3 upload for book {}: {}. URL: {}", bookId, e.getMessage(), imageUrl, e);
                return Mono.just(new com.williamcallahan.book_recommendation_engine.types.ImageDetails(imageUrl, source, "download-failed-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL));
            });
    }

    private Mono<com.williamcallahan.book_recommendation_engine.types.ImageDetails> uploadToS3Internal(String s3Key, byte[] imageBytesForS3, String mimeTypeForS3, String bookId, String fileExtensionForS3, String s3Source, ProcessedImage processedImage, ImageProvenanceData provenanceData) {
        return Mono.fromCallable(() -> {
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

            if (provenanceData != null && environmentService.isBookCoverDebugMode()) {
                ImageProvenanceData.SelectedImageInfo selectedInfo = provenanceData.getSelectedImageInfo();
                if (selectedInfo == null) selectedInfo = new ImageProvenanceData.SelectedImageInfo();
                selectedInfo.setS3Key(s3Key);
                selectedInfo.setStorageLocation("S3");
                selectedInfo.setFinalUrl(cdnUrl);
                provenanceData.setSelectedImageInfo(selectedInfo);
                uploadProvenanceData(s3Key, provenanceData);
            }
            return new com.williamcallahan.book_recommendation_engine.types.ImageDetails(cdnUrl, "S3_UPLOAD", s3Key, CoverImageSource.S3_CACHE, ImageResolutionPreference.ORIGINAL, processedImage.getWidth(), processedImage.getHeight());
        }).subscribeOn(Schedulers.boundedElastic());
    }
 
    /**
     * Synchronous version of cover upload with explicit source
     * 
     * @param imageUrl URL of the image to upload
     * @param bookId Book identifier
     * @param source Source identifier
     * @return ImageDetails for the uploaded cover
     */
    public com.williamcallahan.book_recommendation_engine.types.ImageDetails uploadCoverToS3(String imageUrl, String bookId, String source) {
        try {
            return uploadCoverToS3Async(imageUrl, bookId, source).block(Duration.ofSeconds(15));
        } catch (Exception e) {
            logger.error("Error or timeout uploading cover to S3 for book {} from URL {}: {}", bookId, imageUrl, e.getMessage());
            return new com.williamcallahan.book_recommendation_engine.types.ImageDetails(imageUrl, source, imageUrl, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL);
        }
    }
    
    /**
     * Convenience method that derives source from URL
     */
    public com.williamcallahan.book_recommendation_engine.types.ImageDetails uploadCoverToS3(String imageUrl, String bookId) {
        String source = "google-books"; 
        if (imageUrl != null) {
            if (imageUrl.contains("openlibrary.org")) source = "open-library";
            else if (imageUrl.contains("longitood.com")) source = "longitood";
        }
        return uploadCoverToS3(imageUrl, bookId, source);
    }

    /**
     * Generates CDN URL for a book cover with known source
     */
    public String getS3CoverUrl(String bookId, String fileExtension, String source) {
        String s3Key = generateS3Key(bookId, fileExtension, source);
        return (s3PublicCdnUrl != null && !s3PublicCdnUrl.isEmpty() ? s3PublicCdnUrl : s3CdnUrl) + "/" + s3Key;
    }

    /**
     * Finds existing cover URL by checking multiple sources
     * 
     * @param bookId Book identifier
     * @param fileExtension File extension with leading dot
     * @return CDN URL for the first found cover or null if none exists
     */
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

    /**
     * Extracts file extension from URL
     * 
     * @param url Image URL to parse
     * @return File extension with leading dot or default (.jpg) if none found
     */
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

    /**
     * Converts CoverImageSource enum to standardized string for S3 key generation
     * - Maps enum values to consistent lowercase hyphenated strings
     * - Returns "unknown" for null or unrecognized values
     * 
     * @param source CoverImageSource enum value to convert
     * @return Standardized string representation of the source
     */
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
            default: return "unknown";
        }
    }
    
    /**
     * Overloaded version without provenance data
     */
    public Mono<com.williamcallahan.book_recommendation_engine.types.ImageDetails> uploadProcessedCoverToS3Async(byte[] processedImageBytes, String fileExtension, String mimeType, int width, int height, String bookId, String originalSourceForS3Key) {
        return uploadProcessedCoverToS3Async(processedImageBytes, fileExtension, mimeType, width, height, bookId, originalSourceForS3Key, null);
    }

    /**
     * Uploads pre-processed cover image to S3
     * 
     * @param processedImageBytes The processed image bytes
     * @param fileExtension File extension with leading dot
     * @param mimeType MIME type for content header
     * @param width Image width
     * @param height Image height
     * @param bookId Book identifier
     * @param originalSourceForS3Key Source identifier for S3 key
     * @param provenanceData Optional image provenance data 
     * @return Mono with ImageDetails for the uploaded cover
     */
    public Mono<com.williamcallahan.book_recommendation_engine.types.ImageDetails> uploadProcessedCoverToS3Async(byte[] processedImageBytes, String fileExtension, String mimeType, int width, int height, String bookId, String originalSourceForS3Key, ImageProvenanceData provenanceData) {
        if (!s3EnabledCheck || s3Client == null || processedImageBytes == null || processedImageBytes.length == 0 || bookId == null || bookId.isEmpty()) {
            logger.debug("S3 upload of processed cover skipped: S3 disabled/S3Client not available, or image bytes/bookId is null/empty. BookId: {}", bookId);
            return Mono.just(new com.williamcallahan.book_recommendation_engine.types.ImageDetails(null, originalSourceForS3Key, "processed-upload-skipped-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL, width, height));
        }
        final String s3Source = (originalSourceForS3Key != null && !originalSourceForS3Key.isEmpty()) ? originalSourceForS3Key : "unknown";

        try {
            if (processedImageBytes.length > this.maxFileSizeBytes) {
                logger.warn("Book ID {}: Processed image too large for S3 (size: {} bytes, max: {} bytes). Will not upload.", 
                            bookId, processedImageBytes.length, this.maxFileSizeBytes);
                return Mono.just(new com.williamcallahan.book_recommendation_engine.types.ImageDetails(null, originalSourceForS3Key, "processed-image-too-large-for-s3-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL, width, height));
            }

            String s3Key = generateS3Key(bookId, fileExtension, s3Source);

            // Asynchronous check for existing object
            return coverExistsInS3Async(bookId, fileExtension, s3Source)
                .flatMap(exists -> {
                    if (exists) {
                        return Mono.fromCallable(() -> s3Client.headObject(HeadObjectRequest.builder().bucket(s3BucketName).key(s3Key).build()))
                            .subscribeOn(Schedulers.boundedElastic())
                            .flatMap(headResponse -> {
                                if (headResponse.contentLength() == processedImageBytes.length) {
                                    logger.info("Processed cover for book {} (from source {}) already exists in S3 with same size, skipping upload. Key: {}", bookId, s3Source, s3Key);
                                    String cdnUrl = getS3CoverUrl(bookId, fileExtension, s3Source);
                                    return Mono.just(new com.williamcallahan.book_recommendation_engine.types.ImageDetails(cdnUrl, "S3_CACHE", s3Key, CoverImageSource.S3_CACHE, ImageResolutionPreference.ORIGINAL, width, height));
                                }
                                return uploadToS3Internal(s3Key, processedImageBytes, mimeType, bookId, fileExtension, s3Source, new ProcessedImage(processedImageBytes, fileExtension, mimeType, width, height, true, null), provenanceData);
                            })
                            .onErrorResume(NoSuchKeyException.class, e -> uploadToS3Internal(s3Key, processedImageBytes, mimeType, bookId, fileExtension, s3Source, new ProcessedImage(processedImageBytes, fileExtension, mimeType, width, height, true, null), provenanceData))
                            .onErrorResume(e -> {
                                 logger.warn("Error checking existing S3 object for book {}: {}. Proceeding with upload.", bookId, e.getMessage());
                                 return uploadToS3Internal(s3Key, processedImageBytes, mimeType, bookId, fileExtension, s3Source, new ProcessedImage(processedImageBytes, fileExtension, mimeType, width, height, true, null), provenanceData);
                            });
                    } else {
                        return uploadToS3Internal(s3Key, processedImageBytes, mimeType, bookId, fileExtension, s3Source, new ProcessedImage(processedImageBytes, fileExtension, mimeType, width, height, true, null), provenanceData);
                    }
                });
        
        } catch (Exception e) { // Catch synchronous exceptions from this method's setup
            logger.error("Unexpected exception during S3 upload setup for processed cover for book {}: {}.", bookId, e.getMessage(), e);
            return Mono.just(new com.williamcallahan.book_recommendation_engine.types.ImageDetails(null, originalSourceForS3Key, "processed-upload-setup-exception-" + bookId, CoverImageSource.ANY, ImageResolutionPreference.ORIGINAL, width, height));
        }
    }

    private void uploadProvenanceData(String imageS3Key, ImageProvenanceData provenanceData) {
        if (s3Client == null || !s3EnabledCheck) {
            logger.warn("S3 client not available or S3 disabled. Skipping provenance data upload for image key: {}", imageS3Key);
            return;
        }
        if (provenanceData == null) {
            logger.debug("Provenance data is null. Skipping upload for image key: {}", imageS3Key);
            return;
        }

        // Extract filename from the original imageS3Key
        String filename = imageS3Key.substring(imageS3Key.lastIndexOf('/') + 1);
        // Replace image extension with .txt for provenance file
        String provenanceFilename = filename.replaceAll("\\.(?i)(jpg|jpeg|png|gif|webp|svg)$", ".txt");
        if (provenanceFilename.equals(filename)) { 
            provenanceFilename = filename + ".txt";
            logger.warn("Image S3 key {} (filename: {}) did not have a recognized image extension. Appending .txt for provenance: {}", imageS3Key, filename, provenanceFilename);
        }
        
        String provenanceS3Key = PROVENANCE_DATA_DIRECTORY + provenanceFilename;
        
        try {
            String jsonProvenance = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(provenanceData);
            byte[] jsonDataBytes = jsonProvenance.getBytes(StandardCharsets.UTF_8);

            PutObjectRequest putProvenanceRequest = PutObjectRequest.builder()
                    .bucket(s3BucketName)
                    .key(provenanceS3Key)
                    .contentType("application/json; charset=utf-8")
                    .acl(ObjectCannedACL.PUBLIC_READ) 
                    .build();
            s3Client.putObject(putProvenanceRequest, RequestBody.fromBytes(jsonDataBytes));
            logger.info("Successfully uploaded provenance data for image {} to S3. Key: {}", imageS3Key, provenanceS3Key);
        } catch (Exception e) {
            logger.error("Failed to upload provenance data for image {} to S3 key {}: {}", imageS3Key, provenanceS3Key, e.getMessage(), e);
        }
    }
}
