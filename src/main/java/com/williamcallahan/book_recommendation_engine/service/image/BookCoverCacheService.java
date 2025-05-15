package com.williamcallahan.book_recommendation_engine.service.image;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import jakarta.annotation.PostConstruct;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.imageio.ImageIO;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.EnvironmentService;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import com.williamcallahan.book_recommendation_engine.types.CoverImages;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.types.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.types.ImageAttemptStatus;

/**
 * Service to download and cache book cover images locally, providing fast initial URLs
 * and then updating with higher quality images in the background
 * It manages local cache, handles bad URLs, and downloads images asynchronously
 *
 * Key features:
 * - Local disk caching of cover images
 * - Fast provisional URL (e.g., from Google Books API, or a provided hint) for immediate display. If a hint is used for a specific ISBN, that URL is attempted first for caching
 * - Background processing to find the best available cover from multiple sources (OpenLibrary, Longitood, then Google Books API as a final fallback if others fail)
 * - In-memory caches for recently accessed provisional URLs and final local paths
 * - Mechanism to avoid re-downloading unchanged images using hash comparison
 * - Fallback to a local placeholder image if no cover can be found
 */
@Service
public class BookCoverCacheService {

    private static final Logger logger = LoggerFactory.getLogger(BookCoverCacheService.class);
    private static final String LOCAL_PLACEHOLDER_PATH = "/images/placeholder-book-cover.svg"; // Corrected path
    private static final int MAX_MEMORY_CACHE_SIZE = 1000;

    @Value("${app.cover-cache.enabled:true}")
    private boolean cacheEnabled;
    @Value("${app.cover-cache.dir:/tmp/book-covers}")
    private String cacheDirString;
    @Value("${app.cover-cache.max-age-days:30}")
    private int maxCacheAgeDays;
    @Value("${google.books.api.key:}")
    private String googleBooksApiKey;
    
    @Value("${s3.cdn-url}")
    private String s3CdnUrl;
    
    @Value("${s3.public-cdn-url:${S3_PUBLIC_CDN_URL:}}")
    private String s3PublicCdnUrl;


    private Path cacheDir;
    private String cacheDirName;
    private final WebClient webClient;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private final OpenLibraryServiceImpl openLibraryService;
    private final LongitoodServiceImpl longitoodService;
    private final GoogleBooksService googleBooksService;
    private final S3BookCoverService s3BookCoverService;
    private final ImageProcessingService imageProcessingService;
    private final ApplicationEventPublisher eventPublisher;
    private final EnvironmentService environmentService;

    private final ConcurrentHashMap<String, String> urlToPathCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> identifierToProvisionalUrlCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ImageDetails> identifierToFinalImageDetailsCache = new ConcurrentHashMap<>();
    private final Set<String> knownBadImageUrls = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private byte[] openLibraryPlaceholderHash;
    private static final String GOOGLE_PLACEHOLDER_PATH = "/images/image-not-available.png"; 
    private byte[] googlePlaceholderHash;

    private final Set<String> knownBadOpenLibraryIsbns = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<String> knownBadLongitoodIsbns = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public boolean isKnownBadOpenLibraryIsbn(String isbn) {
        return isbn != null && knownBadOpenLibraryIsbns.contains(isbn);
    }

    public void addKnownBadOpenLibraryIsbn(String isbn) {
        if (isbn != null) knownBadOpenLibraryIsbns.add(isbn);
    }

    public boolean isKnownBadLongitoodIsbn(String isbn) {
        return isbn != null && knownBadLongitoodIsbns.contains(isbn);
    }

    public void addKnownBadLongitoodIsbn(String isbn) {
        if (isbn != null) knownBadLongitoodIsbns.add(isbn);
    }

    @Autowired
    public BookCoverCacheService(WebClient.Builder webClientBuilder,
                                 OpenLibraryServiceImpl openLibraryService,
                                 LongitoodServiceImpl longitoodService,
                                 GoogleBooksService googleBooksService,
                                 S3BookCoverService s3BookCoverService,
                                 ImageProcessingService imageProcessingService,
                                 ApplicationEventPublisher eventPublisher,
                                 EnvironmentService environmentService) {
        this.webClient = webClientBuilder.build();
        this.openLibraryService = openLibraryService;
        this.longitoodService = longitoodService;
        this.googleBooksService = googleBooksService;
        this.s3BookCoverService = s3BookCoverService;
        this.imageProcessingService = imageProcessingService;
        this.eventPublisher = eventPublisher;
        this.environmentService = environmentService;
    }

    @PostConstruct
    public void init() {
        if (!cacheEnabled) {
            logger.info("Book cover caching is disabled");
            return;
        }
        try {
            cacheDirName = cacheDirString.substring(cacheDirString.lastIndexOf("/") + 1);
            cacheDir = Paths.get(cacheDirString);
            if (!Files.exists(cacheDir)) Files.createDirectories(cacheDir);
            else logger.info("Using existing book cover cache directory: {}", cacheDir);

            // Load Google placeholder image for hash comparison more robustly
            try (InputStream placeholderStream = getClass().getResourceAsStream(GOOGLE_PLACEHOLDER_PATH)) {
                if (placeholderStream != null) {
                    byte[] placeholderBytes = placeholderStream.readAllBytes();
                    if (placeholderBytes.length > 0) {
                        googlePlaceholderHash = computeImageHash(placeholderBytes);
                        logger.info("Loaded Google Books placeholder image hash for detection from classpath: {}", GOOGLE_PLACEHOLDER_PATH);
                    } else {
                        logger.warn("Google Books placeholder image from classpath {} was empty, hash-based detection disabled", GOOGLE_PLACEHOLDER_PATH);
                    }
                } else {
                    logger.warn("Google Books placeholder image not found in classpath at {}, hash-based detection disabled", GOOGLE_PLACEHOLDER_PATH);
                }
            } catch (Exception e) {
                logger.warn("Failed to load Google Books placeholder image for hash comparison from classpath {}: {}", GOOGLE_PLACEHOLDER_PATH, e.getMessage(), e);
            }

            scheduler.scheduleAtFixedRate(this::cleanupOldCachedCovers, (long)maxCacheAgeDays, (long)maxCacheAgeDays, TimeUnit.DAYS);
        } catch (IOException e) {
            logger.error("Failed to create book cover cache directory", e);
            cacheEnabled = false;
        }
    }

    private byte[] computeImageHash(byte[] imageData) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return digest.digest(imageData);
    }

    private boolean isHashSimilar(byte[] hash1, byte[] hash2) {
        if (hash1 == null || hash2 == null || hash1.length != hash2.length) return false;
        for (int i = 0; i < hash1.length; i++) if (hash1[i] != hash2[i]) return false;
        return true;
    }

    private String getFileExtensionFromUrl(String url) {
        String extension = ".jpg"; 
        if (url != null && url.contains(".")) {
            int queryParamIndex = url.indexOf("?");
            String urlWithoutParams = queryParamIndex > 0 ? url.substring(0, queryParamIndex) : url;
            int lastDotIndex = urlWithoutParams.lastIndexOf(".");
            if (lastDotIndex > 0 && lastDotIndex < urlWithoutParams.length() - 1) {
                String ext = urlWithoutParams.substring(lastDotIndex).toLowerCase();
                if (ext.matches("\\.(jpg|jpeg|png|gif|webp|svg|bmp|tiff)")) {
                    extension = ext;
                }
            }
        }
        return extension;
    }

    private String generateFilenameFromUrl(String url) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(url.getBytes());
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        String extension = getFileExtensionFromUrl(url);
        return encoded.substring(0, Math.min(encoded.length(), 32)) + extension;
    }

    private void addToMemoryCache(String url, String cachedPath) {
        if (urlToPathCache.size() >= MAX_MEMORY_CACHE_SIZE) {
            if (!urlToPathCache.isEmpty()) {
                String keyToRemove = urlToPathCache.keys().nextElement();
                urlToPathCache.remove(keyToRemove);
            }
        }
        urlToPathCache.put(url, cachedPath);
    }
    
    private ImageDetails createPlaceholderImageDetails(String bookId, String reasonSuffix) {
        return new ImageDetails(LOCAL_PLACEHOLDER_PATH, "SYSTEM", "placeholder-" + reasonSuffix + "-" + bookId, CoverImageSource.LOCAL_CACHE, ImageResolutionPreference.ORIGINAL);
    }

    private CompletableFuture<ImageDetails> downloadAndCacheImageInternalAsync(String imageUrl, Path destination, String bookIdForLog, ImageProvenanceData provenanceData, String sourceNameString) {
        ImageProvenanceData.AttemptedSourceInfo attemptInfo = null;
        ImageSourceName sourceNameEnum = mapStringToImageSourceName(sourceNameString);

        if (provenanceData != null) {
            attemptInfo = new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, imageUrl, ImageAttemptStatus.SKIPPED);
            if (provenanceData.getAttemptedImageSources() == null) {
                provenanceData.setAttemptedImageSources(new ArrayList<>());
            }
            provenanceData.getAttemptedImageSources().add(attemptInfo);
        }

        if (knownBadImageUrls.contains(imageUrl)) {
            logger.debug("Skipping download for known bad URL: {}", imageUrl);
            if (attemptInfo != null) attemptInfo.setStatus(ImageAttemptStatus.SKIPPED);
            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "badurl"));
        }

        String webSafeCachedPath = "/" + cacheDirName + "/" + destination.getFileName().toString();
        final ImageProvenanceData.AttemptedSourceInfo finalAttemptInfo = attemptInfo;

        if (finalAttemptInfo != null) {
            finalAttemptInfo.setStatus(ImageAttemptStatus.SKIPPED);
        }

        return webClient.get().uri(imageUrl).retrieve().bodyToMono(byte[].class)
            .timeout(Duration.ofSeconds(10))
            .toFuture()
            .thenCompose(imageBytes -> {
                if (imageBytes == null || imageBytes.length == 0) {
                    logger.warn("Download failed or resulted in empty content for URL: {} (BookID: {}). Adding to known bad URLs.", imageUrl, bookIdForLog);
                    knownBadImageUrls.add(imageUrl);
                    if (finalAttemptInfo != null) {
                        finalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_404);
                    }
                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "downloadfail"));
                }
                try {
                    byte[] currentHash = computeImageHash(imageBytes);
                    if (googlePlaceholderHash != null && isHashSimilar(currentHash, googlePlaceholderHash)) {
                        logger.info("Downloaded image from {} for BookID: {} matched Google placeholder hash. Treating as bad URL.", imageUrl, bookIdForLog);
                        knownBadImageUrls.add(imageUrl);
                        if (finalAttemptInfo != null) finalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_GENERIC);
                        return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "googleplaceholder"));
                    }

                    if (openLibraryPlaceholderHash != null && isHashSimilar(currentHash, openLibraryPlaceholderHash)) {
                        logger.info("Downloaded image from {} for BookID: {} matched OpenLibrary placeholder hash. Treating as bad URL.", imageUrl, bookIdForLog);
                        knownBadImageUrls.add(imageUrl);
                        if (finalAttemptInfo != null) finalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_GENERIC);
                        return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "olplaceholder"));
                    }

                    Files.write(destination, imageBytes);
                    addToMemoryCache(imageUrl, webSafeCachedPath);
                    logger.info("Successfully cached image for BookID: {} from URL: {} to {}", bookIdForLog, imageUrl, webSafeCachedPath);
                    
                    try (InputStream is = new ByteArrayInputStream(imageBytes)) {
                        BufferedImage bufferedImage = ImageIO.read(is);
                        if (bufferedImage != null) {
                            if (finalAttemptInfo != null) finalAttemptInfo.setStatus(ImageAttemptStatus.SUCCESS);
                            return CompletableFuture.completedFuture(new ImageDetails(webSafeCachedPath, sourceNameEnum.getDisplayName(), imageUrl, mapImageSourceNameEnumToCoverImageSource(sourceNameEnum), ImageResolutionPreference.ORIGINAL, bufferedImage.getWidth(), bufferedImage.getHeight()));
                        } else {
                            logger.warn("Could not read dimensions for cached image (BookID: {}), but file saved: {}", bookIdForLog, webSafeCachedPath);
                            if (finalAttemptInfo != null) finalAttemptInfo.setStatus(ImageAttemptStatus.SUCCESS);
                            return CompletableFuture.completedFuture(new ImageDetails(webSafeCachedPath, sourceNameEnum.getDisplayName(), imageUrl, mapImageSourceNameEnumToCoverImageSource(sourceNameEnum), ImageResolutionPreference.ORIGINAL));
                        }
                    } catch (IOException e) {
                        logger.warn("IOException while reading dimensions for cached image (BookID: {}): {}. File saved, but dimensions unknown.", bookIdForLog, e.getMessage());
                        if (finalAttemptInfo != null) finalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_GENERIC);
                        return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "readfail"));
                    }

                } catch (NoSuchAlgorithmException e) {
                     logger.error("NoSuchAlgorithmException during image hash computation for BookID: {} (URL: {}). This should not happen.", bookIdForLog, imageUrl, e);
                     if (finalAttemptInfo != null) finalAttemptInfo.setStatus(ImageAttemptStatus.PROCESSING_FAILED);
                     return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "hashalgoerror"));
                } catch (IOException e) {
                    logger.warn("IOException during image caching for BookID: {} (URL: {}): {}", bookIdForLog, imageUrl, e.getMessage());
                    if (finalAttemptInfo != null) {
                        if (e instanceof java.net.SocketTimeoutException) {
                           finalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_TIMEOUT);
                        } else {
                           finalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_GENERIC);
                        }
                    }
                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "ioexception"));
                } catch (Exception e) {
                    logger.error("Unexpected error caching image for BookID: {} (URL: {}): {}", bookIdForLog, imageUrl, e.getMessage(), e);
                    if (finalAttemptInfo != null) {
                         finalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_GENERIC);
                    }
                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "unexpected"));
                }
            })
            .exceptionally(ex -> {
                logger.error("Exception during image download/cache for URL {}: {}", imageUrl, ex.getMessage());
                knownBadImageUrls.add(imageUrl);
                if (finalAttemptInfo != null) {
                    finalAttemptInfo.setStatus(ImageAttemptStatus.FAILURE_GENERIC);
                }
                return createPlaceholderImageDetails(bookIdForLog, "exception");
            });
    }

    private void cleanupOldCachedCovers() {
        if (!cacheEnabled || cacheDir == null) return;
        logger.info("Starting cleanup of old cached book covers older than {} days.", maxCacheAgeDays);
        try {
            long cutoffTime = System.currentTimeMillis() - (maxCacheAgeDays * 24L * 60L * 60L * 1000L);
            Files.list(cacheDir)
                .filter(Files::isRegularFile)
                .filter(p -> {
                    try {
                        return Files.getLastModifiedTime(p).toMillis() < cutoffTime;
                    } catch (IOException e) {
                        logger.warn("Could not get last modified time for {}, skipping in cleanup.", p, e);
                        return false;
                    }
                })
                .forEach(p -> {
                    try {
                        Files.delete(p);
                        logger.debug("Deleted old cached cover: {}", p.getFileName());
                    } catch (IOException e) {
                        logger.warn("Failed to delete old cached cover: {}", p.getFileName(), e);
                    }
                });
            logger.info("Completed cleanup of old cached book covers.");
        } catch (IOException e) {
            logger.error("Error during cleanup of cached book covers", e);
        }
    }

    public String getLocalPlaceholderPath() {
        return LOCAL_PLACEHOLDER_PATH;
    }

    public CoverImages getInitialCoverUrlAndTriggerBackgroundUpdate(Book book) {
        CoverImages result = new CoverImages();
        result.setSource(CoverImageSource.UNDEFINED); 

        if (!cacheEnabled) {
            logger.warn("Cache disabled, returning placeholder for book ID: {}", book != null ? book.getId() : "null");
            result.setPreferredUrl(getLocalPlaceholderPath());
            result.setFallbackUrl(getLocalPlaceholderPath());
            result.setSource(CoverImageSource.LOCAL_CACHE);
            return result;
        }
        if (book == null || (book.getIsbn13() == null && book.getIsbn10() == null && book.getId() == null)) {
            logger.warn("Book or all relevant identifiers are null. Cannot process for initial cover URL.");
            result.setPreferredUrl(getLocalPlaceholderPath());
            result.setFallbackUrl(getLocalPlaceholderPath());
            result.setSource(CoverImageSource.LOCAL_CACHE);
            return result;
        }

        String identifierKey = getIdentifierKey(book);
        if (identifierKey == null) { 
            logger.warn("Could not determine a valid identifierKey for book with ID: {}. Returning placeholder.", book.getId());
            result.setPreferredUrl(getLocalPlaceholderPath());
            result.setFallbackUrl(getLocalPlaceholderPath());
            result.setSource(CoverImageSource.LOCAL_CACHE);
            return result;
        }

        ImageDetails finalCachedImageDetails = identifierToFinalImageDetailsCache.get(identifierKey);
        if (finalCachedImageDetails != null && finalCachedImageDetails.getUrlOrPath() != null) {
            logger.debug("Returning final cached ImageDetails for identifierKey {}: Path: {}, Source: {}", 
                identifierKey, finalCachedImageDetails.getUrlOrPath(), finalCachedImageDetails.getCoverImageSource());
            result.setPreferredUrl(finalCachedImageDetails.getUrlOrPath());
            result.setSource(finalCachedImageDetails.getCoverImageSource() != null ? finalCachedImageDetails.getCoverImageSource() : CoverImageSource.UNDEFINED);
            if (finalCachedImageDetails.getUrlOrPath().equals(getLocalPlaceholderPath()) && book.getCoverImageUrl() != null && !book.getCoverImageUrl().isEmpty()) {
                result.setFallbackUrl(book.getCoverImageUrl());
            } else {
                result.setFallbackUrl(getLocalPlaceholderPath()); 
            }
            return result;
        }

        String provisionalUrl = identifierToProvisionalUrlCache.get(identifierKey);
        String urlToUseAsPreferred;
        CoverImageSource inferredProvisionalSource = CoverImageSource.UNDEFINED;

        if (provisionalUrl != null) {
            logger.debug("Returning provisional cached URL for identifierKey {}: {}", identifierKey, provisionalUrl);
            urlToUseAsPreferred = provisionalUrl;
            if (provisionalUrl.startsWith("/" + this.cacheDirName)) inferredProvisionalSource = CoverImageSource.LOCAL_CACHE;
            else if (provisionalUrl.contains("googleapis.com/books")) inferredProvisionalSource = CoverImageSource.GOOGLE_BOOKS;
            else if (provisionalUrl.contains("openlibrary.org")) inferredProvisionalSource = CoverImageSource.OPEN_LIBRARY;
            else if (provisionalUrl.contains("longitood.com")) inferredProvisionalSource = CoverImageSource.LONGITOOD;
            else if (provisionalUrl.contains("s3.") || provisionalUrl.contains(this.s3CdnUrl) || (this.s3PublicCdnUrl != null && !this.s3PublicCdnUrl.isEmpty() && provisionalUrl.contains(this.s3PublicCdnUrl))) inferredProvisionalSource = CoverImageSource.S3_CACHE;
            else if (provisionalUrl.equals(getLocalPlaceholderPath())) inferredProvisionalSource = CoverImageSource.LOCAL_CACHE;
            else inferredProvisionalSource = CoverImageSource.ANY;
        } else {
            if (book.getCoverImageUrl() != null && !book.getCoverImageUrl().isEmpty() && !book.getCoverImageUrl().equals(LOCAL_PLACEHOLDER_PATH)) {
                urlToUseAsPreferred = book.getCoverImageUrl();
                logger.debug("Using existing coverImageUrl from book object as provisional for identifierKey {}: {}", identifierKey, urlToUseAsPreferred);
                if (urlToUseAsPreferred.contains("googleapis.com/books")) inferredProvisionalSource = CoverImageSource.GOOGLE_BOOKS;
                else inferredProvisionalSource = CoverImageSource.ANY;
            } else {
                urlToUseAsPreferred = getLocalPlaceholderPath();
                inferredProvisionalSource = CoverImageSource.LOCAL_CACHE;
                logger.debug("No provisional URL for identifierKey {}, will use placeholder and process in background.", identifierKey);
            }
            if (identifierToProvisionalUrlCache.size() >= MAX_MEMORY_CACHE_SIZE) identifierToProvisionalUrlCache.clear();
            identifierToProvisionalUrlCache.put(identifierKey, urlToUseAsPreferred);
        }
        
        result.setPreferredUrl(urlToUseAsPreferred);
        result.setSource(inferredProvisionalSource);

        if (urlToUseAsPreferred.equals(getLocalPlaceholderPath()) && book.getCoverImageUrl() != null && !book.getCoverImageUrl().isEmpty() && !book.getCoverImageUrl().equals(getLocalPlaceholderPath())) {
            result.setFallbackUrl(book.getCoverImageUrl());
        } else if (!urlToUseAsPreferred.equals(getLocalPlaceholderPath())) {
            if (book.getCoverImageUrl() != null && !book.getCoverImageUrl().isEmpty() && !book.getCoverImageUrl().equals(urlToUseAsPreferred)) {
                result.setFallbackUrl(book.getCoverImageUrl());
            } else {
                result.setFallbackUrl(getLocalPlaceholderPath());
            }
        } else {
            result.setFallbackUrl(getLocalPlaceholderPath());
        }

        processCoverInBackground(book, urlToUseAsPreferred.equals(getLocalPlaceholderPath()) ? null : urlToUseAsPreferred);
        return result;
    }

    @Async
    public void processCoverInBackground(Book book, String provisionalUrlHint) {
        if (!cacheEnabled || book == null) return;

        ImageProvenanceData provenanceData = new ImageProvenanceData(); 
        String effectiveBookIdForProvenance = book.getId() != null ? book.getId() : 
                                             (book.getIsbn13() != null ? "isbn-" + book.getIsbn13() : 
                                             (book.getIsbn10() != null ? "isbn-" + book.getIsbn10() : 
                                             "unknown-" + System.currentTimeMillis()));
        provenanceData.setBookId(effectiveBookIdForProvenance);
        
        // TODO: Capture raw Google Books API response if available from the 'book' object or fetch it
        // This might require modifying GoogleBooksService or how Book objects store initial API responses
        // Example: if (book.getRawGoogleApiJson() != null) { provenanceData.setGoogleBooksApiResponse(book.getRawGoogleApiJson()); }


        String identifierKey = getIdentifierKey(book);
        if (identifierKey == null) {
            logger.warn("Background: Could not determine identifierKey for book with ID: {}. Aborting background processing.", book.getId());
            return;
        }
        final String bookIdForLog = book.getId() != null ? book.getId() : identifierKey;
        provenanceData.setBookId(bookIdForLog); 

        logger.info("Background: Starting full cover processing for identifierKey: {}, Book ID: {}, Title: {}",
            identifierKey, bookIdForLog, book.getTitle());
        try {
            getCoverImageUrlAsync(book, provisionalUrlHint, provenanceData)
                .thenAcceptAsync(finalImageDetails -> {
                    if (finalImageDetails == null || finalImageDetails.getUrlOrPath() == null || finalImageDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH)) {
                        logger.warn("Background: Final processing for {} (BookID {}) yielded null/placeholder. Final cache not updated with a real image.", identifierKey, bookIdForLog);
                        if (provenanceData.getSelectedImageInfo() == null) {
                            ImageProvenanceData.SelectedImageInfo selectedInfo = new ImageProvenanceData.SelectedImageInfo();
                            selectedInfo.setSourceName(ImageSourceName.UNKNOWN);
                            selectedInfo.setFinalUrl(LOCAL_PLACEHOLDER_PATH);
                            selectedInfo.setResolution("N/A");
                            selectedInfo.setStorageLocation("Local");
                            provenanceData.setSelectedImageInfo(selectedInfo);
                        }
                        identifierToFinalImageDetailsCache.put(identifierKey, createPlaceholderImageDetails(bookIdForLog, "background-failed"));
                        identifierToProvisionalUrlCache.remove(identifierKey);
                        eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, LOCAL_PLACEHOLDER_PATH, book.getId(), CoverImageSource.LOCAL_CACHE));
                        return; 
                    }

                    ImageDetails currentFinalDetails = identifierToFinalImageDetailsCache.get(identifierKey);
                    boolean updateCache = true;
                    if (currentFinalDetails != null && currentFinalDetails.getUrlOrPath() != null && currentFinalDetails.getUrlOrPath().equals(finalImageDetails.getUrlOrPath())) {
                        if (currentFinalDetails.getCoverImageSource() == finalImageDetails.getCoverImageSource()) {
                            updateCache = false;
                        }
                    }

                    if (updateCache) {
                        if (identifierToFinalImageDetailsCache.size() >= MAX_MEMORY_CACHE_SIZE) identifierToFinalImageDetailsCache.clear(); 
                        identifierToFinalImageDetailsCache.put(identifierKey, finalImageDetails);
                        logger.info("Background: Final best image for {} (BookID {}) is {}. Source: {}. Final cache updated.",
                            identifierKey, bookIdForLog, finalImageDetails.getUrlOrPath(), finalImageDetails.getCoverImageSource());
                    }
                    
                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, finalImageDetails.getUrlOrPath(), book.getId(), finalImageDetails.getCoverImageSource()));
                    logger.info("Background: Published BookCoverUpdatedEvent for {} (BookID {}) with URL: {} and Source: {}", 
                        identifierKey, bookIdForLog, finalImageDetails.getUrlOrPath(), finalImageDetails.getCoverImageSource());
                    
                    identifierToProvisionalUrlCache.remove(identifierKey);

                    if (!finalImageDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH)) {
                        boolean needsNewS3Upload = finalImageDetails.getCoverImageSource() != CoverImageSource.S3_CACHE &&
                                                   finalImageDetails.getUrlOrPath().startsWith("/" + this.cacheDirName);
                        if (needsNewS3Upload) {
                            logger.info("Background: {} (BookID {}) is locally cached from external source ({}). Triggering S3 upload sequence.",
                                identifierKey, bookIdForLog, finalImageDetails.getCoverImageSource());
                            uploadLocallyCachedFileToS3(finalImageDetails, bookIdForLog, identifierKey, book.getId(), provenanceData);
                        } else if (finalImageDetails.getCoverImageSource() == CoverImageSource.S3_CACHE && environmentService.isBookCoverDebugMode()) {
                             logger.debug("Background: Image for {} is already from S3_CACHE. Provenance logging for existing S3 objects handled by S3BookCoverService if applicable.", identifierKey);
                        }
                        
                        else {
                            logger.debug("Background: No new S3 upload needed for {} (BookID {}). Source: {}, Path: {}", 
                                identifierKey, bookIdForLog, finalImageDetails.getCoverImageSource(), finalImageDetails.getUrlOrPath());
                        }
                    }

                }, java.util.concurrent.ForkJoinPool.commonPool())
                .exceptionally(ex -> {
                    logger.error("Background: Top-level exception in processCoverInBackground for {} (BookID {}): {}", 
                        identifierKey, bookIdForLog, ex.getMessage(), ex);
                    identifierToProvisionalUrlCache.remove(identifierKey);
                    identifierToFinalImageDetailsCache.remove(identifierKey); 
                    return null;
                });
        } catch (Exception e) {
            logger.error("Background: Synchronous error initiating full cover processing for identifierKey {}: {}", identifierKey, e.getMessage(), e); 
            identifierToProvisionalUrlCache.remove(identifierKey);
        }
    }
    
    public CompletableFuture<ImageDetails> getCoverImageUrlAsync(Book book, String provisionalUrlHint) {
        ImageProvenanceData provenanceData = new ImageProvenanceData();
        String effectiveBookIdForProvenance = book.getId() != null ? book.getId() : 
                                             (book.getIsbn13() != null ? "isbn-" + book.getIsbn13() : 
                                             (book.getIsbn10() != null ? "isbn-" + book.getIsbn10() : 
                                             "unknown-" + System.currentTimeMillis()));
        provenanceData.setBookId(effectiveBookIdForProvenance);
        return getCoverImageUrlAsync(book, provisionalUrlHint, provenanceData);
    }

    public CompletableFuture<ImageDetails> getCoverImageUrlAsync(Book book, String provisionalUrlHint, ImageProvenanceData provenanceData) {
        String bookIdForLog = book.getId() != null ? book.getId() : (book.getIsbn13() != null ? book.getIsbn13() : "unknown_book");

        if (provisionalUrlHint != null && !provisionalUrlHint.isEmpty() && 
            !provisionalUrlHint.equals(LOCAL_PLACEHOLDER_PATH) && 
            !provisionalUrlHint.startsWith("/" + this.cacheDirName)) {
            
            Path destinationPath;
            try {
                destinationPath = cacheDir.resolve(generateFilenameFromUrl(provisionalUrlHint));
            } catch (NoSuchAlgorithmException e) {
                 logger.error("CRITICAL: SHA-256 algorithm not found for provisionalUrlHint {}. Falling back to processCoverSources. Book ID for log: {}", provisionalUrlHint, bookIdForLog, e);
                return processCoverSources(book, provenanceData); 
            }
            
            CoverImageSource hintSource = CoverImageSource.ANY; 
            String sourceName = "ProvisionalHint";
            // Improved Google URL Detection
            if (provisionalUrlHint.contains("googleapis.com/books") || provisionalUrlHint.contains("books.google.com/books")) { 
                hintSource = CoverImageSource.GOOGLE_BOOKS; 
                sourceName = ImageSourceName.GOOGLE_BOOKS.getDisplayName(); 
            }
            else if (provisionalUrlHint.contains("openlibrary.org")) { hintSource = CoverImageSource.OPEN_LIBRARY; sourceName = ImageSourceName.OPEN_LIBRARY.getDisplayName(); }
            else if (provisionalUrlHint.contains("longitood.com")) { hintSource = CoverImageSource.LONGITOOD; sourceName = ImageSourceName.LONGITOOD.getDisplayName(); }
            
            final CoverImageSource finalHintSource = hintSource;
            final String finalSourceNameString = sourceName;
            final ImageSourceName finalSourceNameEnum = mapStringToImageSourceName(finalSourceNameString);
            final int MIN_ACCEPTABLE_DIMENSION = 200; // Minimum width or height for a provisional image to be considered "good enough"

            String urlToDownload = provisionalUrlHint;
            if (finalHintSource == CoverImageSource.GOOGLE_BOOKS) {
                logger.debug("Provisional hint is from Google Books. Ensuring URL parameters are optimal: {}", urlToDownload);
                // Remove or neutralize problematic Google Books URL parameters
                if (urlToDownload.contains("zoom=")) {
                    urlToDownload = urlToDownload.replaceAll("zoom=\\d+", "zoom=0"); 
                }
                if (urlToDownload.contains("&fife=")) {
                    urlToDownload = urlToDownload.replaceAll("&fife=w\\d+", "");
                } else if (urlToDownload.contains("?fife=")) {
                    urlToDownload = urlToDownload.replaceAll("\\?fife=w\\d+", "?");
                    if (urlToDownload.endsWith("?")) {
                        urlToDownload = urlToDownload.substring(0, urlToDownload.length() -1);
                    }
                }
                // Remove trailing '&' if fife was the last parameter and removed
                if (urlToDownload.endsWith("&")) {
                    urlToDownload = urlToDownload.substring(0, urlToDownload.length() - 1);
                }
                logger.debug("Optimized Google Books URL for download: {}", urlToDownload);
            }

            return downloadAndCacheImageInternalAsync(urlToDownload, destinationPath, bookIdForLog, provenanceData, finalSourceNameString)
                .thenCompose(cachedFromHintDetails -> {
                    boolean isGoodQualityProvisional = cachedFromHintDetails.getWidth() >= MIN_ACCEPTABLE_DIMENSION &&
                                                       cachedFromHintDetails.getHeight() >= MIN_ACCEPTABLE_DIMENSION &&
                                                       !cachedFromHintDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH);

                    // Adjust "Early Exit" Logic: If hint is from Google, OR if it's not good quality, always process other sources
                    if (finalHintSource != CoverImageSource.GOOGLE_BOOKS && isGoodQualityProvisional) {
                        // Only take early exit if NOT a Google hint AND it's good quality
                        if (provenanceData != null && provenanceData.getSelectedImageInfo() == null) { 
                            ImageProvenanceData.SelectedImageInfo selectedInfo = new ImageProvenanceData.SelectedImageInfo();
                            selectedInfo.setSourceName(finalSourceNameEnum);
                            selectedInfo.setFinalUrl(cachedFromHintDetails.getUrlOrPath());
                            selectedInfo.setResolution(cachedFromHintDetails.getResolutionPreference() != null ? cachedFromHintDetails.getResolutionPreference().name() : "ORIGINAL");
                            selectedInfo.setStorageLocation(cachedFromHintDetails.getUrlOrPath().startsWith("/" + cacheDirName) ? "LocalCache" : "Remote");
                            provenanceData.setSelectedImageInfo(selectedInfo);
                        }
                        return CompletableFuture.completedFuture(new ImageDetails(
                            cachedFromHintDetails.getUrlOrPath(), 
                            finalHintSource.getDisplayName(), 
                            "hint-" + finalHintSource.name() + "-" + bookIdForLog, 
                            finalHintSource, 
                            ImageResolutionPreference.ORIGINAL, 
                            cachedFromHintDetails.getWidth(), 
                            cachedFromHintDetails.getHeight()
                        ));
                    }
                    logger.warn("Provisional URL hint {} did not yield a valid cached image for Book ID for log: {}. Proceeding to full source scan.", provisionalUrlHint, bookIdForLog);
                    return processCoverSources(book, provenanceData);
                })
                .exceptionally(ex -> {
                    logger.error("Exception processing provisionalUrlHint {} for Book ID for log: {}. Falling back to processCoverSources. Error: {}", provisionalUrlHint, bookIdForLog, ex.getMessage());
                    return processCoverSources(book, provenanceData).getNow(createPlaceholderImageDetails(bookIdForLog, "fallback-provisional-ex"));
                });
        }
        return processCoverSources(book, provenanceData);
    }

    private CompletableFuture<ImageDetails> processCoverSources(Book book, ImageProvenanceData provenanceData) {
        String googleVolumeId = book.getId(); 
        String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();
        String bookIdForLog = googleVolumeId != null ? googleVolumeId : (isbn != null ? isbn : "unknown_book_for_log");

        if (provenanceData != null && provenanceData.getAttemptedImageSources() == null) {
            provenanceData.setAttemptedImageSources(new ArrayList<>());
        }
        
        return tryS3(book, bookIdForLog, provenanceData)
            .thenCompose(detailsS3 -> {
                if (detailsS3.getWidth() > 0 && detailsS3.getCoverImageSource() == CoverImageSource.S3_CACHE) {
                    if (provenanceData != null && provenanceData.getSelectedImageInfo() == null) {
                        ImageProvenanceData.SelectedImageInfo selectedInfo = new ImageProvenanceData.SelectedImageInfo();
                        selectedInfo.setSourceName(ImageSourceName.S3_CACHE);
                        selectedInfo.setFinalUrl(detailsS3.getUrlOrPath());
                        selectedInfo.setS3Key(detailsS3.getSourceSystemId()); 
                        selectedInfo.setResolution(detailsS3.getResolutionPreference() != null ? detailsS3.getResolutionPreference().name() : "ORIGINAL");
                        selectedInfo.setStorageLocation("S3");
                        provenanceData.setSelectedImageInfo(selectedInfo);
                    }
                    return CompletableFuture.completedFuture(detailsS3);
                }
                
                if (isbn != null && !isbn.isEmpty()) {
                    logger.debug("S3 failed or not found for Book ID {}. ISBN {} is available. Trying Google Books API (by ISBN), then OpenLibrary, then Longitood.", bookIdForLog, isbn);
                    return tryGoogleBooksApiByIsbn(isbn, bookIdForLog, provenanceData)
                        .thenCompose(detailsGoogleIsbn -> {
                            if (detailsGoogleIsbn.getWidth() > 0 && detailsGoogleIsbn.getCoverImageSource() == CoverImageSource.GOOGLE_BOOKS) return CompletableFuture.completedFuture(detailsGoogleIsbn);
                            logger.debug("Google Books API (by ISBN) failed for ISBN {}. Trying OpenLibrary.", isbn);
                            return tryOpenLibrary(isbn, bookIdForLog, "L", provenanceData);
                        })
                        .thenCompose(detailsL -> {
                            if (detailsL.getWidth() > 0 && detailsL.getCoverImageSource() == CoverImageSource.OPEN_LIBRARY) return CompletableFuture.completedFuture(detailsL);
                            return tryOpenLibrary(isbn, bookIdForLog, "M", provenanceData);
                        })
                        .thenCompose(detailsM -> {
                            if (detailsM.getWidth() > 0 && detailsM.getCoverImageSource() == CoverImageSource.OPEN_LIBRARY) return CompletableFuture.completedFuture(detailsM);
                            return tryOpenLibrary(isbn, bookIdForLog, "S", provenanceData);
                        })
                        .thenCompose(detailsS -> {
                            if (detailsS.getWidth() > 0 && detailsS.getCoverImageSource() == CoverImageSource.OPEN_LIBRARY) return CompletableFuture.completedFuture(detailsS);
                            logger.warn("All S3, Google (by ISBN), and OpenLibrary attempts failed for ISBN {}. Trying Longitood.", isbn);
                            return tryLongitood(book, bookIdForLog, provenanceData);
                        });
                } else if (googleVolumeId != null && !googleVolumeId.isEmpty()) {
                    logger.debug("S3 failed for Book ID {}. No ISBN available. Trying Google Books API directly by Google Volume ID {}.", bookIdForLog, googleVolumeId);
                    return tryGoogleBooksApiByVolumeId(googleVolumeId, bookIdForLog, provenanceData)
                         .thenCompose(detailsGoogleId -> { 
                            if (detailsGoogleId.getWidth() > 0 && detailsGoogleId.getCoverImageSource() == CoverImageSource.GOOGLE_BOOKS) return CompletableFuture.completedFuture(detailsGoogleId);
                            logger.warn("Google Books API (by Volume ID {}) also failed for Book ID {}. No other sources to try without ISBN. Returning placeholder.", googleVolumeId, bookIdForLog);
                            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "google-id-failed"));
                        });
                } else {
                    logger.warn("No usable identifier (ISBN or Google Volume ID) for Book ID {}. Returning placeholder.", bookIdForLog);
                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "no-identifier"));
                }
            })
            .exceptionally(ex -> { 
                 logger.error("All cover sources failed for Book ID {}: {}", bookIdForLog, ex.getMessage());
                 return createPlaceholderImageDetails(bookIdForLog, "all-sources-failed-ex");
            });
    }

    private CompletableFuture<ImageDetails> tryS3(Book book, String bookIdForLog, ImageProvenanceData provenanceData) {
        logger.debug("Trying S3 for Book ID {}", bookIdForLog);
        // TODO: Add AttemptedSourceInfo for S3 fetch attempt
        // For now, a simplified approach; proper provenance would involve creating an AttemptedSourceInfo here.
        // Example:
        // ImageProvenanceData.AttemptedSourceInfo s3Attempt = new ImageProvenanceData.AttemptedSourceInfo(ImageSourceName.S3_CACHE, "S3 Direct Fetch for " + bookIdForLog, ImageAttemptStatus.PENDING);
        // if (provenanceData != null && provenanceData.getAttemptedImageSources() != null) { provenanceData.getAttemptedImageSources().add(s3Attempt); }
        // else if (provenanceData != null) { provenanceData.setAttemptedImageSources(Collections.singletonList(s3Attempt)); }


        return s3BookCoverService.fetchCover(book)
            .toFuture()
            .thenCompose(s3RemoteDetails -> { 
                if (s3RemoteDetails != null && s3RemoteDetails.getUrlOrPath() != null && !s3RemoteDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH) && s3RemoteDetails.getCoverImageSource() == CoverImageSource.S3_CACHE) {
                    Path dest;
                    try {
                        dest = cacheDir.resolve(generateFilenameFromUrl(s3RemoteDetails.getUrlOrPath()));
                    } catch (NoSuchAlgorithmException e) {
                        logger.error("CRITICAL: SHA-256 algorithm not found for S3 URL {} (Book ID for log: {}). Error: {}", s3RemoteDetails.getUrlOrPath(), bookIdForLog, e.getMessage());
                        return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "hash-error-s3"));
                    }
                    
                    if (urlToPathCache.containsKey(s3RemoteDetails.getUrlOrPath())) {
                        String localPath = urlToPathCache.get(s3RemoteDetails.getUrlOrPath());
                        logger.debug("S3 URL {} already in local cache at {} (Book ID for log: {})", s3RemoteDetails.getUrlOrPath(), localPath, bookIdForLog);
                        return CompletableFuture.completedFuture(s3RemoteDetails); 
                    }
                    
                    return downloadAndCacheImageInternalAsync(s3RemoteDetails.getUrlOrPath(), dest, bookIdForLog, provenanceData, "S3_Cache_To_Local")
                        .thenApply(cachedFromS3 -> { 
                            if (cachedFromS3.getWidth() > 0 && cachedFromS3.getUrlOrPath() != null && !cachedFromS3.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH)) {
                                return new ImageDetails(
                                    cachedFromS3.getUrlOrPath(), 
                                    s3RemoteDetails.getSourceName(), 
                                    s3RemoteDetails.getSourceSystemId(), 
                                    s3RemoteDetails.getCoverImageSource(), 
                                    s3RemoteDetails.getResolutionPreference(),
                                    cachedFromS3.getWidth(), 
                                    cachedFromS3.getHeight()
                                );
                            }
                            return cachedFromS3; 
                        });
                }
                logger.debug("S3 did not provide a valid remote URL for Book ID {} or source was not S3_CACHE. Using placeholder.", bookIdForLog);
                return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "s3-no-url-or-wrong-source"));
            })
            .exceptionally(ex -> {
                logger.error("Exception calling S3 or processing its result for Book ID {}: {}", bookIdForLog, ex.getMessage());
                return createPlaceholderImageDetails(bookIdForLog, "s3-exception");
            });
    }

    private CompletableFuture<ImageDetails> tryOpenLibrary(String isbn, String bookIdForLog, String sizeSuffix, ImageProvenanceData provenanceData) {
        if (isKnownBadOpenLibraryIsbn(isbn)) {
            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "known-bad-ol"));
        }
        String sourceNameString = "OpenLibrary-" + sizeSuffix;
        ImageSourceName sourceNameEnum = ImageSourceName.OPEN_LIBRARY;


        return this.openLibraryService.fetchOpenLibraryCoverDetails(isbn, sizeSuffix)
            .toFuture()
            .thenCompose(remoteImageDetails -> { 
                if (remoteImageDetails == null || remoteImageDetails.getUrlOrPath() == null || remoteImageDetails.getUrlOrPath().isEmpty() || remoteImageDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH)) {
                    logger.warn("OpenLibraryService did not provide a valid remote URL for ISBN {} size {}. Using placeholder.", isbn, sizeSuffix);
                    if (provenanceData != null) {
                        provenanceData.getAttemptedImageSources().add(new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, "isbn:" + isbn + ", size:" + sizeSuffix, ImageAttemptStatus.FAILURE_404));
                    }
                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "ol-" + sizeSuffix + "-no-url"));
                }

                String openLibraryUrl = remoteImageDetails.getUrlOrPath();
                Path destinationPath;
                try {
                    destinationPath = cacheDir.resolve(generateFilenameFromUrl(openLibraryUrl));
                } catch (NoSuchAlgorithmException e) {
                    logger.error("CRITICAL: SHA-256 algorithm not found for OpenLibrary URL ({} from service): {}. Error: {}", sizeSuffix, openLibraryUrl, e.getMessage());
                    addKnownBadOpenLibraryIsbn(isbn);
                    if (provenanceData != null) {
                        provenanceData.getAttemptedImageSources().add(new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, openLibraryUrl, ImageAttemptStatus.PROCESSING_FAILED));
                    }
                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "hash-error-ol"));
                }

                return downloadAndCacheImageInternalAsync(openLibraryUrl, destinationPath, bookIdForLog, provenanceData, sourceNameString)
                    .thenApply(cachedImageDetails -> {
                        if (cachedImageDetails.getWidth() > 0 && cachedImageDetails.getUrlOrPath() != null && !cachedImageDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH)) {
                             if (provenanceData != null && provenanceData.getSelectedImageInfo() == null) {
                                ImageProvenanceData.SelectedImageInfo selectedInfo = new ImageProvenanceData.SelectedImageInfo();
                                selectedInfo.setSourceName(sourceNameEnum);
                                selectedInfo.setFinalUrl(cachedImageDetails.getUrlOrPath());
                                selectedInfo.setResolution(sizeSuffix);
                                selectedInfo.setStorageLocation("LocalCache");
                                provenanceData.setSelectedImageInfo(selectedInfo);
                            }
                            return new ImageDetails(
                                cachedImageDetails.getUrlOrPath(), 
                                remoteImageDetails.getSourceName(), 
                                remoteImageDetails.getSourceSystemId(), 
                                remoteImageDetails.getCoverImageSource(), 
                                remoteImageDetails.getResolutionPreference(), 
                                cachedImageDetails.getWidth(), 
                                cachedImageDetails.getHeight()
                            );
                        }
                        logger.warn("Download/cache failed for OpenLibrary URL ({} from service): {} for ISBN {}, size {}. Resulted in placeholder or invalid image.", sizeSuffix, openLibraryUrl, isbn, sizeSuffix);
                        addKnownBadOpenLibraryIsbn(isbn);
                        return createPlaceholderImageDetails(bookIdForLog, "ol-" + sizeSuffix + "-failed-dl");
                    });
            })
            .exceptionally(ex -> {
                logger.error("Exception calling OpenLibraryService or processing its result for ISBN {}, size {}: {}", isbn, sizeSuffix, ex.getMessage());
                addKnownBadOpenLibraryIsbn(isbn); 
                if (provenanceData != null) {
                    ImageProvenanceData.AttemptedSourceInfo failureInfo = new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, "isbn:" + isbn + ", size:" + sizeSuffix, ImageAttemptStatus.FAILURE_GENERIC);
                    failureInfo.setFailureReason(ex.getMessage());
                    provenanceData.getAttemptedImageSources().add(failureInfo);
                }
                return createPlaceholderImageDetails(bookIdForLog, "ol-" + sizeSuffix + "-exception");
            });
    }

    private CompletableFuture<ImageDetails> tryGoogleBooksApiByIsbn(String isbn, String bookIdForLog, ImageProvenanceData provenanceData) {
        logger.debug("Attempting Google Books API fallback by ISBN {} (Book ID for log: {})", isbn, bookIdForLog);
        String sourceNameString = "GoogleBooksAPI-ISBN";
        ImageSourceName sourceNameEnum = ImageSourceName.GOOGLE_BOOKS;
        // TODO: Ensure GoogleBooksService.searchBooksByISBN and Book model provide raw JSON for gBook.getRawJsonResponse() for provenance.
        return googleBooksService.searchBooksByISBN(isbn) // This needs to potentially return raw JSON too
            .toFuture()
            .thenComposeAsync(books -> {
                if (books != null && !books.isEmpty()) {
                    com.williamcallahan.book_recommendation_engine.model.Book gBook = books.get(0);
                    if (provenanceData != null && gBook.getRawJsonResponse() != null && provenanceData.getGoogleBooksApiResponse() == null) {
                         provenanceData.setGoogleBooksApiResponse(gBook.getRawJsonResponse());
                    }
                    if (gBook != null && gBook.getCoverImageUrl() != null && !gBook.getCoverImageUrl().isEmpty() &&
                        !knownBadImageUrls.contains(gBook.getCoverImageUrl()) &&
                        !gBook.getCoverImageUrl().contains("image-not-available.png")) {
                        Path destinationPath;
                        try {
                            destinationPath = cacheDir.resolve(generateFilenameFromUrl(gBook.getCoverImageUrl()));
                        } catch (NoSuchAlgorithmException e) {
                            logger.error("Book ID {}: SHA-256 Hashing error for Google ISBN {}: {}", bookIdForLog, isbn, e.getMessage());
                            if (provenanceData != null) provenanceData.getAttemptedImageSources().add(new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, gBook.getCoverImageUrl(), ImageAttemptStatus.PROCESSING_FAILED));
                            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "hash-error-google-isbn"));
                        }
                        return downloadAndCacheImageInternalAsync(gBook.getCoverImageUrl(), destinationPath, bookIdForLog, provenanceData, sourceNameString)
                            .thenApply(cachedDetails -> {
                                if (cachedDetails.getWidth() > 0 && cachedDetails.getUrlOrPath() != null && !cachedDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH)) {
                                    if (provenanceData != null && provenanceData.getSelectedImageInfo() == null) {
                                        ImageProvenanceData.SelectedImageInfo selectedInfo = new ImageProvenanceData.SelectedImageInfo();
                                        selectedInfo.setSourceName(sourceNameEnum);
                                        selectedInfo.setFinalUrl(cachedDetails.getUrlOrPath());
                                        selectedInfo.setResolution("ORIGINAL"); // Assuming original from Google
                                        selectedInfo.setStorageLocation("LocalCache");
                                        provenanceData.setSelectedImageInfo(selectedInfo);
                                    }
                                    return new ImageDetails(
                                        cachedDetails.getUrlOrPath(), 
                                        "GoogleBooks", 
                                        "gb-isbn-" + isbn, 
                                        CoverImageSource.GOOGLE_BOOKS, 
                                        ImageResolutionPreference.ORIGINAL, 
                                        cachedDetails.getWidth(),
                                        cachedDetails.getHeight()
                                    );
                                }
                                return cachedDetails; 
                            });
                    }
                }
                logger.warn("Google Books API (by ISBN) did not yield a usable image for ISBN {} (Book ID for log: {})", isbn, bookIdForLog);
                 if (provenanceData != null) {
                    provenanceData.getAttemptedImageSources().add(new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, "isbn:" + isbn, ImageAttemptStatus.FAILURE_404));
                 }
                return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "google-isbn-failed-or-no-image"));
            })
            .exceptionally(ex -> {
                logger.error("Exception during Google Books API fallback by ISBN {} (Book ID for log: {}): {}", isbn, bookIdForLog, ex.getMessage(), ex);
                if (provenanceData != null) {
                    ImageProvenanceData.AttemptedSourceInfo failureInfo = new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, "isbn:" + isbn, ImageAttemptStatus.FAILURE_GENERIC);
                    failureInfo.setFailureReason(ex.getMessage());
                    provenanceData.getAttemptedImageSources().add(failureInfo);
                }
                return createPlaceholderImageDetails(bookIdForLog, "google-isbn-exception");
            });
    }

    private CompletableFuture<ImageDetails> tryGoogleBooksApiByVolumeId(String googleVolumeId, String bookIdForLog, ImageProvenanceData provenanceData) {
        logger.debug("Attempting Google Books API directly by Google Volume ID {} (Book ID for log: {})", googleVolumeId, bookIdForLog);
        String sourceNameString = "GoogleBooksAPI-VolumeID";
        ImageSourceName sourceNameEnum = ImageSourceName.GOOGLE_BOOKS;
        // TODO: Ensure GoogleBooksService.getBookById and Book model provide raw JSON for gBook.getRawJsonResponse() for provenance.
        return googleBooksService.getBookById(googleVolumeId)
            .toFuture()
            .thenComposeAsync(gBook -> {
                if (provenanceData != null && gBook != null && gBook.getRawJsonResponse() != null && provenanceData.getGoogleBooksApiResponse() == null) {
                    provenanceData.setGoogleBooksApiResponse(gBook.getRawJsonResponse());
                }
                if (gBook != null && gBook.getCoverImageUrl() != null && !gBook.getCoverImageUrl().isEmpty() &&
                    !knownBadImageUrls.contains(gBook.getCoverImageUrl()) &&
                    !gBook.getCoverImageUrl().contains("image-not-available.png")) {
                    Path destinationPath;
                    try {
                        destinationPath = cacheDir.resolve(generateFilenameFromUrl(gBook.getCoverImageUrl()));
                    } catch (NoSuchAlgorithmException e) {
                        logger.error("Book ID {}: SHA-256 Hashing error for Google Volume ID {}: {}", bookIdForLog, googleVolumeId, e.getMessage());
                        if (provenanceData != null) provenanceData.getAttemptedImageSources().add(new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, gBook.getCoverImageUrl(), ImageAttemptStatus.PROCESSING_FAILED));
                        return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "hash-error-google-volid"));
                    }
                    return downloadAndCacheImageInternalAsync(gBook.getCoverImageUrl(), destinationPath, bookIdForLog, provenanceData, sourceNameString)
                        .thenApply(cachedDetails -> {
                            if (cachedDetails.getWidth() > 0 && cachedDetails.getUrlOrPath() != null && !cachedDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH)) {
                                if (provenanceData != null && provenanceData.getSelectedImageInfo() == null) {
                                    ImageProvenanceData.SelectedImageInfo selectedInfo = new ImageProvenanceData.SelectedImageInfo();
                                    selectedInfo.setSourceName(sourceNameEnum);
                                    selectedInfo.setFinalUrl(cachedDetails.getUrlOrPath());
                                    selectedInfo.setResolution("ORIGINAL");
                                    selectedInfo.setStorageLocation("LocalCache");
                                    provenanceData.setSelectedImageInfo(selectedInfo);
                                }
                                return new ImageDetails(
                                    cachedDetails.getUrlOrPath(), 
                                    "GoogleBooks", 
                                    "gb-volid-" + googleVolumeId, 
                                    CoverImageSource.GOOGLE_BOOKS, 
                                    ImageResolutionPreference.ORIGINAL, 
                                    cachedDetails.getWidth(),
                                    cachedDetails.getHeight()
                                );
                            }
                            return cachedDetails; 
                        });
                }
                logger.warn("Google Books API (by Volume ID) did not yield a usable image for Volume ID {} (Book ID for log: {})", googleVolumeId, bookIdForLog);
                if (provenanceData != null) {
                    provenanceData.getAttemptedImageSources().add(new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, "volumeId:" + googleVolumeId, ImageAttemptStatus.FAILURE_404));
                }
                return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "google-volid-failed-or-no-image"));
            })
            .exceptionally(ex -> {
                logger.error("Exception during Google Books API call by Volume ID {} (Book ID for log: {}): {} - {}", 
                             googleVolumeId, bookIdForLog, ex.getClass().getName(), ex.getMessage(), ex);
                if (provenanceData != null) {
                    ImageProvenanceData.AttemptedSourceInfo failureInfo = new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, "volumeId:" + googleVolumeId, ImageAttemptStatus.FAILURE_GENERIC);
                    failureInfo.setFailureReason(ex.getMessage());
                    provenanceData.getAttemptedImageSources().add(failureInfo);
                }
                return createPlaceholderImageDetails(bookIdForLog, "google-volid-exception");
            });
    }
    
    private CompletableFuture<ImageDetails> tryLongitood(Book book, String bookIdForLog, ImageProvenanceData provenanceData) {
        String sourceNameString = "Longitood";
        ImageSourceName sourceNameEnum = ImageSourceName.LONGITOOD;
        String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();

        return longitoodService.fetchCover(book).toFuture()
            .thenCompose(longitoodRemoteDetails -> { 
                if (longitoodRemoteDetails != null && longitoodRemoteDetails.getUrlOrPath() != null && !longitoodRemoteDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH) && !knownBadImageUrls.contains(longitoodRemoteDetails.getUrlOrPath())) {
                    Path dest; 
                    try { 
                        dest = cacheDir.resolve(generateFilenameFromUrl(longitoodRemoteDetails.getUrlOrPath())); 
                    } catch (NoSuchAlgorithmException e) { 
                        if (provenanceData != null) provenanceData.getAttemptedImageSources().add(new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, longitoodRemoteDetails.getUrlOrPath(), ImageAttemptStatus.PROCESSING_FAILED));
                        return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "hash-error-longitood")); 
                    }
                    return downloadAndCacheImageInternalAsync(longitoodRemoteDetails.getUrlOrPath(), dest, bookIdForLog, provenanceData, sourceNameString)
                        .thenApply(cachedDetails -> {
                            if (cachedDetails.getWidth() > 0 && cachedDetails.getUrlOrPath() != null && !cachedDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH)) {
                                if (provenanceData != null && provenanceData.getSelectedImageInfo() == null) {
                                    ImageProvenanceData.SelectedImageInfo selectedInfo = new ImageProvenanceData.SelectedImageInfo();
                                    selectedInfo.setSourceName(sourceNameEnum);
                                    selectedInfo.setFinalUrl(cachedDetails.getUrlOrPath());
                                    selectedInfo.setResolution("ORIGINAL"); // Assuming
                                    selectedInfo.setStorageLocation("LocalCache");
                                    provenanceData.setSelectedImageInfo(selectedInfo);
                                }
                                return new ImageDetails(
                                    cachedDetails.getUrlOrPath(),
                                    longitoodRemoteDetails.getSourceName(), 
                                    longitoodRemoteDetails.getSourceSystemId(),
                                    longitoodRemoteDetails.getCoverImageSource(), 
                                    longitoodRemoteDetails.getResolutionPreference(),
                                    cachedDetails.getWidth(),
                                    cachedDetails.getHeight()
                                );
                            }
                            return cachedDetails; 
                        });
                }
                logger.warn("Longitood also failed for ISBN {}. Returning placeholder.", isbn);
                addKnownBadLongitoodIsbn(isbn); 
                if (provenanceData != null) provenanceData.getAttemptedImageSources().add(new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, "isbn:" + isbn, ImageAttemptStatus.FAILURE_404));
                return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "longitood-failed"));
            })
            .exceptionally(exL -> {
                logger.error("Exception with Longitood for ISBN {}: {}. Returning placeholder.", isbn, exL.getMessage());
                addKnownBadLongitoodIsbn(isbn); 
                if (provenanceData != null) {
                    ImageProvenanceData.AttemptedSourceInfo failureInfo = new ImageProvenanceData.AttemptedSourceInfo(sourceNameEnum, "isbn:" + isbn, ImageAttemptStatus.FAILURE_GENERIC);
                    failureInfo.setFailureReason(exL.getMessage());
                    provenanceData.getAttemptedImageSources().add(failureInfo);
                }
                return createPlaceholderImageDetails(bookIdForLog, "longitood-exception");
            });
    }


    private String getIdentifierKey(Book book) {
        if (book == null) return null;
        if (book.getIsbn13() != null && !book.getIsbn13().isEmpty()) return book.getIsbn13();
        if (book.getIsbn10() != null && !book.getIsbn10().isEmpty()) return book.getIsbn10();
        if (book.getId() != null && !book.getId().isEmpty()) return book.getId(); 
        return null;
    }

    private void uploadLocallyCachedFileToS3(ImageDetails localImageDetails, String bookIdForLog, String identifierKey, String googleBookId, ImageProvenanceData provenanceData) {
        if (localImageDetails == null || localImageDetails.getUrlOrPath() == null || !localImageDetails.getUrlOrPath().startsWith("/" + this.cacheDirName)) {
            logger.warn("BackgroundS3: Invalid localImageDetails for S3 upload. URL/Path: {}. BookID: {}", 
                localImageDetails != null ? localImageDetails.getUrlOrPath() : "null", bookIdForLog);
            return;
        }
        Path localImagePath = Paths.get(this.cacheDirString, localImageDetails.getUrlOrPath().substring(("/" + this.cacheDirName + "/").length()));
        
        String s3KeyOriginalSourcePart = localImageDetails.getCoverImageSource() != null ? 
                                         localImageDetails.getCoverImageSource().name().toLowerCase().replaceAll("[^a-z0-9_-]", "-") : 
                                         "unknown";
        
        if (provenanceData != null) {
            ImageProvenanceData.SelectedImageInfo selectedInfo = provenanceData.getSelectedImageInfo();
            if (selectedInfo == null) {
                selectedInfo = new ImageProvenanceData.SelectedImageInfo();
                provenanceData.setSelectedImageInfo(selectedInfo);
                selectedInfo.setSourceName(localImageDetails.getCoverImageSource() != null ? mapCoverImageSourceToImageSourceName(localImageDetails.getCoverImageSource()) : ImageSourceName.UNKNOWN);
                selectedInfo.setFinalUrl(localImageDetails.getUrlOrPath());
                selectedInfo.setResolution(localImageDetails.getResolutionPreference() != null ? localImageDetails.getResolutionPreference().name() : "ORIGINAL");
            }
            selectedInfo.setStorageLocation("LocalCachePendingS3");
        }


        CompletableFuture.supplyAsync(() -> {
            try {
                logger.debug("BackgroundS3: Reading local file {} for BookID {}", localImagePath, bookIdForLog);
                if (Files.exists(localImagePath)) return Files.readAllBytes(localImagePath);
                throw new IOException("Local file for S3 upload not found: " + localImagePath);
            } catch (IOException e) {
                logger.error("BackgroundS3: Failed to read local file {} (BookID {}): {}", localImagePath, bookIdForLog, e.getMessage());
                throw new RuntimeException("Failed to read local for S3", e); 
            }
        }, java.util.concurrent.ForkJoinPool.commonPool())
        .thenApplyAsync(localBytes -> {
            logger.debug("BackgroundS3: Processing image ({} bytes) from {} for BookID {}", localBytes.length, localImagePath, bookIdForLog);
            return imageProcessingService.processImageForS3(localBytes, bookIdForLog);
        }, java.util.concurrent.ForkJoinPool.commonPool())
        .thenComposeAsync(processedImage -> {
            if (processedImage.isProcessingSuccessful()) {
                logger.debug("BackgroundS3: Uploading processed image for BookID {} (orig source enum {}) to S3.", bookIdForLog, s3KeyOriginalSourcePart);
                return s3BookCoverService.uploadProcessedCoverToS3Async(
                    processedImage.getProcessedBytes(), processedImage.getNewFileExtension(),
                    processedImage.getNewMimeType(), processedImage.getWidth(),
                    processedImage.getHeight(), bookIdForLog, s3KeyOriginalSourcePart, provenanceData 
                ).toFuture();
            } else {
                logger.warn("BackgroundS3: Image processing failed for BookID {}, orig source enum {}. Error: {}. S3 upload skipped.", 
                    bookIdForLog, s3KeyOriginalSourcePart, processedImage.getProcessingError());
                return CompletableFuture.completedFuture(null); 
            }
        }, java.util.concurrent.ForkJoinPool.commonPool())
        .thenAcceptAsync(s3ImageDetails -> {
            if (s3ImageDetails != null && s3ImageDetails.getCoverImageSource() == CoverImageSource.S3_CACHE) {
                logger.info("BackgroundS3: Successfully uploaded to S3 for BookID {}. S3 URL is: {}", 
                    bookIdForLog, s3ImageDetails.getUrlOrPath());
                
                ImageDetails finalS3Details = new ImageDetails(
                    s3ImageDetails.getUrlOrPath(), 
                    s3ImageDetails.getSourceName(), 
                    s3ImageDetails.getSourceSystemId(), 
                    CoverImageSource.S3_CACHE, 
                    s3ImageDetails.getResolutionPreference(),
                    s3ImageDetails.getWidth(),
                    s3ImageDetails.getHeight()
                );
                identifierToFinalImageDetailsCache.put(identifierKey, finalS3Details); 
                logger.info("BackgroundS3: Updated identifierToFinalImageDetailsCache for {} to S3 URL: {} with source S3_CACHE", identifierKey, s3ImageDetails.getUrlOrPath());
                
                eventPublisher.publishEvent(new BookCoverUpdatedEvent(identifierKey, s3ImageDetails.getUrlOrPath(), googleBookId, CoverImageSource.S3_CACHE));
                logger.info("BackgroundS3: Published BookCoverUpdatedEvent for {} (BookID {}) with S3 URL: {} and Source: S3_CACHE", identifierKey, bookIdForLog, s3ImageDetails.getUrlOrPath());
            } else {
                logger.warn("BackgroundS3: S3 upload for BookID {} (orig source enum {}) did not result in S3_CACHE details or failed. S3 returned: {}. Final local cache path remains unchanged.", 
                    bookIdForLog, s3KeyOriginalSourcePart, s3ImageDetails);
            }
        }, java.util.concurrent.ForkJoinPool.commonPool())
        .exceptionally(ex -> {
            logger.error("BackgroundS3: Exception in S3 upload chain for BookID {}, orig_source_enum {}: {}", 
                bookIdForLog, s3KeyOriginalSourcePart, ex.getMessage(), ex);
            return null;
        });
    }

    // Helper method to map String to ImageSourceName
    private ImageSourceName mapStringToImageSourceName(String sourceNameString) {
        if (sourceNameString == null) return ImageSourceName.UNKNOWN;
        // Simplified mapping logic, can be expanded
        if (sourceNameString.toLowerCase().contains("openlibrary")) return ImageSourceName.OPEN_LIBRARY;
        if (sourceNameString.toLowerCase().contains("google") || sourceNameString.toLowerCase().contains("googlebooks")) return ImageSourceName.GOOGLE_BOOKS;
        if (sourceNameString.toLowerCase().contains("longitood")) return ImageSourceName.LONGITOOD;
        if (sourceNameString.toLowerCase().contains("s3")) return ImageSourceName.S3_CACHE;
        if (sourceNameString.toLowerCase().contains("local")) return ImageSourceName.LOCAL_CACHE;
        if (sourceNameString.equalsIgnoreCase("ProvisionalHint")) return ImageSourceName.UNKNOWN; // Or a more specific category if hints are treated distinctly

        try {
            return ImageSourceName.valueOf(sourceNameString.toUpperCase().replaceAll("[^A-Z0-9_]", ""));
        } catch (IllegalArgumentException e) {
            return ImageSourceName.UNKNOWN;
        }
    }

    // Helper method to map CoverImageSource to ImageSourceName
    private ImageSourceName mapCoverImageSourceToImageSourceName(CoverImageSource coverImageSource) {
        if (coverImageSource == null) return ImageSourceName.UNKNOWN;
        try {
            // Direct mapping if names match (e.g., GOOGLE_BOOKS to GOOGLE_BOOKS)
            return ImageSourceName.valueOf(coverImageSource.name());
        } catch (IllegalArgumentException e) {
            // Fallback logic for specific cases or if names differ (e.g. CoverImageSource.NONE or .UNDEFINED)
            switch (coverImageSource) {
                case GOOGLE_BOOKS: return ImageSourceName.GOOGLE_BOOKS;
                case OPEN_LIBRARY: return ImageSourceName.OPEN_LIBRARY;
                case LONGITOOD: return ImageSourceName.LONGITOOD;
                case S3_CACHE: return ImageSourceName.S3_CACHE;
                case LOCAL_CACHE: return ImageSourceName.LOCAL_CACHE;
                case ANY: // fall-through
                case NONE: // fall-through
                case UNDEFINED: // fall-through
                default: return ImageSourceName.UNKNOWN;
            }
        }
    }
    
    // Helper method to map ImageSourceName back to CoverImageSource (if needed by ImageDetails)
    private CoverImageSource mapImageSourceNameEnumToCoverImageSource(ImageSourceName sourceNameEnum) {
        if (sourceNameEnum == null) return CoverImageSource.UNDEFINED; // Corrected
        try {
            return CoverImageSource.valueOf(sourceNameEnum.name());
        } catch (IllegalArgumentException e) {
             switch (sourceNameEnum) {
                case GOOGLE_BOOKS: return CoverImageSource.GOOGLE_BOOKS;
                case OPEN_LIBRARY: return CoverImageSource.OPEN_LIBRARY;
                case LONGITOOD: return CoverImageSource.LONGITOOD;
                case S3_CACHE: return CoverImageSource.S3_CACHE;
                case LOCAL_CACHE: return CoverImageSource.LOCAL_CACHE;
                // case INTERNAL_PROCESSING: // No direct CoverImageSource equivalent, map to UNDEFINED or a specific one
                // case UNKNOWN: // fall-through
                default: return CoverImageSource.UNDEFINED; // Corrected
            }
        }
    }
}
