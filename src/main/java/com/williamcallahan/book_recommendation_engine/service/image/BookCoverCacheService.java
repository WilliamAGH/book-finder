package com.williamcallahan.book_recommendation_engine.service.image;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import reactor.core.publisher.Mono;

/**
 * Service to download and cache book cover images locally, providing fast initial URLs
 * and then updating with higher quality images in the background.
 * It manages local cache, handles bad URLs, and downloads images asynchronously.
 *
 * Key features:
 * - Local disk caching of cover images.
 * - Fast provisional URL (e.g., from Google Books API, or a provided hint) for immediate display. If a hint is used for a specific ISBN, that URL is attempted first for caching.
 * - Background processing to find the best available cover from multiple sources (OpenLibrary, Longitood, then Google Books API as a final fallback if others fail).
 * - In-memory caches for recently accessed provisional URLs and final local paths.
 * - Mechanism to avoid re-downloading unchanged images using hash comparison.
 * - Fallback to a local placeholder image if no cover can be found.
 */
@Service
public class BookCoverCacheService {

    private static final Logger logger = LoggerFactory.getLogger(BookCoverCacheService.class);
    private static final String LOCAL_PLACEHOLDER_PATH = "/images/book-placeholder.png";
    private static final int MAX_MEMORY_CACHE_SIZE = 1000;

    @Value("${app.cover-cache.enabled:true}")
    private boolean cacheEnabled;
    @Value("${app.cover-cache.dir:/tmp/book-covers}")
    private String cacheDirString;
    @Value("${app.cover-cache.max-age-days:30}")
    private int maxCacheAgeDays;
    @Value("${google.books.api.key:}")
    private String googleBooksApiKey;

    private Path cacheDir;
    private String cacheDirName;
    private final WebClient webClient;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // Injected services for fetching covers
    private final OpenLibraryServiceImpl openLibraryService;
    private final LongitoodServiceImpl longitoodService;
    private final GoogleBooksService googleBooksService;
    private final S3BookCoverService s3BookCoverService;

    private final ConcurrentHashMap<String, String> urlToPathCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> identifierToProvisionalUrlCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> identifierToFinalLocalPathCache = new ConcurrentHashMap<>();
    private final Set<String> knownBadImageUrls = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // private static final String OPEN_LIBRARY_PLACEHOLDER_PATH = "https://openlibrary.org/images/icons/avatar_book-sm.png"; // Unused
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
                                 S3BookCoverService s3BookCoverService) {
        this.webClient = webClientBuilder.build();
        this.openLibraryService = openLibraryService;
        this.longitoodService = longitoodService;
        this.googleBooksService = googleBooksService;
        this.s3BookCoverService = s3BookCoverService;
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

            try {
                Path placeholderPath = Paths.get(GOOGLE_PLACEHOLDER_PATH.substring(1));
                if (Files.exists(placeholderPath)) {
                    googlePlaceholderHash = computeImageHash(Files.readAllBytes(placeholderPath));
                    logger.info("Loaded Google Books placeholder image hash for detection");
                } else {
                    logger.warn("Google Books placeholder image not found at {}, hash-based detection disabled", placeholderPath);
                }
            } catch (Exception e) {
                logger.warn("Failed to load Google Books placeholder image for hash comparison", e);
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
        String extension = ".jpg"; // Default extension
        if (url != null && url.contains(".")) {
            int queryParamIndex = url.indexOf("?");
            String urlWithoutParams = queryParamIndex > 0 ? url.substring(0, queryParamIndex) : url;
            int lastDotIndex = urlWithoutParams.lastIndexOf(".");
            if (lastDotIndex > 0 && lastDotIndex < urlWithoutParams.length() - 1) {
                String ext = urlWithoutParams.substring(lastDotIndex).toLowerCase();
                // Basic validation for common image extensions
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
        // Use a portion of the hash to keep filenames manageable but unique enough
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
        return new ImageDetails(LOCAL_PLACEHOLDER_PATH, "SYSTEM", "placeholder-" + reasonSuffix + "-" + bookId, CoverImageSource.SYSTEM_PLACEHOLDER, ImageResolutionPreference.ORIGINAL);
    }

    private CompletableFuture<ImageDetails> downloadAndCacheImageInternalAsync(String imageUrl, Path destination, String bookIdForLog) {
        if (knownBadImageUrls.contains(imageUrl)) {
            logger.debug("Skipping download for known bad URL: {}", imageUrl);
            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "badurl"));
        }

        String webSafeCachedPath = "/" + cacheDirName + "/" + destination.getFileName().toString();

        return webClient.get().uri(imageUrl).retrieve().bodyToMono(byte[].class)
            .timeout(Duration.ofSeconds(10)) 
            .toFuture()
            .thenCompose(imageBytes -> {
                if (imageBytes == null || imageBytes.length == 0) {
                    logger.warn("Download failed or resulted in empty content for URL: {} (BookID: {}). Adding to known bad URLs.", imageUrl, bookIdForLog);
                    knownBadImageUrls.add(imageUrl);
                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "empty"));
                }

                if (googlePlaceholderHash != null) {
                    try {
                        if (isHashSimilar(googlePlaceholderHash, computeImageHash(imageBytes))) {
                            logger.warn("Downloaded image from {} matches Google placeholder hash (BookID: {}). Adding to known bad URLs.", imageUrl, bookIdForLog);
                            knownBadImageUrls.add(imageUrl);
                            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "googlematch"));
                        }
                    } catch (NoSuchAlgorithmException e) {
                        logger.warn("Could not compute hash for downloaded image from {}: {}", imageUrl, e.getMessage());
                    }
                }
                if (openLibraryPlaceholderHash != null) {
                     try {
                        if (isHashSimilar(openLibraryPlaceholderHash, computeImageHash(imageBytes))) {
                            logger.warn("Downloaded image from {} matches OpenLibrary placeholder hash (BookID: {}). Adding to known bad URLs.", imageUrl, bookIdForLog);
                            knownBadImageUrls.add(imageUrl);
                            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "olmatch"));
                        }
                    } catch (NoSuchAlgorithmException e) {
                        logger.warn("Could not compute hash for downloaded image from {}: {}", imageUrl, e.getMessage());
                    }
                }

                try {
                    Files.write(destination, imageBytes);
                    logger.info("Successfully downloaded and cached image from {} to {} (BookID: {})", imageUrl, destination, bookIdForLog);
                    addToMemoryCache(imageUrl, webSafeCachedPath);
                    try (InputStream byteStream = new ByteArrayInputStream(imageBytes)) {
                        BufferedImage bufferedImage = ImageIO.read(byteStream);
                        if (bufferedImage != null) {
                            return CompletableFuture.completedFuture(new ImageDetails(webSafeCachedPath, "LOCAL_CACHE", "lc-" + bookIdForLog + "-" + destination.getFileName().toString(), CoverImageSource.LOCAL_CACHE, ImageResolutionPreference.ORIGINAL, bufferedImage.getWidth(), bufferedImage.getHeight()));
                        } else {
                            logger.warn("Could not read dimensions for cached image: {}. Still using it.", webSafeCachedPath);
                            return CompletableFuture.completedFuture(new ImageDetails(webSafeCachedPath, "LOCAL_CACHE", "lc-nodim-" + bookIdForLog + "-" + destination.getFileName().toString(), CoverImageSource.LOCAL_CACHE, ImageResolutionPreference.ORIGINAL));
                        }
                    }
                } catch (IOException e) {
                    logger.error("IOException while writing or reading cached image from {}: {}", imageUrl, e.getMessage());
                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "ioex"));
                }
            })
            .exceptionally(ex -> {
                logger.error("Exception during image download/cache for URL {}: {}", imageUrl, ex.getMessage());
                knownBadImageUrls.add(imageUrl); 
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

    public String getInitialCoverUrlAndTriggerBackgroundUpdate(Book book) {
        if (!cacheEnabled) {
            logger.warn("Cache disabled, returning placeholder for book ID: {}", book != null ? book.getId() : "null");
            return getLocalPlaceholderPath();
        }
        if (book == null || (book.getIsbn13() == null && book.getIsbn10() == null && book.getId() == null)) {
            logger.warn("Book or all relevant identifiers (ISBNs, Google Book ID) are null. Cannot process for initial cover URL.");
            return getLocalPlaceholderPath();
        }

        String identifierKey = getIdentifierKey(book);
        if (identifierKey == null) { // Should be caught by above, but as a safeguard
             logger.warn("Could not determine a valid identifierKey for book with ID: {}. Returning placeholder.", book.getId());
            return getLocalPlaceholderPath();
        }


        String finalLocalPath = identifierToFinalLocalPathCache.get(identifierKey);
        if (finalLocalPath != null) {
            logger.debug("Returning final cached path for identifierKey {}: {}", identifierKey, finalLocalPath);
            return finalLocalPath;
        }

        String provisionalUrl = identifierToProvisionalUrlCache.get(identifierKey);
        String urlToReturn;

        if (provisionalUrl != null) {
            logger.debug("Returning provisional cached URL for identifierKey {}: {}", identifierKey, provisionalUrl);
            urlToReturn = provisionalUrl;
        } else {
            // If book has a coverImageUrl already (e.g. from initial Google Books fetch), use it as provisional.
            if (book.getCoverImageUrl() != null && !book.getCoverImageUrl().isEmpty() && !book.getCoverImageUrl().equals(LOCAL_PLACEHOLDER_PATH)) {
                urlToReturn = book.getCoverImageUrl();
                 logger.debug("Using existing coverImageUrl from book object as provisional for identifierKey {}: {}", identifierKey, urlToReturn);
            } else {
                urlToReturn = getLocalPlaceholderPath();
                logger.debug("No provisional URL for identifierKey {}, will use placeholder and process in background.", identifierKey);
            }
            if (identifierToProvisionalUrlCache.size() >= MAX_MEMORY_CACHE_SIZE) identifierToProvisionalUrlCache.clear();
            identifierToProvisionalUrlCache.put(identifierKey, urlToReturn);
        }

        processCoverInBackground(book, urlToReturn.equals(getLocalPlaceholderPath()) ? null : urlToReturn);
        return urlToReturn;
    }

    @Async
    public void processCoverInBackground(Book book, String provisionalUrlHint) {
        if (!cacheEnabled || book == null) return;
        
        String identifierKey = getIdentifierKey(book);
        if (identifierKey == null) {
            logger.warn("Background: Could not determine identifierKey for book with ID: {}. Aborting background processing.", book.getId());
            return;
        }
        logger.info("Background: Starting full cover processing for identifierKey: {}, Book ID: {}, Title: {}", 
            identifierKey, book.getId(), book.getTitle());
        try {
            getCoverImageUrlAsync(book, provisionalUrlHint)
                .thenAccept(imageDetails -> {
                    if (imageDetails != null && imageDetails.getUrlOrPath() != null && !imageDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH)) {
                        if (identifierToFinalLocalPathCache.size() >= MAX_MEMORY_CACHE_SIZE) identifierToFinalLocalPathCache.clear();
                        identifierToFinalLocalPathCache.put(identifierKey, imageDetails.getUrlOrPath());
                        identifierToProvisionalUrlCache.remove(identifierKey);
                    } else {
                        identifierToProvisionalUrlCache.remove(identifierKey); // Remove placeholder if processing failed
                    }
                }).exceptionally(ex -> {
                    logger.error("Background: Exception during full cover processing for identifierKey {}: {}", identifierKey, ex.getMessage(), ex);
                    identifierToProvisionalUrlCache.remove(identifierKey);
                    return null;
                });
        } catch (Exception e) { 
            logger.error("Background: Unexpected error initiating full cover processing for identifierKey {}: {}", identifierKey, e.getMessage(), e); 
            identifierToProvisionalUrlCache.remove(identifierKey); // Clean up provisional on error
        }
    }
    
    private String getIdentifierKey(Book book) {
        if (book == null) return null;
        if (book.getIsbn13() != null && !book.getIsbn13().isEmpty()) return book.getIsbn13();
        if (book.getIsbn10() != null && !book.getIsbn10().isEmpty()) return book.getIsbn10();
        if (book.getId() != null && !book.getId().isEmpty()) return book.getId(); // Google Volume ID
        return null;
    }

    public CompletableFuture<ImageDetails> getCoverImageUrlAsync(Book book, String provisionalUrlHint) {
        String bookIdForLog = book.getId() != null ? book.getId() : (book.getIsbn13() != null ? book.getIsbn13() : "unknown_book");

        if (provisionalUrlHint != null && !provisionalUrlHint.isEmpty() && 
            !provisionalUrlHint.equals(LOCAL_PLACEHOLDER_PATH) && 
            !provisionalUrlHint.startsWith("/" + cacheDirName)) {
            
            Path destinationPath;
            try {
                destinationPath = cacheDir.resolve(generateFilenameFromUrl(provisionalUrlHint));
            } catch (NoSuchAlgorithmException e) {
                 logger.error("CRITICAL: SHA-256 algorithm not found for provisionalUrlHint {}. Falling back to processCoverSources. Book ID for log: {}", provisionalUrlHint, bookIdForLog, e);
                return processCoverSources(book); 
            }
            // If provisional hint is a remote URL, try to download and cache it first.
            return downloadAndCacheImageInternalAsync(provisionalUrlHint, destinationPath, bookIdForLog)
                .thenCompose(imageDetails -> {
                    // If successfully cached from hint and dimensions are valid, use it. Otherwise, proceed to full source check.
                    if (imageDetails.getWidth() > 0 && imageDetails.getHeight() > 0) {
                        return CompletableFuture.completedFuture(imageDetails);
                    }
                    logger.warn("Provisional URL hint {} did not yield a valid cached image for Book ID for log: {}. Proceeding to full source scan.", provisionalUrlHint, bookIdForLog);
                    return processCoverSources(book);
                })
                .exceptionally(ex -> {
                    logger.error("Exception processing provisionalUrlHint {} for Book ID for log: {}. Falling back to processCoverSources. Error: {}", provisionalUrlHint, bookIdForLog, ex.getMessage());
                    return processCoverSources(book).getNow(createPlaceholderImageDetails(bookIdForLog, "fallback-provisional-ex"));
                });
        }
        return processCoverSources(book);
    }

    private CompletableFuture<ImageDetails> processCoverSources(Book book) {
        String googleVolumeId = book.getId(); // This is the Google Books Volume ID
        String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();
        String bookIdForLog = googleVolumeId != null ? googleVolumeId : (isbn != null ? isbn : "unknown_book_for_log");

        // Attempt S3 first, as it might contain already processed covers from any source.
        // S3BookCoverService uses book.getId() (Google Volume ID) if available, then ISBN.
        return tryS3(book, bookIdForLog)
            .thenCompose(detailsS3 -> {
                if (detailsS3.getWidth() > 0) return CompletableFuture.completedFuture(detailsS3);
                
                // If S3 fails or no image, proceed based on available identifiers
                if (isbn != null && !isbn.isEmpty()) {
                    logger.debug("S3 failed for Book ID {}. ISBN {} is available. Trying Google Books API (by ISBN), then OpenLibrary, then Longitood.", bookIdForLog, isbn);
                    // Existing ISBN-based flow
                    return tryGoogleBooksApiByIsbn(isbn, bookIdForLog)
                        .thenCompose(detailsGoogleIsbn -> {
                            if (detailsGoogleIsbn.getWidth() > 0) return CompletableFuture.completedFuture(detailsGoogleIsbn);
                            logger.debug("Google Books API (by ISBN) failed for ISBN {}. Trying OpenLibrary.", isbn);
                            return tryOpenLibrary(isbn, bookIdForLog, "L");
                        })
                        .thenCompose(detailsL -> {
                            if (detailsL.getWidth() > 0) return CompletableFuture.completedFuture(detailsL);
                            return tryOpenLibrary(isbn, bookIdForLog, "M");
                        })
                        .thenCompose(detailsM -> {
                            if (detailsM.getWidth() > 0) return CompletableFuture.completedFuture(detailsM);
                            return tryOpenLibrary(isbn, bookIdForLog, "S");
                        })
                        .thenCompose(detailsS -> {
                            if (detailsS.getWidth() > 0) return CompletableFuture.completedFuture(detailsS);
                            logger.warn("All S3, Google (by ISBN), and OpenLibrary attempts failed for ISBN {}. Trying Longitood.", isbn);
                            return longitoodService.fetchCover(book).toFuture() // longitoodService.fetchCover uses ISBN from Book object
                                .thenCompose(longitoodDetails -> {
                                    if (longitoodDetails != null && longitoodDetails.getUrlOrPath() != null && !longitoodDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH) && !knownBadImageUrls.contains(longitoodDetails.getUrlOrPath())) {
                                        Path dest; 
                                        try { 
                                            dest = cacheDir.resolve(generateFilenameFromUrl(longitoodDetails.getUrlOrPath())); 
                                        } catch (NoSuchAlgorithmException e) { 
                                            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "hash-error-longitood")); 
                                        }
                                        return downloadAndCacheImageInternalAsync(longitoodDetails.getUrlOrPath(), dest, bookIdForLog);
                                    }
                                    logger.warn("Longitood also failed for ISBN {}. Returning placeholder.", isbn);
                                    addKnownBadLongitoodIsbn(isbn); 
                                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "longitood-failed"));
                                })
                                .exceptionally(exL -> {
                                    logger.error("Exception with Longitood for ISBN {}: {}. Returning placeholder.", isbn, exL.getMessage());
                                    addKnownBadLongitoodIsbn(isbn); 
                                    return createPlaceholderImageDetails(bookIdForLog, "longitood-exception");
                                });
                        });
                } else if (googleVolumeId != null && !googleVolumeId.isEmpty()) {
                    // ISBN is not available, but Google Volume ID is.
                    logger.debug("S3 failed for Book ID {}. No ISBN available. Trying Google Books API directly by Google Volume ID {}.", bookIdForLog, googleVolumeId);
                    return tryGoogleBooksApiByVolumeId(googleVolumeId, bookIdForLog)
                         .thenCompose(detailsGoogleId -> {
                            if (detailsGoogleId.getWidth() > 0) return CompletableFuture.completedFuture(detailsGoogleId);
                            logger.warn("Google Books API (by Volume ID {}) also failed for Book ID {}. No other sources to try without ISBN. Returning placeholder.", googleVolumeId, bookIdForLog);
                            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "google-id-failed"));
                        });
                } else {
                    // No ISBN and no Google Volume ID. Should not happen if initial checks are done.
                    logger.warn("No usable identifier (ISBN or Google Volume ID) for Book ID {}. Returning placeholder.", bookIdForLog);
                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "no-identifier"));
                }
            })
            .exceptionally(ex -> { 
                 logger.error("All cover sources failed for Book ID {}: {}", bookIdForLog, ex.getMessage());
                 return createPlaceholderImageDetails(bookIdForLog, "all-sources-failed-ex");
            });
    }

    private CompletableFuture<ImageDetails> tryS3(Book book, String bookIdForLog) { // bookIdForLog is for logging, S3 service uses book.getId() or ISBN
        logger.debug("Trying S3 for Book ID {}", bookIdForLog);
        // S3BookCoverService.fetchCover internally prefers book.getId() (Google Volume ID) then ISBNs.
        return s3BookCoverService.fetchCover(book).toFuture() 
            .thenCompose(s3Details -> {
                if (s3Details != null && s3Details.getUrlOrPath() != null && !s3Details.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH)) {
                    Path dest;
                    try {
                        dest = cacheDir.resolve(generateFilenameFromUrl(s3Details.getUrlOrPath()));
                    } catch (NoSuchAlgorithmException e) {
                        logger.error("CRITICAL: SHA-256 algorithm not found for S3 URL {} (Book ID for log: {}). Error: {}", s3Details.getUrlOrPath(), bookIdForLog, e.getMessage());
                        return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "hash-error-s3"));
                    }
                    if (urlToPathCache.containsKey(s3Details.getUrlOrPath())) {
                        String localPath = urlToPathCache.get(s3Details.getUrlOrPath());
                        logger.debug("S3 URL {} already in local cache at {} (Book ID for log: {})", s3Details.getUrlOrPath(), localPath, bookIdForLog);
                        // Construct ImageDetails with correct source if known it's from local cache referencing S3
                        return CompletableFuture.completedFuture(new ImageDetails(localPath, "LOCAL_CACHE_OF_S3", "lc-s3ref-" + bookIdForLog, CoverImageSource.LOCAL_CACHE, ImageResolutionPreference.ORIGINAL)); 
                    }
                    // If not in local URL-to-path cache, download it from S3 URL to local cache
                    return downloadAndCacheImageInternalAsync(s3Details.getUrlOrPath(), dest, bookIdForLog)
                        .thenApply(cachedFromS3 -> { // Ensure the source reflects it was from S3 then cached locally
                            if (cachedFromS3.getWidth() > 0) {
                                return new ImageDetails(cachedFromS3.getUrlOrPath(), CoverImageSource.S3_CACHE.toString(), cachedFromS3.getSourceSystemId(), CoverImageSource.S3_CACHE, cachedFromS3.getResolutionPreference(), cachedFromS3.getWidth(), cachedFromS3.getHeight());
                            }
                            return cachedFromS3; // Return placeholder if download failed
                        });
                }
                logger.debug("S3 did not provide a valid remote URL for Book ID {}. Using placeholder.", bookIdForLog);
                return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "s3-no-url"));
            })
            .exceptionally(ex -> {
                logger.error("Exception calling S3 or processing its result for Book ID {}: {}", bookIdForLog, ex.getMessage());
                return createPlaceholderImageDetails(bookIdForLog, "s3-exception");
            });
    }

    private CompletableFuture<ImageDetails> tryOpenLibrary(String isbn, String bookId, String sizeSuffix) {
        if (isKnownBadOpenLibraryIsbn(isbn)) {
            return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookId, "known-bad-ol"));
        }

        return this.openLibraryService.fetchOpenLibraryCoverDetails(isbn, sizeSuffix)
            .toFuture()
            .thenCompose(remoteImageDetails -> {
                if (remoteImageDetails == null || remoteImageDetails.getUrlOrPath() == null || remoteImageDetails.getUrlOrPath().isEmpty() || remoteImageDetails.getUrlOrPath().equals(LOCAL_PLACEHOLDER_PATH)) {
                    logger.warn("OpenLibraryService did not provide a valid remote URL for ISBN {} size {}. Using placeholder.", isbn, sizeSuffix);
                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookId, "ol-" + sizeSuffix + "-no-url"));
                }

                String openLibraryUrl = remoteImageDetails.getUrlOrPath();
                Path destinationPath;
                try {
                    destinationPath = cacheDir.resolve(generateFilenameFromUrl(openLibraryUrl));
                } catch (NoSuchAlgorithmException e) {
                    logger.error("CRITICAL: SHA-256 algorithm not found for OpenLibrary URL ({} from service): {}. Error: {}", sizeSuffix, openLibraryUrl, e.getMessage());
                    addKnownBadOpenLibraryIsbn(isbn); 
                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookId, "hash-error-ol"));
                }

                return downloadAndCacheImageInternalAsync(openLibraryUrl, destinationPath, bookId)
                    .thenApply(cachedImageDetails -> {
                        if (cachedImageDetails.getWidth() > 0) {
                            return cachedImageDetails;
                        }
                        logger.warn("Download/cache failed for OpenLibrary URL ({} from service): {} for ISBN {}, size {}. Resulted in placeholder or invalid image.", sizeSuffix, openLibraryUrl, isbn, sizeSuffix);
                        addKnownBadOpenLibraryIsbn(isbn);
                        return createPlaceholderImageDetails(bookId, "ol-" + sizeSuffix + "-failed-dl");
                    });
            })
            .exceptionally(ex -> {
                logger.error("Exception calling OpenLibraryService or processing its result for ISBN {}, size {}: {}", isbn, sizeSuffix, ex.getMessage());
                addKnownBadOpenLibraryIsbn(isbn); 
                return createPlaceholderImageDetails(bookId, "ol-" + sizeSuffix + "-exception");
            });
    }

    private CompletableFuture<ImageDetails> tryGoogleBooksApiByIsbn(String isbn, String bookIdForLog) {
        logger.debug("Attempting Google Books API fallback by ISBN {} (Book ID for log: {})", isbn, bookIdForLog);
        try {
            // .block() is used here as it's in a chain of CompletableFutures. Consider alternatives if this causes issues.
            List<com.williamcallahan.book_recommendation_engine.model.Book> books = googleBooksService.searchBooksByISBN(isbn).block(); 
            if (books != null && !books.isEmpty()) {
                com.williamcallahan.book_recommendation_engine.model.Book gBook = books.get(0);
                if (gBook != null && gBook.getCoverImageUrl() != null && !gBook.getCoverImageUrl().isEmpty() &&
                    !knownBadImageUrls.contains(gBook.getCoverImageUrl()) &&
                    !gBook.getCoverImageUrl().contains("image-not-available.png")) { 
                    Path destinationPath;
                    try {
                        destinationPath = cacheDir.resolve(generateFilenameFromUrl(gBook.getCoverImageUrl()));
                    } catch (NoSuchAlgorithmException e) {
                        return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "hash-error-google-isbn"));
                    }
                    return downloadAndCacheImageInternalAsync(gBook.getCoverImageUrl(), destinationPath, bookIdForLog);
                }
            }
        } catch (Exception e) {
            logger.error("Exception during Google Books API fallback by ISBN {} (Book ID for log: {}): {}", isbn, bookIdForLog, e.getMessage());
        }
        return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "google-isbn-failed"));
    }

    private CompletableFuture<ImageDetails> tryGoogleBooksApiByVolumeId(String googleVolumeId, String bookIdForLog) {
        logger.debug("Attempting Google Books API directly by Google Volume ID {} (Book ID for log: {})", googleVolumeId, bookIdForLog);
        try {
            Mono<com.williamcallahan.book_recommendation_engine.model.Book> bookMono = googleBooksService.getBookById(googleVolumeId);
            // .block() is used here. As above, consider alternatives if problematic.
            com.williamcallahan.book_recommendation_engine.model.Book gBook = bookMono.block(); 
            
            if (gBook != null && gBook.getCoverImageUrl() != null && !gBook.getCoverImageUrl().isEmpty() &&
                !knownBadImageUrls.contains(gBook.getCoverImageUrl()) &&
                !gBook.getCoverImageUrl().contains("image-not-available.png")) {
                Path destinationPath;
                try {
                    destinationPath = cacheDir.resolve(generateFilenameFromUrl(gBook.getCoverImageUrl()));
                } catch (NoSuchAlgorithmException e) {
                    return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "hash-error-google-volid"));
                }
                return downloadAndCacheImageInternalAsync(gBook.getCoverImageUrl(), destinationPath, bookIdForLog);
            }
        } catch (Exception e) {
            logger.error("Exception during Google Books API call by Volume ID {} (Book ID for log: {}): {}", googleVolumeId, bookIdForLog, e.getMessage());
        }
        return CompletableFuture.completedFuture(createPlaceholderImageDetails(bookIdForLog, "google-volid-failed"));
    }

    // Removed unused inner static classes: GoogleBooksApiResponse, GoogleBookItem, VolumeInfo, ImageLinks
}
