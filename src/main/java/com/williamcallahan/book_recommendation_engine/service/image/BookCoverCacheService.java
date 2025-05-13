package com.williamcallahan.book_recommendation_engine.service.image;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import jakarta.annotation.PreDestroy;

/**
 * Service to download and cache book cover images locally
 * to prevent "disappearing covers" issue when remote services are unavailable.
 */
@Service
public class BookCoverCacheService {
    private static final Logger logger = LoggerFactory.getLogger(BookCoverCacheService.class);
    
    @Value("${app.cover-cache.enabled:true}")
    private boolean cacheEnabled;
    
    @Value("${app.cover-cache.dir:covers}")
    private String cacheDirName;
    
    @Value("${app.cover-cache.max-age-days:30}")
    private int maxAgeDays;
    
    @Value("${app.cover-cache.cleanup-interval-hours:24}")
    private int cleanupIntervalHours;
    
    private Path cacheDir;
    private final WebClient webClient;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    // In-memory cache to avoid hitting the disk for recent lookups
    private final ConcurrentHashMap<String, String> urlToPathCache = new ConcurrentHashMap<>();
    private static final int MAX_MEMORY_CACHE_SIZE = 200;
    private final Set<String> knownBadImageUrls = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private static final String LOCAL_PLACEHOLDER_PATH = "/images/placeholder-book-cover.svg";
    private static final int MIN_IMAGE_SIZE_BYTES = 2048; // Threshold to detect placeholder or empty images
    private static final String GOOGLE_PLACEHOLDER_PATH = "/images/image-not-available.png";
    private byte[] googlePlaceholderHash;
    
    // Cache for OpenLibrary 404 responses to avoid repeated requests
    private final Set<String> knownBadOpenLibraryIsbns = Collections.newSetFromMap(new ConcurrentHashMap<>());
    
    // Cache for Longitood bad ISBNs to avoid repeated requests
    private final Set<String> knownBadLongitoodIsbns = Collections.newSetFromMap(new ConcurrentHashMap<>());
    
    /**
     * Check if an ISBN is known to have no cover in OpenLibrary
     * @param isbn The ISBN to check
     * @return true if this ISBN is known to have no cover in OpenLibrary
     */
    public boolean isKnownBadOpenLibraryIsbn(String isbn) {
        return isbn != null && knownBadOpenLibraryIsbns.contains(isbn);
    }
    
    /**
     * Add an ISBN to the list of ISBNs known to have no cover in OpenLibrary
     * @param isbn The ISBN to add
     */
    public void addBadOpenLibraryIsbn(String isbn) {
        if (isbn != null && !isbn.isEmpty()) {
            logger.debug("Adding ISBN {} to known bad OpenLibrary ISBNs list", isbn);
            knownBadOpenLibraryIsbns.add(isbn);
        }
    }
    
    /**
     * Check if an ISBN is known to have no cover in Longitood
     * @param isbn The ISBN to check
     * @return true if this ISBN is known to have no cover in Longitood
     */
    public boolean isKnownBadLongitoodIsbn(String isbn) {
        return isbn != null && knownBadLongitoodIsbns.contains(isbn);
    }
    
    /**
     * Add an ISBN to the list of ISBNs known to have no cover in Longitood
     * @param isbn The ISBN to add
     */
    public void addBadLongitoodIsbn(String isbn) {
        if (isbn != null && !isbn.isEmpty()) {
            logger.debug("Adding ISBN {} to known bad Longitood ISBNs list", isbn);
            knownBadLongitoodIsbns.add(isbn);
        }
    }
    
    /**
     * Get the path to the local placeholder image
     * @return The path to the local placeholder image
     */
    public String getLocalPlaceholderPath() {
        return LOCAL_PLACEHOLDER_PATH;
    }
    
    @Autowired
    public BookCoverCacheService(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }
    
    @PostConstruct
    public void init() {
        if (!cacheEnabled) {
            logger.info("Book cover caching is disabled");
            return;
        }
        
        try {
            // Create cache directory inside static resources
            Path staticDir = Paths.get("src/main/resources/static");
            cacheDir = staticDir.resolve(cacheDirName);
            
            if (!Files.exists(cacheDir)) {
                Files.createDirectories(cacheDir);
                logger.info("Created book cover cache directory: {}", cacheDir);
            } else {
                logger.info("Using existing book cover cache directory: {}", cacheDir);
            }
            
            // Create a public accessible directory for the cached images
            Path publicCacheDir = staticDir.resolve(cacheDirName);
            if (!Files.exists(publicCacheDir)) {
                Files.createDirectories(publicCacheDir);
            }
            
            // Load Google Books placeholder image for hash comparison
            try {
                Path placeholderPath = staticDir.resolve(GOOGLE_PLACEHOLDER_PATH.substring(1)); // Remove leading slash
                if (Files.exists(placeholderPath)) {
                    googlePlaceholderHash = computeImageHash(Files.readAllBytes(placeholderPath));
                    logger.info("Loaded Google Books placeholder image hash for detection");
                } else {
                    logger.warn("Google Books placeholder image not found at {}, hash-based detection disabled", placeholderPath);
                }
            } catch (Exception e) {
                logger.warn("Failed to load Google Books placeholder image for hash comparison", e);
            }
            
            // Schedule cache cleanup
            scheduler.scheduleAtFixedRate(
                this::cleanupOldCachedCovers, 
                cleanupIntervalHours, 
                cleanupIntervalHours, 
                TimeUnit.HOURS
            );
            
        } catch (IOException e) {
            logger.error("Failed to create book cover cache directory", e);
            cacheEnabled = false;
        }
    }

    @PreDestroy
    public void destroy() {
        logger.info("Shutting down BookCoverCacheService executors...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
                logger.warn("Scheduled executor did not terminate in time, forcing shutdown.");
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for executors to terminate", e);
            if (scheduler != null) scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("BookCoverCacheService executors shut down.");
    }
    
    /**
     * Asynchronously get a book cover URL using CompletableFuture.
     * This method implements a promise-based approach to book cover images:
     * 1. First, check if a high-resolution version exists in S3
     * 2. If not in S3, check local cache
     * 3. If not in local cache, initiate parallel downloads from multiple sources
     * 4. Return the first successful result from any source
     * 
     * @param originalUrl The original remote URL of the book cover
     * @param bookId The ID of the book (used for logging)
     * @return CompletableFuture with a URL that points to the cached/S3 image or the original URL if all attempts fail
     */
    public CompletableFuture<ImageDetails> getCachedCoverUrlAsync(String originalUrl, String bookId) {
        if (!cacheEnabled || originalUrl == null || originalUrl.isEmpty()) {
            return CompletableFuture.completedFuture(new ImageDetails(originalUrl));
        }
        
        if (originalUrl.startsWith("/") && !originalUrl.startsWith("//")) {
            // If it's already a local path, try to get dimensions if it's a cache path
            if (originalUrl.startsWith("/" + cacheDirName + "/")) {
                Path localFile = cacheDir.resolve(originalUrl.substring(cacheDirName.length() + 2)); // +2 for the two slashes
                if (Files.exists(localFile)) {
                    try (InputStream diskIs = Files.newInputStream(localFile)) {
                        BufferedImage img = ImageIO.read(diskIs);
                        if (img != null) {
                            logger.debug("Local path {} dimensions: {}x{}", originalUrl, img.getWidth(), img.getHeight());
                            return CompletableFuture.completedFuture(new ImageDetails(originalUrl, img.getWidth(), img.getHeight(), true));
                        }
                    } catch (IOException e) {
                        logger.warn("Could not read dimensions for existing local file {}: {}", originalUrl, e.getMessage());
                    }
                }
            }
            return CompletableFuture.completedFuture(new ImageDetails(originalUrl));
        }
        
        if (knownBadImageUrls.contains(originalUrl)) {
            logger.warn("Returning local placeholder for known bad image URL for book {}: {}", bookId, originalUrl);
            return CompletableFuture.completedFuture(new ImageDetails(LOCAL_PLACEHOLDER_PATH));
        }
        
        // 1. Check in-memory cache (Note: this cache only stores path, not dimensions directly)
        // For a more robust solution, memory cache could store ImageDetails if populated from a disk read.
        // For now, if found in memory, we'll assume dimensions might need to be re-checked or are unknown.
        if (urlToPathCache.containsKey(originalUrl)) {
            String cachedPath = urlToPathCache.get(originalUrl);
            logger.debug("Using memory-cached cover path for book {}: {}. Dimensions might be re-checked.", bookId, cachedPath);
             // Attempt to read dimensions from disk if it's a valid cache path
            Path localFileFromMemory = cacheDir.resolve(cachedPath.substring(cacheDirName.length() + 2));
            if (Files.exists(localFileFromMemory)) {
                try (InputStream diskIs = Files.newInputStream(localFileFromMemory)) {
                    BufferedImage img = ImageIO.read(diskIs);
                    if (img != null) {
                        return CompletableFuture.completedFuture(new ImageDetails(cachedPath, img.getWidth(), img.getHeight(), true));
                    }
                } catch (IOException e) {
                    logger.warn("Could not read dimensions for memory-cached file {}: {}", cachedPath, e.getMessage());
                }
            }
            return CompletableFuture.completedFuture(new ImageDetails(cachedPath)); // Dimensions unknown from memory alone
        }
        
        // 2. Check local file system cache
        String filename;
        try {
            filename = generateFilenameFromUrl(originalUrl);
        } catch (NoSuchAlgorithmException e) {
            logger.error("CRITICAL: SHA-256 algorithm not found while generating filename for URL: {}", originalUrl, e);
            throw new IllegalStateException("Required cryptographic algorithm SHA-256 not available", e);
        }
        String cachedPath = "/" + cacheDirName + "/" + filename;
        Path localFile = cacheDir.resolve(filename);
        
        if (Files.exists(localFile)) {
            addToMemoryCache(originalUrl, cachedPath); // Add to memory cache if found on disk
            logger.debug("Using disk-cached cover for book {}: {}", bookId, cachedPath);
            try (InputStream diskIs = Files.newInputStream(localFile)) {
                BufferedImage img = ImageIO.read(diskIs);
                if (img != null) {
                    return CompletableFuture.completedFuture(new ImageDetails(cachedPath, img.getWidth(), img.getHeight(), true));
                } else {
                     logger.warn("Could not read image from disk cache file: {}", localFile);
                    return CompletableFuture.completedFuture(new ImageDetails(cachedPath)); // Path known, dimensions not
                }
            } catch (IOException e) {
                logger.warn("IOException reading disk cached file {} for book {}: {}", localFile, bookId, e.getMessage());
                return CompletableFuture.completedFuture(new ImageDetails(cachedPath)); // Path known, dimensions not
            }
        }
        
        // 3. If not in local cache, download it asynchronously
        logger.debug("Cover not in local cache for book {}: {}. Attempting download to cache.", bookId, originalUrl);
        return downloadAndCacheImageInternalAsync(originalUrl, localFile, bookId)
            .thenApply(imageDetails -> {
                if (imageDetails != null && imageDetails.areDimensionsKnown()) {
                    addToMemoryCache(originalUrl, imageDetails.getUrlOrPath());
                    logger.info("Successfully downloaded and locally cached cover for book {}: {} -> {}",
                               bookId, originalUrl, imageDetails.getUrlOrPath());
                    return imageDetails;
                } else {
                    logger.warn("Failed to download to local cache for book {}: {}. Returning original URL as fallback.", bookId, originalUrl);
                    return new ImageDetails(originalUrl); 
                }
            })
            .exceptionally(ex -> {
                 logger.error("Exception during local cache download for book {}: {}. Error: {}", bookId, originalUrl, ex.getMessage());
                 return new ImageDetails(originalUrl); // Fallback on exception
            });
    }
    
    /**
     * Initiate a download attempt for a book cover to the local cache.
     * This method will try to download from the original URL.
     * 
     * @param originalUrl The original remote URL of the book cover
     * @param bookId The ID of the book (used for logging)
     * @return CompletableFuture with ImageDetails containing local cache path and dimensions, or details with original URL if attempt fails
     */
    public CompletableFuture<ImageDetails> initiateLocalCacheDownloadAsync(String originalUrl, String bookId) {
        if (!cacheEnabled || originalUrl == null || originalUrl.isEmpty() ||
            (originalUrl.startsWith("/") && !originalUrl.startsWith("//"))) {
            return CompletableFuture.completedFuture(new ImageDetails(originalUrl));
        }
        
        if (knownBadImageUrls.contains(originalUrl)) {
            logger.debug("Skipping local cache download for known bad URL: {}", originalUrl);
            return CompletableFuture.completedFuture(new ImageDetails(LOCAL_PLACEHOLDER_PATH)); 
        }
        
        final String filename;
        try {
            filename = generateFilenameFromUrl(originalUrl);
        } catch (NoSuchAlgorithmException e) {
            logger.error("CRITICAL: SHA-256 algorithm not found while generating filename for URL: {}", originalUrl, e);
            throw new IllegalStateException("Required cryptographic algorithm SHA-256 not available", e);
        }
        final String cachedPath = "/" + cacheDirName + "/" + filename;
        final Path localFile = cacheDir.resolve(filename);
        
        if (Files.exists(localFile)) {
            addToMemoryCache(originalUrl, cachedPath);
            logger.debug("Image already in local disk cache for book {}: {}", bookId, cachedPath);
            try (InputStream diskIs = Files.newInputStream(localFile)) {
                BufferedImage img = ImageIO.read(diskIs);
                if (img != null) {
                    return CompletableFuture.completedFuture(new ImageDetails(cachedPath, img.getWidth(), img.getHeight(), true));
                } else {
                    logger.warn("Could not read image from disk cache file on initiate: {}", localFile);
                    // Mark as bad if unreadable from disk? Or just return unknown dimensions?
                    // For now, return path with unknown dimensions. Orchestration service might try to re-fetch.
                    return CompletableFuture.completedFuture(new ImageDetails(cachedPath));
                }
            } catch (IOException e) {
                logger.warn("IOException reading disk cached file {} for book {} on initiate: {}", localFile, bookId, e.getMessage());
                return CompletableFuture.completedFuture(new ImageDetails(cachedPath));
            }
        }

        return downloadAndCacheImageInternalAsync(originalUrl, localFile, bookId)
            .thenApply(imageDetails -> {
                if (imageDetails != null && imageDetails.areDimensionsKnown()) {
                    addToMemoryCache(originalUrl, imageDetails.getUrlOrPath());
                    logger.info("Successfully downloaded and cached locally for book {}: {} -> {} ({}x{})",
                               bookId, originalUrl, imageDetails.getUrlOrPath(), imageDetails.getWidth(), imageDetails.getHeight());
                    return imageDetails;
                }
                // If download failed or dimensions unknown, return ImageDetails with original URL and unknown dimensions
                return new ImageDetails(originalUrl);
            });
    }
    
    /**
     * Add a URL->Path mapping to the in-memory cache, evicting old entries if needed
     */
    private void addToMemoryCache(String url, String cachedPath) {
        if (urlToPathCache.size() >= MAX_MEMORY_CACHE_SIZE) {
            // Simple eviction: just remove a random entry when full
            if (!urlToPathCache.isEmpty()) {
                String keyToRemove = urlToPathCache.keys().nextElement();
                urlToPathCache.remove(keyToRemove);
            }
        }
        urlToPathCache.put(url, cachedPath);
    }
    
    
    /**
     * Generate a filename from a URL that is safe for file systems
     */
    private String generateFilenameFromUrl(String url) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(url.getBytes());
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        String extension = getFileExtensionFromUrl(url);
        return encoded.substring(0, Math.min(encoded.length(), 16)) + extension;
    }
    
    /**
     * Extract file extension from URL
     * @param url The URL
     * @return The file extension (with dot)
     */
    private String getFileExtensionFromUrl(String url) {
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
     * Delete cached covers older than the configured max age
     */
    private void cleanupOldCachedCovers() {
        if (!cacheEnabled) return;
        
        try {
            logger.info("Starting cleanup of old cached book covers");
            long cutoffTime = System.currentTimeMillis() - (maxAgeDays * 24L * 60L * 60L * 1000L);
            
            Files.list(cacheDir)
                .filter(Files::isRegularFile)
                .filter(p -> {
                    try {
                        return Files.getLastModifiedTime(p).toMillis() < cutoffTime;
                    } catch (IOException e) {
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
            
            logger.info("Completed cleanup of old cached book covers");
        } catch (IOException e) {
            logger.error("Error during cleanup of cached book covers", e);
        }
    }
    
    /**
     * Manually clear the entire cache
     */
    public void clearCache() {
        if (!cacheEnabled) return;
        
        try {
            Files.list(cacheDir)
                .filter(Files::isRegularFile)
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                        logger.warn("Failed to delete cached cover during clear: {}", p.getFileName(), e);
                    }
                });
            
            urlToPathCache.clear();
            knownBadImageUrls.clear(); // Also clear the set of known bad URLs
            logger.info("Book cover cache cleared (including known bad URLs)");
        } catch (IOException e) {
            logger.error("Error clearing book cover cache", e);
        }
    }
    
    /**
     * Compute a simple hash of an image for comparison purposes
     * This is a basic implementation that could be improved with a proper perceptual hash algorithm
     * 
     * @param imageData The raw image bytes
     * @return A hash value as byte array
     * @throws NoSuchAlgorithmException If the hashing algorithm is not available
     */
    private byte[] computeImageHash(byte[] imageData) throws NoSuchAlgorithmException {
        // For now, we'll use a simple SHA-256 hash of the image data
        // A more sophisticated approach would use a perceptual hash algorithm
        // that's resistant to small changes in the image
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return digest.digest(imageData);
    }
    
    /**
     * Compare two image hashes to determine if they are similar
     * For SHA-256 hashes, we're looking for exact matches
     * For a perceptual hash, we would calculate a Hamming distance
     * and return true if below a threshold
     * 
     * @param hash1 First hash
     * @param hash2 Second hash
     * @return True if the hashes are similar enough to be considered the same image
     */
    private boolean isHashSimilar(byte[] hash1, byte[] hash2) {
        // For SHA-256, we're looking for exact matches
        // With a perceptual hash, we would calculate a Hamming distance
        // and return true if below a threshold
        if (hash1 == null || hash2 == null || hash1.length != hash2.length) {
            return false;
        }
        
        // Compare the hashes byte by byte
        for (int i = 0; i < hash1.length; i++) {
            if (hash1[i] != hash2[i]) {
                return false;
            }
        }
        
        return true;
    }

    private CompletableFuture<ImageDetails> downloadAndCacheImageInternalAsync(String imageUrl, Path destination, String bookIdForLog) {
        if (knownBadImageUrls.contains(imageUrl)) {
            logger.debug("Skipping download for known bad image URL for book {}: {}", bookIdForLog, imageUrl);
            return CompletableFuture.completedFuture(new ImageDetails(LOCAL_PLACEHOLDER_PATH));
        }
        
        String webSafeCachedPath = "/" + cacheDirName + "/" + destination.getFileName().toString();

        logger.debug("Attempting to download and cache image for book {}: {}", bookIdForLog, imageUrl);
        return webClient.get()
            .uri(imageUrl)
            .retrieve()
            .bodyToMono(byte[].class)
            .timeout(Duration.ofSeconds(10))
            .toFuture()
            .thenCompose(imageBytes -> {
                if (imageBytes == null) {
                    knownBadImageUrls.add(imageUrl);
                    logger.warn("Null image data received for URL {}, marking as bad.", imageUrl);
                    return CompletableFuture.completedFuture(new ImageDetails(imageUrl)); // Return original URL, dimensions unknown
                }
                if (imageUrl.contains("image-not-available.png")) {
                    knownBadImageUrls.add(imageUrl);
                    logger.warn("Detected 'image-not-available.png' in URL {}, marking as bad.", imageUrl);
                    return CompletableFuture.completedFuture(new ImageDetails(LOCAL_PLACEHOLDER_PATH));
                }
                if (imageBytes.length < MIN_IMAGE_SIZE_BYTES) {
                    knownBadImageUrls.add(imageUrl);
                    logger.warn("Downloaded image size {} for URL {} below threshold, marking as bad.",
                                imageBytes.length, imageUrl);
                    return CompletableFuture.completedFuture(new ImageDetails(LOCAL_PLACEHOLDER_PATH));
                }
                if (googlePlaceholderHash != null) {
                    try {
                        byte[] downloadedImageHash = computeImageHash(imageBytes);
                        if (isHashSimilar(googlePlaceholderHash, downloadedImageHash)) {
                            knownBadImageUrls.add(imageUrl);
                            logger.warn("Detected Google Books placeholder image by hash comparison for URL {}, marking as bad.", imageUrl);
                            return CompletableFuture.completedFuture(new ImageDetails(LOCAL_PLACEHOLDER_PATH));
                        }
                    } catch (NoSuchAlgorithmException e) {
                        logger.debug("Failed to compute hash for downloaded image (NoSuchAlgorithmException): {}", e.getMessage());
                    } catch (Exception e) {
                        logger.warn("Unexpected error during hash computation/comparison for URL {}: {}", imageUrl, e.getMessage());
                    }
                }
                try {
                    Files.write(destination, imageBytes);
                    logger.debug("Successfully wrote image to {}", destination);
                    // Now read dimensions
                    try (InputStream byteStream = new ByteArrayInputStream(imageBytes)) {
                        BufferedImage bufferedImage = ImageIO.read(byteStream);
                        if (bufferedImage != null) {
                            return CompletableFuture.completedFuture(new ImageDetails(webSafeCachedPath, bufferedImage.getWidth(), bufferedImage.getHeight(), true));
                        } else {
                            logger.warn("Could not read dimensions from downloaded image for URL: {}", imageUrl);
                            return CompletableFuture.completedFuture(new ImageDetails(webSafeCachedPath)); // Dimensions unknown
                        }
                    }
                } catch (IOException e) {
                    logger.error("Failed to write image to cache for book {} from URL {}: {}", bookIdForLog, imageUrl, e.getMessage());
                    return CompletableFuture.completedFuture(new ImageDetails(imageUrl)); // Fallback to original URL
                }
            })
            .exceptionally(ex -> {
                logger.warn("Exception in reactive chain for image URL {}: {}", imageUrl, ex.getClass().getSimpleName(), ex);
                if (ex instanceof WebClientResponseException) {
                    WebClientResponseException wcre = (WebClientResponseException) ex;
                    logger.warn("Failed to download image from URL {} (WebClientResponseException). HTTP Status: {} {}. Response body: '{}'",
                                imageUrl, wcre.getStatusCode(), wcre.getStatusText(), wcre.getResponseBodyAsString());
                } else if (ex instanceof IOException) { 
                    logger.error("IOException during image download/processing for URL {}: {}", imageUrl, ex.getMessage());
                } else {
                    logger.error("Generic error downloading/processing image for URL {}: {}", imageUrl, ex.getMessage());
                }
                knownBadImageUrls.add(imageUrl);
                return new ImageDetails(LOCAL_PLACEHOLDER_PATH); // Fallback to placeholder on any exception during download
            });
    }
}
