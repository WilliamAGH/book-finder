package com.williamcallahan.book_recommendation_engine.service.image;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Manages various Caffeine caches for book cover images
 * - Provides centralized configuration and access to in-memory caches
 * - Handles caches for URL to path mappings, provisional URLs, final image details, and known bad URLs/ISBNs
 *
 * @author William Callahan
 */
@Service
public class CoverCacheManager {

    private static final int MAX_MEMORY_CACHE_SIZE_STANDARD = 1000;
    private static final int MAX_MEMORY_CACHE_SIZE_LARGE = 5000;
    private static final int MAX_MEMORY_CACHE_SIZE_MEDIUM = 2000;

    private final Cache<String, String> urlToPathCache;
    private final Cache<String, String> identifierToProvisionalUrlCache;
    private final Cache<String, ImageDetails> identifierToFinalImageDetailsCache;
    private final Cache<String, Boolean> knownBadImageUrls;
    private final Cache<String, Boolean> knownBadOpenLibraryIsbns;
    private final Cache<String, Boolean> knownBadLongitoodIsbns;

    /**
     * Constructs the CoverCacheManager and initializes all Caffeine caches
     * - Configures cache sizes and expiration policies
     */
    public CoverCacheManager() {
        this.urlToPathCache = Caffeine.newBuilder()
                .maximumSize(MAX_MEMORY_CACHE_SIZE_STANDARD)
                .expireAfterAccess(1, TimeUnit.DAYS)
                .build();

        this.identifierToProvisionalUrlCache = Caffeine.newBuilder()
                .maximumSize(MAX_MEMORY_CACHE_SIZE_STANDARD)
                .expireAfterWrite(6, TimeUnit.HOURS)
                .build();

        this.identifierToFinalImageDetailsCache = Caffeine.newBuilder()
                .maximumSize(MAX_MEMORY_CACHE_SIZE_STANDARD)
                .expireAfterAccess(7, TimeUnit.DAYS)
                .build();

        this.knownBadImageUrls = Caffeine.newBuilder()
                .maximumSize(MAX_MEMORY_CACHE_SIZE_LARGE)
                .expireAfterWrite(24, TimeUnit.HOURS)
                .build();

        this.knownBadOpenLibraryIsbns = Caffeine.newBuilder()
                .maximumSize(MAX_MEMORY_CACHE_SIZE_MEDIUM)
                .expireAfterWrite(24, TimeUnit.HOURS)
                .build();

        this.knownBadLongitoodIsbns = Caffeine.newBuilder()
                .maximumSize(MAX_MEMORY_CACHE_SIZE_MEDIUM)
                .expireAfterWrite(24, TimeUnit.HOURS)
                .build();
    }

    /**
     * Retrieves the cached local path for a given image URL
     * @param url The image URL
     * @return The cached local path, or null if not found
     */
    public String getPathFromUrlCache(String url) {
        return urlToPathCache.getIfPresent(url);
    }

    /**
     * Adds a mapping from an image URL to its cached local path
     * @param url The image URL
     * @param cachedPath The local path where the image is cached
     */
    public void putPathToUrlCache(String url, String cachedPath) {
        urlToPathCache.put(url, cachedPath);
    }

    /**
     * Retrieves the cached provisional URL for a given book identifier
     * @param identifierKey The book identifier (e.g., ISBN, Google ID)
     * @return The cached provisional URL, or null if not found
     */
    public String getProvisionalUrl(String identifierKey) {
        return identifierToProvisionalUrlCache.getIfPresent(identifierKey);
    }

    /**
     * Caches a provisional URL for a given book identifier
     * @param identifierKey The book identifier
     * @param provisionalUrl The provisional URL to cache
     */
    public void putProvisionalUrl(String identifierKey, String provisionalUrl) {
        identifierToProvisionalUrlCache.put(identifierKey, provisionalUrl);
    }
    
    /**
     * Invalidates the provisional URL for a given book identifier
     * @param identifierKey The book identifier
     */
    public void invalidateProvisionalUrl(String identifierKey) {
        identifierToProvisionalUrlCache.invalidate(identifierKey);
    }

    /**
     * Retrieves the cached final ImageDetails for a given book identifier
     * @param identifierKey The book identifier
     * @return The cached ImageDetails, or null if not found
     */
    public ImageDetails getFinalImageDetails(String identifierKey) {
        return identifierToFinalImageDetailsCache.getIfPresent(identifierKey);
    }

    /**
     * Caches the final ImageDetails for a given book identifier
     * @param identifierKey The book identifier
     * @param imageDetails The ImageDetails to cache
     */
    public void putFinalImageDetails(String identifierKey, ImageDetails imageDetails) {
        identifierToFinalImageDetailsCache.put(identifierKey, imageDetails);
    }

    /**
     * Invalidates the final ImageDetails for a given book identifier
     * @param identifierKey The book identifier
     */
    public void invalidateFinalImageDetails(String identifierKey) {
        identifierToFinalImageDetailsCache.invalidate(identifierKey);
    }
    
    /**
     * Checks if an image URL is known to be bad (e.g., leads to placeholder, 404)
     * @param imageUrl The image URL to check
     * @return True if the URL is known to be bad, false otherwise
     */
    public boolean isKnownBadImageUrl(String imageUrl) {
        return knownBadImageUrls.getIfPresent(imageUrl) != null;
    }

    /**
     * Marks an image URL as known to be bad
     * @param imageUrl The image URL to mark
     */
    public void addKnownBadImageUrl(String imageUrl) {
        knownBadImageUrls.put(imageUrl, Boolean.TRUE);
    }

    /**
     * Checks if an ISBN is known to cause issues with OpenLibrary
     * @param isbn The ISBN to check
     * @return True if the ISBN is known bad for OpenLibrary, false otherwise
     */
    public boolean isKnownBadOpenLibraryIsbn(String isbn) {
        return isbn != null && knownBadOpenLibraryIsbns.getIfPresent(isbn) != null;
    }

    /**
     * Marks an ISBN as known to cause issues with OpenLibrary
     * @param isbn The ISBN to mark
     */
    public void addKnownBadOpenLibraryIsbn(String isbn) {
        if (isbn != null) {
            knownBadOpenLibraryIsbns.put(isbn, Boolean.TRUE);
        }
    }

    /**
     * Checks if an ISBN is known to cause issues with Longitood
     * @param isbn The ISBN to check
     * @return True if the ISBN is known bad for Longitood, false otherwise
     */
    public boolean isKnownBadLongitoodIsbn(String isbn) {
        return isbn != null && knownBadLongitoodIsbns.getIfPresent(isbn) != null;
    }

    /**
     * Marks an ISBN as known to cause issues with Longitood
     * @param isbn The ISBN to mark
     */
    public void addKnownBadLongitoodIsbn(String isbn) {
        if (isbn != null) {
            knownBadLongitoodIsbns.put(isbn, Boolean.TRUE);
        }
    }
}
