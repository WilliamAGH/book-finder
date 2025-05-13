/**
 * Orchestrates the retrieval of book cover image URLs.
 * Prioritizes fetching from S3 cache, then delegates to BookCoverCacheService
 * to provide a fast initial URL and trigger background processing for optimal image quality.
 */
package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;

@Service
public class BookImageOrchestrationService {

    private static final Logger logger = LoggerFactory.getLogger(BookImageOrchestrationService.class);
    private static final String DEFAULT_PLACEHOLDER_IMAGE = "/images/placeholder-book-cover.svg";

    private final S3BookCoverService s3BookCoverService;
    private final BookCoverCacheService bookCoverCacheService;

    @Value("${s3.enabled:true}")
    private boolean s3Enabled;

    @Value("${app.cover-sources.prefer-s3:true}")
    private boolean preferS3;

    @Value("${app.cover-cache.dir:covers}")
    private String cacheDirName;

    @Value("${app.cover-cache.max-file-size-bytes:5242880}") // 5MB default
    private long maxFileSizeBytes;

    @Autowired
    public BookImageOrchestrationService(S3BookCoverService s3BookCoverService,
                                         BookCoverCacheService bookCoverCacheService) {
        this.s3BookCoverService = s3BookCoverService;
        this.bookCoverCacheService = bookCoverCacheService;
    }

    /**
     * Asynchronously retrieves the best cover URL for a given book
     *
     * @param book The book to fetch the cover for
     * @return A CompletableFuture that resolves to the best cover URL
     */
    public CompletableFuture<String> getBestCoverUrlAsync(Book book) {
        return getBestCoverUrlAsync(book, CoverImageSource.ANY);
    }
    
    /**
     * Asynchronously retrieves the best cover URL for a given book from a specified source
     *
     * @param book The book to fetch the cover for
     * @param preferredSource The preferred source to fetch the cover from
     * @return A CompletableFuture that resolves to the best cover URL
     */
    public CompletableFuture<String> getBestCoverUrlAsync(Book book, CoverImageSource preferredSource) {
        return getBestCoverUrlAsync(book, preferredSource, ImageResolutionPreference.ANY);
    }
    
    /**
     * Asynchronously retrieves the best cover URL for a given book from a specified source with resolution preference
     *
     * @param book The book to fetch the cover for
     * @param preferredSource The preferred source to fetch the cover from
     * @param resolutionPreference The preferred resolution quality
     * @return A CompletableFuture that resolves to the best cover URL
     */
    public CompletableFuture<String> getBestCoverUrlAsync(Book book, CoverImageSource preferredSource, ImageResolutionPreference resolutionPreference) {
        if (book == null) {
            logger.warn("Book object is null, cannot fetch cover. Returning default placeholder.");
            return CompletableFuture.completedFuture(DEFAULT_PLACEHOLDER_IMAGE);
        }
        if (book.getId() == null) {
            logger.warn("Book ID is null for book object, cannot fetch cover effectively. Setting defaults on book and returning placeholder.");
            book.setCoverImageWidth(0);
            book.setCoverImageHeight(0);
            book.setIsCoverHighResolution(false);
            return CompletableFuture.completedFuture(DEFAULT_PLACEHOLDER_IMAGE);
        }

        String bookId = book.getId();
        String primaryExtension = getPrimaryExtensionFromBook(book);

        // S3 Pre-check (still returns String URL, dimensions are not known from HEAD request)
        if (s3Enabled && preferS3 && book.getId() != null) { // Added null check for book.getId() for S3 operations
            if (preferredSource != CoverImageSource.ANY) {
                String sourceStr = getSourceString(preferredSource);
                if (s3BookCoverService.coverExistsInS3(bookId, primaryExtension, sourceStr)) {
                    String s3Url = s3BookCoverService.getS3CoverUrl(bookId, primaryExtension, sourceStr);
                    logger.debug("S3 Pre-check: Found existing cover in S3 for book {} from preferred source {}: {}", bookId, preferredSource, s3Url);
                    book.setCoverImageWidth(null);
                    book.setCoverImageHeight(null);
                    book.setIsCoverHighResolution(null);
                    return CompletableFuture.completedFuture(s3Url);
                }
            }
            if (s3BookCoverService.coverExistsInS3(bookId, primaryExtension)) {
                String s3Url = s3BookCoverService.getS3CoverUrl(bookId, primaryExtension);
                logger.debug("S3 Pre-check: Found existing cover in S3 for book {}: {}", bookId, s3Url);
                book.setCoverImageWidth(null);
                book.setCoverImageHeight(null);
                book.setIsCoverHighResolution(null);
                return CompletableFuture.completedFuture(s3Url);
            }
        }

        logger.debug("Fetching cover for book {} with source preference {} and resolution preference {}", bookId, preferredSource, resolutionPreference);

        // The BookCoverCacheService now expects the full Book object
        // and will internally determine the best identifier (ISBN or Google Book ID).
        // The null/empty check for ISBN before calling getInitialCoverUrlAndTriggerBackgroundUpdate
        // is now handled within BookCoverCacheService by its getIdentifierKey method.
        
        // Pass the whole book object to the updated BookCoverCacheService method.
        String initialImageUrl = bookCoverCacheService.getInitialCoverUrlAndTriggerBackgroundUpdate(book);

        if (initialImageUrl == null) {
            // This case should ideally be handled by BookCoverCacheService returning its own placeholder.
            logger.error("BookCoverCacheService.getInitialCoverUrlAndTriggerBackgroundUpdate returned null for bookId: {}. This indicates an issue. Returning default placeholder.", bookId);
            book.setCoverImageUrl(DEFAULT_PLACEHOLDER_IMAGE);
            book.setCoverImageWidth(0);
            book.setCoverImageHeight(0);
            book.setIsCoverHighResolution(false);
            return CompletableFuture.completedFuture(DEFAULT_PLACEHOLDER_IMAGE);
        }

        book.setCoverImageUrl(initialImageUrl);
        book.setCoverImageWidth(null);
        book.setCoverImageHeight(null);
        book.setIsCoverHighResolution(null);

        logger.debug("BookCoverCacheService provided initial URL: {} for bookId: {}. Background processing initiated by cache service.", initialImageUrl, bookId);
        return CompletableFuture.completedFuture(initialImageUrl);
    }

    private String getPrimaryExtensionFromBook(Book book) {
        if (book.getCoverImageUrl() != null) {
            String ext = s3BookCoverService.getFileExtensionFromUrl(book.getCoverImageUrl());
            if (!ext.equals(".jpg")) return ext;
        }
        if (book.getOtherEditions() != null) {
            for (Book.EditionInfo edition : book.getOtherEditions()) {
                if (edition.getCoverImageUrl() != null) {
                    String ext = s3BookCoverService.getFileExtensionFromUrl(edition.getCoverImageUrl());
                     if (!ext.equals(".jpg")) return ext;
                }
            }
        }
        return ".jpg";
    }

    private String getSourceString(CoverImageSource source) {
        if (source == null) {
            return "unknown";
        }
        
        switch (source) {
            case GOOGLE_BOOKS_API:
                return "google-books";
            case OPEN_LIBRARY_API:
                return "open-library";
            case LONGITOOD_API:
                return "longitood";
            case LOCAL_CACHE:
                return "local-cache";
            case S3_CACHE:
                return "s3-cache";
            case SYSTEM_PLACEHOLDER:
                return "system-placeholder";
            default:
                return "unknown";
        }
    }

    // Enum for preferred source (if any)
    public enum CoverImageSource {
        GOOGLE_BOOKS_API,       // For images sourced directly from Google Books API
        OPEN_LIBRARY_API,       // For images sourced directly from OpenLibrary API
        LONGITOOD_API,          // For images sourced directly from Longitood API
        LOCAL_CACHE,            // For images served from the local disk cache
        S3_CACHE,               // For images served from S3 cache
        SYSTEM_PLACEHOLDER,     // For system-provided placeholder images
        ANY                     // Used for requests indicating any source is acceptable
    }
}
