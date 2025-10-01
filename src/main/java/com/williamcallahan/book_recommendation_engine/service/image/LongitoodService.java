package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for the Longitood book cover service
 *
 * @author William Callahan
 *
 * Features:
 * - Specializes ExternalCoverService for the Longitood API
 * - Provides high-resolution book cover images
 * - Supports lookups by ISBN and title
 * - Offers access to publisher-provided cover artwork
 */
public interface LongitoodService extends ExternalCoverService {
    /**
     * Fetches a cover via Longitood and caches it locally when present.
     * Implementations should add appropriate provenance entries and fallbacks.
     *
     * @param book The book to fetch a cover for
     * @param bookIdForLog Identifier for logging and cache keys
     * @param provenanceData Mutable carrier for provenance details
     * @return CompletableFuture resolving to ImageDetails (placeholder if none available)
     */
    CompletableFuture<ImageDetails> fetchAndCacheCover(Book book, String bookIdForLog, ImageProvenanceData provenanceData);
}
