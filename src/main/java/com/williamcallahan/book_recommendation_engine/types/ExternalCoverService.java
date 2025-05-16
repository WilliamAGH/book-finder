package com.williamcallahan.book_recommendation_engine.types;

import com.williamcallahan.book_recommendation_engine.model.Book;
// Update import to use the canonical ImageDetails from the types package
// import com.williamcallahan.book_recommendation_engine.types.ImageDetails; // Removing import
// import reactor.core.publisher.Mono; // Removed unused import
import java.util.concurrent.CompletableFuture; // Added import

/**
 * Base interface for all external book cover image services
 *
 * @author William Callahan
 *
 * Features:
 * - Defines standard contract for retrieving cover images from external APIs
 * - Uses reactive Mono return types for non-blocking operation
 * - Returns structured ImageDetails with metadata about the source
 * - Enables consistent error handling across different image providers
 */
public interface ExternalCoverService {
    /**
     * Fetches a cover for a book from the specific external service
     *
     * @param book The book to fetch a cover for
     * @return A CompletableFuture emitting ImageDetails if a cover is found, or null/exception otherwise
     */
    CompletableFuture<com.williamcallahan.book_recommendation_engine.types.ImageDetails> fetchCover(Book book); // Fully qualified
}
