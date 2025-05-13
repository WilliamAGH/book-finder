package com.williamcallahan.book_recommendation_engine.types;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService.CoverCandidate;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ExternalCoverService {
    /**
     * Fetches covers for a book from any available source
     * 
     * @param book The book to fetch covers for
     * @return A CompletableFuture that resolves to a list of cover candidates
     */
    CompletableFuture<List<CoverCandidate>> fetchCovers(Book book);
    
    /**
     * Fetches covers for a book from a specific source if available
     * 
     * @param book The book to fetch covers for
     * @param preferredSource The preferred source to fetch covers from
     * @return A CompletableFuture that resolves to a list of cover candidates
     */
    default CompletableFuture<List<CoverCandidate>> fetchCovers(Book book, CoverImageSource preferredSource) {
        // Default implementation just calls the regular fetchCovers method
        return fetchCovers(book);
    }
}
