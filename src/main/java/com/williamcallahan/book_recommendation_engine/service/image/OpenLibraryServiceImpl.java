package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.OpenLibraryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.ArrayList;
import com.williamcallahan.book_recommendation_engine.service.image.BookImageOrchestrationService.CoverCandidate;

@Service
public class OpenLibraryServiceImpl implements OpenLibraryService {

    private static final Logger logger = LoggerFactory.getLogger(OpenLibraryServiceImpl.class);

    @Override
    public CompletableFuture<List<CoverCandidate>> fetchCovers(Book book) {
        return fetchCovers(book, CoverImageSource.ANY);
    }
    
    @Override
    public CompletableFuture<List<CoverCandidate>> fetchCovers(Book book, CoverImageSource preferredSource) {
        // If a specific source is requested that isn't OpenLibrary, return empty list
        if (preferredSource != CoverImageSource.ANY && preferredSource != CoverImageSource.OPEN_LIBRARY) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }
        return CompletableFuture.supplyAsync(() -> {
            List<CoverCandidate> candidates = new ArrayList<>();
            String isbn = book.getIsbn13() != null ? book.getIsbn13() : book.getIsbn10();
            if (isbn != null) {
                // Add large size (highest priority)
                String largeUrl = "https://covers.openlibrary.org/b/isbn/" + isbn + "-L.jpg";
                candidates.add(new CoverCandidate(
                    largeUrl,
                    "OpenLibrary (Large)",
                    70,
                    null,
                    "OpenLibrary-L-" + isbn
                ));
                
                // Add medium size (medium priority)
                String mediumUrl = "https://covers.openlibrary.org/b/isbn/" + isbn + "-M.jpg";
                candidates.add(new CoverCandidate(
                    mediumUrl,
                    "OpenLibrary (Medium)",
                    50,
                    null,
                    "OpenLibrary-M-" + isbn
                ));
                
                // Add small size (lowest priority)
                String smallUrl = "https://covers.openlibrary.org/b/isbn/" + isbn + "-S.jpg";
                candidates.add(new CoverCandidate(
                    smallUrl,
                    "OpenLibrary (Small)",
                    30,
                    null,
                    "OpenLibrary-S-" + isbn
                ));
            } else {
                logger.warn("No ISBN found for book {}, cannot fetch cover from OpenLibrary", book.getId());
            }
            return candidates;
        });
    }
}
