package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.*;
// import java.util.concurrent.CompletableFuture; // Removed unused import
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import reactor.core.publisher.Mono;

/**
 * Service for tracking and managing recently viewed books
 *
 * @author William Callahan
 *
 * Features:
 * - Maintains a thread-safe list of recently viewed books
 * - Limits the number of books in the history to avoid memory issues
 * - Provides fallback recommendations when no books have been viewed
 * - Avoids duplicate entries by removing existing books before re-adding
 * - Sorts fallback books by publication date for relevance
 */
@Service
public class RecentlyViewedService {

    private static final Logger logger = LoggerFactory.getLogger(RecentlyViewedService.class);
    private final GoogleBooksService googleBooksService;

    // In-memory storage for recently viewed books
    private final LinkedList<Book> recentlyViewedBooks = new LinkedList<>();
    private static final int MAX_RECENT_BOOKS = 10;

    @Autowired
    public RecentlyViewedService(GoogleBooksService googleBooksService) {
        this.googleBooksService = googleBooksService;
    }

    /**
     * Add a book to the recently viewed list.
     *
     * @param book the book to add
     */
    public void addToRecentlyViewed(Book book) {
        synchronized (recentlyViewedBooks) {
            // Remove the book if it already exists to avoid duplicates
            recentlyViewedBooks.removeIf(b -> b.getId().equals(book.getId()));

            // Add the book to the beginning of the list
            recentlyViewedBooks.addFirst(book);

            // Trim the list if it exceeds the maximum size
            while (recentlyViewedBooks.size() > MAX_RECENT_BOOKS) {
                recentlyViewedBooks.removeLast();
            }
        }
    }

    /**
     * Asynchronously fetches and processes default books if no books have been viewed.
     * This method is now fully non-blocking internally.
     *
     * @return A Mono containing a list of default books.
     */
    @Async("mvcTaskExecutor") // Still run on async executor, but the Mono itself is non-blocking
    public Mono<List<Book>> fetchDefaultBooksAsync() {
        logger.debug("Fetching default books reactively.");
        return googleBooksService.searchBooksAsyncReactive("java programming")
            .map(books -> books.stream()
                .filter(book -> isValidCoverImage(book.getCoverImageUrl()))
                .sorted((b1, b2) -> {
                    if (b1.getPublishedDate() == null && b2.getPublishedDate() == null) return 0;
                    if (b1.getPublishedDate() == null) return 1; // nulls last
                    if (b2.getPublishedDate() == null) return -1; // nulls last
                    return b2.getPublishedDate().compareTo(b1.getPublishedDate()); // newest first
                })
                .limit(MAX_RECENT_BOOKS)
                .collect(Collectors.toList())
            )
            .doOnSuccess(defaultBooks -> logger.debug("Successfully fetched and processed {} default books.", defaultBooks.size()))
            .onErrorResume(e -> {
                logger.error("Error fetching default books reactively", e);
                return Mono.just(Collections.emptyList());
            });
    }
    
    /**
     * Get the list of recently viewed books.
     * If the list is empty, it attempts to fetch default books.
     *
     * @return a list of recently viewed books or default books
     */
    public List<Book> getRecentlyViewedBooks() {
        // Optimistic check outside the lock
        if (recentlyViewedBooks.isEmpty()) {
            List<Book> defaultBooks = Collections.emptyList();
            try {
                // This call blocks the current thread, but not while holding the recentlyViewedBooks lock.
                // The underlying fetchDefaultBooksAsync is now non-blocking until .toFuture().get()
                logger.debug("Recently viewed is empty, attempting to fetch default books.");
                defaultBooks = fetchDefaultBooksAsync().toFuture().get(); // Blocks here
            } catch (InterruptedException e) {
                logger.warn("Fetching default books was interrupted.", e);
                Thread.currentThread().interrupt(); // Restore interruption status
                return Collections.emptyList();
            } catch (ExecutionException e) {
                logger.error("Error executing default book fetch.", e.getCause());
                return Collections.emptyList();
            }

            // After fetching, re-check under lock to handle concurrent additions
            synchronized (recentlyViewedBooks) {
                if (recentlyViewedBooks.isEmpty()) {
                    logger.debug("Returning {} default books as recently viewed is still empty.", defaultBooks.size());
                    return defaultBooks; // Return default books if still empty
                }
                // If another thread added books while we were fetching defaults, return the actual recently viewed books
                logger.debug("Recently viewed was populated while fetching defaults. Returning actual list.");
                return new ArrayList<>(recentlyViewedBooks);
            }
        }

        // If not empty initially, return a copy under lock
        synchronized (recentlyViewedBooks) {
            logger.debug("Returning {} recently viewed books.", recentlyViewedBooks.size());
            return new ArrayList<>(recentlyViewedBooks);
        }
    }

    /**
     * Check if a book cover image is valid (not a placeholder)
     *
     * @param imageUrl the image URL to check
     * @return true if the image is a valid cover, false otherwise
     */
    private boolean isValidCoverImage(String imageUrl) {
        if (imageUrl == null) {
            return false;
        }

        // Check if it's our placeholder image
        // Consider making "placeholder-book-cover.svg" a constant
        return !imageUrl.contains("placeholder-book-cover.svg");
    }

    /**
     * Clear the recently viewed books list.
     */
    public void clearRecentlyViewedBooks() {
        synchronized (recentlyViewedBooks) {
            recentlyViewedBooks.clear();
            logger.debug("Recently viewed books cleared.");
        }
    }
}
