/**
 * Service responsible for tracking, managing, and providing recently viewed books
 * 
 * Coordinates with {@link BookDeduplicationService} (via {@link DuplicateBookService})
 * to resolve canonical book identifiers (ISBN-13, ISBN-10, or Google Books ID)
 * and prevent duplicate entries in the history
 *
 * This component provides functionality for:
 * - Maintaining a thread-safe list of recently viewed books
 * - Handling canonical book ID resolution to prevent duplicates
 * - Providing fallback recommendations when history is empty
 * - Supporting both synchronous and reactive APIs
 * - Ensuring memory efficiency through size limits
 * 
 * Used throughout the application to enhance user experience by enabling
 * "recently viewed" sections and personalized recommendations
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import java.util.ArrayList;
import java.util.*;
import java.util.stream.Collectors;
import reactor.core.publisher.Mono;
import java.util.concurrent.CompletableFuture;

/**
 * Service for tracking and managing recently viewed books
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
    private final DuplicateBookService duplicateBookService;
    private final AsyncTaskExecutor mvcTaskExecutor;

    // In-memory storage for recently viewed books
    private final LinkedList<Book> recentlyViewedBooks = new LinkedList<>();
    private static final int MAX_RECENT_BOOKS = 10;

    /**
     * Constructs a RecentlyViewedService with required dependencies
     * 
     * @param duplicateBookService Service for handling duplicate book detection and canonical ID resolution
     * @param mvcTaskExecutor Executor for async tasks
     * 
     * @implNote Initializes the in-memory linked list for storing recently viewed books
     */
    public RecentlyViewedService(DuplicateBookService duplicateBookService,
                                 @Qualifier("mvcTaskExecutor") AsyncTaskExecutor mvcTaskExecutor) {
        this.duplicateBookService = duplicateBookService;
        this.mvcTaskExecutor = mvcTaskExecutor;
    }

    /**
     * Asynchronously adds a book to the recently viewed list, ensuring canonical ID is used.
     *
     * @param book The book to add to recently viewed history
     * @return CompletableFuture<Void> indicating completion of the operation.
     */
    @Async("mvcTaskExecutor")
    public CompletableFuture<Void> addToRecentlyViewedAsync(Book book) {
        if (book == null) {
            logger.warn("RECENT_VIEWS_DEBUG: Attempted to add a null book to recently viewed.");
            return CompletableFuture.completedFuture(null);
        }

        String originalBookId = book.getId();
        logger.info("RECENT_VIEWS_DEBUG: Attempting to add book. Original ID: '{}', Title: '{}'", originalBookId, book.getTitle());

        return duplicateBookService.findPrimaryCanonicalBookAsync(book)
            .thenAcceptAsync(canonicalCachedBookOpt -> {
                String canonicalId = originalBookId; // Default to original

                if (canonicalCachedBookOpt.isPresent()) {
                    com.williamcallahan.book_recommendation_engine.model.CachedBook cachedCanonical = canonicalCachedBookOpt.get();
                    if (cachedCanonical.getGoogleBooksId() != null && !cachedCanonical.getGoogleBooksId().isEmpty()) {
                        canonicalId = cachedCanonical.getGoogleBooksId();
                    } else if (cachedCanonical.getId() != null && !cachedCanonical.getId().isEmpty()) { // Fallback to CachedBook's own ID if Google ID is missing
                        canonicalId = cachedCanonical.getId();
                    }
                    logger.info("RECENT_VIEWS_DEBUG: Resolved original ID '{}' to canonical ID '{}' for book title '{}'", originalBookId, canonicalId, book.getTitle());
                } else {
                    logger.info("RECENT_VIEWS_DEBUG: No canonical CachedBook found for book ID '{}', Title '{}'. Using original ID as canonical.", originalBookId, book.getTitle());
                }
                
                if (canonicalId == null || canonicalId.isEmpty()) {
                    logger.warn("RECENT_VIEWS_DEBUG: Null or empty canonical ID determined for book title '{}' (original ID '{}'). Skipping add.", book.getTitle(), originalBookId);
                    return;
                }

                final String finalCanonicalId = canonicalId;
                Book bookToAdd = book;

                if (!java.util.Objects.equals(originalBookId, finalCanonicalId)) {
                    logger.info("RECENT_VIEWS_DEBUG: Book ID mismatch. Original: '{}', Canonical: '{}'. Creating new Book instance for recent views.", originalBookId, finalCanonicalId);
                    bookToAdd = new Book();
                    bookToAdd.setId(finalCanonicalId);
                    bookToAdd.setTitle(book.getTitle());
                    bookToAdd.setAuthors(book.getAuthors());
                    bookToAdd.setCoverImageUrl(book.getCoverImageUrl());
                    bookToAdd.setPublishedDate(book.getPublishedDate());
                }

                synchronized (recentlyViewedBooks) {
                    String existingIds = recentlyViewedBooks.stream()
                                            .map(b -> b != null ? b.getId() : "null")
                                            .collect(Collectors.joining(", "));
                    logger.info("RECENT_VIEWS_DEBUG: Existing IDs in recent views before removal: [{}] for new canonical ID '{}'", existingIds, finalCanonicalId);
            
                    boolean removed = recentlyViewedBooks.removeIf(b -> 
                        b != null && java.util.Objects.equals(b.getId(), finalCanonicalId)
                    );

                    if (removed) {
                        logger.info("RECENT_VIEWS_DEBUG: Found and removed existing entry for canonical ID '{}'", finalCanonicalId);
                    } else {
                        logger.info("RECENT_VIEWS_DEBUG: No existing entry found for canonical ID '{}'", finalCanonicalId);
                    }
            
                    recentlyViewedBooks.addFirst(bookToAdd); 
                    logger.info("RECENT_VIEWS_DEBUG: Added book with canonical ID '{}'. List size now: {}", finalCanonicalId, recentlyViewedBooks.size());
            
                    while (recentlyViewedBooks.size() > MAX_RECENT_BOOKS) {
                        Book removedLastBook = recentlyViewedBooks.removeLast();
                        logger.info("RECENT_VIEWS_DEBUG: Trimmed book. ID: '{}'. List size now: {}", removedLastBook.getId(), recentlyViewedBooks.size());
                    }
                }
            }, mvcTaskExecutor)
            .exceptionally(ex -> {
                logger.error("Error in addToRecentlyViewedAsync for book ID {}: {}", originalBookId, ex.getMessage(), ex);
                return null;
            });
    }
    
    /**
     * @deprecated Use {@link #addToRecentlyViewedAsync(Book)} instead.
     */
    @Deprecated
    public void addToRecentlyViewed(Book book) {
        logger.warn("Deprecated synchronous addToRecentlyViewed called. Consider using addToRecentlyViewedAsync().");
        try {
            addToRecentlyViewedAsync(book).join(); // Blocking for deprecated method
        } catch (Exception e) {
            logger.error("Error in synchronous addToRecentlyViewed for book ID {}: {}", (book != null ? book.getId() : "null"), e.getMessage(), e);
            // Depending on desired behavior, you might rethrow or handle differently
        }
    }
    
    /**
     * Gets the list of recently viewed books
     *
     * @return A Mono emitting the user's recently viewed books (or empty list if none)
     * 
     * @implNote Returns only actual recently viewed books, no fallback API calls
     * Returns a defensive copy of the list to prevent external modifications
     */
    public Mono<List<Book>> getRecentlyViewedBooksReactive() {
        synchronized (recentlyViewedBooks) {
            logger.debug("Returning {} recently viewed books via reactive.", recentlyViewedBooks.size());
            return Mono.just(new ArrayList<>(recentlyViewedBooks));
        }
    }

    /**
     * Asynchronously gets the list of recently viewed books.
     * @return CompletableFuture emitting the user's recently viewed books (or empty list if none)
     */
    @Async("mvcTaskExecutor")
    public CompletableFuture<List<Book>> getRecentlyViewedBooksAsync() {
        synchronized (recentlyViewedBooks) {
            logger.debug("Returning {} recently viewed books via async.", recentlyViewedBooks.size());
            return CompletableFuture.completedFuture(new ArrayList<>(recentlyViewedBooks));
        }
    }

    /**
     * @deprecated Use {@link #getRecentlyViewedBooksAsync()} or {@link #getRecentlyViewedBooksReactive()} for non-blocking behavior.
     */
    @Deprecated
    public List<Book> getRecentlyViewedBooks() {
        logger.warn("Deprecated synchronous getRecentlyViewedBooks called. Consider using getRecentlyViewedBooksAsync() or getRecentlyViewedBooksReactive().");
        try {
            return getRecentlyViewedBooksAsync().join(); // Blocking for deprecated method
        } catch (Exception e) {
            logger.error("Error in synchronous getRecentlyViewedBooks: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            return Collections.emptyList();
        }
    }

    /**
     * Asynchronously clears the list of recently viewed books.
     * @return CompletableFuture<Void> indicating completion of the clear operation
     */
    @Async("mvcTaskExecutor")
    public CompletableFuture<Void> clearRecentlyViewedBooksAsync() {
        synchronized (recentlyViewedBooks) {
            logger.info("RECENT_VIEWS_DEBUG: Clearing all recently viewed books. Previous size: {}", recentlyViewedBooks.size());
            recentlyViewedBooks.clear();
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * @deprecated Use {@link #clearRecentlyViewedBooksAsync()} for non-blocking behavior.
     */
    @Deprecated
    public void clearRecentlyViewedBooks() {
        logger.warn("Deprecated synchronous clearRecentlyViewedBooks called. Consider using clearRecentlyViewedBooksAsync().");
        try {
            clearRecentlyViewedBooksAsync().join();
        } catch (Exception e) {
            logger.error("Error in synchronous clearRecentlyViewedBooks: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }
}
