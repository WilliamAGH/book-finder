/**
 * Service responsible for tracking, managing, and providing recently viewed books
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
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import reactor.core.publisher.Mono;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.*;
import java.util.stream.Collectors;

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
    private final GoogleBooksService googleBooksService;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final DuplicateBookService duplicateBookService;

    // In-memory storage for recently viewed books
    private final LinkedList<Book> recentlyViewedBooks = new LinkedList<>();
    private static final int MAX_RECENT_BOOKS = 10;
    private static final String DEFAULT_FALLBACK_QUERY = "java programming";

    /**
     * Constructs a RecentlyViewedService with required dependencies
     * 
     * @param googleBooksService Service for fetching book data from Google Books API
     * @param duplicateBookService Service for handling duplicate book detection and canonical ID resolution
     * 
     * @implNote Initializes the in-memory linked list for storing recently viewed books
     */
    public RecentlyViewedService(GoogleBooksService googleBooksService,
                                 DuplicateBookService duplicateBookService,
                                 BookDataOrchestrator bookDataOrchestrator) {
        this.googleBooksService = googleBooksService;
        this.duplicateBookService = duplicateBookService;
        this.bookDataOrchestrator = bookDataOrchestrator;
    }

    /**
     * Adds a book to the recently viewed list, ensuring canonical ID is used
     *
     * @param book The book to add to recently viewed history
     * 
     * @implNote Uses DuplicateBookService to find canonical representation of the book
     * Creates a new Book instance with the canonical ID if needed to avoid modifying the original
     * Removes any existing entry for the same book before adding it to the front of the list
     * Maintains a maximum size limit by removing oldest entries when necessary
     * Thread-safe implementation with synchronized blocks
     */
    public void addToRecentlyViewed(Book book) {
        if (book == null) {
            logger.warn("RECENT_VIEWS_DEBUG: Attempted to add a null book to recently viewed.");
            return;
        }

        String originalBookId = book.getId();
        logger.info("RECENT_VIEWS_DEBUG: Attempting to add book. Original ID: '{}', Title: '{}'", originalBookId, book.getTitle());

        String canonicalId = originalBookId; // Default to original

        // Attempt to find a canonical representation (disabled after Redis removal)
        Optional<Book> canonicalBookOpt = duplicateBookService.findPrimaryCanonicalBook(book);
        if (canonicalBookOpt.isPresent()) {
            Book canonicalBook = canonicalBookOpt.get();
            if (canonicalBook.getId() != null && !canonicalBook.getId().isEmpty()) {
                canonicalId = canonicalBook.getId();
            }
            logger.info("RECENT_VIEWS_DEBUG: Resolved original ID '{}' to canonical ID '{}' for book title '{}'", originalBookId, canonicalId, book.getTitle());
        } else {
            logger.info("RECENT_VIEWS_DEBUG: No canonical book found for book ID '{}', Title '{}'. Using original ID as canonical.", originalBookId, book.getTitle());
        }
        
        if (canonicalId == null || canonicalId.isEmpty()) {
            logger.warn("RECENT_VIEWS_DEBUG: Null or empty canonical ID determined for book title '{}' (original ID '{}'). Skipping add.", book.getTitle(), originalBookId);
            return;
        }

        final String finalCanonicalId = canonicalId;

        Book bookToAdd = book;
        // If the canonical ID is different from the book's current ID,
        // create a new Book object (or clone) for storage in the list with the canonical ID
        // This avoids modifying the original 'book' object which might be used elsewhere
        if (!java.util.Objects.equals(originalBookId, finalCanonicalId)) {
            logger.info("RECENT_VIEWS_DEBUG: Book ID mismatch. Original: '{}', Canonical: '{}'. Creating new Book instance for recent views.", originalBookId, finalCanonicalId);
            bookToAdd = new Book();
            // Copy essential properties for display in recent views
            bookToAdd.setId(finalCanonicalId);
            bookToAdd.setTitle(book.getTitle());
            bookToAdd.setAuthors(book.getAuthors());
            bookToAdd.setS3ImagePath(book.getS3ImagePath());
            bookToAdd.setPublishedDate(book.getPublishedDate());
            // Add other fields if they are displayed in the "Recent Views" section
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
    
            recentlyViewedBooks.addFirst(bookToAdd); // Add the (potentially new) book instance with canonical ID
            logger.info("RECENT_VIEWS_DEBUG: Added book with canonical ID '{}'. List size now: {}", finalCanonicalId, recentlyViewedBooks.size());
    
            while (recentlyViewedBooks.size() > MAX_RECENT_BOOKS) {
                Book removedLastBook = recentlyViewedBooks.removeLast();
                logger.info("RECENT_VIEWS_DEBUG: Trimmed book. ID: '{}'. List size now: {}", removedLastBook.getId(), recentlyViewedBooks.size());
            }
        }
    }

    /**
     * Asynchronously fetches and processes default books if no books have been viewed
     *
     * @return A Mono containing a list of default book recommendations
     * 
     * @implNote Uses reactive programming model for non-blocking operation
     * Filters books to ensure they have valid cover images
     * Sorts by publication date with newest books first
     * Limits results to the maximum number of books in history
     * Provides error handling with fallback to empty list
     */
    public Mono<List<Book>> fetchDefaultBooksAsync() {
        logger.debug("Fetching default books reactively (Postgres-first).");

        Mono<List<Book>> postgresResults = Mono.defer(() -> {
            if (bookDataOrchestrator == null) {
                return Mono.just(Collections.emptyList());
            }
            return bookDataOrchestrator.searchBooksTiered(DEFAULT_FALLBACK_QUERY, null, MAX_RECENT_BOOKS, null)
                .defaultIfEmpty(Collections.emptyList())
                .onErrorResume(ex -> {
                    logger.warn("Postgres lookup for recently viewed defaults failed: {}", ex.getMessage());
                    return Mono.just(Collections.emptyList());
                });
        });

        return postgresResults.flatMap(results -> {
                List<Book> prepared = prepareDefaultBooks(results);
                if (!prepared.isEmpty()) {
                    logger.debug("Returning {} default books from Postgres tier.", prepared.size());
                    return Mono.just(prepared);
                }

                logger.debug("Postgres tier returned empty defaults; falling back to Google Books.");
                return googleBooksService.searchBooksAsyncReactive(DEFAULT_FALLBACK_QUERY, null, MAX_RECENT_BOOKS, null)
                    .defaultIfEmpty(Collections.emptyList())
                    .map(this::prepareDefaultBooks)
                    .doOnSuccess(list -> logger.debug("Returning {} default books from Google fallback tier.", list.size()))
                    .onErrorResume(e -> {
                        logger.error("Error fetching default books from Google fallback", e);
                        return Mono.just(Collections.emptyList());
                    });
            }
        );
    }
    
    /**
     * Gets the list of recently viewed books with fallback to recommendations
     *
     * @return A Mono emitting either the user's recently viewed books or default recommendations
     * 
     * @implNote Performs optimistic check for empty list before acquiring lock
     * Uses reactive approach for fetching default books when needed
     * Handles race conditions where another thread might have populated the list
     * Returns a defensive copy of the list to prevent external modifications
     * Properly handles thread interruption with status restoration
     */
    public Mono<List<Book>> getRecentlyViewedBooksReactive() {
        // Optimistic check outside the lock
        if (recentlyViewedBooks.isEmpty()) {
            return fetchDefaultBooksAsync()
                .onErrorResume(e -> {
                    if (e instanceof InterruptedException) {
                        logger.warn("Fetching default books was interrupted.", e);
                        Thread.currentThread().interrupt(); // Restore interruption status
                    } else {
                        logger.error("Error executing default book fetch.", e);
                    }
                    return Mono.just(Collections.emptyList());
                })
                .flatMap(defaultBooks -> {
                    synchronized (recentlyViewedBooks) {
                        if (recentlyViewedBooks.isEmpty()) {
                            logger.debug("Returning {} default books as recently viewed is still empty.", defaultBooks.size());
                            return Mono.just(defaultBooks); // Return default books if still empty
                        }
                        // If another thread added books while we were fetching defaults, return the actual recently viewed books
                        logger.debug("Recently viewed was populated while fetching defaults. Returning actual list.");
                        return Mono.just(new ArrayList<>(recentlyViewedBooks));
                    }
                });
        }

        // If not empty initially, return a copy under lock
        synchronized (recentlyViewedBooks) {
            logger.debug("Returning {} recently viewed books.", recentlyViewedBooks.size());
            return Mono.just(new ArrayList<>(recentlyViewedBooks));
        }
    }

    /**
     * Gets the list of recently viewed books using a blocking approach
     * 
     * @return A list of recently viewed books or default recommendations
     * 
     * @implNote Blocking version of getRecentlyViewedBooksReactive for backward compatibility
     * Delegates to the reactive method and blocks until completion
     * Use only when reactive programming is not suitable for the calling context
     */
    public List<Book> getRecentlyViewedBooks() {
        return getRecentlyViewedBooksReactive().block();
    }

    /**
     * Checks if a book cover image URL points to a valid cover image
     *
     * @param imageUrl The image URL to check
     * @return true if the image is a valid cover, false if null or a placeholder
     * 
     * @implNote Checks for null URLs
     * Detects placeholder images by checking for specific filename patterns
     * Used to filter out books with missing or placeholder covers in recommendations
     */
    private boolean isValidCoverImage(String imageUrl) {
        if (imageUrl == null) {
            return false;
        }

        // Check if it's our placeholder image
        return !imageUrl.contains("placeholder-book-cover.svg");
    }

    private List<Book> prepareDefaultBooks(List<Book> books) {
        if (books == null || books.isEmpty()) {
            return Collections.emptyList();
        }

        return books.stream()
            .filter(book -> book != null && isValidCoverImage(book.getS3ImagePath()))
            .sorted((b1, b2) -> {
                if (b1 == null && b2 == null) return 0;
                if (b1 == null) return 1;
                if (b2 == null) return -1;
                if (b1.getPublishedDate() == null && b2.getPublishedDate() == null) return 0;
                if (b1.getPublishedDate() == null) return 1;
                if (b2.getPublishedDate() == null) return -1;
                return b2.getPublishedDate().compareTo(b1.getPublishedDate());
            })
            .limit(MAX_RECENT_BOOKS)
            .collect(Collectors.toList());
    }

    /**
     * Clears the recently viewed books list
     * 
     * @implNote Thread-safe implementation using synchronized block
     * Logs the action for debugging and audit purposes
     * Does not affect default recommendations which are generated dynamically
     */
    public void clearRecentlyViewedBooks() {
        synchronized (recentlyViewedBooks) {
            recentlyViewedBooks.clear();
            logger.debug("Recently viewed books cleared.");
        }
    }
}
