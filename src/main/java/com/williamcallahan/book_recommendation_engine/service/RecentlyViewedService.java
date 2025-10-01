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

import com.williamcallahan.book_recommendation_engine.dto.BookCard;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.UrlPatternMatcher;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import reactor.core.publisher.Mono;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
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
@Slf4j
public class RecentlyViewedService {

    private final BookSearchService bookSearchService;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final BookQueryRepository bookQueryRepository;
    private final DuplicateBookService duplicateBookService;
    private final RecentBookViewRepository recentBookViewRepository;

    // In-memory storage for recently viewed books (lock-free for better concurrency)
    private final ConcurrentLinkedDeque<Book> recentlyViewedBooks = new ConcurrentLinkedDeque<>();
    private static final int MAX_RECENT_BOOKS = ApplicationConstants.Paging.DEFAULT_TIERED_LIMIT / 2;
    private static final String DEFAULT_FALLBACK_QUERY = ApplicationConstants.Search.DEFAULT_RECENT_FALLBACK_QUERY;

    /**
     * Constructs a RecentlyViewedService with required dependencies
     * 
     * @param googleBooksService Service for fetching book data from Google Books API
     * @param duplicateBookService Service for handling duplicate book detection and canonical ID resolution
     * 
     * @implNote Initializes the in-memory linked list for storing recently viewed books
     */
    public RecentlyViewedService(@Nullable BookSearchService bookSearchService,
                                 DuplicateBookService duplicateBookService,
                                 BookDataOrchestrator bookDataOrchestrator,
                                 @Nullable BookQueryRepository bookQueryRepository,
                                 RecentBookViewRepository recentBookViewRepository) {
        this.bookSearchService = bookSearchService;
        this.duplicateBookService = duplicateBookService;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.bookQueryRepository = bookQueryRepository;
        this.recentBookViewRepository = recentBookViewRepository;
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
            log.warn("RECENT_VIEWS_DEBUG: Attempted to add a null book to recently viewed.");
            return;
        }

        String originalBookId = book.getId();
        log.info("RECENT_VIEWS_DEBUG: Attempting to add book. Original ID: '{}', Title: '{}'", originalBookId, book.getTitle());

        String canonicalId = originalBookId; // Default to original

        // Attempt to find a canonical representation (currently disabled)
        Optional<Book> canonicalBookOpt = duplicateBookService.findPrimaryCanonicalBook(book);
        if (canonicalBookOpt.isPresent()) {
            Book canonicalBook = canonicalBookOpt.get();
            if (ValidationUtils.hasText(canonicalBook.getId())) {
                canonicalId = canonicalBook.getId();
            }
            log.info("RECENT_VIEWS_DEBUG: Resolved original ID '{}' to canonical ID '{}' for book title '{}'", originalBookId, canonicalId, book.getTitle());
        } else {
            log.info("RECENT_VIEWS_DEBUG: No canonical book found for book ID '{}', Title '{}'. Using original ID as canonical.", originalBookId, book.getTitle());
        }
        
        if (!ValidationUtils.hasText(canonicalId)) {
            log.warn("RECENT_VIEWS_DEBUG: Null or empty canonical ID determined for book title '{}' (original ID '{}'). Skipping add.", book.getTitle(), originalBookId);
            return;
        }

        final String finalCanonicalId = canonicalId;

        Book bookToAdd = book;
        // If the canonical ID is different from the book's current ID,
        // create a new Book object (or clone) for storage in the list with the canonical ID
        // This avoids modifying the original 'book' object which might be used elsewhere
        if (!Objects.equals(originalBookId, finalCanonicalId)) {
            log.info("RECENT_VIEWS_DEBUG: Book ID mismatch. Original: '{}', Canonical: '{}'. Creating new Book instance for recent views.", originalBookId, finalCanonicalId);
            bookToAdd = new Book();
            // Copy essential properties for display in recent views
            bookToAdd.setId(finalCanonicalId);
            bookToAdd.setTitle(book.getTitle());
            bookToAdd.setAuthors(book.getAuthors());
            bookToAdd.setS3ImagePath(book.getS3ImagePath());
            bookToAdd.setPublishedDate(book.getPublishedDate());
            // Add other fields if they are displayed in the "Recent Views" section
        }

        if (!ValidationUtils.hasText(bookToAdd.getSlug())) {
            bookToAdd.setSlug(finalCanonicalId);
        }

        // Use a final reference for lambda capture below
        final Book bookRef = bookToAdd;

        // Lock-free operations using ConcurrentLinkedDeque
        // Remove existing entry for this book
        recentlyViewedBooks.removeIf(b ->
            b != null && Objects.equals(b.getId(), finalCanonicalId)
        );

        // Add to front
        recentlyViewedBooks.addFirst(bookToAdd);
        log.info("RECENT_VIEWS_DEBUG: Added book with canonical ID '{}'. List size now: {}", finalCanonicalId, recentlyViewedBooks.size());

        // Trim to max size
        while (recentlyViewedBooks.size() > MAX_RECENT_BOOKS) {
            Book removedLastBook = recentlyViewedBooks.pollLast();
            if (removedLastBook != null) {
                log.debug("RECENT_VIEWS_DEBUG: Trimmed book. ID: '{}'", removedLastBook.getId());
            }
        }

        if (recentBookViewRepository != null && recentBookViewRepository.isEnabled()) {
            recentBookViewRepository.recordView(finalCanonicalId, Instant.now(), "web");
            recentBookViewRepository.fetchStatsForBook(finalCanonicalId)
                    .ifPresent(stats -> applyViewStats(bookRef, stats));
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
    /**
     * @deprecated Surface default recommendations via DTO projections (e.g. {@code BookCard}) using
     * {@link BookQueryRepository} and avoid hydrating legacy {@link Book} lists.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<Book>> fetchDefaultBooksAsync() {
        log.debug("Fetching default books reactively (Postgres-first).");

        return loadFromRepositoryMono()
            .flatMap(prepared -> {
                if (!prepared.isEmpty()) {
                    log.debug("Returning {} default books from recent-view repository.", prepared.size());
                    return Mono.just(prepared);
                }

                return fetchDefaultBooksFromSearch();
            });
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
    /**
     * @deprecated Use {@link #getRecentlyViewedBookIds(int)} with DTO retrieval to present recently viewed items.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<Book>> getRecentlyViewedBooksReactive() {
        return Mono.defer(() -> {
            // Lock-free read
            if (!recentlyViewedBooks.isEmpty()) {
                log.debug("Returning {} recently viewed books from cache.", recentlyViewedBooks.size());
                return Mono.just(new ArrayList<>(recentlyViewedBooks));
            }

            return fetchDefaultBooksAsync()
                .onErrorResume(e -> {
                    if (e instanceof InterruptedException) {
                        LoggingUtils.warn(log, e, "Fetching default books was interrupted.");
                        Thread.currentThread().interrupt();
                    } else {
                        LoggingUtils.error(log, e, "Error executing default book fetch.");
                    }
                    return Mono.just(Collections.emptyList());
                })
                .map(defaultBooks -> defaultBooks == null ? Collections.<Book>emptyList() : defaultBooks)
                .doOnNext(defaultBooks -> {
                    if (!defaultBooks.isEmpty() && recentlyViewedBooks.isEmpty()) {
                        // Only add if still empty (another thread might have populated)
                        recentlyViewedBooks.addAll(defaultBooks);
                    }
                })
                .map(defaultBooks -> {
                    // Return current state (might have been populated by another thread)
                    if (!recentlyViewedBooks.isEmpty()) {
                        return new ArrayList<>(recentlyViewedBooks);
                    }
                    return defaultBooks;
                });
        });
    }

    /**
     * Gets list of recently viewed book IDs for use with BookQueryRepository.
     * This is THE SINGLE SOURCE for recently viewed book IDs.
     * 
     * Performance: Returns only UUIDs, caller fetches as BookCard DTOs with single query.
     * 
     * @param limit Maximum number of book IDs to return
     * @return List of book UUIDs (as Strings) for recently viewed books
     */
    public List<String> getRecentlyViewedBookIds(int limit) {
        // Check repository first (persistent storage)
        if (recentBookViewRepository != null && recentBookViewRepository.isEnabled()) {
            try {
                List<RecentBookViewRepository.ViewStats> stats = 
                    recentBookViewRepository.fetchMostRecentViews(limit);
                
                if (!stats.isEmpty()) {
                    return stats.stream()
                        .map(RecentBookViewRepository.ViewStats::bookId)
                        .filter(ValidationUtils::hasText)
                        .limit(limit)
                        .collect(Collectors.toList());
                }
            } catch (Exception e) {
                log.warn("Failed to fetch book IDs from repository: {}", e.getMessage());
            }
        }
        
        // Fallback to in-memory cache
        return recentlyViewedBooks.stream()
            .filter(b -> b != null && ValidationUtils.hasText(b.getId()))
            .map(Book::getId)
            .limit(limit)
            .collect(Collectors.toList());
    }
    
    /**
     * @deprecated Use {@link #getRecentlyViewedBookIds(int)} with BookQueryRepository instead.
     * This method returns hydrated Book entities which trigger multiple queries.
     * Will be removed in v2.0.
     */
    @Deprecated(since = "1.5", forRemoval = true)
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
    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector#isExternalUrl(String)}
     * and {@link com.williamcallahan.book_recommendation_engine.util.cover.CoverImagesFactory} based checks instead of
     * {@link ValidationUtils.BookValidator} helpers.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private boolean isValidCoverImage(String imageUrl) {
        return ValidationUtils.hasText(imageUrl) && !UrlPatternMatcher.isPlaceholderUrl(imageUrl);
    }

    private List<Book> prepareDefaultBooks(List<Book> books) {
        if (ValidationUtils.isNullOrEmpty(books)) {
            return Collections.emptyList();
        }

        return books.stream()
            .filter(book -> book != null && isValidCoverImage(book.getS3ImagePath()))
            .sorted(this::compareByLastViewedThenPublished)
            .limit(MAX_RECENT_BOOKS)
            .collect(Collectors.toList());
    }

    private List<Book> booksFromCards(List<BookCard> cards) {
        if (cards == null || cards.isEmpty()) {
            return Collections.emptyList();
        }

        List<Book> books = new ArrayList<>(cards.size());
        for (BookCard card : cards) {
            if (card == null) {
                continue;
            }
            Book book = new Book();
            book.setId(card.id());
            book.setSlug(ValidationUtils.hasText(card.slug()) ? card.slug() : card.id());
            book.setTitle(card.title());
            book.setAuthors(card.authors());
            book.setExternalImageUrl(card.coverUrl());
            book.setS3ImagePath(card.coverUrl());
            book.setAverageRating(card.averageRating());
            book.setRatingsCount(card.ratingsCount());
            if (card.tags() != null && !card.tags().isEmpty()) {
                book.setQualifiers(new java.util.HashMap<>(card.tags()));
            }
            books.add(book);
        }
        return books;
    }

    private Mono<List<Book>> loadFromRepositoryMono() {
        if (recentBookViewRepository == null || !recentBookViewRepository.isEnabled()) {
            return Mono.just(Collections.emptyList());
        }

        return Mono.fromCallable(() -> {
                List<RecentBookViewRepository.ViewStats> stats = recentBookViewRepository.fetchMostRecentViews(MAX_RECENT_BOOKS);
                if (stats.isEmpty()) {
                    return Collections.<Book>emptyList();
                }

                List<Book> hydrated = new ArrayList<>();
                for (RecentBookViewRepository.ViewStats stat : stats) {
                    if (stat == null || !ValidationUtils.hasText(stat.bookId())) {
                        continue;
                    }

                    Optional<Book> bookOptional = bookDataOrchestrator != null
                            ? bookDataOrchestrator.getBookFromDatabase(stat.bookId())
                            : Optional.empty();

                    bookOptional.ifPresent(book -> {
                        if (!ValidationUtils.hasText(book.getSlug())) {
                            book.setSlug(book.getId());
                        }
                        applyViewStats(book, stat);
                        hydrated.add(book);
                    });
                }

                return prepareDefaultBooks(hydrated);
            })
            .subscribeOn(Schedulers.boundedElastic())
            .onErrorResume(ex -> {
                log.warn("Failed to load recent books from repository: {}", ex.getMessage());
                return Mono.just(Collections.emptyList());
            });
    }

    private Mono<List<Book>> fetchDefaultBooksFromSearch() {
        if (bookSearchService == null || bookQueryRepository == null) {
            return Mono.just(Collections.emptyList());
        }

        return Mono.fromCallable(() -> bookSearchService.searchBooks(DEFAULT_FALLBACK_QUERY, MAX_RECENT_BOOKS))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(results -> {
                if (results == null || results.isEmpty()) {
                    return Mono.just(Collections.<Book>emptyList());
                }

                List<UUID> ids = results.stream()
                    .map(BookSearchService.SearchResult::bookId)
                    .filter(Objects::nonNull)
                    .limit(MAX_RECENT_BOOKS)
                    .collect(Collectors.toList());

                if (ids.isEmpty()) {
                    return Mono.just(Collections.<Book>emptyList());
                }

                return Mono.fromCallable(() -> bookQueryRepository.fetchBookCards(ids))
                    .subscribeOn(Schedulers.boundedElastic())
                    .map(this::booksFromCards)
                    .map(this::prepareDefaultBooks);
            })
            .doOnSuccess(list -> {
                if (!list.isEmpty()) {
                    log.debug("Returning {} default books from Postgres search tier.", list.size());
                }
            })
            .onErrorResume(ex -> {
                LoggingUtils.warn(log, ex, "Search-based fallback for recently viewed defaults failed");
                return Mono.just(Collections.<Book>emptyList());
            });
    }

    private void applyViewStats(Book book, RecentBookViewRepository.ViewStats stats) {
        if (book == null || stats == null) {
            return;
        }
        book.addQualifier("recent.views.lastViewedAt", stats.lastViewedAt());
        book.addQualifier("recent.views.24h", stats.viewsLast24h());
        book.addQualifier("recent.views.7d", stats.viewsLast7d());
        book.addQualifier("recent.views.30d", stats.viewsLast30d());
    }

    private int compareByLastViewedThenPublished(Book first, Book second) {
        if (first == null && second == null) {
            return 0;
        }
        if (first == null) {
            return 1;
        }
        if (second == null) {
            return -1;
        }

        Instant firstViewed = getInstantQualifier(first, "recent.views.lastViewedAt");
        Instant secondViewed = getInstantQualifier(second, "recent.views.lastViewedAt");

        if (firstViewed != null || secondViewed != null) {
            if (firstViewed == null) {
                return 1;
            }
            if (secondViewed == null) {
                return -1;
            }
            int compare = secondViewed.compareTo(firstViewed);
            if (compare != 0) {
                return compare;
            }
        }

        if (first.getPublishedDate() == null && second.getPublishedDate() == null) {
            return 0;
        }
        if (first.getPublishedDate() == null) {
            return 1;
        }
        if (second.getPublishedDate() == null) {
            return -1;
        }
        return second.getPublishedDate().compareTo(first.getPublishedDate());
    }

    private Instant getInstantQualifier(Book book, String qualifierKey) {
        if (book == null) {
            return null;
        }
        Map<String, Object> qualifiers = book.getQualifiers();
        if (qualifiers == null) {
            return null;
        }
        Object value = qualifiers.get(qualifierKey);
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof java.util.Date date) {
            return date.toInstant();
        }
        if (value instanceof String str) {
            try {
                return Instant.parse(str);
            } catch (Exception ignored) {
                return null;
            }
        }
        return null;
    }

    /**
     * Clears the recently viewed books list
     * 
     * @implNote Thread-safe implementation using synchronized block
     * Logs the action for debugging and audit purposes
     * Does not affect default recommendations which are generated dynamically
     */
    public void clearRecentlyViewedBooks() {
        recentlyViewedBooks.clear();
        log.debug("Recently viewed books cleared.");
    }
}
