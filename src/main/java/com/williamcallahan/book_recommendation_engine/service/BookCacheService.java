/**
 * Service for multi-level caching of book data with both synchronous and reactive APIs
 *
 * @author William Callahan
 *
 * Features:
 * - Implements multi-level caching strategy (in-memory, Spring Cache, database)
 * - Provides reactive and traditional APIs for book data retrieval
 * - Supports querying by Google Books ID, ISBN-10, and ISBN-13
 * - Automatically caches book data fetched from Google Books API
 * - Handles vector embeddings for book similarity recommendations
 * - Manages cache invalidation for book cover updates
 * - Includes scheduled cache cleanup for memory optimization
 * - Provides fallback mechanisms when database is unavailable
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import com.williamcallahan.book_recommendation_engine.types.PgVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
@Service
public class BookCacheService {
    private static final Logger logger = LoggerFactory.getLogger(BookCacheService.class);
    
    private final GoogleBooksService googleBooksService;
    private final CachedBookRepository cachedBookRepository;
    private final ObjectMapper objectMapper;
    private final WebClient embeddingClient;
    private final ConcurrentHashMap<String, Book> bookDetailCache = new ConcurrentHashMap<>(); // In-memory cache for Book objects by ID
    private final CacheManager cacheManager;
    private final DuplicateBookService duplicateBookService;
    
    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled; // This refers to the database/persistent cache primarily
    
    @Value("${app.embedding.service.url:#{null}}")
    private String embeddingServiceUrl;
    
    /**
     * Initializes the BookCacheService with required dependencies for multi-level caching, external API integration, and duplicate book management.
     *
     * If the database-backed cache repository is unavailable, the service operates in API-only mode with in-memory and Spring caching. The embedding service client is configured with a provided URL or defaults to localhost if not set.
     */
    @Autowired
    public BookCacheService(
            GoogleBooksService googleBooksService,
            ObjectMapper objectMapper,
            WebClient.Builder webClientBuilder,
            CacheManager cacheManager,
            @Autowired(required = false) CachedBookRepository cachedBookRepository,
            DuplicateBookService duplicateBookService) {
        this.googleBooksService = googleBooksService;
        this.cachedBookRepository = cachedBookRepository;
        this.objectMapper = objectMapper;
        this.embeddingClient = webClientBuilder.baseUrl(
                embeddingServiceUrl != null ? embeddingServiceUrl : "http://localhost:8080/api/embedding"
        ).build();
        this.cacheManager = cacheManager;
        this.duplicateBookService = duplicateBookService;

        // Disable cache if repository is not available
        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false;
            logger.info("Database cache is not available. Running in API-only mode.");
        }
    }
    
    /**
     * Retrieves a book by its Google Books ID using a multi-level caching strategy.
     *
     * Checks the persistent database cache first (if enabled), then fetches from the Google Books API on a cache miss. Results from the API are automatically cached for future requests.
     *
     * @param id the Google Books ID of the book to retrieve
     * @return the corresponding Book if found, or null if not available
     */
    @Cacheable(value = "books", key = "#id", unless = "#result == null")
    public Book getBookById(String id) {
        // Try to get from cache first
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> cachedBook = cachedBookRepository.findByGoogleBooksId(id);
                if (cachedBook.isPresent()) {
                    logger.info("Cache hit for book ID: {}", id);
                    return cachedBook.get().toBook();
                }
            } catch (Exception e) {
                logger.warn("Error accessing database cache for book ID {}: {}", id, e.getMessage());
            }
        }
        
        logger.info("Cache miss for book ID: {}, fetching from Google Books API", id);
        Book book = null;
        try {
            book = Mono.fromCompletionStage(googleBooksService.getBookById(id)) // Convert CompletionStage to Mono
                .onErrorResume(ex -> {
                    logger.error("Error fetching book by ID {} from GoogleBooksService: {}", id, ex.getMessage());
                    return Mono.empty(); // Return empty Mono on error
                })
                .block(Duration.ofSeconds(5)); // Block with timeout
        } catch (IllegalStateException e) { 
             logger.error("Timeout or no item fetching book by ID {} from GoogleBooksService: {}", id, e.getMessage());
        } catch (RuntimeException e) { 
            logger.error("Error fetching book by ID {} from GoogleBooksService: {}", id, e.getMessage());
        }
        
        if (book != null && cacheEnabled && cachedBookRepository != null) {
            // Asynchronously cache to database using the reactive method
            cacheBookReactive(book).subscribe(
                null,
                e -> logger.error("Error during background DB caching for book ID {}: {}", id, e.getMessage())
            );
        }

        return book;
    }
    
    /**
     * Retrieves books by ISBN using multi-level cache approach
     * 
     * @param isbn The book ISBN (supports both ISBN-10 and ISBN-13 formats)
     * @return List of books matching the ISBN
     * 
     * @implNote Handles both ISBN-10 and ISBN-13 formats with appropriate cache lookups
     * Multiple books may match a single ISBN due to different editions
     */
    public List<Book> getBooksByIsbn(String isbn) {
        // Try to get from cache first
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> cachedBook;
                if (isbn.length() == 10) {
                    cachedBook = cachedBookRepository.findByIsbn10(isbn);
                } else if (isbn.length() == 13) {
                    cachedBook = cachedBookRepository.findByIsbn13(isbn);
                } else {
                    cachedBook = Optional.empty();
                }
                
                if (cachedBook.isPresent()) {
                    logger.info("Cache hit for book ISBN: {}", isbn);
                    return Collections.singletonList(cachedBook.get().toBook());
                }
            } catch (Exception e) {
                logger.warn("Error accessing database cache for ISBN {}: {}", isbn, e.getMessage());
                // Continue to fallback instead of letting the exception bubble up
            }
        }
        
        // Fall back to Google Books API
        logger.info("Cache miss for book ISBN: {}, fetching from Google Books API", isbn);
        List<Book> books = googleBooksService.searchBooksByISBN(isbn).blockOptional().orElse(Collections.emptyList());
        
        // If found, save to cache asynchronously
        if (!books.isEmpty() && cacheEnabled && cachedBookRepository != null) {
            CompletableFuture.runAsync(() -> {
                for (Book book : books) {
                    // Asynchronously cache to database using the reactive method
                    cacheBookReactive(book).subscribe(
                        null,
                        e -> logger.error("Error during background DB caching for book ISBN {}: {}", isbn, e.getMessage())
                    );
                }
            });
        }
        
        return books;
    }
    
    /**
     * Searches for books with the specified query and caches results
     * 
     * @param query The search query string
     * @param startIndex The pagination starting index (0-based)
     * @param maxResults Maximum number of results to return
     * @return List of books matching the query with pagination applied
     * 
     * @implNote Results are cached asynchronously for future retrieval
     * Implements client-side pagination on the full result set
     */
    public List<Book> searchBooks(String query, int startIndex, int maxResults) {
        logger.info("BookCacheService searching books with query: {}, requested startIndex: {}, maxResults: {}", query, startIndex, maxResults);
        List<Book> fetchedBooks = googleBooksService.searchBooksAsyncReactive(query).blockOptional().orElse(Collections.emptyList());
        
        final List<Book> booksToCacheAndReturn;
        if (fetchedBooks.size() > maxResults && maxResults > 0) {
            booksToCacheAndReturn = fetchedBooks.subList(0, maxResults);
        } else {
            booksToCacheAndReturn = fetchedBooks;
        }

        // Cache all results asynchronously
        if (!booksToCacheAndReturn.isEmpty() && cacheEnabled && cachedBookRepository != null) {
            CompletableFuture.runAsync(() -> {
                for (Book book : booksToCacheAndReturn) {
                    // Asynchronously cache to database using the reactive method
                    cacheBookReactive(book).subscribe(
                        null,
                        e -> logger.error("Error during background DB caching for book (query {}): {}. ID: {}", query, e.getMessage(), book.getId())
                    );
                }
            });
        }
        
        return booksToCacheAndReturn;
    }
    
    /**
     * Finds books similar to the specified book using vector similarity when available
     * 
     * @param bookId The source book ID to find similar books for
     * @param count Maximum number of similar books to return
     * @return List of books similar to the specified book
     * 
     * @implNote Attempts to use vector similarity from database cache first
     * Falls back to Google Books API category/author matching if necessary
     */
    public List<Book> getSimilarBooks(String bookId, int count) {
        // First try using vector similarity if the book is in the cache and database is available
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> sourceCachedBookOpt = cachedBookRepository.findByGoogleBooksId(bookId);
                if (sourceCachedBookOpt.isPresent()) {
                    // Use vector similarity for recommendations
                    List<CachedBook> similarCachedBooks = cachedBookRepository.findSimilarBooksById(sourceCachedBookOpt.get().getId(), count);
                    if (!similarCachedBooks.isEmpty()) {
                        logger.info("Found {} similar books using vector similarity for book ID: {}", 
                                similarCachedBooks.size(), bookId);
                        return similarCachedBooks.stream()
                                .map(CachedBook::toBook)
                                .collect(Collectors.toList());
                    }
                }
            } catch (Exception e) {
                logger.warn("Error retrieving similar books from database: {}", e.getMessage());
            }
        }
        
        logger.info("No vector similarity data for book ID: {}, using GoogleBooksService category/author matching", bookId);
        // Need the Book object to call the new getSimilarBooks method
        // Adapt CompletableFuture to blocking Optional for the synchronous method
        Book sourceBook = null;
        try {
            sourceBook = Mono.fromCompletionStage(googleBooksService.getBookById(bookId)) // Convert CompletionStage to Mono
                .onErrorResume(ex -> {
                    logger.error("Error fetching source book by ID {} for similar search: {}", bookId, ex.getMessage());
                    return Mono.empty();
                })
                .block(Duration.ofSeconds(5));
        } catch (IllegalStateException e) {
            logger.error("Timeout or no item fetching source book for similar search (ID {}): {}", bookId, e.getMessage());
        } catch (RuntimeException e) {
            logger.error("Error fetching source book for similar search (ID {}): {}", bookId, e.getMessage());
        }
        if (sourceBook == null) {
            logger.warn("Source book for similar search not found (ID: {}), returning empty list.", bookId);
            return Collections.emptyList();
        }
        // Now call the corrected getSimilarBooks method
        return googleBooksService.getSimilarBooks(sourceBook).blockOptional().orElse(Collections.emptyList());
    }
    
    
    
    /**
     * Cleans expired cache entries from the in-memory cache
     * 
     * @implNote Scheduled to run daily at midnight
     * Helps prevent memory leaks and ensures fresh data
     */
    @CacheEvict(value = "books", allEntries = true)
    @Scheduled(cron = "0 0 0 * * ?") 
    public void cleanExpiredCacheEntries() {
        logger.info("Cleaning expired in-memory cache entries");
    }

    /**
     * Retrieves a book by ID using reactive cache-first approach
     * 
     * @param id The Google Books ID to look up
     * @return Mono emitting the book if found, or empty if not found
     * 
     * @implNote Implements reactive lookup through all cache layers
     * Provides non-blocking alternative to getBookById method
     */
    // @Cacheable(value = "books", key = "#id", unless = "#result == null") // Reactive caching needs different setup
    public Mono<Book> getBookByIdReactive(String id) {
        // 1. Try in-memory cache first
        Book cachedBookInMemory = bookDetailCache.get(id);
        if (cachedBookInMemory != null) {
            logger.info("In-memory cache hit for book ID: {}", id);
            return Mono.just(cachedBookInMemory);
        }

        // 2. Try database cache (if enabled)
        if (cacheEnabled && cachedBookRepository != null) {
            return Mono.fromCallable(() -> cachedBookRepository.findByGoogleBooksId(id))
                .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                .flatMap(cachedBookOpt -> {
                    if (cachedBookOpt.isPresent()) {
                        logger.info("Database cache hit for book ID: {}", id);
                        Book dbBook = cachedBookOpt.get().toBook();
                        bookDetailCache.put(id, dbBook); // Populate in-memory cache
                        return Mono.just(dbBook);
                    }
                    // Cache miss, fetch from Google Books API
                    logger.info("Database cache miss for book ID: {}, fetching from Google Books API", id);
                    return fetchFromGoogleAndUpdateCaches(id);
                })
                .onErrorResume(e -> {
                    logger.warn("Error accessing database cache for book ID {}: {}. Falling back to API.", id, e.getMessage());
                    return fetchFromGoogleAndUpdateCaches(id); // Fallback if cache read fails
                });
        }
        // Cache disabled or repository not available, fetch directly from Google & update in-memory cache
        logger.info("Persistent cache disabled or not available, fetching book ID: {} directly from Google Books API", id);
        return fetchFromGoogleAndUpdateCaches(id);
    }

    /****
     * Retrieves a book from the Google Books API by ID and updates both in-memory and database caches asynchronously.
     *
     * @param id the Google Books ID of the book to fetch
     * @return a Mono emitting the fetched Book, or empty if not found
     */
    private Mono<Book> fetchFromGoogleAndUpdateCaches(String id) {
        // googleBooksService.getBookById(id) already returns Mono<Book>
        return Mono.fromCompletionStage(googleBooksService.getBookById(id)) // Convert CompletionStage to Mono
            .flatMap(book -> {
                if (book != null) {
                    bookDetailCache.put(id, book); // Populate in-memory cache
                    if (cacheEnabled && cachedBookRepository != null) {
                        // Asynchronously cache to database
                        cacheBookReactive(book).subscribe(
                            null, 
                            e -> logger.error("Error during reactive background DB caching for book ID {}: {}", id, e.getMessage())
                        );
                    }
                }
                return Mono.justOrEmpty(book); 
            });
    }
    
    /**
     * Retrieves books by ISBN using reactive cache-first approach
     * 
     * @param isbn The book ISBN (supports both ISBN-10 and ISBN-13 formats)
     * @return Mono emitting list of books matching the ISBN
     * 
     * @implNote Provides non-blocking ISBN lookup with multi-layer cache checking
     * Automatically determines ISBN format and uses appropriate repository method
     */
    public Mono<List<Book>> getBooksByIsbnReactive(String isbn) {
        if (cacheEnabled && cachedBookRepository != null) {
            Mono<Optional<CachedBook>> cachedBookMono;
            if (isbn.length() == 10) {
                cachedBookMono = Mono.fromCallable(() -> cachedBookRepository.findByIsbn10(isbn));
            } else if (isbn.length() == 13) {
                cachedBookMono = Mono.fromCallable(() -> cachedBookRepository.findByIsbn13(isbn));
            } else {
                cachedBookMono = Mono.just(Optional.empty());
            }

            return cachedBookMono.subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                .flatMap(cachedBookOpt -> {
                    if (cachedBookOpt.isPresent()) {
                        logger.info("Cache hit for book ISBN: {}", isbn);
                        return Mono.just(Collections.singletonList(cachedBookOpt.get().toBook()));
                    }
                    logger.info("Cache miss for book ISBN: {}, fetching from Google Books API", isbn);
                    return fetchByIsbnFromGoogleAndCache(isbn);
                })
                .onErrorResume(e -> {
                    logger.warn("Error accessing database cache for ISBN {}: {}. Falling back to API.", isbn, e.getMessage());
                    return fetchByIsbnFromGoogleAndCache(isbn);
                });
        }
        logger.info("Cache disabled, fetching ISBN: {} directly from Google Books API", isbn);
        return fetchByIsbnFromGoogleAndCache(isbn);
    }

    /**
     * Fetches books by ISBN from Google Books API and updates caches
     * 
     * @param isbn The ISBN to search for (ISBN-10 or ISBN-13)
     * @return Mono emitting list of books matching the ISBN
     * 
     * @implNote Handles both ISBN-10 and ISBN-13 formats
     * Caches each matched book individually for future retrieval
     */
    private Mono<List<Book>> fetchByIsbnFromGoogleAndCache(String isbn) {
        return googleBooksService.searchBooksByISBN(isbn) // Returns Mono<List<Book>>
            .flatMap(books -> {
                List<Book> currentBooks = (books == null) ? Collections.emptyList() : books;
                if (!currentBooks.isEmpty() && cacheEnabled && cachedBookRepository != null) {
                    Flux.fromIterable(currentBooks)
                        .flatMap(book -> cacheBookReactive(book)
                            .onErrorResume(e -> {
                                logger.error("Error during reactive background caching for book (ISBN {}): {}. ID: {}", isbn, e.getMessage(), book.getId());
                                return Mono.empty(); // Continue with other books
                            }))
                        .subscribe(); // Fire and forget for the collection
                }
                return Mono.just(currentBooks);
            });
    }

    /**
     * Performs a reactive search for books matching the given query, applying pagination and caching results asynchronously.
     *
     * @param query the search query string
     * @param startIndex the starting index for pagination (0-based)
     * @param maxResults the maximum number of results to return
     * @return a Mono emitting a paginated list of books matching the query
     *
     * <p>
     * This method retrieves the full result set from the Google Books API, applies client-side pagination, and asynchronously caches the results. It delegates to the overloaded method that supports optional year filtering.
     * </p>
     */
    public Mono<List<Book>> searchBooksReactive(String query, int startIndex, int maxResults) {
        return searchBooksReactive(query, startIndex, maxResults, null);
    }
    
    /**
     * Performs a reactive search for books with optional publication year filtering, deduplication, and paginated results.
     *
     * Fetches books from the Google Books API, optionally filters by publication year, deduplicates results using canonical book detection, asynchronously caches all fetched books, and returns a paginated list of unique books.
     *
     * @param query the search query string
     * @param startIndex the starting index for pagination (0-based)
     * @param maxResults the maximum number of results to return
     * @param publishedYear if provided, filters results to books published in this year
     * @return a Mono emitting a paginated list of unique books matching the criteria
     */
    public Mono<List<Book>> searchBooksReactive(String query, int startIndex, int maxResults, Integer publishedYear) {
        logger.info("BookCacheService searching books (reactive) with query: {}, startIndex: {}, maxResults: {}, publishedYear: {}", query, startIndex, maxResults, publishedYear);

        String finalQuery = query; 
        if (publishedYear != null) {
            logger.info("Year filter {} will be applied post-query to results for query: '{}'", publishedYear, query);
        }
        
        int resultsToRequestFromGoogle;
        String orderByGoogle = "newest"; // Default to newest for all searches from BookCacheService

        if (publishedYear != null) {
            resultsToRequestFromGoogle = 200; // Request more if year filtering
            logger.info("Requesting {} initial results from GoogleBooksService, ordered by '{}', due to year filter.", resultsToRequestFromGoogle, orderByGoogle);
        } else {
            resultsToRequestFromGoogle = maxResults; // Standard request size if no year filter
            logger.info("Requesting {} initial results from GoogleBooksService, ordered by '{}' (no year filter).", resultsToRequestFromGoogle, orderByGoogle);
        }

        return googleBooksService.searchBooksAsyncReactive(finalQuery, null, resultsToRequestFromGoogle, orderByGoogle)
            .flatMap((List<Book> fetchedBooks) -> { // Explicitly type fetchedBooks
                if (fetchedBooks == null || fetchedBooks.isEmpty()) {
                    logger.info("No books found by GoogleBooksService for query: {}", query);
                    return Mono.just(Collections.<Book>emptyList()); // Explicitly typed empty list
                }
                logger.info("GoogleBooksService returned {} books for query: {}", fetchedBooks.size(), query);

                // Apply year filtering if needed
                List<Book> yearFilteredBooks = fetchedBooks;
                if (publishedYear != null) {
                    logger.info("YEAR FILTER: Starting year filtering for {} books with year={}", fetchedBooks.size(), publishedYear);
                    yearFilteredBooks = fetchedBooks.stream()
                        .filter(book -> {
                            if (book.getPublishedDate() != null) {
                                java.util.Calendar cal = java.util.Calendar.getInstance();
                                cal.setTime(book.getPublishedDate());
                                int bookYear = cal.get(java.util.Calendar.YEAR);
                                boolean matches = bookYear == publishedYear;
                                
                                // For debugging, log details about each book's year
                                if (matches) {
                                    logger.debug("YEAR FILTER: Book '{}' (ID: {}) MATCHES year {}", 
                                            book.getTitle(), book.getId(), publishedYear);
                                } else {
                                    logger.debug("YEAR FILTER: Book '{}' (ID: {}) has year {} which does NOT match {}",
                                            book.getTitle(), book.getId(), bookYear, publishedYear);
                                }
                                
                                return matches;
                            }
                            logger.debug("YEAR FILTER: Book '{}' (ID: {}) has NO publish date, excluding",
                                    book.getTitle(), book.getId());
                            return false; // No published date means we can't verify year
                        })
                        .collect(Collectors.toList());
                    logger.info("YEAR FILTER: Year filter {} reduced {} books to {}", 
                            publishedYear, fetchedBooks.size(), yearFilteredBooks.size());
                }
                
                // Deduplication step
                Map<String, Book> canonicalBooksMap = new LinkedHashMap<>(); // Preserve order somewhat
                
                for (Book book : yearFilteredBooks) {
                    if (book == null || book.getId() == null) {
                        logger.debug("Skipping a null book or book with null ID in search results for query: {}", query);
                        continue;
                    }
                
                    Optional<CachedBook> canonicalCachedOpt = duplicateBookService.findPrimaryCanonicalBook(book);
                    String representativeId;
                
                    if (canonicalCachedOpt.isPresent()) {
                        CachedBook canonicalCached = canonicalCachedOpt.get();
                        // Prefer GoogleBooksId if available as it's more universal, otherwise fallback to internal ID.
                        representativeId = canonicalCached.getGoogleBooksId() != null ? canonicalCached.getGoogleBooksId() : canonicalCached.getId();
                        // If the canonical book is found, we prefer to use its representation if it's the first time.
                        // However, the current 'book' from 'fetchedBooks' might have fresher API data initially.
                        // For now, we map to the canonical ID and take the first 'Book' object encountered for that ID.
                        // If 'book' itself is the canonical or becomes the canonical, 'representativeId' will be 'book.getId()'.
                    } else {
                        representativeId = book.getId(); // Use its own ID if no existing canonical found
                    }
                
                    if (!canonicalBooksMap.containsKey(representativeId)) {
                        canonicalBooksMap.put(representativeId, book); 
                    } else {
                        // If already present, we could add merging logic here if 'book' is better.
                        // For now, first-encountered for a canonical ID wins.
                        logger.debug("Skipping duplicate book {} (maps to canonical ID {}) in search results for query: {}", book.getId(), representativeId, query);
                    }
                }
                
                List<Book> deduplicatedBooks = new ArrayList<>(canonicalBooksMap.values());
                logger.info("Deduplicated {} fetched books down to {} unique canonical books for query: {}", fetchedBooks.size(), deduplicatedBooks.size(), query);

                // Asynchronously cache all *original* fetched books that have an ID
                // The caching logic itself handles merging into canonical entries in the DB.
                Flux.fromIterable(fetchedBooks) // Still cache all raw books from original fetch
                    .filter(b -> b != null && b.getId() != null) // Ensure book and ID are not null
                    .flatMap(this::cacheBookReactive) // Using the reactive cache method
                    .doOnError(e -> logger.error("Error during background DB caching for books from query '{}': {}", query, e.getMessage()))
                    .subscribe(); // Subscribe to trigger the caching

                // Apply pagination to the *deduplicated* list
                int fromIndex = Math.min(startIndex, deduplicatedBooks.size());
                int toIndex = Math.min(startIndex + maxResults, deduplicatedBooks.size());
                
                List<Book> paginatedBooks = deduplicatedBooks.subList(fromIndex, toIndex);
                logger.info("Returning {} paginated books ({} to {}) for query: {}", paginatedBooks.size(), fromIndex, toIndex, query);
                
                return Mono.just(paginatedBooks);
            })
            .onErrorResume(e -> {
                logger.error("Error in searchBooksReactive for query '{}': {}", query, e.getMessage(), e);
                return Mono.just(new ArrayList<Book>()); // Return new empty ArrayList<Book> on error
            });
    }
    
    /**
     * Finds books similar to the specified book using reactive vector similarity
     * 
     * @param bookId The source book ID to find similar books for
     * @param count Maximum number of similar books to return
     * @return Mono emitting list of books similar to the specified book
     * 
     * @implNote Provides non-blocking version of similarity search with fallbacks
     * Prioritizes vector similarity when available and falls back to API-based similarity
     */
    public Mono<List<Book>> getSimilarBooksReactive(String bookId, int count) {
        if (cacheEnabled && cachedBookRepository != null) {
            return Mono.fromCallable(() -> cachedBookRepository.findByGoogleBooksId(bookId))
                .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                .flatMap(sourceCachedBookOpt -> {
                    if (sourceCachedBookOpt.isPresent()) {
                        return Mono.fromCallable(() -> cachedBookRepository.findSimilarBooksById(sourceCachedBookOpt.get().getId(), count))
                            .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                            .flatMap(similarCachedBooks -> {
                                if (!similarCachedBooks.isEmpty()) {
                                    logger.info("Found {} similar books using vector similarity for book ID: {}", similarCachedBooks.size(), bookId);
                                    List<Book> books = similarCachedBooks.stream()
                                        .map(CachedBook::toBook)
                                        .collect(Collectors.toList());
                                    return Mono.just(books);
                                }
                                // If vector search yields nothing, fall through to Google logic by returning empty here, 
                                // which will trigger switchIfEmpty on this specific path.
                                return Mono.empty(); 
                            })
                            .switchIfEmpty(fallbackToGoogleSimilarBooks(bookId, count)); // Fallback if DB vector search empty
                    }
                    // Source book not in DB cache, fall back to Google logic
                    return fallbackToGoogleSimilarBooks(bookId, count);
                })
                .onErrorResume(e -> {
                    logger.warn("Error retrieving similar books from database for book ID {}: {}. Falling back to Google Books Service.", bookId, e.getMessage());
                    return fallbackToGoogleSimilarBooks(bookId, count);
                });
        }
        // Cache disabled, go directly to Google logic
        return fallbackToGoogleSimilarBooks(bookId, count);
    }

    /**
     * Retrieves similar books for a given book ID using the Google Books API as a fallback.
     *
     * Attempts to find similar books by matching categories and authors when vector similarity search in the database fails or yields no results.
     *
     * @param bookId the ID of the source book to find similar books for
     * @param count the maximum number of similar books to return
     * @return a Mono emitting a list of similar books, or an empty list if the source book is not found
     */
    private Mono<List<Book>> fallbackToGoogleSimilarBooks(String bookId, int count) {
        logger.info("Falling back to GoogleBooksService for similar books for ID: {} (or vector search yielded no results/source not in DB cache)", bookId);
        // googleBooksService.getBookById(bookId) already returns Mono<Book>
        return Mono.fromCompletionStage(googleBooksService.getBookById(bookId)) // Convert CompletionStage to Mono
            .flatMap(sourceBook -> {
                if (sourceBook == null) {
                    logger.warn("Source book for similar search (Google fallback) not found (ID: {}), returning empty list.", bookId);
                    return Mono.just(Collections.<Book>emptyList());
                }
                return googleBooksService.getSimilarBooks(sourceBook) // This returns Mono<List<Book>>
                    .map(list -> (List<Book>) list); // Explicit map to help compiler with type, though often not needed if signatures are clear.
            })
            .switchIfEmpty(Mono.<List<Book>>defer(() -> {
                 logger.warn("GoogleBooksService.getBookById returned empty for ID {} during similar books fallback.", bookId);
                 return Mono.just(Collections.<Book>emptyList());
            }));
    }

    /**
     * Generates vector embedding for a book to enable semantic similarity search
     * 
     * @param book The book to generate embedding for
     * @return Mono emitting float array containing the embedding vector
     * 
     * @implNote Uses book metadata (title, authors, description, categories) to create text
     * Calls external embedding service when available, falls back to placeholder implementation
     */
    private Mono<float[]> generateEmbeddingReactive(Book book) {
        try {
            StringBuilder textBuilder = new StringBuilder();
            textBuilder.append(book.getTitle() != null ? book.getTitle() : "").append(" ");
            
            if (book.getAuthors() != null && !book.getAuthors().isEmpty()) {
                textBuilder.append(String.join(" ", book.getAuthors())).append(" ");
            }
            if (book.getDescription() != null) {
                textBuilder.append(book.getDescription()).append(" ");
            }
            if (book.getCategories() != null && !book.getCategories().isEmpty()) {
                textBuilder.append(String.join(" ", book.getCategories()));
            }
            String text = textBuilder.toString().trim();

            if (text.isEmpty()) {
                logger.warn("Cannot generate embedding for book ID {} as constructed text is empty.", book.getId());
                return Mono.just(new float[384]); // Return zero vector
            }

            if (embeddingServiceUrl != null) {
                return embeddingClient.post()
                    .bodyValue(Collections.singletonMap("text", text))
                    .retrieve()
                    .bodyToMono(float[].class)
                    .onErrorResume(e -> {
                        logger.warn("Error generating embedding from service for book ID {}: {}. Falling back to placeholder.", book.getId(), e.getMessage());
                        return Mono.just(createPlaceholderEmbedding(text));
                    });
            }
            return Mono.just(createPlaceholderEmbedding(text));
        } catch (Exception e) {
            logger.error("Unexpected error in generateEmbeddingReactive for book ID {}: {}", book.getId(), e.getMessage(), e);
            return Mono.just(new float[384]); // Fallback to zero vector
        }
    }

    /**
     * Creates a deterministic placeholder embedding when embedding service unavailable
     * 
     * @param text The text to generate placeholder embedding for
     * @return Float array containing deterministic pseudo-embedding based on text hash
     * 
     * @implNote Uses text hash with sine function to create repeatable unique vectors
     * Ensures consistency for the same text input across multiple calls
     */
    private float[] createPlaceholderEmbedding(String text) {
        float[] placeholder = new float[384];
        if (text == null || text.isEmpty()) return placeholder; // Should not happen if checked before
        int hash = text.hashCode();
        for (int i = 0; i < placeholder.length; i++) {
            placeholder[i] = (float) Math.sin(hash * (i + 1) / 100.0);
        }
        return placeholder;
    }

    /**
     * Reactively caches a book in the database, handling duplicate detection and embedding generation.
     *
     * If a primary canonical cached book exists, merges new data if it improves the existing entry and updates the cache.
     * If no primary is found, checks for existing cache entry by Google Books ID to avoid duplicates, generates an embedding vector, and saves the new book.
     * All database operations are performed on a bounded elastic scheduler to prevent blocking.
     *
     * @param book The book to cache.
     * @return A Mono that completes when the caching operation finishes or if no action is needed.
     */
    private Mono<Void> cacheBookReactive(Book book) {
        if (!cacheEnabled || cachedBookRepository == null || book == null || book.getId() == null) {
            return Mono.empty();
        }

        // Attempt to find a primary/canonical book for the new book from API
        Optional<CachedBook> primaryBookOpt = duplicateBookService.findPrimaryCanonicalBook(book);

        if (primaryBookOpt.isPresent()) {
            CachedBook primaryCachedBook = primaryBookOpt.get();
            logger.info("Found existing primary book (ID: {}) for new book (Title: {}). Will not create new cache entry. May merge data.", 
                        primaryCachedBook.getId(), book.getTitle());
            
            boolean updated = duplicateBookService.mergeDataIfBetter(primaryCachedBook, book);
            if (updated) {
                 return Mono.fromCallable(() -> cachedBookRepository.save(primaryCachedBook))
                    .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                    .doOnSuccess(savedBook -> logger.info("Updated primary cached book ID: {} with data from new book.", savedBook.getId()))
                    .doOnError(e -> logger.error("Error updating primary cached book ID {}: {}", primaryCachedBook.getId(), e.getMessage()))
                    .then();
            } else {
                // No data merged, primary book remains as is. No save needed.
                logger.debug("No data from new book was merged into primary book ID: {}.", primaryCachedBook.getId());
                return Mono.empty(); // Nothing to save
            }
        } else {
            // No primary book found, proceed to cache the new book as a new entry
            logger.debug("No existing primary book found for new book (Title: {}). Caching as new entry.", book.getTitle());
            return Mono.defer(() ->
                Mono.fromCallable(() -> cachedBookRepository.findByGoogleBooksId(book.getId())) // Check if this specific ID already exists (e.g. race condition)
                    .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                    .flatMap(existingOpt -> {
                        if (existingOpt.isPresent()) {
                            logger.warn("Book with Google ID {} already in cache. Skipping save for new book object.", book.getId());
                            return Mono.empty(); // Already exists, do nothing
                        }

                        // Generate embedding for the book
                        return generateEmbeddingReactive(book)
                            .flatMap(embedding -> {
                                try {
                                    JsonNode rawJsonNode = objectMapper.readTree(book.getRawJsonResponse());
                                    CachedBook cachedBookToSave = CachedBook.fromBook(book, rawJsonNode, new PgVector(embedding));
                                    return Mono.fromCallable(() -> cachedBookRepository.save(cachedBookToSave))
                                        .doOnSuccess(savedBook -> logger.info("Successfully cached new book ID: {} Title: {}", savedBook.getId(), savedBook.getTitle()))
                                        .doOnError(e -> logger.error("Error saving book ID {} to cache: {}", book.getId(), e.getMessage()));
                                } catch (Exception e) {
                                    logger.error("Error processing raw JSON or creating CachedBook for ID {}: {}", book.getId(), e.getMessage());
                                    return Mono.error(e);
                                }
                            })
                            .onErrorResume(e -> {
                                logger.error("Error generating embedding or during caching for book ID {}: {}", book.getId(), e.getMessage());
                                // Decide if you want to cache without embedding or just log and complete
                                return Mono.empty(); // Or Mono.error(e) if failure should propagate
                            });
                    })
                    .then() // Converts the Mono<CachedBook> or Mono<Object> to Mono<Void>
            );
        }
    }

    /**
     * Handles book cover update events by invalidating cached entries
     * 
     * @param event The event containing information about the updated book cover
     * 
     * @implNote Removes book from all cache layers when cover image is updated
     * Prevents stale cover image URLs from being served to clients
     */
    @EventListener
    public void handleBookCoverUpdate(BookCoverUpdatedEvent event) {
        if (event.getGoogleBookId() == null) {
            logger.warn("BookCoverUpdatedEvent received with null googleBookId. Cannot process cache eviction.");
            return;
        }
        String bookId = event.getGoogleBookId();

        // Clear from Spring's cache if it holds Book objects directly
        Optional.ofNullable(cacheManager.getCache("books")).ifPresent(c -> {
            logger.debug("Evicting book ID {} from Spring 'books' cache due to cover update.", bookId);
            c.evict(bookId);
        });
        // Clear from local ConcurrentHashMap
        if (bookDetailCache.containsKey(bookId)) {
            bookDetailCache.remove(bookId);
            logger.debug("Removed book ID {} from local bookDetailCache due to cover update.", bookId);
        }
        // The database cache (CachedBook) would ideally be updated with the new URL if that event also carries it,
        // or this event primarily serves to invalidate other caches that might hold the old URL.
        // If CachedBookRepository.save() is called elsewhere with the updated Book object, that handles persistence.
        logger.info("Processed cache invalidations for book ID {} due to BookCoverUpdatedEvent.", bookId);
    }

    /**
     * Retrieves all distinct Google Books IDs from the persistent cache
     * 
     * @return Set of unique Google Books IDs for sitemap generation
     * 
     * @implNote Returns empty set if caching is disabled or repository unavailable
     * Used primarily for sitemap generation and bulk operations
     */
    public java.util.Set<String> getAllCachedBookIds() {
        // Prioritize persistent repository if available and enabled
        if (cacheEnabled && cachedBookRepository != null && 
            !(cachedBookRepository instanceof com.williamcallahan.book_recommendation_engine.repository.NoOpCachedBookRepository)) {
            try {
                logger.info("Fetching all distinct Google Books IDs from active persistent CachedBookRepository.");
                java.util.Set<String> ids = cachedBookRepository.findAllDistinctGoogleBooksIds();
                logger.info("Retrieved {} distinct book IDs from persistent repository for sitemap.", ids != null ? ids.size() : 0);
                return ids != null ? ids : Collections.emptySet();
            } catch (Exception e) {
                logger.error("Error fetching all distinct Google Books IDs from persistent repository: {}", e.getMessage(), e);
                // Fall through to in-memory cache if persistent fetch fails, or return empty if preferred.
                // For now, let's fall through to ensure some IDs might still be available for sitemap.
            }
        }

        // If persistent repository is not used, or failed, try the in-memory ConcurrentHashMap
        if (!bookDetailCache.isEmpty()) {
            logger.info("Persistent repository not used or failed; fetching IDs from in-memory bookDetailCache ({} items).", bookDetailCache.size());
            // Return a new HashSet to avoid concurrent modification issues if the cache is modified elsewhere
            return new java.util.HashSet<>(bookDetailCache.keySet());
        }
        
        logger.warn("No active persistent cache and in-memory bookDetailCache is empty. Cannot provide book IDs for sitemap.");
        return Collections.emptySet();
    }
}
