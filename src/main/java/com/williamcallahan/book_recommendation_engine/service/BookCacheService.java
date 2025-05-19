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
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Objects;

@Service
public class BookCacheService {
    private static final Logger logger = LoggerFactory.getLogger(BookCacheService.class);
    private static final String BOOK_SEARCH_RESULTS_CACHE_NAME = "bookSearchResults";

    private final GoogleBooksService googleBooksService; 
    private final CachedBookRepository cachedBookRepository;
    private final ObjectMapper objectMapper;
    private final WebClient embeddingClient;
    private final ConcurrentHashMap<String, Book> bookDetailCache = new ConcurrentHashMap<>();
    private final CacheManager cacheManager;
    private final DuplicateBookService duplicateBookService;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final GoogleBooksCachingStrategy googleBooksCachingStrategy;
    
    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled;
    
    @Value("${app.embedding.service.url:#{null}}")
    private String embeddingServiceUrl;
    
    @Autowired
    public BookCacheService(
            GoogleBooksService googleBooksService,
            ObjectMapper objectMapper,
            WebClient.Builder webClientBuilder,
            CacheManager cacheManager,
            @Autowired(required = false) CachedBookRepository cachedBookRepository,
            DuplicateBookService duplicateBookService,
            BookDataOrchestrator bookDataOrchestrator, 
            GoogleBooksCachingStrategy googleBooksCachingStrategy) { 
        this.googleBooksService = googleBooksService;
        this.cachedBookRepository = cachedBookRepository;
        this.objectMapper = objectMapper;
        this.embeddingClient = webClientBuilder.baseUrl(
                embeddingServiceUrl != null ? embeddingServiceUrl : "http://localhost:8080/api/embedding"
        ).build();
        this.cacheManager = cacheManager;
        this.duplicateBookService = duplicateBookService;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.googleBooksCachingStrategy = googleBooksCachingStrategy;

        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false;
            logger.info("Database cache is not available. Running in API-only mode.");
        }
    }
    
    /**
     * Retrieves a book by its Google Books ID using multi-level cache approach
     * 
     * @param id The Google Books ID to look up
     * @return The book if found, null otherwise
     * 
     * @implNote First checks in-memory cache, then database cache, finally calls Google Books API
     * Automatically caches results from API calls for future use
     */
    @Cacheable(value = "books", key = "#id", unless = "#result == null")
    public Book getBookById(String id) {
        logger.debug("BookCacheService.getBookById (sync) called for ID: {}", id);
        try {
            return getBookByIdReactive(id).block(Duration.ofSeconds(10));
        } catch (Exception e) {
            logger.error("Error in synchronous getBookById for ID {}: {}", id, e.getMessage(), e);
            return null;
        }
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
        logger.debug("BookCacheService.getBooksByIsbn (sync) called for ISBN: {}", isbn);
        return getBooksByIsbnReactive(isbn).blockOptional().orElse(Collections.emptyList());
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
        logger.info("BookCacheService.searchBooks (sync) with query: {}, startIndex: {}, maxResults: {}", query, startIndex, maxResults);
        return searchBooksReactive(query, startIndex, maxResults, null, null, "relevance")
                                    .blockOptional().orElse(Collections.emptyList());
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
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> sourceCachedBookOpt = cachedBookRepository.findByGoogleBooksId(bookId);
                if (sourceCachedBookOpt.isPresent()) {
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
        Book sourceBook = getBookByIdReactive(bookId).block(Duration.ofSeconds(5));

        if (sourceBook == null) {
            logger.warn("Source book for similar search not found (ID: {}), returning empty list.", bookId);
            return Collections.emptyList();
        }
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
    public Mono<Book> getBookByIdReactive(String id) {
        Book cachedBookInMemory = bookDetailCache.get(id);
        if (cachedBookInMemory != null) {
            logger.info("In-memory cache hit for book ID: {}", id);
            return Mono.just(cachedBookInMemory);
        }

        logger.info("In-memory cache miss for book ID: {}, delegating to GoogleBooksCachingStrategy.", id);
        return googleBooksCachingStrategy.getReactive(id)
            .doOnSuccess(book -> {
                if (book != null) {
                    bookDetailCache.put(id, book); 
                    logger.info("BookCacheService.getBookByIdReactive: Successfully retrieved book ID {} via strategy.", id);
                } else {
                    logger.info("BookCacheService.getBookByIdReactive: Book ID {} not found via strategy.", id);
                }
            })
            .onErrorResume(e -> {
                logger.error("Error in BookCacheService.getBookByIdReactive for ID {} from strategy: {}", id, e.getMessage(), e);
                return Mono.empty();
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
                logger.warn("Invalid ISBN format: {}", isbn);
                return Mono.just(Collections.emptyList());
            }

            return cachedBookMono.subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                .flatMap(cachedBookOpt -> {
                    if (cachedBookOpt.isPresent()) {
                        logger.info("DB Cache hit for book ISBN: {}", isbn);
                        return Mono.just(Collections.singletonList(cachedBookOpt.get().toBook()));
                    }
                    logger.info("DB Cache miss for book ISBN: {}, fetching via searchBooksReactive.", isbn);
                    return searchBooksReactive("isbn:" + isbn, 0, 5, null, null, "relevance");
                })
                .onErrorResume(e -> {
                    logger.warn("Error accessing database cache for ISBN {}: {}. Falling back to searchBooksReactive.", isbn, e.getMessage());
                    return searchBooksReactive("isbn:" + isbn, 0, 5, null, null, "relevance");
                });
        }
        logger.info("DB Cache disabled, fetching ISBN: {} via searchBooksReactive.", isbn);
        return searchBooksReactive("isbn:" + isbn, 0, 5, null, null, "relevance");
    }

    /**
     * Searches for books with reactive API and result caching, including language and order parameters
     * 
     * @param query The search query string
     * @param startIndex The pagination starting index (0-based)
     * @param maxResults Maximum number of results to return
     * @param publishedYear Optional filter for the published year
     * @param langCode Optional language code (e.g., "en", "fr")
     * @param orderBy Optional sort order (e.g., "relevance", "newest")
     * @return Mono emitting list of books matching the query with pagination applied
     * 
     * @implNote Implements caching of search results by query parameters
     * Supports client-side filtering and pagination
     */
    public Mono<List<Book>> searchBooksReactive(String query, int startIndex, int maxResults, Integer publishedYear, String langCode, String orderBy) {
        String cacheKey = generateSearchCacheKey(query, startIndex, maxResults, publishedYear, langCode, orderBy);
        org.springframework.cache.Cache searchCache = this.cacheManager.getCache(BOOK_SEARCH_RESULTS_CACHE_NAME);

        if (searchCache != null) {
            @SuppressWarnings("unchecked")
            List<String> cachedBookIds = (List<String>) searchCache.get(cacheKey, List.class);
            if (cachedBookIds != null) {
                logger.info("Search cache HIT for key: {}", cacheKey);
                if (cachedBookIds.isEmpty()) {
                    return Mono.just(Collections.emptyList());
                }
                return Flux.fromIterable(cachedBookIds)
                           .flatMap(this::getBookByIdReactive)
                           .filter(Objects::nonNull)
                           .collectList();
            }
        }

        logger.info("Search cache MISS for key: {}. Delegating to BookDataOrchestrator.", cacheKey);
        
        final String effectiveOrderBy = (orderBy != null && !orderBy.trim().isEmpty()) ? orderBy : "relevance";
        int orchestratorDesiredResults = (publishedYear != null) ? 200 : Math.max(40, startIndex + maxResults * 2);
        orchestratorDesiredResults = Math.min(orchestratorDesiredResults, 200);

        return this.bookDataOrchestrator.searchBooksTiered(query, langCode, orchestratorDesiredResults, effectiveOrderBy)
            .flatMap(fetchedBooksGlobalList -> {
                List<Book> booksToProcess = (fetchedBooksGlobalList == null) ? Collections.emptyList() : fetchedBooksGlobalList;
                logger.debug("Fetched {} books from BookDataOrchestrator for query '{}' before local filtering/pagination.", booksToProcess.size(), query);

                List<Book> yearFilteredBooks = booksToProcess;
                if (publishedYear != null) {
                    yearFilteredBooks = booksToProcess.stream()
                        .filter(book -> {
                            if (book != null && book.getPublishedDate() != null) {
                                Calendar cal = Calendar.getInstance();
                                cal.setTime(book.getPublishedDate());
                                return cal.get(Calendar.YEAR) == publishedYear;
                            }
                            return false;
                        })
                        .collect(Collectors.toList());
                    logger.debug("Filtered by year {}: {} books remaining.", publishedYear, yearFilteredBooks.size());
                }

                List<Book> finalPaginatedBooks;
                if (yearFilteredBooks.isEmpty()) {
                    finalPaginatedBooks = Collections.emptyList();
                } else {
                    int from = Math.min(startIndex, yearFilteredBooks.size());
                    int to = Math.min(startIndex + maxResults, yearFilteredBooks.size());
                    finalPaginatedBooks = (from >= to) ? Collections.emptyList() : yearFilteredBooks.subList(from, to);
                }
                logger.debug("Paginated to {} books for query '{}'.", finalPaginatedBooks.size(), query);

                List<String> bookIdsToCacheForThisSearch = finalPaginatedBooks.stream()
                                                              .map(Book::getId)
                                                              .filter(Objects::nonNull)
                                                              .collect(Collectors.toList());
                
                if (searchCache != null && this.cacheEnabled) {
                    searchCache.put(cacheKey, bookIdsToCacheForThisSearch);
                    logger.info("Cached search key '{}' with {} book IDs.", cacheKey, bookIdsToCacheForThisSearch.size());
                }
                
                Mono<Void> cacheIndividualBooksMono = Flux.fromIterable(booksToProcess)
                        .filter(book -> book != null && book.getId() != null)
                        .flatMap(this::cacheBookReactive)
                        .then(); 
                
                return cacheIndividualBooksMono.thenReturn(finalPaginatedBooks);
            })
            .onErrorResume(e -> {
                logger.error("Error during searchBooksReactive for query '{}' (key {}): {}. Returning empty list.", query, cacheKey, e.getMessage(), e);
                if (searchCache != null && this.cacheEnabled) {
                    searchCache.put(cacheKey, Collections.emptyList()); 
                }
                return Mono.just(Collections.emptyList());
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
                                    return Mono.just(similarCachedBooks.stream().map(CachedBook::toBook).collect(Collectors.toList()));
                                }
                                return Mono.<List<Book>>empty(); 
                            })
                            .switchIfEmpty(fallbackToGoogleSimilarBooksViaOrchestrator(bookId, count));
                    }
                    return fallbackToGoogleSimilarBooksViaOrchestrator(bookId, count);
                })
                .onErrorResume(e -> {
                    logger.warn("Error retrieving similar books from database for book ID {}: {}. Falling back to orchestrator.", bookId, e.getMessage());
                    return fallbackToGoogleSimilarBooksViaOrchestrator(bookId, count);
                });
        }
        return fallbackToGoogleSimilarBooksViaOrchestrator(bookId, count);
    }

    /**
     * Falls back to Google Books API for similar book recommendations
     * 
     * @param bookId The source book ID to find similar books for
     * @param count Maximum number of similar books to return
     * @return Mono emitting list of similar books
     * 
     * @implNote Called when database similarity search fails or returns no results
     * Uses category and author matching instead of vector similarity
     */
    private Mono<List<Book>> fallbackToGoogleSimilarBooksViaOrchestrator(String bookId, int count) {
        logger.info("Falling back to GoogleBooksService (via Orchestrator) for similar books for ID: {}", bookId);
        return getBookByIdReactive(bookId)
            .flatMap(sourceBook -> {
                if (sourceBook == null) {
                    logger.warn("Source book for similar search (Google fallback) not found (ID: {}), returning empty list.", bookId);
                    return Mono.just(Collections.<Book>emptyList());
                }
                return this.googleBooksService.getSimilarBooks(sourceBook); 
            })
            .switchIfEmpty(Mono.fromSupplier(() -> {
                 logger.warn("getBookByIdReactive returned empty for ID {} during similar books fallback.", bookId);
                 return Collections.<Book>emptyList();
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
                return Mono.just(new float[384]);
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
            return Mono.just(new float[384]);
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
        if (text == null || text.isEmpty()) return placeholder;
        int hash = text.hashCode();
        for (int i = 0; i < placeholder.length; i++) {
            placeholder[i] = (float) Math.sin(hash * (i + 1) / 100.0);
        }
        return placeholder;
    }

    /**
     * Caches a book in the database using reactive approach
     * 
     * @param book The book to cache in the database
     * @return Mono completing when caching operation completes or error occurs
     * 
     * @implNote Checks if book already exists in cache before saving
     * Generates embedding vector for similarity search functionality
     * Executes database operations on bounded elastic scheduler to avoid blocking
     */
    private Mono<Void> cacheBookReactive(Book book) {
        if (!cacheEnabled || cachedBookRepository == null || book == null || book.getId() == null) {
            return Mono.empty();
        }

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
                logger.debug("No data from new book was merged into primary book ID: {}.", primaryCachedBook.getId());
                return Mono.empty();
            }
        } else {
            logger.debug("No existing primary book found for new book (Title: {}). Caching as new entry.", book.getTitle());
            return Mono.defer(() ->
                Mono.fromCallable(() -> cachedBookRepository.findByGoogleBooksId(book.getId()))
                    .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                    .flatMap(existingOpt -> {
                        if (existingOpt.isPresent()) {
                            logger.warn("Book with Google ID {} already in cache. Skipping save for new book object.", book.getId());
                            return Mono.empty();
                        }
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
                                return Mono.empty();
                            });
                    })
                    .then()
            );
        }
    }

    /**
     * Handles book cover update events by updating cached entries with new cover URL
     * 
     * @param event The event containing information about the updated book cover
     * 
     * @implNote Updates book in all cache layers when cover image is updated,
     * rather than just invalidating for more efficient cache propagation
     */
    @EventListener
    public void handleBookCoverUpdate(BookCoverUpdatedEvent event) {
        if (event.getGoogleBookId() == null) {
            logger.warn("BookCoverUpdatedEvent received with null googleBookId.");
            return;
        }
        String bookId = event.getGoogleBookId();
        String newCoverUrl = event.getNewCoverUrl();
        if (newCoverUrl == null || newCoverUrl.isEmpty()) {
            logger.warn("BookCoverUpdatedEvent received with null/empty cover URL for book ID {}.", bookId);
            return;
        }
        Book cachedBook = bookDetailCache.get(bookId);
        if (cachedBook != null) {
            cachedBook.setCoverImageUrl(newCoverUrl);
            logger.debug("Updated cover URL in memory cache for book ID: {}", bookId);
        } else {
            logger.debug("Book ID {} not found in memory cache, will be refreshed on next request", bookId);
        }
        if (cacheEnabled && cachedBookRepository != null) {
            Mono.fromCallable(() -> cachedBookRepository.findByGoogleBooksId(bookId))
                .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                .flatMap(optionalCachedBook -> {
                    if (optionalCachedBook.isPresent()) {
                        CachedBook dbBook = optionalCachedBook.get();
                        dbBook.setCoverImageUrl(newCoverUrl);
                        return Mono.fromCallable(() -> cachedBookRepository.save(dbBook));
                    }
                    return Mono.empty();
                })
                .subscribe(
                    updatedBook -> logger.info("Updated cover URL in database for book ID: {}", bookId),
                    error -> logger.error("Error updating cover URL in database for book ID {}: {}", bookId, error.getMessage())
                );
        }
        Optional.ofNullable(cacheManager.getCache("books")).ifPresent(c -> {
            logger.debug("Evicting book ID {} from Spring 'books' cache due to cover update.", bookId);
            c.evict(bookId);
        });
        logger.info("Processed cache updates for book ID {} due to BookCoverUpdatedEvent.", bookId);
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
        if (cacheEnabled && cachedBookRepository != null && 
            !(cachedBookRepository instanceof com.williamcallahan.book_recommendation_engine.repository.NoOpCachedBookRepository)) {
            try {
                logger.info("Fetching all distinct Google Books IDs from active persistent CachedBookRepository.");
                java.util.Set<String> ids = cachedBookRepository.findAllDistinctGoogleBooksIds();
                logger.info("Retrieved {} distinct book IDs from persistent repository for sitemap.", ids != null ? ids.size() : 0);
                return ids != null ? ids : Collections.emptySet();
            } catch (Exception e) {
                logger.error("Error fetching all distinct Google Books IDs from persistent repository: {}", e.getMessage(), e);
            }
        }
        if (!bookDetailCache.isEmpty()) {
            logger.info("Persistent repository not used or failed; fetching IDs from in-memory bookDetailCache ({} items).", bookDetailCache.size());
            return new java.util.HashSet<>(bookDetailCache.keySet());
        }
        logger.warn("No active persistent cache and in-memory bookDetailCache is empty. Cannot provide book IDs for sitemap.");
        return Collections.emptySet();
    }

    /**
     * Checks if a book is present in any of the cache layers
     *
     * @param id The Google Books ID to check
     * @return true if the book is found in any cache, false otherwise
     * 
     * @implNote Checks in-memory cache, Spring cache, and database cache in sequence
     */
    public boolean isBookInCache(String id) {
        if (id == null || id.isEmpty()) return false;
        if (bookDetailCache.containsKey(id)) return true;
        org.springframework.cache.Cache booksCache = cacheManager.getCache("books");
        if (booksCache != null && booksCache.get(id) != null) return true;
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                return cachedBookRepository.findByGoogleBooksId(id).isPresent();
            } catch (Exception e) {
                logger.warn("Error checking persistent cache for book ID {}: {}", id, e.getMessage());
            }
        }
        return false;
    }
    
    /**
     * Retrieves a book from the cache layers only. Does not fall back to API.
     * Used primarily for testing and specific cache-only scenarios.
     *
     * @param id The Google Books ID to look up in the cache
     * @return Optional containing the book if found in any cache, empty otherwise
     * @throws IllegalArgumentException if id is null or empty
     * 
     * @implNote Checks all cache layers but does not call external APIs
     */
    public Optional<Book> getCachedBook(String id) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Book ID cannot be null or empty for getCachedBook");
        }
        Book book = bookDetailCache.get(id);
        if (book != null) return Optional.of(book);
        org.springframework.cache.Cache booksSpringCache = cacheManager.getCache("books");
        if (booksSpringCache != null) {
            Book cachedValue = booksSpringCache.get(id, Book.class);
            if (cachedValue != null) return Optional.of(cachedValue);
        }
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                return cachedBookRepository.findByGoogleBooksId(id).map(CachedBook::toBook);
            } catch (Exception e) {
                logger.warn("Error reading from persistent cache for getCachedBook (ID {}): {}", id, e.getMessage());
            }
        }
        return Optional.empty();
    }

    /**
     * Public method to explicitly cache a book in all relevant layers
     *
     * @param book The Book object to cache
     * @throws IllegalArgumentException if book or book.getId() is null
     * 
     * @implNote Updates all cache layers (in-memory, Spring cache, database)
     * Useful for preemptively caching books before they are requested
     */
    public void cacheBook(Book book) {
        if (book == null || book.getId() == null) {
            throw new IllegalArgumentException("Book and Book ID must not be null for cacheBook");
        }
        bookDetailCache.put(book.getId(), book);
        org.springframework.cache.Cache booksSpringCache = cacheManager.getCache("books");
        if (booksSpringCache != null) {
            booksSpringCache.put(book.getId(), book);
        }
        if (cacheEnabled && cachedBookRepository != null) {
            cacheBookReactive(book)
                .doOnError(e -> logger.error("Error in synchronous cacheBook via reactive for ID {}: {}", book.getId(), e.getMessage()))
                .subscribe();
        }
        logger.info("Explicitly cached book ID: {}", book.getId());
    }

    /**
     * Evicts a book from all cache layers
     *
     * @param id The Google Books ID of the book to evict
     * @throws IllegalArgumentException if id is null or empty
     * 
     * @implNote Removes from in-memory cache, Spring cache, and database cache
     * Used when book data becomes stale or invalid
     */
    public void evictBook(String id) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Book ID cannot be null or empty for evictBook");
        }
        bookDetailCache.remove(id);
        org.springframework.cache.Cache booksSpringCache = cacheManager.getCache("books");
        if (booksSpringCache != null) {
            booksSpringCache.evictIfPresent(id);
        }
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                cachedBookRepository.findByGoogleBooksId(id).ifPresent(cachedBook -> {
                    cachedBookRepository.deleteById(cachedBook.getId());
                    logger.info("Evicted book with Google ID {} (DB ID {}) from persistent cache.", id, cachedBook.getId());
                });
            } catch (Exception e) {
                logger.error("Error evicting book ID {} from persistent cache: {}", id, e.getMessage());
            }
        }
        logger.info("Evicted book ID {} from caches.", id);
    }

    /**
     * Clears all entries from all relevant cache layers
     * Use with caution.
     * 
     * @implNote Clears in-memory cache, Spring cache, and database cache
     * Typically used for testing or major data refreshes
     */
    public void clearAll() {
        bookDetailCache.clear();
        org.springframework.cache.Cache booksSpringCache = cacheManager.getCache("books");
        if (booksSpringCache != null) {
            booksSpringCache.clear();
        }
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                cachedBookRepository.deleteAll();
                logger.info("Cleared all entries from persistent cache.");
            } catch (Exception e) {
                logger.error("Error clearing persistent cache: {}", e.getMessage());
            }
        }
        logger.info("Cleared all caches.");
    }
    
    /**
     * Updates an existing book in the cache
     * 
     * @param book The updated book data to persist
     * @throws IllegalArgumentException if book or book ID is null
     */
    /**
     * Updates an existing book in the cache
     * 
     * @param book The updated book data to persist
     * @throws IllegalArgumentException if book or book ID is null
     * 
     * @implNote Removes book from all caches then adds the updated version
     * More efficient than separate eviction and caching operations
     */
    public void updateBook(Book book) {
        if (book == null || book.getId() == null) {
            throw new IllegalArgumentException("Book and Book ID must not be null for updateBook");
        }
        
        // First remove the book from all caches
        evictBook(book.getId());
        
        // Then add the updated book
        cacheBook(book);
        
        logger.info("Updated book ID: {} in all caches", book.getId());
    }
    
    /**
     * Removes a book from all caches and persistent storage
     * 
     * @param id The Google Books ID of the book to remove
     * @throws IllegalArgumentException if ID is null or empty
     */
    /**
     * Removes a book from all caches and persistent storage
     * 
     * @param id The Google Books ID of the book to remove
     * @throws IllegalArgumentException if ID is null or empty
     * 
     * @implNote Completely removes book from all cache layers
     * Used when a book should no longer be accessible through the cache
     */
    public void removeBook(String id) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Book ID cannot be null or empty for removeBook");
        }
        
        // Use evictBook to remove the book from all caches
        evictBook(id);
        
        logger.info("Removed book ID: {} from all caches", id);
    }

    /**
     * Generates a unique cache key for search results based on search parameters
     * 
     * @param query The search query string
     * @param startIndex The pagination starting index
     * @param maxResults Maximum results per page
     * @param publishedYear Optional year filter
     * @param langCode Optional language code filter
     * @param orderBy Optional sort order
     * @return A string key that uniquely identifies this search configuration
     * 
     * @implNote Creates consistent, deterministic keys for caching search results
     * Handles null parameters gracefully with default values
     */
    private String generateSearchCacheKey(String query, int startIndex, int maxResults, Integer publishedYear, String langCode, String orderBy) {
        String year = (publishedYear == null) ? "null" : publishedYear.toString();
        String lc = (langCode == null || langCode.trim().isEmpty()) ? "null" : langCode.trim().toLowerCase();
        String ob = (orderBy == null || orderBy.trim().isEmpty()) ? "default" : orderBy.trim().toLowerCase();
        
        return String.format("q:%s_s:%d_m:%d_y:%s_l:%s_o:%s", 
            query, startIndex, maxResults, year, lc, ob);
    }
}