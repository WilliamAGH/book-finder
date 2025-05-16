package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
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
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;

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
@Service
public class BookCacheService {
    private static final Logger logger = LoggerFactory.getLogger(BookCacheService.class);
    
    private final GoogleBooksService googleBooksService;
    private final CachedBookRepository cachedBookRepository;
    private final ObjectMapper objectMapper;
    private final WebClient embeddingClient;
    private final ConcurrentHashMap<String, Book> bookDetailCache = new ConcurrentHashMap<>(); // In-memory cache for Book objects by ID
    private final CacheManager cacheManager;
    
    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled; // This refers to the database/persistent cache primarily
    
    @Value("${app.embedding.service.url:#{null}}")
    private String embeddingServiceUrl;
    
    /**
     * Constructs a BookCacheService with required dependencies
     * 
     * @param googleBooksService Service for fetching book data from Google Books API
     * @param objectMapper JSON mapper for serializing book data
     * @param webClientBuilder WebClient builder for embedding service communication
     * @param cacheManager Spring cache manager for managing in-memory caches
     * @param cachedBookRepository Repository for database-backed caching (optional)
     * 
     * @implNote Detects if database repository is available and adjusts caching behavior accordingly
     * Initializes embedding service client with fallback to localhost when URL not configured
     */
    @Autowired
    public BookCacheService(
            GoogleBooksService googleBooksService,
            ObjectMapper objectMapper,
            WebClient.Builder webClientBuilder,
            CacheManager cacheManager,
            @Autowired(required = false) CachedBookRepository cachedBookRepository) {
        this.googleBooksService = googleBooksService;
        this.cachedBookRepository = cachedBookRepository;
        this.objectMapper = objectMapper;
        this.embeddingClient = webClientBuilder.baseUrl(
                embeddingServiceUrl != null ? embeddingServiceUrl : "http://localhost:8080/api/embedding"
        ).build();
        this.cacheManager = cacheManager; // Initialize CacheManager
        
        // Disable cache if repository is not available
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
        // Adapt CompletableFuture to blocking Optional for the synchronous method
        Book book = googleBooksService.getBookById(id).handle((res, ex) -> {
            if (ex != null) {
                logger.error("Error fetching book by ID {} from GoogleBooksService: {}", id, ex.getMessage());
                return null;
            }
            return res;
        }).join(); // .join() to block and get the result
        
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
        Book sourceBook = googleBooksService.getBookById(bookId).handle((res, ex) -> {
            if (ex != null) {
                logger.error("Error fetching source book by ID {} for similar search: {}", bookId, ex.getMessage());
                return null;
            }
            return res;
        }).join(); // .join() to block and get the result
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

    /**
     * Fetches a book from Google Books API and updates all cache layers
     * 
     * @param id The Google Books ID to fetch
     * @return Mono emitting the fetched book or empty if not found
     * 
     * @implNote Updates both in-memory and database caches asynchronously
     * Called when book is not found in any cache layer
     */
    // Renamed from fetchFromGoogleAndCache to be more specific about updating caches
    private Mono<Book> fetchFromGoogleAndUpdateCaches(String id) {
        // Convert CompletableFuture to Mono for reactive chain
        return Mono.fromFuture(googleBooksService.getBookById(id))
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
     * Searches for books with reactive API and result caching
     * 
     * @param query The search query string
     * @param startIndex The pagination starting index (0-based)
     * @param maxResults Maximum number of results to return
     * @return Mono emitting list of books matching the query with pagination applied
     * 
     * @implNote Applies client-side pagination after retrieving full result set
     * Background caches results while returning immediately to client
     */
    public Mono<List<Book>> searchBooksReactive(String query, int startIndex, int maxResults) {
        logger.info("BookCacheService searching books (reactive) with query: {}, startIndex: {}, maxResults: {}", query, startIndex, maxResults);
        
        return googleBooksService.searchBooksAsyncReactive(query) // This fetches up to a certain limit (e.g., 200) from GoogleBooksService
            .map(fetchedBooks -> {
                List<Book> allFetchedBooks = (fetchedBooks == null) ? Collections.emptyList() : fetchedBooks;
                
                List<Book> paginatedBooks;
                if (startIndex >= allFetchedBooks.size()) {
                    paginatedBooks = Collections.emptyList();
                } else {
                    int endIndex = Math.min(startIndex + maxResults, allFetchedBooks.size());
                    paginatedBooks = allFetchedBooks.subList(startIndex, endIndex);
                }

                if (!paginatedBooks.isEmpty() && cacheEnabled && cachedBookRepository != null) {
                    Flux.fromIterable(paginatedBooks)
                        .flatMap(bookToCache -> cacheBookReactive(bookToCache)
                            .onErrorResume(e -> {
                                logger.error("Error during reactive background caching for book (query {}): {}. ID: {}", query, e.getMessage(), bookToCache.getId());
                                return Mono.empty();
                            }))
                        .subscribe();
                }
                return paginatedBooks;
            })
            .defaultIfEmpty(Collections.emptyList());
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
     * Falls back to Google Books API for similar book recommendations
     * 
     * @param bookId The source book ID to find similar books for
     * @param count Maximum number of similar books to return
     * @return Mono emitting list of similar books
     * 
     * @implNote Called when database similarity search fails or returns no results
     * Uses category and author matching instead of vector similarity
     */
    private Mono<List<Book>> fallbackToGoogleSimilarBooks(String bookId, int count) {
        logger.info("Falling back to GoogleBooksService for similar books for ID: {} (or vector search yielded no results/source not in DB cache)", bookId);
        // Convert CompletableFuture to Mono for reactive chain
        return Mono.fromFuture(googleBooksService.getBookById(bookId))
            .flatMap(sourceBook -> {
                if (sourceBook == null) {
                    logger.warn("Source book for similar search (Google fallback) not found (ID: {}), returning empty list.", bookId);
                    return Mono.just(Collections.<Book>emptyList());
                }
                return googleBooksService.getSimilarBooks(sourceBook) // This returns Mono<List<Book>>
                    .map(list -> (List<Book>) list); // Explicit map to help compiler with type, though often not needed if signatures are clear.
                                                     // Or ensure getSimilarBooks directly provides Mono<List<Book>> that satisfies the chain.
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

        return Mono.defer(() ->
            Mono.fromCallable(() -> cachedBookRepository.findByGoogleBooksId(book.getId()))
                .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                .flatMap(existingCachedBookOptional -> {
                    if (existingCachedBookOptional.isPresent()) {
                        logger.debug("Book already cached (checked reactively): {}", book.getId());
                        return Mono.empty(); // Completes the Mono<Void> chain, meaning no further action
                    }
                    // Not in cache, proceed with embedding and saving
                    return generateEmbeddingReactive(book)
                        .flatMap(embedding -> {
                            try {
                                JsonNode rawData = objectMapper.valueToTree(book);
                                CachedBook cachedBookToSave = CachedBook.fromBook(book, rawData, embedding);
                                // Wrap the blocking save operation
                                return Mono.fromRunnable(() -> {
                                        if (cachedBookRepository != null) { // Final check before save
                                            cachedBookRepository.save(cachedBookToSave);
                                        }
                                    })
                                    .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                                    .doOnSuccess(v -> logger.info("Successfully cached book reactively: {}", book.getId()))
                                    .then(); // ensure this flatMap returns Mono<Void>
                            } catch (Exception e) {
                                logger.error("Error preparing book for reactive caching {}: {}", book.getId(), e.getMessage(), e);
                                return Mono.error(e); // Propagate error to be caught by outer onErrorResume
                            }
                        });
                })
                .onErrorResume(e -> {
                    // This handles errors from findByGoogleBooksId or the subsequent embedding/saving chain
                    logger.warn("Error during DB cache check or subsequent processing for book ID {} in reactive save: {}. Cache attempt aborted.", book.getId(), e.getMessage());
                    return Mono.empty(); // Abort caching on error, completes the Mono<Void> chain
                })
        ).then(); // Ensure the overall method returns Mono<Void> and subscribes to the defer's Mono
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
