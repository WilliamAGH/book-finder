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
 * Service for caching book data from Google Books API to improve performance.
 * Provides both in-memory and database caching with reactive support.
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
     * Get a book by ID with cache-first approach
     * @param id The book ID
     * @return The book if found
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
        Book book = googleBooksService.getBookById(id).blockOptional().orElse(null);
        
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
     * Get a book by ISBN with cache-first approach
     * @param isbn The book ISBN (can be ISBN-10 or ISBN-13)
     * @return List of books matching the ISBN
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
     * Search books with cache augmentation
     * @param query The search query
     * @param startIndex The index to start from
     * @param maxResults The maximum number of results to return
     * @return List of books matching the query
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
     * Get similar books based on semantic similarity
     * @param bookId The source book ID
     * @param count The number of similar books to return
     * @return List of similar books
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
        Book sourceBook = googleBooksService.getBookById(bookId).blockOptional().orElse(null);
        if (sourceBook == null) {
            logger.warn("Source book for similar search not found (ID: {}), returning empty list.", bookId);
            return Collections.emptyList();
        }
        // Now call the corrected getSimilarBooks method
        return googleBooksService.getSimilarBooks(sourceBook).blockOptional().orElse(Collections.emptyList());
    }
    
    
    
    /**
     * Clean expired cache entries (run daily at midnight)
     */
    @CacheEvict(value = "books", allEntries = true)
    @Scheduled(cron = "0 0 0 * * ?") 
    public void cleanExpiredCacheEntries() {
        logger.info("Cleaning expired in-memory cache entries");
    }

    /**
     * Get a book by ID with cache-first approach - Reactive version
     * @param id The book ID
     * @return A Mono of the book if found, or empty Mono if not
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

    // Renamed from fetchFromGoogleAndCache to be more specific about updating caches
    private Mono<Book> fetchFromGoogleAndUpdateCaches(String id) {
        return googleBooksService.getBookById(id) // This is already Mono<Book>
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
     * Get a book by ISBN with cache-first approach - Reactive version
     * @param isbn The book ISBN (can be ISBN-10 or ISBN-13)
     * @return A Mono of a list of books matching the ISBN
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
     * Search books with cache augmentation - Reactive version
     * @param query The search query
     * @param startIndex The index to start from (Note: Google API uses startIndex, this method applies pagination after full fetch if not using DB for search)
     * @param maxResults The maximum number of results to return
     * @return Mono of a list of books matching the query
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
     * Get similar books based on semantic similarity - Reactive version
     * @param bookId The source book ID
     * @param count The number of similar books to return
     * @return Mono of a list of similar books
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

    private Mono<List<Book>> fallbackToGoogleSimilarBooks(String bookId, int count) {
        logger.info("Falling back to GoogleBooksService for similar books for ID: {} (or vector search yielded no results/source not in DB cache)", bookId);
        return googleBooksService.getBookById(bookId) // reactive
            .flatMap(sourceBook -> {
                if (sourceBook == null) {
                    logger.warn("Source book for similar search (Google fallback) not found (ID: {}), returning empty list.", bookId);
                    return Mono.just(Collections.<Book>emptyList());
                }
                return googleBooksService.getSimilarBooks(sourceBook) // This returns Mono<List<Book>>
                    .map(list -> (List<Book>) list); // Explicit map to help compiler with type, though often not needed if signatures are clear.
                                                     // Or ensure getSimilarBooks directly provides Mono<List<Book>> that satisfies the chain.
            })
            .switchIfEmpty(Mono.defer(() -> {
                 logger.warn("GoogleBooksService.getBookById returned empty for ID {} during similar books fallback.", bookId);
                 return Mono.just(Collections.emptyList());
            }));
    }

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

    private float[] createPlaceholderEmbedding(String text) {
        float[] placeholder = new float[384];
        if (text == null || text.isEmpty()) return placeholder; // Should not happen if checked before
        int hash = text.hashCode();
        for (int i = 0; i < placeholder.length; i++) {
            placeholder[i] = (float) Math.sin(hash * (i + 1) / 100.0);
        }
        return placeholder;
    }

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

    @EventListener
    public void handleBookCoverUpdate(BookCoverUpdatedEvent event) {
        logger.info("Received BookCoverUpdatedEvent for googleBookId: {}, new URL: {}", event.getGoogleBookId(), event.getNewCoverUrl());
        if (event.getGoogleBookId() == null || event.getNewCoverUrl() == null) {
            logger.warn("Invalid BookCoverUpdatedEvent received: googleBookId or newCoverUrl is null.");
            return;
        }

        String googleBookId = event.getGoogleBookId();
        String newCoverUrl = event.getNewCoverUrl();

        // 1. Update/Invalidate In-memory bookDetailCache (used by reactive flows)
        Book inMemoryCachedBook = bookDetailCache.get(googleBookId);
        if (inMemoryCachedBook != null) {
            if (!newCoverUrl.equals(inMemoryCachedBook.getCoverImageUrl())) {
                inMemoryCachedBook.setCoverImageUrl(newCoverUrl); 
                logger.info("Updated in-memory bookDetailCache for googleBookId: {}", googleBookId);
            }
        }

        // 2. Update Persistent Database Cache (CachedBookRepository)
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> cachedBookOptional = cachedBookRepository.findByGoogleBooksId(googleBookId);
                if (cachedBookOptional.isPresent()) {
                    CachedBook dbCachedBook = cachedBookOptional.get();
                    if (!newCoverUrl.equals(dbCachedBook.getCoverImageUrl())) {
                        dbCachedBook.setCoverImageUrl(newCoverUrl);
                        cachedBookRepository.save(dbCachedBook);
                        logger.info("Updated cover URL in database cache (CachedBook) for googleBookId: {}", googleBookId);
                    } else {
                        logger.info("Cover URL for database cached book (CachedBook) with googleBookId: {} is already up-to-date.", googleBookId);
                    }
                } else {
                    logger.warn("Book with googleBookId: {} not found in CachedBookRepository for cover update.", googleBookId);
                }
            } catch (Exception e) {
                logger.error("Error updating CachedBookRepository for googleBookId {}: {}", googleBookId, e.getMessage(), e);
            }
        }

        // 3. Invalidate Spring Cache ("books") for the specific book ID
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            springCache.evictIfPresent(googleBookId);
            logger.info("Evicted Spring Cache entry for 'books' with key (googleBookId): {}", googleBookId);
        }
    }
}
