/**
 * Implements a multi-level caching strategy for Book objects
 * Provides efficient book retrieval by checking caches in order of speed before an API fallback
 * Cache layers include: In-memory, Spring Cache, Local Disk (dev), Redis, Database, and S3
 * Populates all relevant cache layers after a successful retrieval from a lower tier or API
 * Offers synchronous and reactive interfaces for fetching and managing cached Book data
 * Supports cache-only queries, existence checks, and cache bypass for debugging
 * Ensures thread-safe operations for concurrent access
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
public class GoogleBooksCachingStrategy implements CachingStrategy<String, Book> {
    private static final Logger logger = LoggerFactory.getLogger(GoogleBooksCachingStrategy.class);
    
    private final BookDataOrchestrator bookDataOrchestrator;
    private final S3RetryService s3RetryService;
    private final CacheManager cacheManager;
    private final CachedBookRepository cachedBookRepository;
    private final ObjectMapper objectMapper;
    private final RedisCacheService redisCacheService; // New dependency
    
    // In-memory cache as fastest layer
    private final ConcurrentHashMap<String, Book> bookDetailCache = new ConcurrentHashMap<>();
    
    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled;
    
    @Value("${app.local-cache.enabled:false}")
    private boolean localCacheEnabled;
    
    @Value("${app.local-cache.directory:.dev-cache}")
    private String localCacheDirectory;

    @Value("${googlebooks.api.override.bypass-caches:false}")
    private boolean bypassCachesOverride;

    /**
     * Constructs a GoogleBooksCachingStrategy with necessary service dependencies
     * Initializes cache settings and creates local disk cache directory if enabled
     * 
     * @param bookDataOrchestrator Orchestrator for fetching book data from APIs if all caches miss
     * @param s3RetryService Service for S3 operations, including retries
     * @param cacheManager Spring's CacheManager for accessing Spring-managed caches
     * @param cachedBookRepository Repository for database caching (optional, app can run without it)
     * @param objectMapper Jackson ObjectMapper for JSON serialization/deserialization
     * @param redisCacheService Service for interacting with the Redis cache layer
     */
    @Autowired
    public GoogleBooksCachingStrategy(
            BookDataOrchestrator bookDataOrchestrator,
            S3RetryService s3RetryService,
            CacheManager cacheManager,
            @Autowired(required = false) CachedBookRepository cachedBookRepository,
            ObjectMapper objectMapper,
            @Autowired(required = false) RedisCacheService redisCacheService) { // New parameter, now optional
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.s3RetryService = s3RetryService;
        this.cacheManager = cacheManager;
        this.cachedBookRepository = cachedBookRepository;
        this.objectMapper = objectMapper;
        this.redisCacheService = redisCacheService; // Assign new dependency
        
        // Disable cache if repository is not available
        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false;
            logger.info("Database cache is not available. Running in API-only mode.");
        }
        
        // Create local cache directory if needed
        if (localCacheEnabled) {
            try {
                Files.createDirectories(Paths.get(localCacheDirectory, "books"));
            } catch (Exception e) {
                logger.warn("Could not create local cache directories: {}", e.getMessage());
            }
        }
    }

    /**
     * Retrieves a book by its ID, checking caches in a specific order:
     * In-memory, Spring Cache, Local Disk, Redis, Database, S3, then API fallback
     * Populates faster caches if a hit occurs in a slower cache
     * Uses CompletableFuture for asynchronous operation
     * 
     * @param bookId The Google Books ID of the book to retrieve
     * @return A CompletionStage containing an Optional<Book>, empty if not found after all checks
     */
    @Override
    public CompletionStage<Optional<Book>> get(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        if (bypassCachesOverride) {
            logger.warn("BYPASS_CACHES_OVERRIDE ENABLED: Skipping all caches for book ID: {}. Going directly to BookDataOrchestrator", bookId);
            return bookDataOrchestrator.getBookByIdTiered(bookId)
                .map(Optional::ofNullable)
                .defaultIfEmpty(Optional.empty())
                .toFuture();
        }

        Optional<Book> bookOpt = checkInMemoryCache(bookId);
        if (bookOpt.isPresent()) return CompletableFuture.completedFuture(bookOpt);

        bookOpt = checkSpringCache(bookId);
        if (bookOpt.isPresent()) return CompletableFuture.completedFuture(bookOpt);

        bookOpt = checkLocalDiskCache(bookId);
        if (bookOpt.isPresent()) return CompletableFuture.completedFuture(bookOpt);
        
        bookOpt = checkRedisCache(bookId);
        if (bookOpt.isPresent()) return CompletableFuture.completedFuture(bookOpt);

        bookOpt = checkDatabaseCache(bookId);
        if (bookOpt.isPresent()) return CompletableFuture.completedFuture(bookOpt);

        return checkS3CacheThenApiFallback(bookId);
    }

    private Optional<Book> checkInMemoryCache(String bookId) {
        Book cachedBook = bookDetailCache.get(bookId);
        if (cachedBook != null) {
            logger.debug("In-memory cache hit for book ID: {}", bookId);
            return Optional.of(cachedBook);
        }
        return Optional.empty();
    }

    private Optional<Book> checkSpringCache(String bookId) {
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            Book springCachedBook = springCache.get(bookId, Book.class);
            if (springCachedBook != null) {
                logger.debug("Spring cache hit for book ID: {}", bookId);
                bookDetailCache.put(bookId, springCachedBook); // Populate faster in-memory cache
                return Optional.of(springCachedBook);
            }
        }
        return Optional.empty();
    }

    private Optional<Book> checkLocalDiskCache(String bookId) {
        if (localCacheEnabled) {
            Book localBook = getBookFromLocalCache(bookId); // Assumes getBookFromLocalCache is synchronous
            if (localBook != null) {
                logger.debug("Local disk cache hit for book ID: {}", bookId);
                updateFasterCaches(bookId, localBook); // Updates in-memory and Spring caches
                return Optional.of(localBook);
            }
        }
        return Optional.empty();
    }

    private Optional<Book> checkRedisCache(String bookId) {
        if (redisCacheService != null) {
            Optional<Book> redisBookOpt = redisCacheService.getBookById(bookId);
            if (redisBookOpt.isPresent()) {
                logger.debug("Redis cache hit for book ID: {}", bookId);
                updateFasterCaches(bookId, redisBookOpt.get()); // Update in-memory, Spring, local disk
                return redisBookOpt;
            }
        }
        return Optional.empty();
    }

    private Optional<Book> checkDatabaseCache(String bookId) {
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> dbCachedBook = cachedBookRepository.findByGoogleBooksId(bookId);
                if (dbCachedBook.isPresent()) {
                    Book dbBook = dbCachedBook.get().toBook();
                    logger.debug("Database cache hit for book ID: {}", bookId);
                    updateFasterCaches(bookId, dbBook); // Update in-memory, Spring, local disk
                    if (redisCacheService != null) {
                        redisCacheService.cacheBook(bookId, dbBook); // Also update Redis
                    }
                    return Optional.of(dbBook);
                }
            } catch (Exception e) {
                logger.warn("Error accessing database cache for book ID {}: {}", bookId, e.getMessage());
            }
        }
        return Optional.empty();
    }

    private CompletionStage<Optional<Book>> checkS3CacheThenApiFallback(String bookId) {
        return s3RetryService.fetchJsonWithRetry(bookId)
            .thenComposeAsync(s3Result -> {
                if (s3Result.isSuccess() && s3Result.getData().isPresent()) {
                    try {
                        JsonNode bookNode = objectMapper.readTree(s3Result.getData().get());
                        Book s3Book = BookJsonParser.convertJsonToBook(bookNode);
                        if (s3Book != null && s3Book.getId() != null) {
                            logger.debug("S3 cache hit for book ID: {}", bookId);
                            putReactive(bookId, s3Book).subscribe(); // Populate all higher caches
                            return CompletableFuture.completedFuture(Optional.of(s3Book));
                        }
                        logger.warn("S3 cache for {} contained JSON, but it parsed to a null/invalid book. Falling back to API", bookId);
                    } catch (Exception e) {
                        logger.warn("Failed to parse book JSON from S3 cache for bookId {}: {}. Falling back to API", bookId, e.getMessage());
                    }
                } else if (s3Result.isNotFound()) {
                    logger.debug("Book {} not found in S3 cache. Fetching from API", bookId);
                } else if (s3Result.isServiceError()) {
                    logger.warn("S3 service error while fetching book {}: {}. All retries failed, falling back to API", 
                                bookId, s3Result.getErrorMessage().orElse("Unknown S3 error"));
                } else if (s3Result.isDisabled()) {
                    logger.debug("S3 is disabled or misconfigured. Fetching book {} directly from API", bookId);
                }
                
                logger.debug("All caches missed for book ID: {}. Calling BookDataOrchestrator.getBookByIdTiered", bookId);
                return bookDataOrchestrator.getBookByIdTiered(bookId)
                    .flatMap(orchestratedBook ->
                        putReactive(bookId, orchestratedBook) // Populate all caches
                            .doOnError(e -> logger.error("Error populating caches from orchestrator result for {}: {}", bookId, e.getMessage()))
                            .then(Mono.just(Optional.of(orchestratedBook)))
                    )
                    .defaultIfEmpty(Optional.empty())
                    .toFuture();
            });
    }

    /**
     * Reactively retrieves a book by its ID, using the same multi-level cache strategy as the synchronous get method
     * Delegates to the synchronous get method and converts its CompletionStage result to a Mono
     * 
     * @param bookId The Google Books ID of the book to retrieve
     * @return A Mono emitting the Book if found, or an empty Mono if not found
     */
    @Override
    public Mono<Book> getReactive(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return Mono.empty();
        }
        
        // Convert the CompletionStage-based method to Reactive
        // get(bookId) now returns CompletionStage<Optional<Book>>
        return Mono.defer(() -> Mono.fromCompletionStage(get(bookId))) // This results in Mono<Optional<Book>>
            .flatMap(optionalBook -> optionalBook.map(Mono::just).orElseGet(Mono::empty)); // Converts Mono<Optional<Book>> to Mono<Book>
    }

    /**
     * Reactively retrieves a book by its ID, checking only cache layers (no S3 or API fallback)
     * Cache check order: In-memory, Spring Cache, Local Disk, Redis, Database
     * Delegates to the synchronous getFromCacheOnly method and converts its Optional result to a Mono
     *
     * @param bookId The Google Books ID of the book to retrieve from caches
     * @return A Mono emitting the Book if found in any cache, or an empty Mono otherwise
     */
    public Mono<Book> getReactiveFromCacheOnly(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return Mono.empty();
        }
        return Mono.fromCallable(() -> getFromCacheOnly(bookId))
            .subscribeOn(Schedulers.boundedElastic()) // getFromCacheOnly might do I/O for local/DB cache
            .flatMap(optionalBook -> optionalBook.map(Mono::just).orElseGet(Mono::empty));
    }

    /**
     * Synchronously updates all relevant cache layers with the given book data
     * Layers updated: In-memory, Spring Cache, Local Disk (if enabled), Redis
     * Also triggers a reactive update for the database cache
     * Does not update S3 cache directly, assuming S3 is populated by the orchestrator on API fetch
     * 
     * @param bookId The Google Books ID to use as the cache key
     * @param book The Book object to cache
     * @return A CompletionStage that completes when synchronous caches are updated
     */
    @Override
    public CompletionStage<Void> put(String bookId, Book book) {
        if (bookId == null || bookId.isEmpty() || book == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        // Step 1: Update in-memory cache (fastest)
        bookDetailCache.put(bookId, book);
        
        // Step 2: Update Spring Cache
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            springCache.put(bookId, book);
        }
        
        // Step 3: Update local disk cache if enabled
        if (localCacheEnabled) {
            saveBookToLocalCache(bookId, book);
        }

        // Step 4: Update Redis Cache
        if (redisCacheService != null) {
            redisCacheService.cacheBook(bookId, book);
        }
        
        // Step 5: Update database cache if enabled
        // This is handled asynchronously using reactive programming
        // The putReactive method below will also handle Redis, so this call is fine
        putReactive(bookId, book).subscribe(); // This will eventually call the reactive Redis put
        
        // Step 6: Update S3 cache
        // This is done by GoogleBooksService when fetching from the API
        // We don't need to do it again here
        
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Reactively updates all relevant cache layers with the given book data
     * Layers updated: In-memory, Spring Cache, Local Disk (if enabled), Redis, and Database (if enabled)
     * S3 cache is not updated here, as it's handled by the BookDataOrchestrator upon API fetch
     * 
     * @param bookId The Google Books ID to use as the cache key
     * @param book The Book object to cache
     * @return A Mono<Void> that completes when all reactive caching operations are initiated or finished
     */
    @Override
    public Mono<Void> putReactive(String bookId, Book book) {
        if (bookId == null || bookId.isEmpty() || book == null) {
            return Mono.empty();
        }
        
        // Step 1: Update in-memory cache (fastest)
        bookDetailCache.put(bookId, book);
        
        // Step 2: Update Spring Cache
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            springCache.put(bookId, book);
        }
        
        // Step 3: Update local disk cache if enabled
        if (localCacheEnabled) {
            Mono.fromRunnable(() -> saveBookToLocalCache(bookId, book))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(); // Fire and forget for local disk
        }

        // Step 4: Update Redis Cache reactively
        Mono<Void> redisPutMono = Mono.empty();
        if (redisCacheService != null) {
            redisPutMono = redisCacheService.cacheBookReactive(bookId, book);
        }
        
        // Step 5: Update database cache if enabled
        Mono<Void> dbPutMono = Mono.empty();
        if (cacheEnabled && cachedBookRepository != null) {
            dbPutMono = Mono.defer(() -> 
                Mono.fromCallable(() -> cachedBookRepository.findByGoogleBooksId(bookId))
                    .subscribeOn(Schedulers.boundedElastic())
                    .flatMap(existingOpt -> {
                        if (existingOpt.isPresent()) {
                            CachedBook existing = existingOpt.get();
                            CachedBook updated = updateCachedBook(existing, book);
                            return Mono.fromCallable(() -> cachedBookRepository.save(updated))
                                .subscribeOn(Schedulers.boundedElastic())
                                .then();
                        } else {
                            try {
                                JsonNode rawJsonNode = objectMapper.readTree(book.getRawJsonResponse());
                                CachedBook newCached = CachedBook.fromBook(book, rawJsonNode, null); // Assuming embedding is handled elsewhere or null for now
                                return Mono.fromCallable(() -> cachedBookRepository.save(newCached))
                                    .subscribeOn(Schedulers.boundedElastic())
                                    .then();
                            } catch (Exception e) {
                                logger.error("Error creating new CachedBook for ID {}: {}", bookId, e.getMessage());
                                return Mono.error(e);
                            }
                        }
                    })
                    .onErrorResume(e -> {
                        logger.error("Error updating database cache for book ID {}: {}", bookId, e.getMessage());
                        return Mono.empty(); // Continue even if DB cache fails
                    })
            );
        }
        
        return redisPutMono.then(dbPutMono); // Chain Redis and DB operations
    }

    /**
     * Synchronously removes a book from all cache layers except S3
     * Layers affected: In-memory, Spring Cache, Local Disk (if enabled), Redis, Database (if enabled)
     * S3 objects are not evicted here to avoid potentially frequent S3 delete operations
     * 
     * @param bookId The Google Books ID of the book to evict
     * @return A CompletionStage that completes when eviction from synchronous caches is done
     */
    @Override
    public CompletionStage<Void> evict(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        
        // Step 1: Remove from in-memory cache
        bookDetailCache.remove(bookId);
        
        // Step 2: Remove from Spring Cache
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            springCache.evict(bookId);
        }
        
        // Step 3: Remove from local disk cache
        if (localCacheEnabled) {
            try {
                Path bookFile = Paths.get(localCacheDirectory, "books", bookId + ".json");
                Files.deleteIfExists(bookFile);
            } catch (Exception e) {
                logger.warn("Error removing book from local disk cache: {}", e.getMessage());
            }
        }
        
        // Step 4: Remove from Redis cache
        if (redisCacheService != null) {
            redisCacheService.evictBook(bookId);
        }

        // Step 5: Remove from database cache
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                cachedBookRepository.findByGoogleBooksId(bookId)
                    .ifPresent(cachedBook -> cachedBookRepository.delete(cachedBook));
            } catch (Exception e) {
                logger.warn("Error removing book from database cache: {}", e.getMessage());
            }
        }
        
        // We don't remove from S3 to avoid unnecessary storage operations
        // S3 objects will be replaced when new data is available
        
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Synchronously checks if a book exists in any of the cache layers (excluding S3)
     * Cache check order: In-memory, Spring Cache, Local Disk, Redis, Database
     * Does not check S3 to avoid network calls for a simple existence check
     * 
     * @param bookId The Google Books ID to check for existence
     * @return true if the book is found in any of the checked caches, false otherwise
     */
    @Override
    public boolean exists(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return false;
        }
        
        // Step 1: Check in-memory cache (fastest)
        if (bookDetailCache.containsKey(bookId)) {
            return true;
        }
        
        // Step 2: Check Spring Cache
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null && springCache.get(bookId) != null) {
            return true;
        }
        
        // Step 3: Check local disk cache
        if (localCacheEnabled) {
            Path bookFile = Paths.get(localCacheDirectory, "books", bookId + ".json");
            if (Files.exists(bookFile)) {
                return true;
            }
        }

        // Step 3.5: Check Redis cache
        if (redisCacheService != null && redisCacheService.getBookById(bookId).isPresent()) {
            return true;
        }
        
        // Step 4: Check database cache
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                return cachedBookRepository.findByGoogleBooksId(bookId).isPresent();
            } catch (Exception e) {
                logger.warn("Error checking database cache: {}", e.getMessage());
            }
        }
        
        // We don't check S3 here to avoid unnecessary network calls
        
        return false;
    }

    /**
     * Synchronously retrieves a book only from cache layers, without falling back to S3 or API calls
     * Cache check order: In-memory, Spring Cache, Local Disk, Redis, Database
     * 
     * @param bookId The Google Books ID of the book to retrieve from caches
     * @return An Optional<Book> containing the book if found in any cache, otherwise an empty Optional
     */
    @Override
    public Optional<Book> getFromCacheOnly(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return Optional.empty();
        }
        
        // Step 1: Check in-memory cache (fastest)
        Book cachedBook = bookDetailCache.get(bookId);
        if (cachedBook != null) {
            return Optional.of(cachedBook);
        }
        
        // Step 2: Check Spring Cache
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            Book springCachedBook = springCache.get(bookId, Book.class);
            if (springCachedBook != null) {
                return Optional.of(springCachedBook);
            }
        }
        
        // Step 3: Check local disk cache
        if (localCacheEnabled) {
            Book localBook = getBookFromLocalCache(bookId);
            if (localBook != null) {
                return Optional.of(localBook);
            }
        }

        // Step 3.5: Check Redis cache
        if (redisCacheService != null) {
            Optional<Book> redisBookOpt = redisCacheService.getBookById(bookId);
            if (redisBookOpt.isPresent()) {
                return redisBookOpt;
            }
        }
        
        // Step 4: Check database cache
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> dbCachedBook = cachedBookRepository.findByGoogleBooksId(bookId);
                if (dbCachedBook.isPresent()) {
                    return Optional.of(dbCachedBook.get().toBook());
                }
            } catch (Exception e) {
                logger.warn("Error accessing database cache: {}", e.getMessage());
            }
        }
        
        return Optional.empty();
    }

    /**
     * Synchronously clears all cache layers except S3
     * Layers affected: In-memory, Spring Cache, Local Disk (if enabled), Database (if enabled)
     * Redis cache is not cleared by this method to prevent unintended broad data removal;
     * specific Redis clearing should be handled by RedisCacheService if needed
     * S3 cache is not cleared to avoid mass S3 delete operations
     * 
     * @return A CompletionStage that completes when clearing operations are done
     */
    @Override
    public CompletionStage<Void> clearAll() {
        // Step 1: Clear in-memory cache
        bookDetailCache.clear();
        
        // Step 2: Clear Spring Cache
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            springCache.clear();
        }
        
        // Step 3: Clear local disk cache
        if (localCacheEnabled) {
            try {
                Path booksDir = Paths.get(localCacheDirectory, "books");
                if (Files.exists(booksDir)) {
                    Files.list(booksDir).forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (Exception e) {
                            logger.warn("Error deleting file from local disk cache: {}", e.getMessage());
                        }
                    });
                }
            } catch (Exception e) {
                logger.warn("Error clearing local disk cache: {}", e.getMessage());
            }
        }
        
        // Step 4: Clear database cache
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                cachedBookRepository.deleteAll();
            } catch (Exception e) {
                logger.warn("Error clearing database cache: {}", e.getMessage());
            }
        }
        
        // We don't clear S3 cache to avoid unnecessary storage operations
        
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Updates in-memory and Spring caches for quick access
     * 
     * @param bookId The Google Books ID as key
     * @param book The book to cache
     */
    private void updateFasterCaches(String bookId, Book book) {
        // Update in-memory cache
        bookDetailCache.put(bookId, book);
        
        // Update Spring Cache
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            springCache.put(bookId, book);
        }
    }

    /**
     * Retrieves a book from the local disk cache
     * 
     * @param bookId The Google Books ID
     * @return Book if found in disk cache, null otherwise
     */
    private Book getBookFromLocalCache(String bookId) {
        if (!localCacheEnabled) {
            return null;
        }
        
        Path bookFile = Paths.get(localCacheDirectory, "books", bookId + ".json");
        
        if (Files.exists(bookFile)) {
            try {
                return objectMapper.readValue(bookFile.toFile(), Book.class);
            } catch (Exception e) {
                logger.warn("Error reading book from local disk cache: {}", e.getMessage());
            }
        }
        
        return null;
    }

    /**
     * Persists a book to the local disk cache
     * 
     * @param bookId The Google Books ID as filename
     * @param book The book to serialize and save
     */
    private void saveBookToLocalCache(String bookId, Book book) {
        if (!localCacheEnabled) {
            return;
        }
        
        Path bookFile = Paths.get(localCacheDirectory, "books", bookId + ".json");
        
        try {
            // Ensure directory exists
            Files.createDirectories(bookFile.getParent());
            
            // Save book to file
            objectMapper.writeValue(bookFile.toFile(), book);
        } catch (Exception e) {
            logger.warn("Error saving book to local disk cache: {}", e.getMessage());
        }
    }

    /**
     * Copies values from Book to CachedBook entity
     * 
     * @param cachedBook The existing CachedBook to update
     * @param book The Book with current values
     * @return The updated CachedBook ready for persistence
     */
    private CachedBook updateCachedBook(CachedBook cachedBook, Book book) {
        // Update all fields from the Book
        cachedBook.setTitle(book.getTitle());
        cachedBook.setAuthors(book.getAuthors());
        cachedBook.setPublisher(book.getPublisher());
        if (book.getPublishedDate() != null) {
            cachedBook.setPublishedDate(book.getPublishedDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
        } else {
            cachedBook.setPublishedDate(null);
        }
        cachedBook.setDescription(book.getDescription());
        cachedBook.setIsbn10(book.getIsbn10());
        cachedBook.setIsbn13(book.getIsbn13());
        cachedBook.setCoverImageUrl(book.getCoverImageUrl());
        cachedBook.setLanguage(book.getLanguage());
        cachedBook.setCategories(book.getCategories());
        
        // Persist complex fields
        cachedBook.setCachedRecommendationIds(book.getCachedRecommendationIds() != null ? new ArrayList<>(book.getCachedRecommendationIds()) : new ArrayList<>());
        cachedBook.setQualifiers(book.getQualifiers() != null ? new HashMap<>(book.getQualifiers()) : new HashMap<>());
        cachedBook.setOtherEditions(book.getOtherEditions() != null ? new ArrayList<>(book.getOtherEditions()) : new ArrayList<>());
        // Note: Embedding vector is typically set only on creation or explicit update, not usually here.

        // Update last modified timestamp
        cachedBook.setLastAccessed(LocalDateTime.now());
        // Access count could be incremented here if desired, or handled by get() methods.
        // For simplicity, only lastAccessed is updated on a general update.

        try {
            // Update raw JSON if available and different from what might be in newBookFromApi.getRawJsonResponse()
            // This ensures the CachedBook.rawData reflects the Book object's state if it was modified
            // after being parsed from an original rawJsonResponse.
            // However, if book.getRawJsonResponse() is the *source* of truth for S3, this might be redundant
            // or lead to discrepancies if 'book' was modified in memory without updating its rawJsonResponse
            // For now, assume book.getRawJsonResponse() is the most current raw form if available
            if (book.getRawJsonResponse() != null && !book.getRawJsonResponse().isEmpty()) {
                JsonNode rawJsonNode = objectMapper.readTree(book.getRawJsonResponse());
                cachedBook.setRawData(rawJsonNode);
            } else {
                // If the book object doesn't have a rawJsonResponse (e.g., constructed manually),
                // we might want to serialize the book object itself to JSON to store in rawData
                // For now, if no rawJsonResponse, we don't update rawData to avoid overwriting
                // potentially more complete S3-derived rawData with a partial in-memory Book object
                // This depends on the exact flow and source of the 'book' parameter
                // Given the plan, 'book' here is usually what BookDataOrchestrator decided is authoritative
                 logger.debug("Book object for ID {} provided to updateCachedBook has no rawJsonResponse. RawData field in CachedBook will not be updated from it.", book.getId());
            }
        } catch (Exception e) {
            logger.warn("Error updating raw JSON in CachedBook for ID {}: {}", book.getId(), e.getMessage());
        }
        
        return cachedBook;
    }
}
