/**
 * Multi-level caching strategy for Book objects with tiered fallback mechanism
 *
 * Provides efficient book retrieval through five cache layers:
 * 1. In-memory cache - Fast ConcurrentHashMap for immediate access
 * 2. Spring Cache - Managed by Spring's caching abstraction
 * 3. Local disk cache - Optional development cache persisting across restarts
 * 4. Database cache - Optional persistent storage using database
 * 5. S3 cache - Distributed storage for resilient, large-scale caching
 *
 * Features:
 * - Checks caches in order of speed before API fallback
 * - Populates all cache layers after successful retrieval
 * - Provides both synchronous and reactive interfaces
 * - Supports cache-only queries and existence checks
 * - Optional cache bypass for testing and debugging
 * - Thread-safe implementation for concurrent access
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
     * Constructs a GoogleBooksCachingStrategy with required dependencies
     * 
     * @param bookDataOrchestrator Orchestrator for coordinating book data access across sources
     * @param s3RetryService Service for S3 operations with retry capabilities
     * @param cacheManager Spring cache manager for managing in-memory caches
     * @param cachedBookRepository Repository for database-backed caching (optional)
     * @param objectMapper JSON mapper for serializing/deserializing book data
     */
    @Autowired
    public GoogleBooksCachingStrategy(
            BookDataOrchestrator bookDataOrchestrator,
            S3RetryService s3RetryService,
            CacheManager cacheManager,
            @Autowired(required = false) CachedBookRepository cachedBookRepository,
            ObjectMapper objectMapper) {
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.s3RetryService = s3RetryService;
        this.cacheManager = cacheManager;
        this.cachedBookRepository = cachedBookRepository;
        this.objectMapper = objectMapper;
        
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
     * Retrieves a book by ID using multi-level cache with API fallback
     * 
     * @param bookId The Google Books ID to look up
     * @return CompletionStage with Optional<Book> result
     */
    @Override
    public CompletionStage<Optional<Book>> get(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        if (bypassCachesOverride) {
            logger.warn("BYPASS_CACHES_OVERRIDE ENABLED: Skipping all caches for book ID: {}. Going directly to BookDataOrchestrator.", bookId);
            // Even with bypass, the orchestrator handles the full API fetch logic now
            return bookDataOrchestrator.getBookByIdTiered(bookId)
                .map(Optional::ofNullable)
                .defaultIfEmpty(Optional.empty())
                .toFuture();
        }
        
        // Step 1: Check in-memory cache (fastest)
        Book cachedBook = bookDetailCache.get(bookId);
        if (cachedBook != null) {
            logger.debug("In-memory cache hit for book ID: {}", bookId);
            return CompletableFuture.completedFuture(Optional.of(cachedBook));
        }
        
        // Step 2: Check Spring Cache
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            Book springCachedBook = springCache.get(bookId, Book.class);
            if (springCachedBook != null) {
                logger.debug("Spring cache hit for book ID: {}", bookId);
                // Update in-memory cache
                bookDetailCache.put(bookId, springCachedBook);
                return CompletableFuture.completedFuture(Optional.of(springCachedBook));
            }
        }
        
        // Step 3: Check local disk cache if enabled (dev/test environments)
        if (localCacheEnabled) {
            Book localBook = getBookFromLocalCache(bookId);
            if (localBook != null) {
                logger.debug("Local disk cache hit for book ID: {}", bookId);
                // Update faster caches
                updateFasterCaches(bookId, localBook);
                return CompletableFuture.completedFuture(Optional.of(localBook));
            }
        }
        
        // Step 4: Check database cache if enabled
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> dbCachedBook = cachedBookRepository.findByGoogleBooksId(bookId);
                if (dbCachedBook.isPresent()) {
                    Book dbBook = dbCachedBook.get().toBook();
                    logger.debug("Database cache hit for book ID: {}", bookId);
                    // Update faster caches
                    updateFasterCaches(bookId, dbBook);
                    return CompletableFuture.completedFuture(Optional.of(dbBook));
                }
            } catch (Exception e) {
                logger.warn("Error accessing database cache for book ID {}: {}", bookId, e.getMessage());
            }
        }
        
        // Step 5: Check S3 cache with retry logic
        return s3RetryService.fetchJsonWithRetry(bookId)
            .thenComposeAsync(s3Result -> {
                if (s3Result.isSuccess() && s3Result.getData().isPresent()) {
                    try {
                        // Parse the JSON from S3
                        JsonNode bookNode = objectMapper.readTree(s3Result.getData().get());
                        Book s3Book = BookJsonParser.convertJsonToBook(bookNode, objectMapper);
                        
                        if (s3Book != null && s3Book.getId() != null) {
                            logger.debug("S3 cache hit for book ID: {}", bookId);
                            // Update all caches
                            putReactive(bookId, s3Book).subscribe();
                            return CompletableFuture.completedFuture(Optional.of(s3Book));
                        }
                        logger.warn("S3 cache for {} contained JSON, but it parsed to a null/invalid book. Falling back to API.", bookId);
                    } catch (Exception e) {
                        logger.warn("Failed to parse book JSON from S3 cache for bookId {}: {}. Falling back to API.", bookId, e.getMessage());
                    }
                } else if (s3Result.isNotFound()) {
                    logger.debug("Book {} not found in S3 cache. Fetching from API.", bookId);
                } else if (s3Result.isServiceError()) {
                    logger.warn("S3 service error while fetching book {}: {}. All retries failed, falling back to API.", 
                                bookId, s3Result.getErrorMessage().orElse("Unknown S3 error"));
                } else if (s3Result.isDisabled()) {
                    logger.debug("S3 is disabled or misconfigured. Fetching book {} directly from API.", bookId);
                }
                
                // Step 6: Fall back to BookDataOrchestrator for Tiers 3 & 4
                logger.debug("All local/S3 caches missed for book ID: {}. Calling BookDataOrchestrator.getBookByIdTiered.", bookId);
                return bookDataOrchestrator.getBookByIdTiered(bookId)
                    .flatMap(orchestratedBook -> {
                        // If orchestrator returns a book, it means it was fetched from API (Tier 3 or 4)
                        // and already saved to S3 by the orchestrator.
                        // We just need to populate our Tier 1 caches here.
                        putReactive(bookId, orchestratedBook).subscribe(
                            null,
                            e -> logger.error("Error populating Tier 1 caches from orchestrator result for {}: {}", bookId, e.getMessage())
                        );
                        return Mono.just(Optional.of(orchestratedBook));
                    })
                    .defaultIfEmpty(Optional.empty()) // If orchestrator also returns empty
                    .toFuture(); // Convert Mono<Optional<Book>> to CompletionStage<Optional<Book>>
            });
    }

    /**
     * Retrieves a book by ID using reactive programming model
     * 
     * @param bookId The Google Books ID to look up
     * @return Mono<Book> emitting book or empty if not found
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
     * Updates all cache layers with a book value
     * 
     * @param bookId The Google Books ID to use as cache key
     * @param book The book to cache
     * @return CompletionStage<Void> completing when caching completes
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
        
        // Step 4: Update database cache if enabled
        // This is handled asynchronously using reactive programming
        putReactive(bookId, book).subscribe();
        
        // Step 5: Update S3 cache
        // This is done by GoogleBooksService when fetching from the API
        // We don't need to do it again here
        
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Reactively updates all cache layers with a book value
     * 
     * @param bookId The Google Books ID to use as cache key
     * @param book The book to cache
     * @return Mono<Void> completing when caching completes
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
                .subscribe();
        }
        
        // Step 4: Update database cache if enabled
        if (cacheEnabled && cachedBookRepository != null) {
            return Mono.defer(() -> 
                Mono.fromCallable(() -> cachedBookRepository.findByGoogleBooksId(bookId))
                    .subscribeOn(Schedulers.boundedElastic())
                    .flatMap(existingOpt -> {
                        if (existingOpt.isPresent()) {
                            CachedBook existing = existingOpt.get();
                            // Update existing entry
                            CachedBook updated = updateCachedBook(existing, book);
                            return Mono.fromCallable(() -> cachedBookRepository.save(updated))
                                .subscribeOn(Schedulers.boundedElastic())
                                .then();
                        } else {
                            // Create new entry
                            try {
                                JsonNode rawJsonNode = objectMapper.readTree(book.getRawJsonResponse());
                                CachedBook newCached = CachedBook.fromBook(book, rawJsonNode, null);
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
                        return Mono.empty();
                    })
            );
        }
        
        return Mono.empty();
    }

    /**
     * Removes a book from all cache layers
     * 
     * @param bookId The Google Books ID to evict
     * @return CompletionStage<Void> completing when eviction completes
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
        
        // Step 4: Remove from database cache
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
     * Checks if a book exists in any cache layer
     * 
     * @param bookId The Google Books ID to check
     * @return true if book exists in any cache, false otherwise
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
     * Gets a book from local caches only, without S3 or API fallback
     * 
     * @param bookId The Google Books ID to look up
     * @return Optional<Book> if found in any local cache
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
     * Clears all cache layers except S3
     * 
     * @return CompletionStage<Void> completing when clear operation completes
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
        // cachedBook.setOtherEditions(book.getOtherEditions()); // CachedBook does not store otherEditions
        cachedBook.setLanguage(book.getLanguage());
        cachedBook.setCategories(book.getCategories());
        // Skip updating embeddingVector here, as it's a special field
        // that should be generated separately
        
        // Update last modified timestamp
        cachedBook.setLastAccessed(LocalDateTime.now());
        
        try {
            // Update raw JSON if available
            if (book.getRawJsonResponse() != null && !book.getRawJsonResponse().isEmpty()) {
                JsonNode rawJsonNode = objectMapper.readTree(book.getRawJsonResponse());
                cachedBook.setRawData(rawJsonNode);
            }
        } catch (Exception e) {
            logger.warn("Error updating raw JSON in CachedBook: {}", e.getMessage());
        }
        
        return cachedBook;
    }
}
