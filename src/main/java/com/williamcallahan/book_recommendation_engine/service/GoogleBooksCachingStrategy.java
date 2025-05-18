/**
 * Implements a multi-level caching strategy for retrieving {@link Book} objects based on their Google Books ID
 * This strategy is designed to be used by services like {@link GoogleBooksService} or {@link BookApiProxy}
 * to minimize direct API calls and improve performance and resilience
 *
 * The caching layers are checked in the following order:
 * 1. In-memory cache ({@code bookDetailCache}): Fastest, simple ConcurrentHashMap
 * 2. Spring Cache (managed by {@code CacheManager}, configured with name "books"): Leverages Spring's caching abstraction
 * 3. Local disk cache (if {@code localCacheEnabled} is true, stored in {@code localCacheDirectory}): Useful for development/testing to persist cache across restarts
 * 4. Database cache (if {@code cacheEnabled} is true and {@code CachedBookRepository} is available): Persistent caching using a database table
 * 5. S3 cache (via {@link S3RetryService#fetchJsonWithRetry(String)}): Distributed, persistent cache, typically the largest and most resilient cache layer before hitting the live API
 *
 * If a book is not found in any cache layer, it falls back to fetching from the {@link GoogleBooksService#getBookById(String)} method
 * Successfully fetched books (from S3 or the live API) are then used to populate the preceding cache layers to ensure subsequent requests are served faster
 *
 * This class also handles cache eviction and provides methods to check for existence and retrieve from cache-only without external fallbacks
 * A property {@code googlebooks.api.override.bypass-caches} can be set to true to bypass all cache lookups and go directly to the GoogleBooksService, primarily for debugging
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
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
    
    private final GoogleBooksService googleBooksService;
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
     * @param googleBooksService Service for fetching book data from Google Books API (used as a final fallback)
     * @param s3RetryService Service for S3 operations with retry capabilities (used for S3 cache layer)
     * @param cacheManager Spring cache manager for managing in-memory caches (used for Spring Cache layer)
     * @param cachedBookRepository Repository for database-backed caching (optional, for database cache layer)
     * @param objectMapper JSON mapper for serializing/deserializing book data for local disk and S3 cache
     */
    @Autowired
    public GoogleBooksCachingStrategy(
            GoogleBooksService googleBooksService,
            S3RetryService s3RetryService,
            CacheManager cacheManager,
            @Autowired(required = false) CachedBookRepository cachedBookRepository,
            ObjectMapper objectMapper) {
        this.googleBooksService = googleBooksService;
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
     * Retrieves a book by its Google Books ID using multi-level cache approach
     * 
     * @param bookId The Google Books ID to look up
     * @return CompletionStage completing with the book if found, or null if not found
     */
    @Override
    public CompletionStage<Book> get(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        if (bypassCachesOverride) {
            logger.warn("BYPASS_CACHES_OVERRIDE ENABLED: Skipping all caches for book ID: {}. Going directly to GoogleBooksService.", bookId);
            return googleBooksService.getBookById(bookId)
                .thenCompose(apiBook -> {
                    // Even when bypassing caches for reads, we might still want to populate them on a successful API fetch
                    // However, for a full bypass, we might skip this. For now, let's assume bypass is for reads
                    // If the intention is to also bypass writes to cache on override, this 'putReactive' should also be conditional
                    if (apiBook != null) {
                        putReactive(bookId, apiBook).subscribe();
                    }
                    return CompletableFuture.completedFuture(apiBook);
                });
        }
        
        // Step 1: Check in-memory cache (fastest)
        Book cachedBook = bookDetailCache.get(bookId);
        if (cachedBook != null) {
            logger.debug("In-memory cache hit for book ID: {}", bookId);
            return CompletableFuture.completedFuture(cachedBook);
        }
        
        // Step 2: Check Spring Cache
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            Book springCachedBook = springCache.get(bookId, Book.class);
            if (springCachedBook != null) {
                logger.debug("Spring cache hit for book ID: {}", bookId);
                // Update in-memory cache
                bookDetailCache.put(bookId, springCachedBook);
                return CompletableFuture.completedFuture(springCachedBook);
            }
        }
        
        // Step 3: Check local disk cache if enabled (dev/test environments)
        if (localCacheEnabled) {
            Book localBook = getBookFromLocalCache(bookId);
            if (localBook != null) {
                logger.debug("Local disk cache hit for book ID: {}", bookId);
                // Update faster caches
                updateFasterCaches(bookId, localBook);
                return CompletableFuture.completedFuture(localBook);
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
                    return CompletableFuture.completedFuture(dbBook);
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
                        Book s3Book = googleBooksService.convertJsonToBook(bookNode);
                        
                        if (s3Book != null && s3Book.getId() != null) {
                            logger.debug("S3 cache hit for book ID: {}", bookId);
                            // Update all caches
                            putReactive(bookId, s3Book).subscribe();
                            return CompletableFuture.completedFuture(s3Book);
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
                
                // Step 6: Fall back to API call after all cache layers miss
                logger.debug("All caches missed for book ID: {}. Calling GoogleBooksService.getBookById.", bookId);
                return googleBooksService.getBookById(bookId)
                    .thenCompose(apiBook -> {
                        if (apiBook != null) {
                            // Update all caches with the book from API
                             putReactive(bookId, apiBook).subscribe();
                        }
                        return CompletableFuture.completedFuture(apiBook);
                    });
            });
    }

    /**
     * Retrieves a book by its Google Books ID using reactive programming
     * 
     * @param bookId The Google Books ID to look up
     * @return Mono emitting the book if found, or empty if not found
     */
    @Override
    public Mono<Book> getReactive(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return Mono.empty();
        }
        
        // Convert the CompletionStage-based method to Reactive
        return Mono.defer(() -> Mono.fromCompletionStage(get(bookId)))
            .flatMap(book -> book != null ? Mono.just(book) : Mono.empty());
    }

    /**
     * Updates all caches with a book value
     * 
     * @param bookId The Google Books ID to associate with the book
     * @param book The book to cache
     * @return CompletionStage that completes when the operation is done
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
     * Updates all caches with a book value using reactive programming
     * 
     * @param bookId The Google Books ID to associate with the book
     * @param book The book to cache
     * @return Mono that completes when the operation is done
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
     * @param bookId The Google Books ID to remove
     * @return CompletionStage that completes when the operation is done
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
     * @return true if the book exists in any cache, false otherwise
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
     * Gets a book from cache only, without fetching from S3 or API
     * 
     * @param bookId The Google Books ID to look up
     * @return Optional containing the book if found in any local cache, empty otherwise
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
     * Clears all cache layers
     * 
     * @return CompletionStage that completes when the operation is done
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
     * Updates faster caches with a book value
     * 
     * @param bookId The Google Books ID
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
     * Gets a book from the local disk cache
     * 
     * @param bookId The Google Books ID
     * @return The book if found, null otherwise
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
     * Saves a book to the local disk cache
     * 
     * @param bookId The Google Books ID
     * @param book The book to save
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
     * Updates a CachedBook with values from a Book
     * 
     * @param cachedBook The existing CachedBook to update
     * @param book The Book with new values
     * @return The updated CachedBook
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
