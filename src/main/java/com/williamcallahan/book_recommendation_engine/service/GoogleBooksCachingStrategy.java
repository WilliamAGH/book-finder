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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
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
    private final RedisCacheService redisCacheService; 
    
    private final ConcurrentHashMap<String, Book> bookDetailCache = new ConcurrentHashMap<>();
    private final Executor localDiskExecutor = Executors.newFixedThreadPool(4); 
    
    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled;
    
    @Value("${app.local-cache.enabled:false}")
    private boolean localCacheEnabled;
    
    @Value("${app.local-cache.directory:.dev-cache}")
    private String localCacheDirectory;

    @Value("${googlebooks.api.override.bypass-caches:false}")
    private boolean bypassCachesOverride;

    public GoogleBooksCachingStrategy(
            BookDataOrchestrator bookDataOrchestrator,
            S3RetryService s3RetryService,
            CacheManager cacheManager,
            CachedBookRepository cachedBookRepository,
            ObjectMapper objectMapper,
            RedisCacheService redisCacheService) {
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.s3RetryService = s3RetryService;
        this.cacheManager = cacheManager;
        this.cachedBookRepository = cachedBookRepository;
        this.objectMapper = objectMapper;
        this.redisCacheService = redisCacheService;
        
        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false;
            logger.info("Database cache is not available. Running in API-only mode.");
        }
        
        if (localCacheEnabled) {
            try {
                Files.createDirectories(Paths.get(localCacheDirectory, "books"));
            } catch (Exception e) {
                logger.warn("Could not create local cache directories: {}", e.getMessage());
            }
        }
    }

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
        if (bookOpt.isPresent()) {
            return CompletableFuture.completedFuture(bookOpt);
        }

        bookOpt = checkSpringCache(bookId);
        if (bookOpt.isPresent()) {
            return CompletableFuture.completedFuture(bookOpt);
        }

        return checkLocalDiskCacheAsync(bookId)
            .thenComposeAsync(localBookOpt -> {
                if (localBookOpt.isPresent()) {
                    return CompletableFuture.completedFuture(localBookOpt);
                }
                return checkRedisCacheAsync(bookId)
                    .thenComposeAsync(redisBookOpt -> {
                        if (redisBookOpt.isPresent()) {
                            return CompletableFuture.completedFuture(redisBookOpt);
                        }
                        return checkDatabaseCacheAsync(bookId)
                            .thenComposeAsync(dbBookOpt -> {
                                if (dbBookOpt.isPresent()) {
                                    return CompletableFuture.completedFuture(dbBookOpt);
                                }
                                return checkS3CacheThenApiFallback(bookId);
                            }, localDiskExecutor);
                    }, localDiskExecutor);
            }, localDiskExecutor);
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
                bookDetailCache.put(bookId, springCachedBook);
                return Optional.of(springCachedBook);
            }
        }
        return Optional.empty();
    }

    private CompletableFuture<Optional<Book>> checkLocalDiskCacheAsync(String bookId) {
        if (localCacheEnabled) {
            return getBookFromLocalCacheAsync(bookId)
                .thenApply(localBookOpt -> {
                    if (localBookOpt.isPresent()) {
                        Book localBook = localBookOpt.get();
                        logger.debug("Local disk cache hit for book ID: {}", bookId);
                        updateFasterCaches(bookId, localBook);
                        return Optional.of(localBook);
                    }
                    return Optional.<Book>empty();
                });
        }
        return CompletableFuture.completedFuture(Optional.empty());
    }

    private CompletableFuture<Optional<Book>> checkRedisCacheAsync(String bookId) {
        if (redisCacheService != null) {
            return redisCacheService.getBookByIdAsync(bookId)
                .thenApply(redisBookOpt -> {
                    if (redisBookOpt.isPresent()) {
                        logger.debug("Redis cache hit for book ID: {}", bookId);
                        updateFasterCaches(bookId, redisBookOpt.get());
                    }
                    return redisBookOpt;
                });
        }
        return CompletableFuture.completedFuture(Optional.empty());
    }

    private CompletableFuture<Optional<Book>> checkDatabaseCacheAsync(String bookId) {
        if (cacheEnabled && cachedBookRepository != null) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Optional<CachedBook> dbCachedBook = cachedBookRepository.findByGoogleBooksId(bookId);
                    if (dbCachedBook.isPresent()) {
                        Book dbBook = dbCachedBook.get().toBook();
                        logger.debug("Database cache hit for book ID: {}", bookId);
                        updateFasterCaches(bookId, dbBook); 
                        if (redisCacheService != null) {
                            // Asynchronously update Redis, don't block the current flow
                            redisCacheService.cacheBookAsync(bookId, dbBook)
                                .exceptionally(ex -> {
                                    logger.warn("Failed to populate Redis from DB for book ID {}: {}", bookId, ex.getMessage());
                                    return null;
                                });
                        }
                        return Optional.of(dbBook);
                    }
                } catch (Exception e) {
                    logger.warn("Error accessing database cache for book ID {}: {}", bookId, e.getMessage());
                }
                return Optional.<Book>empty();
            }, localDiskExecutor);
        }
        return CompletableFuture.completedFuture(Optional.empty());
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
                            putReactive(bookId, s3Book).subscribe(); 
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
                        putReactive(bookId, orchestratedBook) 
                            .doOnError(e -> logger.error("Error populating caches from orchestrator result for {}: {}", bookId, e.getMessage()))
                            .then(Mono.just(Optional.of(orchestratedBook)))
                    )
                    .defaultIfEmpty(Optional.empty())
                    .toFuture();
            });
    }

    @Override
    public Mono<Book> getReactive(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return Mono.empty();
        }
        return Mono.defer(() -> Mono.fromCompletionStage(get(bookId)))
            .subscribeOn(Schedulers.boundedElastic()) 
            .flatMap(optionalBook -> optionalBook.map(Mono::just).orElseGet(Mono::empty)); 
    }

    public Mono<Book> getReactiveFromCacheOnly(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return Mono.empty();
        }
        return Mono.fromCompletionStage(getFromCacheOnlyAsync(bookId)) // Use the new async interface method
            .subscribeOn(Schedulers.boundedElastic()) 
            .flatMap(optionalBook -> optionalBook.map(Mono::just).orElseGet(Mono::empty));
    }

    @Override
    public CompletionStage<Void> put(String bookId, Book book) {
        if (bookId == null || bookId.isEmpty() || book == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        bookDetailCache.put(bookId, book);
        
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            springCache.put(bookId, book);
        }
        
        CompletableFuture<Void> localDiskFuture = CompletableFuture.completedFuture(null);
        if (localCacheEnabled) {
            localDiskFuture = saveBookToLocalCacheAsync(bookId, book);
        }

        localDiskFuture.thenCompose(v -> 
            putReactive(bookId, book).toFuture()
        );
        putReactive(bookId, book).subscribe();
        
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Mono<Void> putReactive(String bookId, Book book) {
        if (bookId == null || bookId.isEmpty() || book == null) {
            return Mono.empty();
        }
        
        bookDetailCache.put(bookId, book);
        
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            springCache.put(bookId, book);
        }
        
        if (localCacheEnabled) {
            Mono.fromRunnable(() -> saveBookToLocalCacheAsync(bookId, book))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(); 
        }

        Mono<Void> redisPutMono = Mono.empty();
        if (redisCacheService != null) {
            redisPutMono = redisCacheService.cacheBookReactive(bookId, book);
        }
        
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
        
        return redisPutMono.then(dbPutMono); 
    }

    @Override
    public CompletionStage<Void> evict(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        
        bookDetailCache.remove(bookId);
        
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            springCache.evict(bookId);
        }
        
        CompletableFuture<Void> localDiskEvictFuture = CompletableFuture.completedFuture(null);
        if (localCacheEnabled) {
            localDiskEvictFuture = CompletableFuture.runAsync(() -> {
                try {
                    Path bookFile = Paths.get(localCacheDirectory, "books", bookId + ".json");
                    Files.deleteIfExists(bookFile);
                } catch (Exception e) {
                    logger.warn("Error removing book from local disk cache: {}", e.getMessage());
                }
            }, localDiskExecutor);
        }
        
        CompletableFuture<Void> redisEvictFuture = CompletableFuture.completedFuture(null);
        if (redisCacheService != null) {
            redisEvictFuture = redisCacheService.evictBookAsync(bookId);
        }

        CompletableFuture<Void> dbEvictFuture = CompletableFuture.completedFuture(null);
        if (cacheEnabled && cachedBookRepository != null) {
            dbEvictFuture = CompletableFuture.runAsync(() -> {
                try {
                    cachedBookRepository.findByGoogleBooksId(bookId)
                        .ifPresent(cachedBook -> cachedBookRepository.delete(cachedBook));
                } catch (Exception e) {
                    logger.warn("Error removing book from database cache: {}", e.getMessage());
                }
            }, localDiskExecutor); 
        }
        
        return CompletableFuture.allOf(localDiskEvictFuture, redisEvictFuture, dbEvictFuture);
    }

    @Override
    public CompletableFuture<Boolean> existsAsync(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }
        
        if (bookDetailCache.containsKey(bookId)) return CompletableFuture.completedFuture(true);
        
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null && springCache.get(bookId) != null) return CompletableFuture.completedFuture(true);
        
        CompletableFuture<Boolean> localDiskCheck = localCacheEnabled ?
            CompletableFuture.supplyAsync(() -> Files.exists(Paths.get(localCacheDirectory, "books", bookId + ".json")), localDiskExecutor) :
            CompletableFuture.completedFuture(false);

        return localDiskCheck.thenComposeAsync(existsInLocal -> {
            if (existsInLocal) return CompletableFuture.completedFuture(true);
            
            CompletableFuture<Boolean> redisCheck = (redisCacheService != null) ?
                redisCacheService.getBookByIdAsync(bookId).thenApply(Optional::isPresent) :
                CompletableFuture.completedFuture(false);

            return redisCheck.thenComposeAsync(existsInRedis -> {
                if (existsInRedis) return CompletableFuture.completedFuture(true);

                if (cacheEnabled && cachedBookRepository != null) {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            return cachedBookRepository.findByGoogleBooksId(bookId).isPresent();
                        } catch (Exception e) {
                            logger.warn("Error checking database cache for existsAsync: {}", e.getMessage());
                            return false;
                        }
                    }, localDiskExecutor); 
                }
                return CompletableFuture.completedFuture(false);
            }, localDiskExecutor);
        }, localDiskExecutor);
    }
    
    @Deprecated
    public boolean exists(String bookId) {
        try {
            return existsAsync(bookId).join(); 
        } catch (Exception e) {
            logger.error("Error in exists(String bookId): {}", e.getMessage(), e);
            return false;
        }
    }

    @Override
    public CompletableFuture<Optional<Book>> getFromCacheOnlyAsync(String bookId) {
        if (bookId == null || bookId.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        
        Book cachedBook = bookDetailCache.get(bookId);
        if (cachedBook != null) return CompletableFuture.completedFuture(Optional.of(cachedBook));
        
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            Book springCachedBook = springCache.get(bookId, Book.class);
            if (springCachedBook != null) return CompletableFuture.completedFuture(Optional.of(springCachedBook));
        }
        
        CompletableFuture<Optional<Book>> localDiskFuture = localCacheEnabled ?
            getBookFromLocalCacheAsync(bookId) : CompletableFuture.completedFuture(Optional.empty());

        return localDiskFuture.thenComposeAsync(localBookOpt -> {
            if (localBookOpt.isPresent()) return CompletableFuture.completedFuture(localBookOpt);

            CompletableFuture<Optional<Book>> redisFuture = (redisCacheService != null) ?
                redisCacheService.getBookByIdAsync(bookId) : CompletableFuture.completedFuture(Optional.empty());

            return redisFuture.thenComposeAsync(redisBookOpt -> {
                if (redisBookOpt.isPresent()) return CompletableFuture.completedFuture(redisBookOpt);

                if (cacheEnabled && cachedBookRepository != null) {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            return cachedBookRepository.findByGoogleBooksId(bookId).map(CachedBook::toBook);
                        } catch (Exception e) {
                            logger.warn("Error accessing database cache for getFromCacheOnlyAsync: {}", e.getMessage());
                            return Optional.empty();
                        }
                    }, localDiskExecutor);
                }
                return CompletableFuture.completedFuture(Optional.empty());
            }, localDiskExecutor);
        }, localDiskExecutor);
    }

    @Deprecated
    public Optional<Book> getFromCacheOnly(String bookId) {
        try {
            return getFromCacheOnlyAsync(bookId).join(); 
        } catch (Exception e) {
            logger.error("Error in getFromCacheOnly(String bookId): {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    @Override
    public CompletableFuture<Void> clearAll() { 
        bookDetailCache.clear();
        
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            springCache.clear();
        }
        
        CompletableFuture<Void> localDiskClearFuture = CompletableFuture.completedFuture(null);
        if (localCacheEnabled) {
            localDiskClearFuture = CompletableFuture.runAsync(() -> {
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
            }, localDiskExecutor);
        }
        
        CompletableFuture<Void> dbClearFuture = CompletableFuture.completedFuture(null);
        if (cacheEnabled && cachedBookRepository != null) {
            dbClearFuture = CompletableFuture.runAsync(() -> {
                try {
                    cachedBookRepository.deleteAll();
                } catch (Exception e) {
                    logger.warn("Error clearing database cache: {}", e.getMessage());
                }
            }, localDiskExecutor); 
        }
        
        return CompletableFuture.allOf(localDiskClearFuture, dbClearFuture);
    }

    private void updateFasterCaches(String bookId, Book book) {
        bookDetailCache.put(bookId, book);
        
        org.springframework.cache.Cache springCache = cacheManager.getCache("books");
        if (springCache != null) {
            springCache.put(bookId, book);
        }
    }

    private CompletableFuture<Optional<Book>> getBookFromLocalCacheAsync(String bookId) {
        if (!localCacheEnabled) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        
        return CompletableFuture.supplyAsync(() -> {
            Path bookFile = Paths.get(localCacheDirectory, "books", bookId + ".json");
            if (Files.exists(bookFile)) {
                try {
                    return Optional.of(objectMapper.readValue(bookFile.toFile(), Book.class));
                } catch (Exception e) {
                    logger.warn("Error reading book from local disk cache for ID {}: {}", bookId, e.getMessage());
                }
            }
            return Optional.empty();
        }, localDiskExecutor);
    }

    private CompletableFuture<Void> saveBookToLocalCacheAsync(String bookId, Book book) {
        if (!localCacheEnabled || book == null) {
            return CompletableFuture.completedFuture(null);
        }
        
        return CompletableFuture.runAsync(() -> {
            Path bookFile = Paths.get(localCacheDirectory, "books", bookId + ".json");
            try {
                Files.createDirectories(bookFile.getParent());
                objectMapper.writeValue(bookFile.toFile(), book);
            } catch (Exception e) {
                logger.warn("Error saving book to local disk cache for ID {}: {}", bookId, e.getMessage());
            }
        }, localDiskExecutor);
    }

    private CachedBook updateCachedBook(CachedBook cachedBook, Book book) {
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
        
        cachedBook.setCachedRecommendationIds(book.getCachedRecommendationIds() != null ? new ArrayList<>(book.getCachedRecommendationIds()) : new ArrayList<>());
        cachedBook.setQualifiers(book.getQualifiers() != null ? new HashMap<>(book.getQualifiers()) : new HashMap<>());
        cachedBook.setOtherEditions(book.getOtherEditions() != null ? new ArrayList<>(book.getOtherEditions()) : new ArrayList<>());

        cachedBook.setLastAccessed(LocalDateTime.now());

        try {
            if (book.getRawJsonResponse() != null && !book.getRawJsonResponse().isEmpty()) {
                JsonNode rawJsonNode = objectMapper.readTree(book.getRawJsonResponse());
                cachedBook.setRawData(rawJsonNode);
            } else {
                 logger.debug("Book object for ID {} provided to updateCachedBook has no rawJsonResponse. RawData field in CachedBook will not be updated from it.", book.getId());
            }
        } catch (Exception e) {
            logger.warn("Error updating raw JSON in CachedBook for ID {}: {}", book.getId(), e.getMessage());
        }
        
        return cachedBook;
    }
}
