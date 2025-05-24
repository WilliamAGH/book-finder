/**
 * Proxy service for smart API usage that minimizes external calls
 *
 * @author William Callahan
 *
 * Features:
 * - Implements multi-level caching for book data
 * - Provides intelligent request merging to reduce duplicate API calls
 * - Logs API usage patterns to help identify optimization opportunities
 * - Supports different caching strategies based on active profiles
 */

package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class BookApiProxy {
    private static final Logger logger = LoggerFactory.getLogger(BookApiProxy.class);
    
    private final GoogleBooksService googleBooksService;
    private final S3StorageService s3StorageService;
    private final ObjectMapper objectMapper;
    private final Optional<GoogleBooksMockService> mockService;
    
    // In-memory cache as first-level cache
    private final Map<String, CompletableFuture<Book>> bookRequestCache = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<List<Book>>> searchRequestCache = new ConcurrentHashMap<>();
    
    private final boolean localCacheEnabled;
    private final String localCacheDirectory;
    private final boolean alwaysCheckS3First;
    private final boolean logApiCalls;

    private final Executor ioExecutor = Executors.newFixedThreadPool(
        Runtime.getRuntime().availableProcessors() * 2,
        new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("book-api-proxy-io-" + counter.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            }
        }
    );

    /**
     * Constructs the BookApiProxy with necessary dependencies
     *
     * @param googleBooksService Service for interacting with Google Books API
     * @param s3StorageService Service for interacting with S3
     * @param objectMapper ObjectMapper for JSON processing
     * @param mockService Optional mock service for testing
     */
    public BookApiProxy(GoogleBooksService googleBooksService, 
                       S3StorageService s3StorageService, 
                       ObjectMapper objectMapper,
                       Optional<GoogleBooksMockService> mockService,
                       @Value("${app.local-cache.enabled:false}") boolean localCacheEnabled,
                       @Value("${app.local-cache.directory:.dev-cache}") String localCacheDirectory,
                       @Value("${app.s3-cache.always-check-first:false}") boolean alwaysCheckS3First,
                       @Value("${app.api-client.log-calls:true}") boolean logApiCalls) {
        this.googleBooksService = googleBooksService;
        this.s3StorageService = s3StorageService;
        this.objectMapper = objectMapper;
        this.mockService = mockService;
        this.localCacheEnabled = localCacheEnabled;
        this.localCacheDirectory = localCacheDirectory;
        this.alwaysCheckS3First = alwaysCheckS3First;
        this.logApiCalls = logApiCalls;
        
        // Create local cache directory if needed
        if (this.localCacheEnabled) {
            try {
                Files.createDirectories(Paths.get(this.localCacheDirectory, "books"));
                Files.createDirectories(Paths.get(this.localCacheDirectory, "searches"));
            } catch (Exception e) {
                logger.warn("Could not create local cache directories: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Smart book retrieval that minimizes API calls using caching
     *
     * @param bookId Book ID to retrieve
     * @return CompletionStage with the Book if found, null otherwise
     */
    @Cacheable(value = "bookRequests", key = "#bookId", condition = "#root.target.cacheEnabled")
    public CompletionStage<Book> getBookById(String bookId) {
        // Use computeIfAbsent for atomic get-or-create to prevent race conditions
        CompletableFuture<Book> future = bookRequestCache.computeIfAbsent(bookId, id -> {
            CompletableFuture<Book> newFuture = new CompletableFuture<>();
            
            // Remove from cache when complete to prevent memory leaks
            newFuture.whenComplete((result, ex) -> bookRequestCache.remove(bookId));
            
            // Start processing pipeline
            processBookRequest(bookId, newFuture);
            
            return newFuture;
        });
        
        return future;
    }
    
    /**
     * Process book request through caching layers
     *
     * @param bookId Book ID to retrieve
     * @param future Future to complete with result
     */
    private void processBookRequest(String bookId, CompletableFuture<Book> future) {
        CompletableFuture<Optional<Book>> localCacheFuture = localCacheEnabled ? 
            getBookFromLocalCacheAsync(bookId) : 
            CompletableFuture.completedFuture(Optional.empty());

        localCacheFuture.thenCompose(localBookOptional -> {
            if (localBookOptional.isPresent()) {
                logger.debug("Retrieved book {} from local cache", bookId);
                return CompletableFuture.completedFuture(localBookOptional.get());
            }
            logger.debug("Local cache MISS for bookId: {}", bookId);

            // Next, check mock data if available
            if (mockService.isPresent() && mockService.get().hasMockDataForBook(bookId)) {
                Book mockBook = mockService.get().getBookById(bookId); // Assuming mock is fast/sync
                if (mockBook != null) {
                    logger.debug("Retrieved book {} from mock data", bookId);
                    // Save to local cache asynchronously
                    if (localCacheEnabled) {
                        saveBookToLocalCacheAsync(bookId, mockBook)
                            .exceptionally(ex -> { 
                                logger.warn("Failed to save mock book {} to local cache: {}", bookId, ex.getMessage()); 
                                return null; 
                            });
                    }
                    return CompletableFuture.completedFuture(mockBook);
                }
            } else if (mockService.isPresent()) {
                logger.debug("Mock service MISS for bookId: {} (or no mock data available)", bookId);
            }

            // S3 or Google Books API flow
            if (alwaysCheckS3First) {
                logger.debug("Checking S3 cache for bookId: {} (alwaysCheckS3First=true)", bookId);
                return s3StorageService.fetchJsonAsync(bookId)
                    .thenCompose(s3Result -> {
                        if (s3Result.isSuccess() && s3Result.getData().isPresent()) {
                            try {
                                JsonNode bookNode = objectMapper.readTree(s3Result.getData().get());
                                Book book = objectMapper.treeToValue(bookNode, Book.class);
                                logger.debug("Retrieved book {} from S3 cache", bookId);
                                CompletableFuture<Void> saveToLocalCacheFuture = localCacheEnabled ? 
                                    saveBookToLocalCacheAsync(bookId, book) : 
                                    CompletableFuture.completedFuture(null);
                                return saveToLocalCacheFuture.thenApply(v -> book);
                            } catch (Exception e) {
                                logger.warn("Error parsing book from S3 cache for {}: {}", bookId, e.getMessage());
                                // Fall through to API call
                            }
                        } else {
                            logger.debug("S3 cache MISS for bookId: {}. Reason: {}", bookId, s3Result.getErrorMessage().orElse("Fetch not successful or data not present"));
                        }
                        // Fallback to Google Books API
                        if (logApiCalls) logger.info("Making REAL API call to Google Books for book ID: {}", bookId);
                        return googleBooksService.getBookById(bookId)
                            .thenCompose(apiBook -> {
                                if (apiBook != null && localCacheEnabled) {
                                    return saveBookToLocalCacheAsync(bookId, apiBook).thenApply(v -> apiBook);
                                }
                                return CompletableFuture.completedFuture(apiBook);
                            });
                    });
            } else {
                // Standard flow: Google Books Service (which might use S3 internally or its own caching)
                if (logApiCalls) logger.info("Making API call via GoogleBooksService for book ID: {}", bookId);
                return googleBooksService.getBookById(bookId)
                    .thenCompose(apiBook -> {
                        if (apiBook != null) {
                            CompletableFuture<Void> saveToLocal = localCacheEnabled ? 
                                saveBookToLocalCacheAsync(bookId, apiBook) : 
                                CompletableFuture.completedFuture(null);
                            
                            CompletableFuture<Void> saveToMock = (mockService.isPresent() && apiBook.getRawJsonResponse() != null) ?
                                CompletableFuture.runAsync(() -> {
                                    try {
                                        JsonNode bookNode = objectMapper.readTree(apiBook.getRawJsonResponse());
                                        mockService.get().saveBookResponse(bookId, bookNode);
                                    } catch (Exception e) {
                                        logger.warn("Error saving book {} to mock service: {}", bookId, e.getMessage());
                                    }
                                }) : CompletableFuture.completedFuture(null);
                            return CompletableFuture.allOf(saveToLocal, saveToMock).thenApply(v -> apiBook);
                        }
                        return CompletableFuture.completedFuture(null);
                    });
            }
        }).whenComplete((book, ex) -> {
            if (ex != null) {
                logger.error("Error processing book request for {}: {}", bookId, ex.getMessage(), ex);
                future.completeExceptionally(ex);
            } else {
                future.complete(book);
            }
        });
    }
    
    /**
     * Smart book search that minimizes API calls using caching
     *
     * @param query Search query
     * @param langCode Language code for search results
     * @return Mono emitting list of books matching the search
     */
    @Cacheable(value = "searchRequests", key = "#query + '-' + #langCode", condition = "#root.target.cacheEnabled")
    public Mono<List<Book>> searchBooks(String query, String langCode) {
        String cacheKey = query + "-" + langCode;
        
        // Use computeIfAbsent for atomic get-or-create to prevent race conditions
        CompletableFuture<List<Book>> future = searchRequestCache.computeIfAbsent(cacheKey, key -> {
            CompletableFuture<List<Book>> newFuture = new CompletableFuture<>();
            
            // Remove from cache when complete to prevent memory leaks
            newFuture.whenComplete((result, ex) -> searchRequestCache.remove(cacheKey));
            
            // Start processing pipeline
            processSearchRequest(query, langCode, newFuture);
            
            return newFuture;
        });
        
        return Mono.fromCompletionStage(future);
    }
    
    /**
     * Process search request through caching layers
     *
     * @param query Search query
     * @param langCode Language code
     * @param future Future to complete with result
     */
    private void processSearchRequest(String query, String langCode, CompletableFuture<List<Book>> future) {
        CompletableFuture<Optional<List<Book>>> localCacheFuture = localCacheEnabled ?
            getSearchFromLocalCacheAsync(query, langCode) :
            CompletableFuture.completedFuture(Optional.empty());

        localCacheFuture.thenCompose(localResultsOptional -> {
            if (localResultsOptional.isPresent() && !localResultsOptional.get().isEmpty()) {
                logger.debug("Retrieved search '{}' from local cache, {} results", query, localResultsOptional.get().size());
                return CompletableFuture.completedFuture(localResultsOptional.get());
            }
            logger.debug("Local cache MISS for search query: '{}'", query);

            // Next, check mock data if available
            if (mockService.isPresent() && mockService.get().hasMockDataForSearch(query)) {
                List<Book> mockResults = mockService.get().searchBooks(query); // Assuming mock is fast/sync
                if (mockResults != null && !mockResults.isEmpty()) {
                    logger.debug("Retrieved search '{}' from mock data, {} results", query, mockResults.size());
                    if (localCacheEnabled) {
                        saveSearchToLocalCacheAsync(query, langCode, mockResults)
                            .exceptionally(ex -> {
                                logger.warn("Failed to save mock search results for '{}' to local cache: {}", query, ex.getMessage());
                                return null;
                            });
                    }
                    return CompletableFuture.completedFuture(mockResults);
                }
            }

            // Fall back to the actual service
            if (logApiCalls) {
                logger.info("Making REAL API call to Google Books for search: '{}'", query);
            }
            
            // Convert Mono to CompletableFuture
            CompletableFuture<List<Book>> apiResultsFuture = new CompletableFuture<>();
            googleBooksService.searchBooksAsyncReactive(query, langCode)
                .subscribe(
                    apiResultsFuture::complete,
                    apiResultsFuture::completeExceptionally
                );

            return apiResultsFuture.thenCompose(results -> {
                if (results != null && !results.isEmpty() && localCacheEnabled) {
                    return saveSearchToLocalCacheAsync(query, langCode, results).thenApply(v -> results);
                }
                return CompletableFuture.completedFuture(results);
            });
        }).whenComplete((results, ex) -> {
            if (ex != null) {
                logger.error("Error processing search request for '{}': {}", query, ex.getMessage(), ex);
                future.completeExceptionally(ex);
            } else {
                future.complete(results);
            }
        });
    }
    
    /**
     * Gets book from local file cache
     *
     * @param bookId Book ID to retrieve
     * @return Book if found in cache, null otherwise
     */
    private CompletableFuture<Optional<Book>> getBookFromLocalCacheAsync(String bookId) {
        if (!localCacheEnabled) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        Path bookFile = Paths.get(localCacheDirectory, "books", bookId + ".json");

        // Consider using a dedicated executor for blocking I/O operations
        // For example: CompletableFuture.supplyAsync(() -> { ... }, ioExecutor);
        return CompletableFuture.supplyAsync(() -> {
            if (Files.exists(bookFile)) { // This is a blocking call
                try {
                    // objectMapper.readTree(Path) is more efficient if available and suitable
                    // Reading all bytes first, then parsing, to keep blocking section clear
                    byte[] jsonData = Files.readAllBytes(bookFile); // This is a blocking call
                    JsonNode bookNode = objectMapper.readTree(jsonData);
                    Book book = objectMapper.treeToValue(bookNode, Book.class);
                    return Optional.ofNullable(book);
                } catch (Exception e) {
                    logger.warn("Error reading book {} from local cache {}: {}", bookId, bookFile, e.getMessage());
                    return Optional.empty();
                }
            }
            logger.debug("Local cache MISS for bookId {} at path {}", bookId, bookFile);
            return Optional.empty();
        }, ioExecutor);
    }
    
    /**
     * Saves book to local file cache
     *
     * @param bookId Book ID to save
     * @param book Book object to save
     */
    private CompletableFuture<Void> saveBookToLocalCacheAsync(String bookId, Book book) {
        if (!localCacheEnabled || book == null) {
            return CompletableFuture.completedFuture(null);
        }
        Path bookFile = Paths.get(localCacheDirectory, "books", bookId + ".json");

        // Consider using a dedicated executor for blocking I/O operations
        return CompletableFuture.runAsync(() -> {
            try {
                Files.createDirectories(bookFile.getParent()); // This is a blocking call
                // objectMapper.writeValue(Path, Object) is more efficient if available
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(bookFile.toFile(), book); // This is a blocking call
                logger.debug("Saved book {} to local cache at {}", bookId, bookFile);
            } catch (Exception e) {
                logger.warn("Error saving book {} to local cache {}: {}", bookId, bookFile, e.getMessage());
                // Depending on requirements, might want to throw a CompletionException here
                // throw new CompletionException(e);
            }
        }, ioExecutor);
    }
    
    /**
     * Gets search results from the local file cache
     * 
     * @param query The search query
     * @param langCode The language code or null
     * @return List of Book objects if found in cache, null otherwise
     */
    private CompletableFuture<Optional<List<Book>>> getSearchFromLocalCacheAsync(String query, String langCode) {
        if (!localCacheEnabled) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        
        String normalizedQuery = query.toLowerCase().trim();
        String langString = langCode != null ? langCode : "any";
        String filename = normalizedQuery.replaceAll("[^a-zA-Z0-9-_]", "_") + "-" + langString + ".json";
        Path searchFile = Paths.get(localCacheDirectory, "searches", filename);

        // Consider using a dedicated executor for blocking I/O operations
        return CompletableFuture.supplyAsync(() -> {
            if (Files.exists(searchFile)) { // Blocking call
                try {
                    byte[] jsonData = Files.readAllBytes(searchFile); // Blocking call
                    List<Book> books = objectMapper.readValue(jsonData, 
                        objectMapper.getTypeFactory().constructCollectionType(List.class, Book.class));
                    return Optional.ofNullable(books);
                } catch (Exception e) {
                    logger.warn("Error reading search results for query '{}' from local cache {}: {}", query, searchFile, e.getMessage());
                    return Optional.empty();
                }
            }
            logger.debug("Local cache MISS for search query '{}' at path {}", query, searchFile);
            return Optional.empty();
        }, ioExecutor);
    }
    
    /**
     * Saves search results to the local file cache asynchronously.
     * 
     * @param query The search query
     * @param langCode The language code or null
     * @param results The search results to save
     * @return CompletableFuture<Void> indicating completion of the save operation
     */
    private CompletableFuture<Void> saveSearchToLocalCacheAsync(String query, String langCode, List<Book> results) {
        if (!localCacheEnabled || results == null) {
            return CompletableFuture.completedFuture(null);
        }

        String normalizedQuery = query.toLowerCase().trim();
        String langString = langCode != null ? langCode : "any";
        String filename = normalizedQuery.replaceAll("[^a-zA-Z0-9-_]", "_") + "-" + langString + ".json";
        Path searchFile = Paths.get(localCacheDirectory, "searches", filename);

        // Consider using a dedicated executor for blocking I/O operations
        return CompletableFuture.runAsync(() -> {
            try {
                Files.createDirectories(searchFile.getParent()); // Blocking call
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(searchFile.toFile(), results); // Blocking call
                logger.debug("Saved search results for query '{}' to local cache at {}", query, searchFile);
            } catch (Exception e) {
                logger.warn("Error saving search results for query '{}' to local cache {}: {}", query, searchFile, e.getMessage());
                // Optionally, rethrow as a CompletionException if callers need to react to save failures
                // throw new CompletionException(e);
            }
        }, ioExecutor);
    }
}
