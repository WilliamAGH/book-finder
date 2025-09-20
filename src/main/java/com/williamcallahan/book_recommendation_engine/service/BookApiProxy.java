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
        // First try the local file cache (in dev/test mode)
        if (localCacheEnabled) {
            Book localBook = getBookFromLocalCache(bookId);
            if (localBook != null) {
                logger.debug("Retrieved book {} from local cache", bookId);
                future.complete(localBook);
                return;
            } else {
                logger.debug("Local cache MISS for bookId: {}", bookId);
            }
        }
        
        // Next, check mock data if available (in dev/test mode)
        if (mockService.isPresent() && mockService.get().hasMockDataForBook(bookId)) {
            Book mockBook = mockService.get().getBookById(bookId);
            if (mockBook != null) {
                logger.debug("Retrieved book {} from mock data", bookId);
                future.complete(mockBook);
                
                // Still persist to local cache for faster future access
                if (localCacheEnabled) {
                    saveBookToLocalCache(bookId, mockBook);
                }
                
                return;
            }
        } else if (mockService.isPresent()) {
            logger.debug("Mock service MISS for bookId: {} (or no mock data available)", bookId);
        }
        
        // Always check S3 cache first in configurations that prefer it
        if (alwaysCheckS3First) {
            logger.debug("Checking S3 cache for bookId: {} (alwaysCheckS3First=true)", bookId);
            // Try to get from S3 cache directly
            s3StorageService.fetchJsonAsync(bookId)
                .<Book>thenCompose(s3Result -> {
                    if (s3Result.isSuccess()) {
                        Optional<String> jsonDataOptional = s3Result.getData();
                        if (jsonDataOptional.isPresent()) {
                            try {
                                JsonNode bookNode = objectMapper.readTree(jsonDataOptional.get());
                                Book book = objectMapper.treeToValue(bookNode, Book.class);

                                // Cache the S3 result locally for faster future access
                                if (localCacheEnabled) {
                                    saveBookToLocalCache(bookId, book);
                                }

                                // Also cache in mock service for future test runs
                                if (mockService.isPresent()) {
                                    mockService.get().saveBookResponse(bookId, bookNode);
                                }

                                logger.debug("Retrieved book {} from S3 cache", bookId);
                                return CompletableFuture.completedFuture(book);
                            } catch (Exception e) {
                                logger.warn("BookApiProxy: Error parsing book {} from S3 cache. Proceeding to API. Message: {}", bookId, e.getMessage());
                            }
                        } else {
                            logger.info("BookApiProxy: S3 cache miss (data absent) for bookId {}. Falling back to API.", bookId);
                        }
                    } else {
                        logger.warn("BookApiProxy: S3 cache unavailable for bookId {}. Reason: {}", bookId, s3Result.getErrorMessage().orElse("Unknown S3 error"));
                    }
                    // If S3 fails or data is not present, fallback to the actual API
                    if (logApiCalls) {
                        logger.info("Making REAL API call to Google Books for book ID: {}", bookId);
                    }
                    
                    return googleBooksService.getBookById(bookId)
                        .thenApply(book -> {
                            // If retrieved successfully, cache the result
                            if (book != null && localCacheEnabled) {
                                saveBookToLocalCache(bookId, book);
                            }
                            return book;
                        });
                })
                .whenComplete((book, ex) -> {
                    if (ex != null) {
                        logger.error("Error retrieving book {}: {}", bookId, ex.getMessage());
                        future.completeExceptionally(ex);
                    } else {
                        future.complete(book);
                    }
                });
        } else {
            // Standard flow - use Google Books Service which already has S3 caching integrated
            googleBooksService.getBookById(bookId)
                .thenAccept(book -> {
                    // Cache to local file system for development
                    if (book != null && localCacheEnabled) {
                        saveBookToLocalCache(bookId, book);
                    }
                    
                    // Cache for mock service if this was a real API call
                    if (book != null && mockService.isPresent() && book.getRawJsonResponse() != null) {
                        try {
                            JsonNode bookNode = objectMapper.readTree(book.getRawJsonResponse());
                            mockService.get().saveBookResponse(bookId, bookNode);
                        } catch (Exception e) {
                            logger.warn("Error saving book to mock service: {}", e.getMessage());
                        }
                    }
                    
                    future.complete(book);
                })
                .exceptionally(ex -> {
                    logger.error("Error retrieving book {}: {}", bookId, ex.getMessage());
                    future.completeExceptionally(ex);
                    return null;
                });
        }
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
        // First try the local file cache (in dev/test mode)
        if (localCacheEnabled) {
            List<Book> localResults = getSearchFromLocalCache(query, langCode);
            if (localResults != null && !localResults.isEmpty()) {
                logger.debug("Retrieved search '{}' from local cache, {} results", query, localResults.size());
                future.complete(localResults);
                return;
            }
        }
        
        // Next, check mock data if available (in dev/test mode)
        if (mockService.isPresent() && mockService.get().hasMockDataForSearch(query)) {
            List<Book> mockResults = mockService.get().searchBooks(query);
            if (mockResults != null && !mockResults.isEmpty()) {
                logger.debug("Retrieved search '{}' from mock data, {} results", query, mockResults.size());
                future.complete(mockResults);
                
                // Still persist to local cache for faster future access
                if (localCacheEnabled) {
                    saveSearchToLocalCache(query, langCode, mockResults);
                }
                
                return;
            }
        }
        
        // Fall back to the actual service (which has its own caching)
        if (logApiCalls) {
            logger.info("Making REAL API call to Google Books for search: '{}'", query);
        }
        
        // This is already a Mono<List<Book>>, no need for collectList()
        googleBooksService.searchBooksAsyncReactive(query, langCode)
            .subscribe(
                results -> {
                    // Cache the results
                    if (results != null && !results.isEmpty() && localCacheEnabled) {
                        saveSearchToLocalCache(query, langCode, results);
                    }
                    future.complete(results);
                },
                error -> {
                    logger.error("Error searching for '{}': {}", query, error.getMessage());
                    future.completeExceptionally(error);
                }
            );
    }
    
    /**
     * Gets book from local file cache
     *
     * @param bookId Book ID to retrieve
     * @return Book if found in cache, null otherwise
     */
    private Book getBookFromLocalCache(String bookId) {
        if (!localCacheEnabled) return null;
        
        Path bookFile = Paths.get(localCacheDirectory, "books", bookId + ".json");
        
        if (Files.exists(bookFile)) {
            try {
                JsonNode bookNode = objectMapper.readTree(bookFile.toFile());
                return objectMapper.treeToValue(bookNode, Book.class);
            } catch (Exception e) {
                logger.warn("Error reading book from local cache: {}", e.getMessage());
            }
        }
        
        return null;
    }
    
    /**
     * Saves book to local file cache
     *
     * @param bookId Book ID to save
     * @param book Book object to save
     */
    private void saveBookToLocalCache(String bookId, Book book) {
        if (!localCacheEnabled || book == null) return;
        
        Path bookFile = Paths.get(localCacheDirectory, "books", bookId + ".json");
        
        try {
            // Ensure directory exists
            Files.createDirectories(bookFile.getParent());
            
            // Convert to JSON and save
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(bookFile.toFile(), book);
            logger.debug("Saved book {} to local cache", bookId);
        } catch (Exception e) {
            logger.warn("Error saving book to local cache: {}", e.getMessage());
        }
    }
    
    /**
     * Gets search results from the local file cache
     * 
     * @param query The search query
     * @param langCode The language code or null
     * @return List of Book objects if found in cache, null otherwise
     */
    private List<Book> getSearchFromLocalCache(String query, String langCode) {
        if (!localCacheEnabled) return null;
        
        String normalizedQuery = query.toLowerCase().trim();
        String langString = langCode != null ? langCode : "any";
        String filename = normalizedQuery.replaceAll("[^a-zA-Z0-9-_]", "_") + "-" + langString + ".json";
        
        Path searchFile = Paths.get(localCacheDirectory, "searches", filename);
        
        if (Files.exists(searchFile)) {
            try {
                return objectMapper.readValue(searchFile.toFile(), 
                        objectMapper.getTypeFactory().constructCollectionType(List.class, Book.class));
            } catch (Exception e) {
                logger.warn("Error reading search results from local cache: {}", e.getMessage());
            }
        }
        
        return null;
    }
    
    /**
     * Saves search results to the local file cache
     * 
     * @param query The search query
     * @param langCode The language code or null
     * @param results The search results to save
     */
    private void saveSearchToLocalCache(String query, String langCode, List<Book> results) {
        if (!localCacheEnabled || results == null) return;
        
        String normalizedQuery = query.toLowerCase().trim();
        String langString = langCode != null ? langCode : "any";
        String filename = normalizedQuery.replaceAll("[^a-zA-Z0-9-_]", "_") + "-" + langString + ".json";
        
        Path searchFile = Paths.get(localCacheDirectory, "searches", filename);
        
        try {
            // Ensure directory exists
            Files.createDirectories(searchFile.getParent());
            
            // Save to JSON
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(searchFile.toFile(), results);
            logger.debug("Saved search results for '{}' ({}) to local cache", query, langCode);
        } catch (Exception e) {
            logger.warn("Error saving search results to local cache: {}", e.getMessage());
        }
    }
}
