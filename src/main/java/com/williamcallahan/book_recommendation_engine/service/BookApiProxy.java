/**
 * Proxy service for smart API usage that minimizes external calls
 * This class acts as the primary entry point for retrieving book data
 * It orchestrates calls to {@link GoogleBooksService} for live API interactions
 * and {@link GoogleBooksMockService} when running in 'dev' or 'test' profiles
 *
 * Key Features:
 * - Implements multi-level caching:
 *   1. In-memory request cache (for in-flight requests)
 *   2. Local file system cache (primarily for 'dev' and 'test' to speed up subsequent runs)
 *   3. Delegates to {@link GoogleBooksService}, which uses {@link GoogleBooksCachingStrategy}
 *      for further caching layers (S3, database, Spring's Cache Abstraction, etc.)
 * - Provides intelligent request merging via in-memory cache to reduce duplicate API calls
 * - Logs API usage patterns to help identify optimization opportunities
 * - Supports different caching strategies and behaviors based on active Spring profiles and configuration properties
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

/**
 * Smart API proxy that minimizes external calls in all environments
 * Implements a multi-level cache strategy with fallbacks:
 * 1. In-memory cache (fastest)
 * 2. Local disk cache (for development and testing)
 * 3. S3 remote cache (for all environments)
 * 4. Mock data (for development and testing)
 * 5. Real API call (last resort)
 */
@Service
public class BookApiProxy {
    private static final Logger logger = LoggerFactory.getLogger(BookApiProxy.class);
    
    private final GoogleBooksService googleBooksService;
    private final S3StorageService s3StorageService;
    private final ObjectMapper objectMapper;
    
    // Optional service that's only available in dev/test profiles
    private final Optional<GoogleBooksMockService> mockService;
    
    // In-memory cache as first-level cache
    private final Map<String, CompletableFuture<Book>> bookRequestCache = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<List<Book>>> searchRequestCache = new ConcurrentHashMap<>();
    
    @Value("${google.books.api.cache.enabled:true}")
    private boolean cacheEnabled;
    
    @Value("${app.local-cache.enabled:false}")
    private boolean localCacheEnabled;
    
    @Value("${app.local-cache.directory:.dev-cache}")
    private String localCacheDirectory;
    
    @Value("${app.s3-cache.always-check-first:false}")
    private boolean alwaysCheckS3First;
    
    @Value("${app.api-client.log-calls:true}")
    private boolean logApiCalls;
    
    @Value("${spring.profiles.active:default}")
    private String activeProfile;

    /**
     * Constructs the BookApiProxy with necessary dependencies
     * Initializes local cache directories if local caching is enabled
     *
     * @param googleBooksService The service for interacting with the actual Google Books API
     * @param s3StorageService The service for interacting with S3 (used by this proxy for S3-first strategy, and by GoogleBooksService)
     * @param objectMapper Jackson ObjectMapper for JSON processing
     * @param mockService Optional mock service, active in 'dev' and 'test' profiles
     */
    @Autowired
    public BookApiProxy(GoogleBooksService googleBooksService, 
                       S3StorageService s3StorageService, 
                       ObjectMapper objectMapper,
                       Optional<GoogleBooksMockService> mockService) {
        this.googleBooksService = googleBooksService;
        this.s3StorageService = s3StorageService;
        this.objectMapper = objectMapper;
        this.mockService = mockService;
        
        // Create local cache directory if needed
        if (localCacheEnabled) {
            try {
                Files.createDirectories(Paths.get(localCacheDirectory, "books"));
                Files.createDirectories(Paths.get(localCacheDirectory, "searches"));
            } catch (Exception e) {
                logger.warn("Could not create local cache directories: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Smart book retrieval that minimizes API calls by employing a multi-level cache strategy
     * and request merging
     *
     * The lookup order is generally:
     * 1. In-memory cache for in-flight requests ({@code bookRequestCache})
     * 2. Local file system cache (if {@code localCacheEnabled} is true)
     * 3. {@link GoogleBooksMockService} (if active profile is 'dev' or 'test' and mock data exists)
     * 4. S3 Cache (if {@code alwaysCheckS3First} is true, checked directly by this proxy)
     * 5. {@link GoogleBooksService#getBookById(String)}, which then uses {@link GoogleBooksCachingStrategy}
     *    (handling S3, database, Spring cache, and finally the live API call)
     *
     * Results from successful retrievals (mock, S3, or live API) are used to populate
     * the local file cache and potentially the mock service's persisted mocks
     * 
     * @param bookId The book ID to retrieve
     * @return CompletionStage with the Book if found, completed with null otherwise
     */
    @Cacheable(value = "bookRequests", key = "#bookId", condition = "#root.target.cacheEnabled")
    public CompletionStage<Book> getBookById(String bookId) {
        // Check for in-flight requests to prevent duplicate API calls
        CompletableFuture<Book> existingRequest = bookRequestCache.get(bookId);
        if (existingRequest != null && !existingRequest.isDone()) {
            logger.debug("Returning in-flight request for book ID: {}", bookId);
            return existingRequest;
        }
        
        CompletableFuture<Book> future = new CompletableFuture<>();
        bookRequestCache.put(bookId, future);
        
        // First try the local file cache (in dev/test mode)
        if (localCacheEnabled) {
            Book localBook = getBookFromLocalCache(bookId);
            if (localBook != null) {
                logger.debug("Retrieved book {} from local cache", bookId);
                future.complete(localBook);
                return future;
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
                
                return future;
            }
        }
        
        // Always check S3 cache first in configurations that prefer it
        if (alwaysCheckS3First) {
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
                                logger.warn("Error parsing book from S3 cache: {}", e.getMessage());
                            }
                        }
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
        
        return future;
    }
    
    /**
     * Smart book search that minimizes API calls by employing a multi-level cache strategy
     *
     * The lookup order is generally:
     * 1. In-memory cache for in-flight requests ({@code searchRequestCache})
     * 2. Local file system cache (if {@code localCacheEnabled} is true)
     * 3. {@link GoogleBooksMockService} (if active profile is 'dev' or 'test' and mock data exists for the query)
     * 4. {@link GoogleBooksService#searchBooksAsyncReactive(String, String)}, which is expected to handle its own
     *    caching and rate limiting (Note: Search caching is less elaborate than single book lookup in current design)
     *
     * Results from successful retrievals are used to populate the local file cache and
     * potentially the mock service's persisted mocks
     * 
     * @param query The search query
     * @param langCode Optional language code (e.g., "en", "fr")
     * @return Mono containing a list of Book objects matching the search criteria
     */
    @Cacheable(value = "searchRequests", key = "#query + '-' + #langCode", condition = "#root.target.cacheEnabled")
    public Mono<List<Book>> searchBooks(String query, String langCode) {
        String cacheKey = query + "-" + (langCode != null ? langCode : "any");
        
        // Check for in-flight requests
        CompletableFuture<List<Book>> existingRequest = searchRequestCache.get(cacheKey);
        if (existingRequest != null && !existingRequest.isDone()) {
            return Mono.fromFuture(existingRequest);
        }
        
        CompletableFuture<List<Book>> future = new CompletableFuture<>();
        searchRequestCache.put(cacheKey, future);
        
        // First, check local file cache
        if (localCacheEnabled) {
            List<Book> localResults = getSearchFromLocalCache(query, langCode);
            if (localResults != null) {
                logger.debug("Retrieved search results for '{}' ({}) from local cache", query, langCode);
                future.complete(localResults);
                return Mono.fromFuture(future);
            }
        }
        
        // Next, check mock data
        if (mockService.isPresent() && mockService.get().hasMockDataForSearch(query)) {
            List<Book> mockResults = mockService.get().searchBooks(query);
            if (mockResults != null && !mockResults.isEmpty()) {
                logger.debug("Retrieved search results for '{}' from mock data", query);
                future.complete(mockResults);
                
                // Still persist to local cache
                if (localCacheEnabled) {
                    saveSearchToLocalCache(query, langCode, mockResults);
                }
                
                return Mono.fromFuture(future);
            }
        }
        
        // Finally, make the API call
        if (logApiCalls) {
            logger.info("Making REAL API call to Google Books for search: '{}' ({})", query, langCode);
        }
        
        return googleBooksService.searchBooksAsyncReactive(query, langCode)
            .doOnSuccess(results -> {
                // Cache the results locally
                if (localCacheEnabled) {
                    saveSearchToLocalCache(query, langCode, results);
                }
                
                // Also cache in mock service for tests
                if (mockService.isPresent()) {
                    mockService.get().saveSearchResults(query, results);
                }
                
                future.complete(results);
            })
            .doOnError(ex -> {
                logger.error("Error searching books for '{}': {}", query, ex.getMessage());
                future.completeExceptionally(ex);
            });
    }
    
    /**
     * Gets a book from the local file cache
     * 
     * @param bookId The book ID to retrieve
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
     * Saves a book to the local file cache
     * 
     * @param bookId The book ID to save
     * @param book The Book object to save
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
