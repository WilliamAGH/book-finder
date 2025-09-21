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
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.SearchQueryUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
@Slf4j
public class BookApiProxy {
        
    private final GoogleBooksService googleBooksService;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final S3StorageService s3StorageService;
    private final ObjectMapper objectMapper;
    private static final int SEARCH_RESULT_LIMIT = 40;

    private final Optional<GoogleBooksMockService> mockService;
    
    // In-memory cache as first-level cache
    private final Map<String, CompletableFuture<Book>> bookRequestCache = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<List<Book>>> searchRequestCache = new ConcurrentHashMap<>();
    
    private final boolean localCacheEnabled;
    private final String localCacheDirectory;
    private final boolean alwaysCheckS3First;
    private final boolean logApiCalls;
    private final boolean externalFallbackEnabled;

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
                       @Value("${app.api-client.log-calls:true}") boolean logApiCalls,
                       @Value("${app.features.external-fallback.enabled:${app.features.google-fallback.enabled:true}}") boolean externalFallbackEnabled,
                       BookDataOrchestrator bookDataOrchestrator) {
        this.googleBooksService = googleBooksService;
        this.s3StorageService = s3StorageService;
        this.objectMapper = objectMapper;
        this.mockService = mockService;
        this.localCacheEnabled = localCacheEnabled;
        this.localCacheDirectory = localCacheDirectory;
        this.alwaysCheckS3First = alwaysCheckS3First;
        this.logApiCalls = logApiCalls;
        this.externalFallbackEnabled = externalFallbackEnabled;
        this.bookDataOrchestrator = bookDataOrchestrator;
        
        // Create local cache directory if needed
        if (this.localCacheEnabled) {
            try {
                Files.createDirectories(Paths.get(this.localCacheDirectory, "books"));
                Files.createDirectories(Paths.get(this.localCacheDirectory, "searches"));
            } catch (Exception e) {
                LoggingUtils.warn(log, e, "Could not create local cache directories");
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
                log.debug("Retrieved book {} from local cache", bookId);
                future.complete(localBook);
                return;
            } else {
                log.debug("Local cache MISS for bookId: {}", bookId);
            }
        }
        
        // Next, check mock data if available (in dev/test mode)
        if (mockService.isPresent() && mockService.get().hasMockDataForBook(bookId)) {
            Book mockBook = mockService.get().getBookById(bookId);
            if (mockBook != null) {
                log.debug("Retrieved book {} from mock data", bookId);
                future.complete(mockBook);
                
                // Still persist to local cache for faster future access
                if (localCacheEnabled) {
                    saveBookToLocalCache(bookId, mockBook);
                }
                
                return;
            }
        } else if (mockService.isPresent()) {
            log.debug("Mock service MISS for bookId: {} (or no mock data available)", bookId);
        }
        
        java.util.concurrent.atomic.AtomicBoolean resolved = new java.util.concurrent.atomic.AtomicBoolean(false);

        if (bookDataOrchestrator != null) {
            bookDataOrchestrator.getBookByIdTiered(bookId)
                .onErrorResume(ex -> {
                    LoggingUtils.warn(log, ex, "BookApiProxy: Orchestrator lookup failed for {}", bookId);
                    return Mono.empty();
                })
                .subscribe(book -> {
                    if (book != null && resolved.compareAndSet(false, true)) {
                        log.debug("BookApiProxy: Retrieved {} via orchestrator tier.", bookId);
                        if (localCacheEnabled) {
                            saveBookToLocalCache(bookId, book);
                        }
                        future.complete(book);
                    }
                }, ex -> {
                    LoggingUtils.warn(log, ex, "BookApiProxy: Orchestrator lookup error for {}", bookId);
                    if (resolved.compareAndSet(false, true)) {
                        continueWithApiFallback(bookId, future);
                    }
                }, () -> {
                    if (resolved.compareAndSet(false, true)) {
                        continueWithApiFallback(bookId, future);
                    }
                });
            return;
        }

        continueWithApiFallback(bookId, future);
    }

    private void continueWithApiFallback(String bookId, CompletableFuture<Book> future) {
        if (future.isDone()) {
            return;
        }

        if (alwaysCheckS3First) {
            log.debug("Checking S3 cache for bookId: {} (alwaysCheckS3First=true)", bookId);
            s3StorageService.fetchJsonAsync(bookId)
                .<Book>thenCompose(s3Result -> {
                    if (s3Result.isSuccess()) {
                        Optional<String> jsonDataOptional = s3Result.getData();
                        if (jsonDataOptional.isPresent()) {
                            try {
                                JsonNode bookNode = objectMapper.readTree(jsonDataOptional.get());
                                Book book = objectMapper.treeToValue(bookNode, Book.class);

                                if (localCacheEnabled) {
                                    saveBookToLocalCache(bookId, book);
                                }

                                if (mockService.isPresent()) {
                                    mockService.get().saveBookResponse(bookId, bookNode);
                                }

                                log.debug("Retrieved book {} from S3 cache", bookId);
                                return CompletableFuture.completedFuture(book);
                            } catch (Exception e) {
                                LoggingUtils.warn(log, e, "BookApiProxy: Error parsing book {} from S3 cache. Proceeding to API.", bookId);
                            }
                        } else {
                            log.info("BookApiProxy: S3 cache miss (data absent) for bookId {}. Falling back to API.", bookId);
                        }
                    } else {
                        log.warn("BookApiProxy: S3 cache unavailable for bookId {}. Reason: {}", bookId, s3Result.getErrorMessage().orElse("Unknown S3 error"));
                    }
                    if (logApiCalls) {
                        log.info("Making REAL API call to Google Books for book ID: {}", bookId);
                    }

                    if (!externalFallbackEnabled) {
                        log.debug("External fallback disabled for book ID '{}'. Skipping external API call.", bookId);
                        return CompletableFuture.completedFuture(null);
                    }

                    return googleBooksService.getBookById(bookId)
                        .thenApply(book -> {
                            if (book != null && localCacheEnabled) {
                                saveBookToLocalCache(bookId, book);
                            }
                            return book;
                        });
                })
                .whenComplete((book, ex) -> {
                    if (ex != null) {
                        LoggingUtils.error(log, ex, "Error retrieving book {}", bookId);
                        future.completeExceptionally(ex);
                    } else {
                        future.complete(book);
                    }
                });
        } else {
            if (!externalFallbackEnabled) {
                log.debug("External fallback disabled for book ID '{}'. Returning empty result.", bookId);
                future.complete(null);
                return;
            }
            googleBooksService.getBookById(bookId)
                .thenAccept(book -> {
                    if (book != null && localCacheEnabled) {
                        saveBookToLocalCache(bookId, book);
                    }

                    if (book != null && mockService.isPresent() && book.getRawJsonResponse() != null) {
                        try {
                            JsonNode bookNode = objectMapper.readTree(book.getRawJsonResponse());
                            mockService.get().saveBookResponse(bookId, bookNode);
                        } catch (Exception e) {
                            LoggingUtils.warn(log, e, "Error saving book to mock service");
                        }
                    }

                    future.complete(book);
                })
                .exceptionally(ex -> {
                    LoggingUtils.error(log, ex, "Error retrieving book {}", bookId);
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
    @Cacheable(
        value = "searchRequests",
        key = "T(com.williamcallahan.book_recommendation_engine.util.SearchQueryUtils).cacheKey(#query, #langCode)",
        condition = "#root.target.cacheEnabled"
    )
    public Mono<List<Book>> searchBooks(String query, String langCode) {
        String normalizedQuery = SearchQueryUtils.normalize(query);
        String cacheKey = SearchQueryUtils.cacheKey(query, langCode);

        // Use computeIfAbsent for atomic get-or-create to prevent race conditions
        CompletableFuture<List<Book>> future = searchRequestCache.computeIfAbsent(cacheKey, key -> {
            CompletableFuture<List<Book>> newFuture = new CompletableFuture<>();

            // Remove from cache when complete to prevent memory leaks
            newFuture.whenComplete((result, ex) -> searchRequestCache.remove(key));

            // Start processing pipeline
            processSearchRequest(normalizedQuery, query, langCode, newFuture);

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
    private void processSearchRequest(String normalizedQuery,
                                      String originalQuery,
                                      String langCode,
                                      CompletableFuture<List<Book>> future) {
        // First try the local file cache (in dev/test mode)
        if (localCacheEnabled) {
            List<Book> localResults = getSearchFromLocalCache(originalQuery, langCode);
            if (localResults != null && !localResults.isEmpty()) {
                log.debug("Retrieved search '{}' from local cache, {} results", normalizedQuery, localResults.size());
                future.complete(localResults);
                return;
            }
        }

        // Next, check mock data if available (in dev/test mode)
        if (mockService.isPresent() && mockService.get().hasMockDataForSearch(originalQuery)) {
            List<Book> mockResults = mockService.get().searchBooks(originalQuery);
            if (mockResults != null && !mockResults.isEmpty()) {
                log.debug("Retrieved search '{}' from mock data, {} results", normalizedQuery, mockResults.size());
                future.complete(mockResults);

                // Still persist to local cache for faster future access
                if (localCacheEnabled) {
                    saveSearchToLocalCache(originalQuery, langCode, mockResults);
                }

                return;
            }
        }
        java.util.concurrent.atomic.AtomicBoolean resolved = new java.util.concurrent.atomic.AtomicBoolean(false);

        if (bookDataOrchestrator != null) {
            bookDataOrchestrator.searchBooksTiered(normalizedQuery, langCode, SEARCH_RESULT_LIMIT, null)
                .onErrorResume(ex -> {
                    LoggingUtils.warn(log, ex, "BookApiProxy: Orchestrator search failed for '{}' (lang {})", normalizedQuery, langCode);
                    return Mono.empty();
                })
                .subscribe(results -> {
                    List<Book> sanitized = sanitizeSearchResults(results);
                    if (!sanitized.isEmpty() && resolved.compareAndSet(false, true)) {
                        if (localCacheEnabled) {
                            saveSearchToLocalCache(originalQuery, langCode, sanitized);
                        }
                        future.complete(sanitized);
                    }
                }, ex -> {
                    LoggingUtils.warn(log, ex, "BookApiProxy: Orchestrator search error for '{}' (lang {})", normalizedQuery, langCode);
                    if (resolved.compareAndSet(false, true)) {
                        continueSearchWithGoogle(normalizedQuery, originalQuery, langCode, future);
                    }
                }, () -> {
                    if (resolved.compareAndSet(false, true)) {
                        continueSearchWithGoogle(normalizedQuery, originalQuery, langCode, future);
                    }
                });
            return;
        }

        continueSearchWithGoogle(normalizedQuery, originalQuery, langCode, future);
    }

    private void continueSearchWithGoogle(String normalizedQuery,
                                          String originalQuery,
                                          String langCode,
                                          CompletableFuture<List<Book>> future) {
        if (future.isDone()) {
            return;
        }

        if (!externalFallbackEnabled) {
            log.debug("External fallback disabled for search '{}'. Returning empty result set.", normalizedQuery);
            future.complete(List.of());
            return;
        }

        if (logApiCalls) {
            log.info("Making REAL API call to Google Books for search: '{}'", normalizedQuery);
        }

        googleBooksService.searchBooksAsyncReactive(normalizedQuery, langCode, SEARCH_RESULT_LIMIT, null)
            .defaultIfEmpty(List.of())
            .map(this::sanitizeSearchResults)
            .subscribe(results -> {
                if (!results.isEmpty() && localCacheEnabled) {
                    saveSearchToLocalCache(originalQuery, langCode, results);
                }
                future.complete(results);
            }, error -> {
                LoggingUtils.error(log, error, "Error searching for '{}'", normalizedQuery);
                future.completeExceptionally(error);
            });
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
                LoggingUtils.warn(log, e, "Error reading book from local cache");
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
            log.debug("Saved book {} to local cache", bookId);
        } catch (Exception e) {
            LoggingUtils.warn(log, e, "Error saving book to local cache");
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
        
        String filename = SearchQueryUtils.cacheKey(query, langCode);

        Path searchFile = Paths.get(localCacheDirectory, "searches", filename);
        
        if (Files.exists(searchFile)) {
            try {
                return objectMapper.readValue(searchFile.toFile(), 
                        objectMapper.getTypeFactory().constructCollectionType(List.class, Book.class));
            } catch (Exception e) {
                LoggingUtils.warn(log, e, "Error reading search results from local cache");
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
        
        String filename = SearchQueryUtils.cacheKey(query, langCode);

        Path searchFile = Paths.get(localCacheDirectory, "searches", filename);
        
        try {
            // Ensure directory exists
            Files.createDirectories(searchFile.getParent());
            
            // Save to JSON
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(searchFile.toFile(), results);
            log.debug("Saved search results for '{}' ({}) to local cache", query, langCode);
        } catch (Exception e) {
            LoggingUtils.warn(log, e, "Error saving search results to local cache");
        }
    }

    private List<Book> sanitizeSearchResults(List<Book> results) {
        if (results == null || results.isEmpty()) {
            return List.of();
        }
        return results.stream()
            .filter(Objects::nonNull)
            .limit(SEARCH_RESULT_LIMIT)
            .collect(Collectors.toList());
    }
}
