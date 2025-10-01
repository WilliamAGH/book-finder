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
import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.dto.BookListItem;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.service.BookIdentifierResolver;
import com.williamcallahan.book_recommendation_engine.service.BookSearchService;
import com.williamcallahan.book_recommendation_engine.util.BookJsonWriter;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.ReactiveErrorUtils;
import com.williamcallahan.book_recommendation_engine.util.SearchQueryUtils;
import com.williamcallahan.book_recommendation_engine.util.BookDomainMapper;
import com.williamcallahan.book_recommendation_engine.util.UuidUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import org.springframework.lang.Nullable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @deprecated Use {@link BookDataOrchestrator},
 * {@link com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository},
 * and {@link GoogleApiFetcher} directly for Postgres-first orchestration.
 */
@Deprecated(since = "2025-10-01", forRemoval = true)
@Service
@Slf4j
public class BookApiProxy {
        
    private final GoogleBooksService googleBooksService;
    private final BookSearchService bookSearchService;
    private final BookQueryRepository bookQueryRepository;
    private final BookIdentifierResolver bookIdentifierResolver;
    private final @Nullable BookDataOrchestrator bookDataOrchestrator;
    private final ObjectMapper objectMapper;
    private static final int SEARCH_RESULT_LIMIT = 40;

    private final Optional<GoogleBooksMockService> mockService;
    
    // In-memory cache as first-level cache
    private final Map<String, CompletableFuture<Book>> bookRequestCache = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<List<Book>>> searchRequestCache = new ConcurrentHashMap<>();
    
    private final boolean localCacheEnabled;
    private final String localCacheDirectory;
    private final boolean logApiCalls;
    private final boolean externalFallbackEnabled;

    /**
     * Constructs the BookApiProxy with necessary dependencies
     *
     * @param googleBooksService Service for interacting with Google Books API
     * @param objectMapper ObjectMapper for JSON processing
     * @param mockService Optional mock service for testing
     */
    public BookApiProxy(GoogleBooksService googleBooksService,
                       ObjectMapper objectMapper,
                       Optional<GoogleBooksMockService> mockService,
                       @Value("${app.local-cache.enabled:false}") boolean localCacheEnabled,
                       @Value("${app.local-cache.directory:.dev-cache}") String localCacheDirectory,
                       @Value("${app.api-client.log-calls:true}") boolean logApiCalls,
                       @Value("${app.features.external-fallback.enabled:${app.features.google-fallback.enabled:true}}") boolean externalFallbackEnabled,
                       BookSearchService bookSearchService,
                       BookQueryRepository bookQueryRepository,
                       BookIdentifierResolver bookIdentifierResolver,
                       @Nullable BookDataOrchestrator bookDataOrchestrator) {
        this.googleBooksService = googleBooksService;
        this.objectMapper = objectMapper;
        this.mockService = mockService;
        this.localCacheEnabled = localCacheEnabled;
        this.localCacheDirectory = localCacheDirectory;
        this.logApiCalls = logApiCalls;
        this.externalFallbackEnabled = externalFallbackEnabled;
        this.bookSearchService = bookSearchService;
        this.bookQueryRepository = bookQueryRepository;
        this.bookIdentifierResolver = bookIdentifierResolver;
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

        if (bookQueryRepository != null) {
            CompletableFuture
                .supplyAsync(() -> fetchBookFromProjections(bookId))
                .whenComplete((book, ex) -> {
                    if (ex != null) {
                        LoggingUtils.warn(log, ex, "BookApiProxy: Projection lookup error for {}", bookId);
                        if (resolved.compareAndSet(false, true)) {
                            resolveViaExternalFallback(bookId, future);
                        }
                        return;
                    }

                    if (book != null && resolved.compareAndSet(false, true)) {
                        log.debug("BookApiProxy: Retrieved {} via Postgres projections.", bookId);
                        if (localCacheEnabled) {
                            saveBookToLocalCache(bookId, book);
                        }
                        future.complete(book);
                        return;
                    }

                    if (resolved.compareAndSet(false, true)) {
                        resolveViaExternalFallback(bookId, future);
                    }
                });
            return;
        }

        resolveViaExternalFallback(bookId, future);
    }

    private void resolveViaExternalFallback(String bookId, CompletableFuture<Book> future) {
        if (future.isDone()) {
            return;
        }

        if (!externalFallbackEnabled) {
            log.debug("External fallback disabled for book ID '{}'. Returning empty result.", bookId);
            future.complete(null);
            return;
        }

        final BookDataOrchestrator orchestrator = bookDataOrchestrator;

        if (orchestrator == null) {
            log.debug("BookDataOrchestrator unavailable; cannot resolve fallback for '{}'", bookId);
            future.complete(null);
            return;
        }

        orchestrator.fetchCanonicalBookReactive(bookId)
            .subscribe(book -> {
                if (future.isDone()) {
                    return;
                }
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
            }, ex -> {
                LoggingUtils.warn(log, ex, "BookDataOrchestrator fallback failed for {}", bookId);
                future.complete(null);
            }, () -> {
                if (!future.isDone()) {
                    future.complete(null);
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

        if (bookSearchService != null && bookQueryRepository != null) {
            CompletableFuture
                .supplyAsync(() -> sanitizeSearchResults(searchBooksViaProjections(normalizedQuery)))
                .whenComplete((results, ex) -> {
                    if (ex != null) {
                        LoggingUtils.warn(log, ex, "BookApiProxy: Projection search error for '{}' (lang {})", normalizedQuery, langCode);
                        if (resolved.compareAndSet(false, true)) {
                            continueSearchWithGoogle(normalizedQuery, originalQuery, langCode, future);
                        }
                        return;
                    }

                    if (results != null && !results.isEmpty() && resolved.compareAndSet(false, true)) {
                        if (localCacheEnabled) {
                            saveSearchToLocalCache(originalQuery, langCode, results);
                        }
                        future.complete(results);
                        return;
                    }

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

        Mono<List<Book>> fallbackMono;
        final BookDataOrchestrator orchestrator = this.bookDataOrchestrator;

        if (orchestrator != null) {
            if (logApiCalls) {
                log.info("Invoking tiered search fallback for query '{}' (lang={})", normalizedQuery, langCode);
            }
            fallbackMono = orchestrator
                .searchBooksTiered(normalizedQuery, langCode, SEARCH_RESULT_LIMIT, null)
                .defaultIfEmpty(List.of());
        } else {
            if (logApiCalls) {
                log.info("Tiered orchestrator unavailable; falling back to legacy Google search for '{}'", normalizedQuery);
            }
            fallbackMono = googleBooksService
                .searchBooksAsyncReactive(normalizedQuery, langCode, SEARCH_RESULT_LIMIT, null)
                .defaultIfEmpty(List.of());
        }

        fallbackMono
            .map(this::sanitizeSearchResults)
            .onErrorResume(error -> {
                LoggingUtils.error(log, error, "Error searching externally for '{}'", normalizedQuery);
                return Mono.just(List.of());
            })
            .subscribe(results -> {
                if (!results.isEmpty() && localCacheEnabled) {
                    saveSearchToLocalCache(originalQuery, langCode, results);
                }
                future.complete(results);
            }, future::completeExceptionally);
    }

    private Book fetchBookFromProjections(String identifier) {
        if (!ValidationUtils.hasText(identifier) || bookQueryRepository == null) {
            return null;
        }

        String trimmed = identifier.trim();

        Optional<BookDetail> bySlug = bookQueryRepository.fetchBookDetailBySlug(trimmed);
        if (bySlug.isPresent()) {
            return BookDomainMapper.fromDetail(bySlug.get());
        }

        Optional<UUID> maybeUuid = bookIdentifierResolver != null
            ? bookIdentifierResolver.resolveToUuid(trimmed)
            : Optional.ofNullable(UuidUtils.parseUuidOrNull(trimmed));

        if (maybeUuid.isEmpty()) {
            return null;
        }

        UUID uuid = maybeUuid.get();

        return bookQueryRepository.fetchBookDetail(uuid)
            .map(BookDomainMapper::fromDetail)
            .or(() -> bookQueryRepository.fetchBookCard(uuid).map(BookDomainMapper::fromCard))
            .orElse(null);
    }

    private List<Book> searchBooksViaProjections(String query) {
        if (!ValidationUtils.hasText(query) || bookSearchService == null || bookQueryRepository == null) {
            return List.of();
        }

        List<BookSearchService.SearchResult> results = bookSearchService.searchBooks(query, SEARCH_RESULT_LIMIT);
        if (results == null || results.isEmpty()) {
            return List.of();
        }

        List<UUID> orderedIds = results.stream()
            .map(BookSearchService.SearchResult::bookId)
            .filter(Objects::nonNull)
            .distinct()
            .limit(SEARCH_RESULT_LIMIT)
            .toList();

        if (orderedIds.isEmpty()) {
            return List.of();
        }

        List<BookListItem> items = bookQueryRepository.fetchBookListItems(orderedIds);
        if (items == null || items.isEmpty()) {
            return List.of();
        }

        return orderBooksBySearchResults(results, items, SEARCH_RESULT_LIMIT);
    }

    private List<Book> orderBooksBySearchResults(List<BookSearchService.SearchResult> results,
                                                 List<BookListItem> items,
                                                 int limit) {
        Map<String, Book> booksById = items.stream()
            .filter(Objects::nonNull)
            .map(BookDomainMapper::fromListItem)
            .filter(Objects::nonNull)
            .peek(book -> {
                book.setRetrievedFrom("POSTGRES");
                book.setInPostgres(true);
            })
            .collect(Collectors.toMap(
                Book::getId,
                Function.identity(),
                (first, second) -> first,
                LinkedHashMap::new
            ));

        if (booksById.isEmpty()) {
            return List.of();
        }

        List<Book> ordered = new ArrayList<>(Math.min(limit, booksById.size()));

        for (BookSearchService.SearchResult result : results) {
            UUID bookId = result.bookId();
            if (bookId == null) {
                continue;
            }
            Book book = booksById.remove(bookId.toString());
            if (book != null) {
                ordered.add(book);
                if (ordered.size() == limit) {
                    return ordered;
                }
            }
        }

        if (ordered.size() < limit && !booksById.isEmpty()) {
            for (Book remaining : booksById.values()) {
                ordered.add(remaining);
                if (ordered.size() == limit) {
                    break;
                }
            }
        }

        return ordered;
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
            BookJsonWriter.writeToFile(book, bookFile);
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
