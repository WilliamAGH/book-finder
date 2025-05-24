/**
 * Service for handling search requests with extreme rate limiting support
 * 
 * @author William Callahan
 *
 * Features:
 * - Returns immediate cached results while searching in background
 * - Alternates between Google Books and OpenLibrary APIs to reduce rate limiting
 * - Integrates with existing circuit breaker and deduplication logic
 * - Sends real-time updates via WebSocket as new results arrive
 * - Respects existing cache hierarchy (Caffeine -> Redis -> S3 -> APIs)
 * - Automatically merges and deduplicates results from multiple sources
 * - Updates S3 and Redis storage with merged book data
 */

package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.event.SearchProgressEvent;
import com.williamcallahan.book_recommendation_engine.service.event.SearchResultsUpdatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class RateLimitedSearchService {

    private static final Logger logger = LoggerFactory.getLogger(RateLimitedSearchService.class);

    private final BookCacheFacadeService bookCacheFacadeService;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final ApiCircuitBreakerService circuitBreakerService;
    private final DuplicateBookService duplicateBookService;
    private final ApplicationEventPublisher eventPublisher;
    private final OpenLibraryBookDataService openLibraryBookDataService;
    
    // Track which API source to use next for alternating strategy
    private final AtomicInteger apiSourceCounter = new AtomicInteger(0);
    
    // Track ongoing searches to prevent duplicates
    private final Set<String> activeSearches = ConcurrentHashMap.newKeySet();

    /**
     * Constructs RateLimitedSearchService with required dependencies
     */
    public RateLimitedSearchService(BookCacheFacadeService bookCacheFacadeService,
                                  BookDataOrchestrator bookDataOrchestrator,
                                  ApiCircuitBreakerService circuitBreakerService,
                                  DuplicateBookService duplicateBookService,
                                  ApplicationEventPublisher eventPublisher,
                                  OpenLibraryBookDataService openLibraryBookDataService) {
        this.bookCacheFacadeService = bookCacheFacadeService;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.circuitBreakerService = circuitBreakerService;
        this.duplicateBookService = duplicateBookService;
        this.eventPublisher = eventPublisher;
        this.openLibraryBookDataService = openLibraryBookDataService;
    }

    /**
     * Performs a rate-limited search with immediate cache results and background API calls
     * 
     * @param query Search query
     * @param startIndex Pagination start index
     * @param maxResults Maximum results to return
     * @param publishedYear Optional year filter
     * @param langCode Optional language filter
     * @param orderBy Optional sort order
     * @return Mono with immediate cached results and search metadata
     */
    public Mono<RateLimitedSearchResult> searchWithRateLimiting(String query, int startIndex, int maxResults,
                                                              Integer publishedYear, String langCode, String orderBy) {
        String queryHash = generateQueryHash(query, publishedYear, langCode, orderBy);
        
        logger.info("Starting rate-limited search for query: '{}', hash: {}", query, queryHash);
        
        // Send initial progress event
        eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.STARTING, 
            "Starting search...", queryHash));

        // Get immediate cached results
        return getCachedResults(query, startIndex, maxResults, publishedYear, langCode, orderBy, queryHash)
            .flatMap(cachedResults -> {
                // Start background search if not already running
                if (activeSearches.add(queryHash)) {
                    startBackgroundSearchAsync(query, maxResults, publishedYear, langCode, orderBy, queryHash, cachedResults.size())
                        .exceptionally(ex -> {
                            logger.error("Error in background search for query '{}': {}", query, ex.getMessage(), ex);
                            return null;
                        });
                } else {
                    logger.debug("Background search already active for query hash: {}", queryHash);
                }
                
                // Return immediate results
                RateLimitedSearchResult result = new RateLimitedSearchResult(
                    cachedResults, 
                    queryHash, 
                    true, // hasMore - background search may find additional results
                    "cached"
                );
                
                return Mono.just(result);
            });
    }

    /**
     * Gets immediate results from cache layers
     */
    private Mono<List<Book>> getCachedResults(String query, int startIndex, int maxResults, 
                                            Integer publishedYear, String langCode, String orderBy, String queryHash) {
        eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.SEARCHING_CACHE, 
            "Checking cache...", queryHash));
            
        return bookCacheFacadeService.searchBooksReactive(query, startIndex, maxResults, publishedYear, langCode, orderBy)
            .map(books -> books != null ? books : Collections.<Book>emptyList())
            .doOnSuccess(books -> {
                if (!books.isEmpty()) {
                    logger.info("Found {} cached results for query: '{}'", books.size(), query);
                } else {
                    logger.info("No cached results found for query: '{}'", query);
                }
            });
    }

    /**
     * Starts background search using alternating API strategy. Returns a CompletableFuture that completes when the background search is done.
     */
    // @Async removed, returns CompletableFuture now
    public CompletableFuture<Void> startBackgroundSearch(String query, int maxResults, Integer publishedYear,
                                    String langCode, String orderBy, String queryHash, int initialResultCount) {
        
        final List<Book> apiResultsThisRun = Collections.synchronizedList(new ArrayList<>());
        final Set<String> seenBookIdsThisRun = ConcurrentHashMap.newKeySet();

        logger.info("Starting background search for query: '{}', hash: {}", query, queryHash);
        
        boolean useGoogleFirst = (apiSourceCounter.getAndIncrement() % 2) == 0;
        String primarySource = useGoogleFirst ? "GOOGLE_BOOKS" : "OPEN_LIBRARY";
        String secondarySource = useGoogleFirst ? "OPEN_LIBRARY" : "GOOGLE_BOOKS";
        
        CompletableFuture<List<Book>> primaryProcessedFuture = 
            searchFromSourceAsync(query, maxResults, primarySource, queryHash)
            .thenComposeAsync(primaryRawResults -> {
                if (primaryRawResults != null && !primaryRawResults.isEmpty()) {
                    return deduplicateAndMergeAsync(primaryRawResults, seenBookIdsThisRun);
                }
                return CompletableFuture.completedFuture(Collections.emptyList());
            });

        return primaryProcessedFuture.thenComposeAsync(deduplicatedPrimary -> {
            if (deduplicatedPrimary != null && !deduplicatedPrimary.isEmpty()) {
                apiResultsThisRun.addAll(deduplicatedPrimary);
                eventPublisher.publishEvent(new SearchResultsUpdatedEvent(
                    query, deduplicatedPrimary, primarySource, 
                    initialResultCount + apiResultsThisRun.size(), queryHash, false));
            }

            if (apiResultsThisRun.size() < maxResults) {
                return circuitBreakerService.isApiCallAllowed().thenComposeAsync(allowed -> {
                    if (allowed) {
                        return searchFromSourceAsync(query, maxResults - apiResultsThisRun.size(), secondarySource, queryHash)
                            .thenComposeAsync(secondaryRawResults -> {
                                if (secondaryRawResults != null && !secondaryRawResults.isEmpty()) {
                                    return deduplicateAndMergeAsync(secondaryRawResults, seenBookIdsThisRun);
                                }
                                return CompletableFuture.completedFuture(Collections.emptyList());
                            })
                            .thenAcceptAsync(deduplicatedSecondary -> {
                                if (deduplicatedSecondary != null && !deduplicatedSecondary.isEmpty()) {
                                    apiResultsThisRun.addAll(deduplicatedSecondary);
                                    eventPublisher.publishEvent(new SearchResultsUpdatedEvent(
                                        query, deduplicatedSecondary, secondarySource, 
                                        initialResultCount + apiResultsThisRun.size(), queryHash, false));
                                }
                            });
                    } else {
                        logger.info("Circuit breaker is open, skipping secondary source {} for query: '{}'", secondarySource, query);
                        eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.RATE_LIMITED, 
                           secondarySource + " rate limited (skipped)", queryHash, secondarySource));
                        return CompletableFuture.completedFuture(null); 
                    }
                });
            }
            return CompletableFuture.completedFuture(null); 
        }).thenRunAsync(() -> {
            eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.COMPLETE, 
                "Search completed", queryHash));
            logger.info("Background search completed for query: '{}'. Found {} additional results from APIs.", query, apiResultsThisRun.size());
        }).exceptionally(e -> {
            logger.error("Error during background search execution for query: '{}': {}", query, e.getMessage(), e);
            eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.ERROR, 
                "Search error: " + e.getMessage(), queryHash));
            return null; 
        }).whenComplete((unused, throwable) -> {
            activeSearches.remove(queryHash);
            logger.debug("Background search for query hash {} removed from active searches.", queryHash);
        });
    }
    
    /**
     * Asynchronously starts the background search process.
     * This method now calls the refactored startBackgroundSearch which returns a CompletableFuture.
     */
    public CompletableFuture<Void> startBackgroundSearchAsync(String query, int maxResults, Integer publishedYear,
            String langCode, String orderBy, String queryHash, int initialResultCount) {
        return CompletableFuture.supplyAsync(() -> 
            startBackgroundSearch(query, maxResults, publishedYear, langCode, orderBy, queryHash, initialResultCount)
        ).thenCompose(Function.identity()); // Flatten CompletableFuture<CompletableFuture<Void>>
    }

    /**
     * Searches from a specific API source asynchronously.
     */
    private CompletableFuture<List<Book>> searchFromSourceAsync(String query, int maxResults, String source, String queryHash) {
        return circuitBreakerService.isApiCallAllowed().thenComposeAsync(allowed -> {
            if (!allowed) {
                logger.info("Circuit breaker is open, skipping {} search for query: '{}'", source, query);
                eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.RATE_LIMITED, 
                    source + " rate limited", queryHash, source));
                return CompletableFuture.completedFuture(Collections.emptyList());
            }

            Mono<List<Book>> resultsMono;
            if ("GOOGLE_BOOKS".equals(source)) {
                eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.SEARCHING_GOOGLE, 
                    "Searching Google Books...", queryHash, source));
                resultsMono = bookDataOrchestrator.searchBooksTiered(query, null, maxResults, null);
            } else if ("OPEN_LIBRARY".equals(source)) {
                eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.SEARCHING_OPENLIBRARY, 
                    "Searching OpenLibrary...", queryHash, source));
                resultsMono = openLibraryBookDataService.searchBooksByTitle(query).collectList();
            } else {
                logger.warn("Unknown search source: {}", source);
                resultsMono = Mono.just(Collections.emptyList());
            }
            
            return resultsMono
                .map(list -> (List<Book>) list) 
                .toFuture() 
                .exceptionally(e -> { 
                    logger.error("Error searching from source {} during reactive execution: {}", source, e.getMessage(), e);
                    eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.ERROR, 
                        "Error searching " + source, queryHash, source));
                    return Collections.emptyList();
                });
        }).exceptionally(e -> { 
            logger.error("Error checking circuit breaker or chaining for source {}: {}", source, e.getMessage(), e);
            eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.ERROR, 
                "Error with circuit breaker for " + source, queryHash, source));
            return Collections.emptyList();
        });
    }

    /**
     * Deduplicates and merges books asynchronously using existing DuplicateBookService logic.
     */
    private CompletableFuture<List<Book>> deduplicateAndMergeAsync(List<Book> books, Set<String> seenBookIds) {
        if (books == null || books.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        
        eventPublisher.publishEvent(new SearchProgressEvent("", SearchProgressEvent.SearchStatus.DEDUPLICATING, 
            "Processing results...", "", ""));
        
        List<Book> potentialBooks = new ArrayList<>();
        for (Book book : books) {
            if (book != null && book.getId() != null && !seenBookIds.contains(book.getId())) {
                potentialBooks.add(book);
            }
        }

        if (potentialBooks.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        List<CompletableFuture<Book>> bookProcessingFutures = new ArrayList<>();
        for (Book book : potentialBooks) {
            bookProcessingFutures.add(
                duplicateBookService.populateDuplicateEditionsAsync(book) // Assuming this returns CompletableFuture<Void>
                    .thenApplyAsync(v -> {
                        // book object is modified by populateDuplicateEditionsAsync
                        seenBookIds.add(book.getId()); // Add after successful processing
                        try {
                            bookCacheFacadeService.cacheBook(book);
                        } catch (Exception e) {
                            logger.warn("Failed to cache book {}: {}", book.getId(), e.getMessage());
                        }
                        return book; // Return the processed book
                    })
                    .exceptionally(ex -> {
                        logger.warn("Error during populateDuplicateEditionsAsync for book {}: {}", book.getId(), ex.getMessage(), ex);
                        return null; // Will be filtered out
                    })
            );
        }

        return CompletableFuture.allOf(bookProcessingFutures.toArray(new CompletableFuture[0]))
            .thenApplyAsync(v -> bookProcessingFutures.stream()
                                    .map(CompletableFuture::join) // Safe after allOf
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList()));
    }
    
    /**
     * Generates a consistent hash for query parameters
     */
    private String generateQueryHash(String query, Integer publishedYear, String langCode, String orderBy) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            String combined = query + "|" + publishedYear + "|" + langCode + "|" + orderBy;
            byte[] hash = md.digest(combined.getBytes());
            
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            logger.error("MD5 algorithm not available", e);
            return String.valueOf(Objects.hash(query, publishedYear, langCode, orderBy));
        }
    }

    /**
     * Result container for rate-limited search responses
     */
    public static class RateLimitedSearchResult {
        private final List<Book> results;
        private final String queryHash;
        private final boolean hasMore;
        private final String source;

        public RateLimitedSearchResult(List<Book> results, String queryHash, boolean hasMore, String source) {
            this.results = results;
            this.queryHash = queryHash;
            this.hasMore = hasMore;
            this.source = source;
        }

        public List<Book> getResults() { return results; }
        public String getQueryHash() { return queryHash; }
        public boolean isHasMore() { return hasMore; }
        public String getSource() { return source; }
    }
}
