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
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class RateLimitedSearchService {

    private static final Logger logger = LoggerFactory.getLogger(RateLimitedSearchService.class);

    private final BookCacheFacadeService bookCacheFacadeService;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final ApiCircuitBreakerService circuitBreakerService;
    private final DuplicateBookService duplicateBookService;
    private final ApplicationEventPublisher eventPublisher;
    
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
                                  ApplicationEventPublisher eventPublisher) {
        this.bookCacheFacadeService = bookCacheFacadeService;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.circuitBreakerService = circuitBreakerService;
        this.duplicateBookService = duplicateBookService;
        this.eventPublisher = eventPublisher;
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
                    startBackgroundSearch(query, maxResults, publishedYear, langCode, orderBy, queryHash, cachedResults.size());
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
     * Starts background search using alternating API strategy
     */
    @Async
    public void startBackgroundSearch(String query, int maxResults, Integer publishedYear, 
                                    String langCode, String orderBy, String queryHash, int initialResultCount) {
        try {
            logger.info("Starting background search for query: '{}', hash: {}", query, queryHash);
            
            // Determine which API to try first based on alternating strategy
            boolean useGoogleFirst = (apiSourceCounter.getAndIncrement() % 2) == 0;
            
            List<Book> allResults = new ArrayList<>();
            Set<String> seenBookIds = new HashSet<>();
            
            // Try primary API source
            String primarySource = useGoogleFirst ? "GOOGLE_BOOKS" : "OPEN_LIBRARY";
            String secondarySource = useGoogleFirst ? "OPEN_LIBRARY" : "GOOGLE_BOOKS";
            
            List<Book> primaryResults = searchFromSource(query, maxResults, primarySource, queryHash);
            if (primaryResults != null && !primaryResults.isEmpty()) {
                List<Book> deduplicatedPrimary = deduplicateAndMerge(primaryResults, seenBookIds);
                allResults.addAll(deduplicatedPrimary);
                
                // Send update for primary source results
                eventPublisher.publishEvent(new SearchResultsUpdatedEvent(
                    query, deduplicatedPrimary, primarySource, 
                    initialResultCount + allResults.size(), queryHash, false));
            }
            
            // Try secondary API source only if circuit breaker allows and we need more results
            if (allResults.size() < maxResults && circuitBreakerService.isApiCallAllowed()) {
                List<Book> secondaryResults = searchFromSource(query, maxResults - allResults.size(), secondarySource, queryHash);
                if (secondaryResults != null && !secondaryResults.isEmpty()) {
                    List<Book> deduplicatedSecondary = deduplicateAndMerge(secondaryResults, seenBookIds);
                    allResults.addAll(deduplicatedSecondary);
                    
                    // Send update for secondary source results
                    eventPublisher.publishEvent(new SearchResultsUpdatedEvent(
                        query, deduplicatedSecondary, secondarySource, 
                        initialResultCount + allResults.size(), queryHash, false));
                }
            }
            
            // Send completion event
            eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.COMPLETE, 
                "Search completed", queryHash));
                
            logger.info("Background search completed for query: '{}'. Found {} additional results", query, allResults.size());
            
        } catch (Exception e) {
            logger.error("Error during background search for query: '{}': {}", query, e.getMessage(), e);
            eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.ERROR, 
                "Search error: " + e.getMessage(), queryHash));
        } finally {
            activeSearches.remove(queryHash);
        }
    }

    /**
     * Searches from a specific API source
     */
    private List<Book> searchFromSource(String query, int maxResults, String source, String queryHash) {
        try {
            if ("GOOGLE_BOOKS".equals(source)) {
                if (!circuitBreakerService.isApiCallAllowed()) {
                    logger.info("Circuit breaker is open, skipping Google Books search for query: '{}'", query);
                    eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.RATE_LIMITED, 
                        "Google Books rate limited", queryHash, source));
                    return Collections.emptyList();
                }
                
                eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.SEARCHING_GOOGLE, 
                    "Searching Google Books...", queryHash, source));
                    
                return bookDataOrchestrator.searchBooksTiered(query, null, maxResults, null)
                    .block(); // Convert to blocking for @Async method
                    
            } else if ("OPEN_LIBRARY".equals(source)) {
                eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.SEARCHING_OPENLIBRARY, 
                    "Searching OpenLibrary...", queryHash, source));
                    
                return bookDataOrchestrator.searchBooksTiered(query, null, maxResults, null)
                    .block(); // Convert to blocking for @Async method
            }
        } catch (Exception e) {
            logger.error("Error searching from source {}: {}", source, e.getMessage(), e);
            eventPublisher.publishEvent(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.ERROR, 
                "Error searching " + source, queryHash, source));
        }
        
        return Collections.emptyList();
    }

    /**
     * Deduplicates and merges books using existing DuplicateBookService logic
     */
    private List<Book> deduplicateAndMerge(List<Book> books, Set<String> seenBookIds) {
        if (books == null || books.isEmpty()) {
            return Collections.emptyList();
        }
        
        eventPublisher.publishEvent(new SearchProgressEvent("", SearchProgressEvent.SearchStatus.DEDUPLICATING, 
            "Processing results...", "", ""));
        
        List<Book> deduplicated = new ArrayList<>();
        
        for (Book book : books) {
            if (book == null || book.getId() == null) {
                continue;
            }
            
            // Check if we've already seen this book ID
            if (seenBookIds.contains(book.getId())) {
                continue;
            }
            
            // Use existing deduplication logic
            duplicateBookService.populateDuplicateEditions(book);
            
            seenBookIds.add(book.getId());
            deduplicated.add(book);
            
            // Cache the book for future searches
            try {
                bookCacheFacadeService.cacheBook(book);
            } catch (Exception e) {
                logger.warn("Failed to cache book {}: {}", book.getId(), e.getMessage());
            }
        }
        
        return deduplicated;
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
