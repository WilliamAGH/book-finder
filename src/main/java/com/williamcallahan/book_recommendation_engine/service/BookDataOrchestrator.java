/**
 * Orchestrates book data retrieval through a tiered fetch strategy
 *
 * @author William Callahan
 * 
 * Features:
 * - Implements multi-tiered data retrieval from cache and APIs
 * - Manages fetching of individual books by ID or search results  
 * - Coordinates between S3 storage and Google Books API
 * - Handles caching of API responses for performance
 * - Supports both authenticated and unauthenticated API usage
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Service that orchestrates data retrieval through a tiered approach
 * 
 * @author William Callahan
 */
@Service
public class BookDataOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(BookDataOrchestrator.class);

    private final S3RetryService s3RetryService;
    private final GoogleApiFetcher googleApiFetcher;
    private final ObjectMapper objectMapper;

    /**
     * Creates a new BookDataOrchestrator with required dependencies
     * 
     * @param s3RetryService Service for S3 storage operations with retry capability
     * @param googleApiFetcher Service for fetching data from Google Books API
     * @param objectMapper JSON object mapper for parsing responses
     */
    @Autowired
    public BookDataOrchestrator(S3RetryService s3RetryService,
                                GoogleApiFetcher googleApiFetcher,
                                ObjectMapper objectMapper) {
        this.s3RetryService = s3RetryService;
        this.googleApiFetcher = googleApiFetcher;
        this.objectMapper = objectMapper;
    }

    /**
     * Implements the 4-tier fetching logic for a single book ID
     * Tier 1 (Memory/DB) is assumed to be checked by the caller (e.g., GoogleBooksCachingStrategy or BookCacheService)
     * This orchestrator handles:
     * Tier 2: S3 JSON Cache
     * Tier 3: Unauthenticated Google Books API
     * Tier 4: Authenticated Google Books API
     *
     * @param bookId The Google Books ID
     * @return A Mono emitting the Book if found, or Mono.empty() if not found in any tier
     */
    public Mono<Book> getBookByIdTiered(String bookId) {
        logger.debug("BookDataOrchestrator: Starting tiered fetch for book ID: {}", bookId);

        // Tier 2: Try S3 Cache
        Mono<Book> s3FetchMono = Mono.fromCompletionStage(s3RetryService.fetchJsonWithRetry(bookId))
            .flatMap(s3Result -> {
                if (s3Result.isSuccess() && s3Result.getData().isPresent()) {
                    try {
                        JsonNode s3JsonNode = objectMapper.readTree(s3Result.getData().get());
                        Book bookFromS3 = BookJsonParser.convertJsonToBook(s3JsonNode, objectMapper);
                        if (bookFromS3 != null && bookFromS3.getId() != null) {
                            logger.info("BookDataOrchestrator: Tier 2 S3 HIT for book ID: {}. Title: {}", bookId, bookFromS3.getTitle());
                            return Mono.just(bookFromS3);
                        }
                        logger.warn("BookDataOrchestrator: S3 data for {} parsed to null/invalid book.", bookId);
                    } catch (Exception e) {
                        logger.warn("BookDataOrchestrator: Failed to parse S3 JSON for bookId {}: {}. Proceeding to API.", bookId, e.getMessage());
                    }
                } else if (s3Result.isNotFound()) {
                    logger.info("BookDataOrchestrator: Tier 2 S3 MISS for book ID: {}.", bookId);
                } else if (s3Result.isServiceError()){
                     logger.warn("BookDataOrchestrator: Tier 2 S3 Service ERROR for book ID: {}. Error: {}", bookId, s3Result.getErrorMessage().orElse("Unknown S3 Error"));
                } else if (s3Result.isDisabled()){
                    logger.info("BookDataOrchestrator: Tier 2 S3 is disabled. Proceeding to API for book ID: {}", bookId);
                }
                return Mono.<Book>empty(); // S3 miss or error, proceed to next tier
            });

        // Tier 3: Unauthenticated Google API
        Mono<Book> tier3ApiMono = Mono.defer(() -> {
            logger.info("BookDataOrchestrator: Tier 3 Unauthenticated API call for book ID: {}", bookId);
            return googleApiFetcher.fetchVolumeByIdUnauthenticated(bookId)
                .flatMap(jsonNode -> processApiJsonResponse(jsonNode, bookId, "Unauthenticated", false));
        });

        // Tier 4: Authenticated Google API
        Mono<Book> tier4ApiMono = Mono.defer(() -> {
            logger.info("BookDataOrchestrator: Tier 4 Authenticated API call for book ID: {}", bookId);
            return googleApiFetcher.fetchVolumeByIdAuthenticated(bookId)
                .flatMap(jsonNode -> processApiJsonResponse(jsonNode, bookId, "Authenticated", true));
        });

        return s3FetchMono
            .switchIfEmpty(tier3ApiMono)
            .switchIfEmpty(tier4ApiMono)
            .doOnSuccess(book -> {
                if (book != null) {
                    logger.info("BookDataOrchestrator: Successfully fetched book ID: {} Title: {}", book.getId(), book.getTitle());
                } else {
                    logger.info("BookDataOrchestrator: Failed to fetch book ID: {} from any tier.", bookId);
                }
            })
            .onErrorResume(e -> {
                logger.error("BookDataOrchestrator: Error during tiered fetch for book ID {}: {}", bookId, e.getMessage(), e);
                return Mono.<Book>empty();
            });
    }

    /**
     * Processes API JSON response and saves to S3 cache
     * 
     * @param jsonNode The JSON node from API response
     * @param bookId Book ID being requested
     * @param apiType Type of API call (Authenticated/Unauthenticated)
     * @param isAuthenticated Whether this was an authenticated call
     * @return Mono emitting the Book if valid, or empty if invalid
     */
    private Mono<Book> processApiJsonResponse(JsonNode jsonNode, String bookId, String apiType, boolean isAuthenticated) {
        if (jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()) {
            logger.warn("BookDataOrchestrator: {} API call for book ID {} returned empty or invalid JSON.", apiType, bookId);
            return Mono.<Book>empty();
        }
        Book book = BookJsonParser.convertJsonToBook(jsonNode, objectMapper);
        if (book != null && book.getId() != null) {
            logger.info("BookDataOrchestrator: {} API HIT for book ID: {}. Title: {}", apiType, bookId, book.getTitle());
            // Save raw JSON to S3
            return Mono.fromCompletionStage(s3RetryService.uploadJsonWithRetry(book.getId(), jsonNode.toString()))
                .doOnSuccess(v -> logger.info("BookDataOrchestrator: Successfully saved raw JSON from {} API to S3 for book ID: {}", apiType, book.getId()))
                .doOnError(e -> logger.error("BookDataOrchestrator: Failed to save raw JSON from {} API to S3 for book ID: {}. Error: {}", apiType, book.getId(), e.getMessage()))
                .thenReturn(book) // Return the book regardless of S3 save success/failure for this operation
                .onErrorReturn(book); // If S3 save fails, still return the book
        } else {
            logger.warn("BookDataOrchestrator: {} API data for {} parsed to null/invalid book.", apiType, bookId);
            return Mono.<Book>empty();
        }
    }

    /**
     * Implements the tiered fetching logic for book searches
     * Tier 1 (Spring "bookSearchResults" cache) is assumed to be checked by BookCacheService
     * This orchestrator handles:
     * Tier 3: Unauthenticated Google Books API search (paged)
     * Tier 4: Authenticated Google Books API search (paged)
     *
     * @param query The search query
     * @param langCode Optional language code
     * @param desiredTotalResults The desired number of results
     * @param orderBy Sort order
     * @return A Mono emitting a List of Books, or an empty list if no results or error
     */
    public Mono<List<Book>> searchBooksTiered(String query, String langCode, int desiredTotalResults, String orderBy) {
        logger.debug("BookDataOrchestrator: Starting tiered search for query: '{}', lang: {}, total: {}, order: {}", query, langCode, desiredTotalResults, orderBy);
        
        final Map<String, Object> queryQualifiers = BookJsonParser.extractQualifiersFromSearchQuery(query);

        // Tier 3: Unauthenticated Search
        return executePagedSearch(query, langCode, desiredTotalResults, orderBy, false, queryQualifiers)
            .flatMap(unauthResults -> {
                if (!unauthResults.isEmpty()) {
                    logger.info("BookDataOrchestrator: Tier 3 Unauthenticated search successful for query '{}', found {} books.", query, unauthResults.size());
                    return Mono.just(unauthResults);
                }
                // Tier 4: Authenticated Search (if Tier 3 was empty or failed implicitly)
                logger.info("BookDataOrchestrator: Tier 3 Unauthenticated search for query '{}' yielded no results or failed. Proceeding to Tier 4 Authenticated search.", query);
                return executePagedSearch(query, langCode, desiredTotalResults, orderBy, true, queryQualifiers);
            })
            .doOnSuccess(books -> {
                if (!books.isEmpty()) {
                    logger.info("BookDataOrchestrator: Successfully searched books for query '{}'. Found {} books.", query, books.size());
                    // BookCacheService will handle populating its "bookSearchResults" cache and individual book caches.
                } else {
                    logger.info("BookDataOrchestrator: Search for query '{}' yielded no results from any tier.", query);
                }
            })
            .onErrorResume(e -> {
                logger.error("BookDataOrchestrator: Error during tiered search for query '{}': {}", query, e.getMessage(), e);
                return Mono.just(Collections.emptyList());
            });
    }

    /**
     * Executes a paged search request against Google Books API
     * 
     * @param query Search query string
     * @param langCode Optional language code filter
     * @param desiredTotalResults Desired number of results
     * @param orderBy Result ordering parameter
     * @param authenticated Whether to use authenticated API
     * @param queryQualifiers Qualifiers extracted from search query
     * @return Mono emitting list of Book objects from search results
     */
    private Mono<List<Book>> executePagedSearch(String query, String langCode, int desiredTotalResults, String orderBy, boolean authenticated, Map<String, Object> queryQualifiers) {
        final int maxResultsPerPage = 40;
        // Ensure desiredTotalResults is positive, default to a reasonable number if not.
        final int maxTotalResultsToFetch = (desiredTotalResults > 0 && desiredTotalResults <= 200) ? desiredTotalResults : (desiredTotalResults <=0 ? 40 : 200) ; // Cap at 200 for sanity
        final String effectiveOrderBy = (orderBy != null && !orderBy.trim().isEmpty()) ? orderBy : "relevance";
        String authType = authenticated ? "Authenticated" : "Unauthenticated";

        logger.debug("BookDataOrchestrator: Executing {} paged search for query: '{}', lang: {}, total_requested: {}, order: {}", authType, query, langCode, maxTotalResultsToFetch, effectiveOrderBy);

        return Flux.range(0, (maxTotalResultsToFetch + maxResultsPerPage - 1) / maxResultsPerPage)
            .map(page -> page * maxResultsPerPage)
            .concatMap(startIndex -> {
                Mono<JsonNode> apiCall = authenticated ?
                    googleApiFetcher.searchVolumesAuthenticated(query, startIndex, effectiveOrderBy, langCode) :
                    googleApiFetcher.searchVolumesUnauthenticated(query, startIndex, effectiveOrderBy, langCode);

                return apiCall.flatMapMany(responseNode -> {
                    if (responseNode != null && responseNode.has("items") && responseNode.get("items").isArray()) {
                        List<JsonNode> items = new ArrayList<>();
                        responseNode.get("items").forEach(items::add);
                        logger.debug("BookDataOrchestrator: {} search, query '{}', startIndex {}: Retrieved {} items from API.", authType, query, startIndex, items.size());
                        return Flux.fromIterable(items);
                    }
                    logger.debug("BookDataOrchestrator: {} search, query '{}', startIndex {}: No items found in API response.", authType, query, startIndex);
                    return Flux.empty();
                });
            })
            .map(jsonItem -> {
                Book book = BookJsonParser.convertJsonToBook(jsonItem, objectMapper);
                if (book != null && book.getId() != null) {
                    // Add search query qualifiers
                    if (!queryQualifiers.isEmpty()) {
                        queryQualifiers.forEach(book::addQualifier);
                    }
                    // Save raw JSON to S3 for this individual book
                    Mono.fromCompletionStage(s3RetryService.uploadJsonWithRetry(book.getId(), jsonItem.toString()))
                        .doOnSuccess(v -> logger.debug("BookDataOrchestrator: {} search - Successfully saved raw JSON to S3 for book ID: {}", authType, book.getId()))
                        .doOnError(e -> logger.error("BookDataOrchestrator: {} search - Failed to save raw JSON to S3 for book ID: {}. Error: {}", authType, book.getId(), e.getMessage()))
                        .subscribe(); // Fire-and-forget S3 save for each item
                    return book;
                }
                return null;
            })
            .filter(Objects::nonNull)
            .collectList()
            .map(books -> {
                if (books.size() > maxTotalResultsToFetch) {
                    return books.subList(0, maxTotalResultsToFetch);
                }
                return books;
            })
            .doOnSuccess(finalList -> logger.info("BookDataOrchestrator: {} paged search for query '{}' completed. Aggregated {} books.", authType, query, finalList.size()))
            .onErrorResume(e -> {
                 logger.error("BookDataOrchestrator: Error during {} paged search for query '{}': {}. Returning empty list.", authType, query, e.getMessage(), e);
                 return Mono.just(Collections.emptyList());
            });
    }
}
