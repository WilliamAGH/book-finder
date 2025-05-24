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
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
public class BookDataOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(BookDataOrchestrator.class);

    private final S3RetryService s3RetryService;
    private final GoogleApiFetcher googleApiFetcher;
    private final ObjectMapper objectMapper;
    private final OpenLibraryBookDataService openLibraryBookDataService;
    // private final LongitoodBookDataService longitoodBookDataService; // Removed
    private final BookDataAggregatorService bookDataAggregatorService;

    public BookDataOrchestrator(S3RetryService s3RetryService,
                                GoogleApiFetcher googleApiFetcher,
                                ObjectMapper objectMapper,
                                OpenLibraryBookDataService openLibraryBookDataService,
                                // LongitoodBookDataService longitoodBookDataService, // Removed
                                BookDataAggregatorService bookDataAggregatorService) {
        this.s3RetryService = s3RetryService;
        this.googleApiFetcher = googleApiFetcher;
        this.objectMapper = objectMapper;
        this.openLibraryBookDataService = openLibraryBookDataService;
        // this.longitoodBookDataService = longitoodBookDataService; // Removed
        this.bookDataAggregatorService = bookDataAggregatorService;
    }

    private Mono<Book> fetchFromApisAndAggregate(String bookId) {
        // This will collect JsonNodes from various API sources

        Mono<JsonNode> googleAuthMono = googleApiFetcher.isApiKeyAvailable() ?
            googleApiFetcher.fetchVolumeByIdAuthenticated(bookId)
                .doOnSuccess(json -> { if (json != null && json.size() > 0) logger.info("BookDataOrchestrator: Tier 4 Google Auth HIT for {}", bookId);})
                .doOnError(e -> logger.warn("Tier 4 Google Auth API error for {}: {}", bookId, e.getMessage()))
            : Mono.empty(); // If no API key, authenticated call is effectively skipped or will use unauth path.

        Mono<JsonNode> googleUnauthMono = googleApiFetcher.fetchVolumeByIdUnauthenticated(bookId)
            .doOnSuccess(json -> { if (json != null && json.size() > 0) logger.info("BookDataOrchestrator: Tier 3 Google Unauth HIT for {}", bookId);})
            .doOnError(e -> logger.warn("Tier 3 Google Unauth API error for {}: {}", bookId, e.getMessage()));

        // Chain Google calls: Try auth, if empty or error, try unauth.
        // If googleAuthMono (after filter) is empty, log and switch to googleUnauthMono.
        // If googleAuthMono (or filter) errors, log and switch to googleUnauthMono.
        Mono<JsonNode> effectiveGoogleMono = googleAuthMono
            .filter(jsonNode -> jsonNode != null && jsonNode.size() > 0)
            .doOnSuccess(item -> {
                if (item == null || item.size() == 0) {
                    // This log indicates that the authenticated call was made (or attempted if no key)
                    // but it either returned empty or was filtered to empty.
                    logger.info("BookDataOrchestrator: Google Auth call for {} resulted in empty or was filtered out. Will attempt unauthenticated.", bookId);
                }
            })
            .switchIfEmpty(googleUnauthMono.doFirst(() -> {
                 // This log indicates that the switch to unauthenticated is about to happen.
                 logger.info("BookDataOrchestrator: Switching to unauthenticated Google API call for bookId: {}", bookId);
            }))
            .onErrorResume(e -> { 
                logger.warn("BookDataOrchestrator: Google Auth call chain for {} failed with error, falling back to Unauth. Error: {}", bookId, e.getMessage());
                return googleUnauthMono; 
            });

        // Dynamically decide how to call OpenLibrary based on available ISBN
        Mono<JsonNode> olMono = effectiveGoogleMono.flatMap(googleJsonNode -> {
            // Try to get ISBN from Google JSON's volumeInfo
            JsonNode volumeInfoNode = googleJsonNode.path("volumeInfo"); // Get volumeInfo node
            String isbnFromGoogle = BookJsonParser.extractIsbn(volumeInfoNode); 
            if (isbnFromGoogle != null) {
                logger.info("BookDataOrchestrator: Found ISBN {} from Google data for bookId {}. Fetching from OpenLibrary with this ISBN.", isbnFromGoogle, bookId);
                return Mono.fromFuture(openLibraryBookDataService.fetchBookByIsbn(isbnFromGoogle))
                    .flatMap(book -> {
                        if (book == null) return Mono.<JsonNode>empty();
                        try { return Mono.just(objectMapper.valueToTree(book)); }
                        catch (IllegalArgumentException exOl) { logger.error("Error converting OL Book to JsonNode for ISBN {}: {}", isbnFromGoogle, exOl.getMessage()); return Mono.<JsonNode>empty(); }
                    });
            } else if (BookJsonParser.isValidIsbn(bookId)) { // If no ISBN from Google, check if original bookId is an ISBN
                 logger.info("BookDataOrchestrator: No ISBN from Google data. bookId {} appears to be an ISBN. Fetching from OpenLibrary directly.", bookId);
                return Mono.fromFuture(openLibraryBookDataService.fetchBookByIsbn(bookId))
                    .flatMap(book -> {
                        if (book == null) return Mono.<JsonNode>empty();
                        try { return Mono.just(objectMapper.valueToTree(book)); }
                        catch (IllegalArgumentException exOl) { logger.error("Error converting OL Book to JsonNode for bookId {}: {}", bookId, exOl.getMessage()); return Mono.<JsonNode>empty(); }
                    });
            } else {
                logger.info("BookDataOrchestrator: No ISBN found from Google data for bookId {}, and bookId itself is not an ISBN. Skipping OpenLibrary fetch by ISBN.", bookId);
                return Mono.<JsonNode>empty(); 
            }
        }).switchIfEmpty(Mono.<JsonNode>defer(() -> { // If effectiveGoogleMono was empty
            if (BookJsonParser.isValidIsbn(bookId)) { // Check if original bookId is an ISBN
                logger.info("BookDataOrchestrator: Google data was empty for bookId {}. Attempting OpenLibrary directly as bookId appears to be an ISBN.", bookId);
                return Mono.fromFuture(openLibraryBookDataService.fetchBookByIsbn(bookId))
                    .flatMap(book -> {
                        if (book == null) return Mono.<JsonNode>empty();
                        try { return Mono.just(objectMapper.valueToTree(book)); }
                        catch (IllegalArgumentException exOl) { logger.error("Error converting OL Book to JsonNode for bookId {}: {}", bookId, exOl.getMessage()); return Mono.<JsonNode>empty(); }
                    });
            } else {
                logger.info("BookDataOrchestrator: Google data was empty for bookId {}, and bookId is not an ISBN. Skipping OpenLibrary.", bookId);
                return Mono.<JsonNode>empty(); 
            }
        }))
        .onErrorResume(e -> { 
            logger.warn("Tier 5 OpenLibrary API call chain error for bookId {}: {}", bookId, e.getMessage());
            return Mono.<JsonNode>empty(); 
        });
        
        // Now combine the result of Google calls (effectiveGoogleMono) with OpenLibrary (olMono)
        Mono<List<JsonNode>> apiResponsesMono = Mono.zip(
                effectiveGoogleMono.defaultIfEmpty(objectMapper.createObjectNode()), // Google result (auth or unauth)
                olMono.defaultIfEmpty(objectMapper.createObjectNode())      // OpenLibrary result
            )
            .map((reactor.util.function.Tuple2<JsonNode, JsonNode> tuple) ->
                java.util.stream.Stream.of(tuple.getT1(), tuple.getT2()) // T1 is Google, T2 is OpenLibrary
                    .filter(jsonNode -> jsonNode != null && jsonNode.size() > 0) // Filter out empty/placeholder nodes
                    .collect(java.util.stream.Collectors.toList())
            );

        return apiResponsesMono.flatMap(jsonList -> {
            if (jsonList.isEmpty()) {
                logger.info("BookDataOrchestrator: No data found from any API source for identifier: {}", bookId);
                return Mono.<Book>empty();
            }
            return Mono.fromCompletionStage(bookDataAggregatorService.aggregateBookDataSourcesAsync(bookId, "id", jsonList.toArray(new JsonNode[0])))
                .flatMap(aggregatedJson -> {
                    Book finalBook = BookJsonParser.convertJsonToBook(aggregatedJson);

                    if (finalBook == null || finalBook.getId() == null) {
                        logger.error("BookDataOrchestrator: Aggregation resulted in null or invalid book for identifier: {}", bookId);
                        return Mono.<Book>empty(); 
                    }
                    // Use the canonical ID from the aggregated book for S3 storage
                    String s3StorageKey = finalBook.getId();
                    logger.info("BookDataOrchestrator: Using s3StorageKey '{}' (from finalBook.getId()) instead of original bookId '{}' for S3 operations.", s3StorageKey, bookId);
                    return intelligentlyUpdateS3CacheAndReturnBook(finalBook, aggregatedJson, "Aggregated", s3StorageKey);
                });
        });
    }

    public Mono<Book> getBookByIdTiered(String bookId) {
        logger.debug("BookDataOrchestrator: Starting tiered fetch for book ID: {}", bookId);

        Mono<Book> s3FetchBookMono = Mono.fromCompletionStage(s3RetryService.fetchJsonWithRetry(bookId))
            .flatMap(s3Result -> {
                if (s3Result.isSuccess() && s3Result.getData().isPresent()) {
                    try {
                        JsonNode s3JsonNode = objectMapper.readTree(s3Result.getData().get());
                        Book bookFromS3 = BookJsonParser.convertJsonToBook(s3JsonNode);
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
                return Mono.empty(); 
            });

        return s3FetchBookMono
            .switchIfEmpty(
                Mono.fromRunnable(() -> logger.info("BookDataOrchestrator: S3 cache miss/error for {}, proceeding to API aggregation.", bookId))
                    .then(fetchFromApisAndAggregate(bookId)) // Directly use the Mono returned by the method
            )
            .doOnSuccess(book -> {
                if (book != null) {
                    logger.info("BookDataOrchestrator: Successfully processed book for identifier: {} Title: {}", bookId, book.getTitle());
                } else {
                    logger.info("BookDataOrchestrator: Failed to fetch/process book for identifier: {} from any tier.", bookId);
                }
            })
            .onErrorResume(e -> {
                logger.error("BookDataOrchestrator: Critical error during tiered fetch for identifier {}: {}", bookId, e.getMessage(), e);
                return Mono.<Book>empty();
            });
    }

    private Mono<Book> intelligentlyUpdateS3CacheAndReturnBook(Book bookToCache, JsonNode jsonToCache, String apiTypeContext, String s3Key) {
        if (s3Key == null || s3Key.trim().isEmpty()) {
            logger.error("BookDataOrchestrator ({}) - S3 Update: S3 key is null or empty for book title: {}. Cannot update S3 cache.", apiTypeContext, bookToCache.getTitle());
            return Mono.just(bookToCache); 
        }
        String newRawJson = jsonToCache.toString();

        return Mono.fromCompletionStage(s3RetryService.fetchJsonWithRetry(s3Key))
            .flatMap(s3Result -> {
                Book bookToReturn = bookToCache; 

                if (s3Result.isSuccess() && s3Result.getData().isPresent()) {
                    String existingRawJson = s3Result.getData().get();
                    try {
                        JsonNode existingS3JsonNode = objectMapper.readTree(existingRawJson);
                        Book existingBookFromS3 = BookJsonParser.convertJsonToBook(existingS3JsonNode);

                        if (shouldUpdateS3(existingBookFromS3, bookToCache, existingRawJson, newRawJson, s3Key)) {
                            logger.info("BookDataOrchestrator ({}) - S3 Update: New data for S3 key {} is better. Overwriting S3.", apiTypeContext, s3Key);
                            return Mono.fromCompletionStage(s3RetryService.uploadJsonWithRetry(s3Key, newRawJson))
                                .doOnSuccess(v -> logger.info("BookDataOrchestrator ({}) - S3 Update: Successfully overwrote S3 for S3 key: {}", apiTypeContext, s3Key))
                                .doOnError(e -> logger.error("BookDataOrchestrator ({}) - S3 Update: Failed to overwrite S3 for S3 key: {}. Error: {}", apiTypeContext, s3Key, e.getMessage()))
                                .thenReturn(bookToCache); 
                        } else {
                            logger.info("BookDataOrchestrator ({}) - S3 Update: Existing S3 data for S3 key {} is preferred or identical. Not overwriting.", apiTypeContext, s3Key);
                            bookToReturn = existingBookFromS3; 
                            return Mono.just(bookToReturn); 
                        }
                    } catch (Exception e) {
                        logger.error("BookDataOrchestrator ({}) - S3 Update: Error processing existing S3 data for S3 key {}. Defaulting to overwrite S3 with new data. Error: {}", apiTypeContext, s3Key, e.getMessage(), e);
                        return Mono.fromCompletionStage(s3RetryService.uploadJsonWithRetry(s3Key, newRawJson))
                                .thenReturn(bookToCache); 
                    }
                } else { 
                    if (s3Result.isNotFound()) {
                         logger.info("BookDataOrchestrator ({}) - S3 Update: No existing S3 data found for S3 key {}. Uploading new data.", apiTypeContext, s3Key);
                    } else if (s3Result.isServiceError()){
                        logger.warn("BookDataOrchestrator ({}) - S3 Update: S3 service error fetching existing data for S3 key {}. Uploading new data. Error: {}", apiTypeContext, s3Key, s3Result.getErrorMessage().orElse("Unknown S3 Error"));
                    } else if (s3Result.isDisabled()){
                         logger.info("BookDataOrchestrator ({}) - S3 Update: S3 is disabled. Storing new data for S3 key {}.", apiTypeContext, s3Key);
                    }
                    return Mono.fromCompletionStage(s3RetryService.uploadJsonWithRetry(s3Key, newRawJson))
                        .doOnSuccess(v -> logger.info("BookDataOrchestrator ({}) - S3 Update: Successfully uploaded new data to S3 for S3 key: {}", apiTypeContext, s3Key))
                        .doOnError(e -> logger.error("BookDataOrchestrator ({}) - S3 Update: Failed to upload new data to S3 for S3 key: {}. Error: {}", apiTypeContext, s3Key, e.getMessage()))
                        .thenReturn(bookToCache); 
                }
            })
            .onErrorReturn(bookToCache); 
    }

    private boolean shouldUpdateS3(Book existingBook, Book newBook, String existingRawJson, String newRawJson, String s3KeyContext) {
        if (existingBook == null || existingRawJson == null || existingRawJson.isEmpty()) {
            return true; 
        }
        if (newBook == null || newRawJson == null || newRawJson.isEmpty()) {
            return false; 
        }
        if (existingRawJson.equals(newRawJson)) {
            return false;
        }
        String oldDesc = existingBook.getDescription();
        String newDesc = newBook.getDescription();
        if (newDesc != null && !newDesc.isEmpty()) {
            if (oldDesc == null || oldDesc.isEmpty()) return true; 
            if (newDesc.length() > oldDesc.length() * 1.1) return true; 
        }
        int oldNonNullFields = countNonNullKeyFields(existingBook);
        int newNonNullFields = countNonNullKeyFields(newBook);
        if (newNonNullFields > oldNonNullFields) {
            return true;
        }
        logger.debug("shouldUpdateS3: Defaulting to keep existing data for S3 key {} as heuristics didn't determine new data was better.", s3KeyContext);
        return false; 
    }

    private int countNonNullKeyFields(Book book) {
        if (book == null) return 0;
        int count = 0;
        if (book.getPublisher() != null && !book.getPublisher().isEmpty()) count++;
        if (book.getPublishedDate() != null) count++;
        if (book.getPageCount() != null && book.getPageCount() > 0) count++;
        if (book.getIsbn10() != null && !book.getIsbn10().isEmpty()) count++;
        if (book.getIsbn13() != null && !book.getIsbn13().isEmpty()) count++;
        if (book.getCategories() != null && !book.getCategories().isEmpty()) count++;
        if (book.getLanguage() != null && !book.getLanguage().isEmpty()) count++;
        return count;
    }

    private static String generateS3SearchPageCacheKey(String query, int startIndex, String orderBy, String langCode, boolean authenticated) {
        String authPart = authenticated ? "auth" : "unauth";
        String langPart = (langCode == null || langCode.isEmpty()) ? "anylang" : langCode.trim().toLowerCase();
        String orderPart = (orderBy == null || orderBy.isEmpty()) ? "defaultOrder" : orderBy.trim().toLowerCase();
        
        // Basic sanitization for query part to make it S3 key friendly
        // Replace non-alphanumeric (excluding hyphen and underscore) with underscore
        String queryPart = query.replaceAll("[^a-zA-Z0-9-_]", "_").toLowerCase();
        // Truncate if too long to avoid overly long S3 keys
        if (queryPart.length() > 100) { 
            queryPart = queryPart.substring(0, 100);
        }
        // Ensure no leading/trailing underscores from replacement, and no multiple underscores
        queryPart = queryPart.replaceAll("_{2,}", "_").replaceAll("^_|_$", "");

        return String.format("search_pages_v1/%s/q_%s/s%d_o%s_l%s.json", authPart, queryPart, startIndex, orderPart, langPart);
    }
    
    public Mono<List<Book>> searchBooksTiered(String query, String langCode, int desiredTotalResults, String orderBy) {
        logger.debug("BookDataOrchestrator: Starting tiered search for query: '{}', lang: {}, total: {}, order: {}", query, langCode, desiredTotalResults, orderBy);
        
        final Map<String, Object> queryQualifiers = BookJsonParser.extractQualifiersFromSearchQuery(query);
        boolean apiKeyAvailable = googleApiFetcher.isApiKeyAvailable(); 

        Mono<List<Book>> primarySearchMono = apiKeyAvailable ?
            executePagedSearch(query, langCode, desiredTotalResults, orderBy, true, queryQualifiers) :
            executePagedSearch(query, langCode, desiredTotalResults, orderBy, false, queryQualifiers);

        Mono<List<Book>> fallbackSearchMono = apiKeyAvailable ?
            executePagedSearch(query, langCode, desiredTotalResults, orderBy, false, queryQualifiers) :
            Mono.just(Collections.emptyList());

        Mono<List<Book>> openLibrarySearchMono = openLibraryBookDataService.searchBooksByTitle(query)
            .collectList()
            .map(olBooks -> {
                if (!olBooks.isEmpty() && !queryQualifiers.isEmpty()) {
                    olBooks.forEach(book -> queryQualifiers.forEach(book::addQualifier));
                }
                return olBooks;
            })
            .onErrorResume(e -> {
                logger.error("Error during OpenLibrary search for query '{}': {}", query, e.getMessage(), e);
                return Mono.just(Collections.emptyList());
            });

        return primarySearchMono
            .flatMap(googleResults1 -> {
                if (!googleResults1.isEmpty()) {
                    logger.info("BookDataOrchestrator: Primary Google search ({}) successful for query '{}', found {} books.", (apiKeyAvailable ? "Authenticated" : "Unauthenticated"), query, googleResults1.size());
                    return Mono.just(googleResults1);
                }
                logger.info("BookDataOrchestrator: Primary Google search ({}) for query '{}' yielded no results. Proceeding to fallback Google search.", (apiKeyAvailable ? "Authenticated" : "Unauthenticated"), query);
                return fallbackSearchMono.flatMap(googleResults2 -> {
                    if (!googleResults2.isEmpty()) {
                        logger.info("BookDataOrchestrator: Fallback Google search successful for query '{}', found {} books.", query, googleResults2.size());
                        return Mono.just(googleResults2);
                    }
                    logger.info("BookDataOrchestrator: Fallback Google search for query '{}' yielded no results. Proceeding to OpenLibrary search.", query);
                    return openLibrarySearchMono;
                });
            })
            .doOnSuccess(books -> {
                if (!books.isEmpty()) {
                    logger.info("BookDataOrchestrator: Successfully searched books for query '{}'. Found {} books.", query, books.size());
                } else {
                    logger.info("BookDataOrchestrator: Search for query '{}' yielded no results from any tier.", query);
                }
            })
            .onErrorResume(e -> {
                logger.error("BookDataOrchestrator: Error during tiered search for query '{}': {}", query, e.getMessage(), e);
                return Mono.just(Collections.emptyList());
            });
    }

    private Mono<List<Book>> executePagedSearch(String query, String langCode, int desiredTotalResults, String orderBy, boolean authenticated, Map<String, Object> queryQualifiers) {
        final int maxResultsPerPage = 40;
        final int maxTotalResultsToFetch = (desiredTotalResults > 0 && desiredTotalResults <= 200) ? desiredTotalResults : (desiredTotalResults <=0 ? 40 : 200) ; 
        final String effectiveOrderBy = (orderBy != null && !orderBy.trim().isEmpty()) ? orderBy : "relevance";
        String authType = authenticated ? "Authenticated" : "Unauthenticated";

        logger.debug("BookDataOrchestrator: Executing {} paged search for query: '{}', lang: {}, total_requested: {}, order: {}", authType, query, langCode, maxTotalResultsToFetch, effectiveOrderBy);

        return Flux.range(0, (maxTotalResultsToFetch + maxResultsPerPage - 1) / maxResultsPerPage)
            .map(page -> page * maxResultsPerPage)
            .concatMap(startIndex -> {
                String s3CacheKey = generateS3SearchPageCacheKey(query, startIndex, effectiveOrderBy, langCode, authenticated);
                String operationDescription = String.format("S3 search page cache fetch for key: %s", s3CacheKey);

                // Try S3 first
                Mono<JsonNode> s3FetchMono = Mono.fromFuture(s3RetryService.fetchJsonWithRetry(s3CacheKey))
                    .flatMap(s3Result -> {
                        if (s3Result.isSuccess() && s3Result.getData().isPresent()) {
                            try {
                                logger.info("S3 search page cache HIT for key: {}", s3CacheKey);
                                return Mono.just(objectMapper.readTree(s3Result.getData().get()));
                            } catch (Exception e) {
                                logger.warn("Failed to parse S3 JSON for search page key {}: {}. Proceeding to API.", s3CacheKey, e.getMessage());
                                return Mono.empty(); // Treat as miss if parsing fails
                            }
                        }
                        if (s3Result.isNotFound()) {
                             logger.info("S3 search page cache MISS for key: {}", s3CacheKey);
                        } else if (s3Result.isServiceError()){
                             logger.warn("S3 search page cache SERVICE ERROR for key {}: {}. Proceeding to API.", s3CacheKey, s3Result.getErrorMessage().orElse("Unknown S3 Error"));
                        } else if (s3Result.isDisabled()){
                             logger.info("S3 search page cache is DISABLED. Proceeding to API for key: {}", s3CacheKey);
                        }
                        return Mono.empty(); // S3 miss or error, proceed to API
                    }).onErrorResume(e -> {
                        logger.error("Error during {}: {}. Proceeding to API.", operationDescription, e.getMessage(), e);
                        return Mono.empty();
                    });

                // API call as fallback, deferred
                Mono<JsonNode> apiCallMono = Mono.<JsonNode>defer(() -> authenticated ?
                    googleApiFetcher.searchVolumesAuthenticated(query, startIndex, effectiveOrderBy, langCode) :
                    googleApiFetcher.searchVolumesUnauthenticated(query, startIndex, effectiveOrderBy, langCode)
                ).doOnSuccess(responseNode -> {
                    if (responseNode != null && responseNode.size() > 0) { // Only cache non-empty valid responses
                        // Asynchronously save to S3, don't block the main flow
                        s3RetryService.uploadJsonWithRetry(s3CacheKey, responseNode.toString())
                            .thenRun(() -> logger.info("Successfully cached search page to S3: {}", s3CacheKey))
                            .exceptionally(ex -> {
                                logger.error("Failed to cache search page to S3 {}: {}", s3CacheKey, ex.getMessage());
                                return null;
                            });
                    }
                });

                return s3FetchMono.switchIfEmpty(apiCallMono)
                    .flatMapMany(responseNode -> {
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
            .flatMap(jsonItem -> { 
                Book bookFromSearchItem = BookJsonParser.convertJsonToBook(jsonItem);
                if (bookFromSearchItem != null && bookFromSearchItem.getId() != null) {
                    if (!queryQualifiers.isEmpty()) {
                        queryQualifiers.forEach(bookFromSearchItem::addQualifier);
                    }
                    return Mono.just(bookFromSearchItem);
                }
                return Mono.<Book>empty(); 
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
