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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import com.williamcallahan.book_recommendation_engine.util.S3Paths;
import com.williamcallahan.book_recommendation_engine.util.IdGenerator;
import com.williamcallahan.book_recommendation_engine.util.IsbnUtils;
import com.williamcallahan.book_recommendation_engine.util.JdbcUtils;
import com.williamcallahan.book_recommendation_engine.util.SlugGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.postgresql.util.PGobject;
import software.amazon.awssdk.services.s3.model.S3Object;
import java.sql.Date;
import java.util.UUID;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Set;
import java.util.function.ToIntBiFunction;
import com.williamcallahan.book_recommendation_engine.service.s3.S3FetchResult;

@Service
public class BookDataOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(BookDataOrchestrator.class);

    private final S3RetryService s3RetryService;
    private final GoogleApiFetcher googleApiFetcher;
    private final ObjectMapper objectMapper;
    private final OpenLibraryBookDataService openLibraryBookDataService;
    // private final LongitoodBookDataService longitoodBookDataService; // Removed
    private final BookDataAggregatorService bookDataAggregatorService;
    private final BookSupplementalPersistenceService supplementalPersistenceService;
    private final BookCollectionPersistenceService collectionPersistenceService;
    private final BookSearchService bookSearchService;
    private JdbcTemplate jdbcTemplate; // Optional DB layer
    private PostgresBookReader postgresBookReader;
    @Autowired(required = false)
    private S3StorageService s3StorageService; // Optional S3 layer
    private TransactionTemplate transactionTemplate;
    private static final long SEARCH_VIEW_REFRESH_INTERVAL_MS = 60_000L;
    private final AtomicLong lastSearchViewRefresh = new AtomicLong(0L);
    private static final Pattern CONTROL_CHAR_PATTERN = Pattern.compile("[\\x01-\\x08\\x0B\\x0C\\x0E-\\x1F\\x7F]");
    private static final Pattern CONCATENATED_OBJECT_PATTERN = Pattern.compile("\\}\\s*\\{");
    private static final int MAX_JSON_START_SCAN = 1000;
    private static final List<String> JSON_START_HINTS = List.of(
        "{\"id\":",
        "{\"kind\":",
        "{\"title\":",
        "{\"volumeInfo\":",
        "{ \"id\":",
        "{ \"kind\":",
        "[{\""
    );

    public BookDataOrchestrator(S3RetryService s3RetryService,
                                GoogleApiFetcher googleApiFetcher,
                                ObjectMapper objectMapper,
                                OpenLibraryBookDataService openLibraryBookDataService,
                                // LongitoodBookDataService longitoodBookDataService, // Removed
                                BookDataAggregatorService bookDataAggregatorService,
                                BookSupplementalPersistenceService supplementalPersistenceService,
                                BookCollectionPersistenceService collectionPersistenceService,
                                BookSearchService bookSearchService) {
        this.s3RetryService = s3RetryService;
        this.googleApiFetcher = googleApiFetcher;
        this.objectMapper = objectMapper;
        this.openLibraryBookDataService = openLibraryBookDataService;
        // this.longitoodBookDataService = longitoodBookDataService; // Removed
        this.bookDataAggregatorService = bookDataAggregatorService;
        this.supplementalPersistenceService = supplementalPersistenceService;
        this.collectionPersistenceService = collectionPersistenceService;
        this.bookSearchService = bookSearchService;
    }

    @Autowired(required = false)
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.postgresBookReader = jdbcTemplate != null ? new PostgresBookReader(jdbcTemplate, objectMapper) : null;
    }

    @Autowired(required = false)
    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        if (transactionManager != null) {
            this.transactionTemplate = new TransactionTemplate(transactionManager);
        }
    }

    public void refreshSearchView() {
        triggerSearchViewRefresh(false);
    }

    public void refreshSearchViewImmediately() {
        triggerSearchViewRefresh(true);
    }

    private Mono<Book> fetchFromApisAndAggregate(String bookId) {
        // This will collect JsonNodes from various API sources
        Mono<List<JsonNode>> apiResponsesMono = Mono.defer(() -> {
            Mono<JsonNode> tier4Mono = googleApiFetcher.fetchVolumeByIdAuthenticated(bookId)
                .doOnSuccess(json -> { if (json != null) logger.info("BookDataOrchestrator: Tier 4 Google Auth HIT for {}", bookId);})
                .onErrorResume(e -> { logger.warn("Tier 4 Google Auth API error for {}: {}", bookId, e.getMessage()); return Mono.<JsonNode>empty(); });

            Mono<JsonNode> tier3Mono = googleApiFetcher.fetchVolumeByIdUnauthenticated(bookId)
                .doOnSuccess(json -> { if (json != null) logger.info("BookDataOrchestrator: Tier 3 Google Unauth HIT for {}", bookId);})
                .onErrorResume(e -> { logger.warn("Tier 3 Google Unauth API error for {}: {}", bookId, e.getMessage()); return Mono.<JsonNode>empty(); });

            Mono<JsonNode> olMono = openLibraryBookDataService.fetchBookByIsbn(bookId)
                .flatMap(book -> {
                    try {
                        logger.info("BookDataOrchestrator: Tier 5 OpenLibrary HIT for {}. Title: {}", bookId, book.getTitle());
                        return Mono.just(objectMapper.valueToTree(book));
                    } catch (IllegalArgumentException e) {
                        logger.error("Error converting OpenLibrary Book to JsonNode for {}: {}", bookId, e.getMessage());
                        return Mono.<JsonNode>empty();
                    }
                })
                .onErrorResume(e -> { logger.warn("Tier 5 OpenLibrary API error for {}: {}", bookId, e.getMessage()); return Mono.<JsonNode>empty(); });
            
            return Mono.zip(
                    tier4Mono.defaultIfEmpty(objectMapper.createObjectNode()),
                    tier3Mono.defaultIfEmpty(objectMapper.createObjectNode()),
                    olMono.defaultIfEmpty(objectMapper.createObjectNode())
                )
                .map(tuple -> 
                    java.util.stream.Stream.of(tuple.getT1(), tuple.getT2(), tuple.getT3())
                        .filter(jsonNode -> jsonNode != null && jsonNode.size() > 0)
                        .collect(java.util.stream.Collectors.toList())
                );
        });

        return apiResponsesMono.flatMap(jsonList -> {
            if (jsonList.isEmpty()) {
                logger.info("BookDataOrchestrator: No data found from any API source for identifier: {}", bookId);
                return Mono.<Book>empty();
            }
            ObjectNode aggregatedJson = bookDataAggregatorService.aggregateBookDataSources(bookId, "id", jsonList.toArray(new JsonNode[0]));
            Book finalBook = BookJsonParser.convertJsonToBook(aggregatedJson);

            if (finalBook == null || finalBook.getId() == null) {
                logger.error("BookDataOrchestrator: Aggregation resulted in null or invalid book for identifier: {}", bookId);
                return Mono.<Book>empty(); 
            }
            // Use the canonical ID from the aggregated book for S3 storage
            String s3StorageKey = finalBook.getId();
            logger.info("BookDataOrchestrator: Using s3StorageKey '{}' (from finalBook.getId()) instead of original bookId '{}' for S3 operations.", s3StorageKey, bookId);
            // Persist to DB first (and external ids) before S3
            return Mono.fromRunnable(() -> saveToDatabase(finalBook, aggregatedJson))
                .subscribeOn(Schedulers.boundedElastic())
                .then(intelligentlyUpdateS3CacheAndReturnBook(finalBook, aggregatedJson, "Aggregated", s3StorageKey));
        });
    }

    public Mono<Book> getBookByIdTiered(String bookId) {
        logger.debug("BookDataOrchestrator: Starting tiered fetch (DB → S3 → APIs) for book ID: {}", bookId);

        // Tier 1: Database (if configured)
        Mono<Book> dbFetchBookMono = Mono.fromCallable(() -> {
            if (jdbcTemplate == null) return null;
            try {
                // Try by canonical ID first
                Book byId = findInDatabaseById(bookId).orElse(null);
                if (byId != null) return byId;
                // Try by ISBNs
                Book byIsbn13 = findInDatabaseByIsbn13(bookId).orElse(null);
                if (byIsbn13 != null) return byIsbn13;
                Book byIsbn10 = findInDatabaseByIsbn10(bookId).orElse(null);
                if (byIsbn10 != null) return byIsbn10;
                // Try by any external id mapping
                return findInDatabaseByAnyExternalId(bookId).orElse(null);
            } catch (Exception e) {
                logger.warn("DB lookup failed for {}: {}", bookId, e.getMessage());
                return null;
            }
        })
        .subscribeOn(Schedulers.boundedElastic())
        .flatMap(b -> b != null ? Mono.just(b) : Mono.empty());

        // Tier 2: S3
        Mono<Book> s3FetchBookMono = Mono.fromCompletionStage(s3RetryService.fetchJsonWithRetry(bookId))
            .flatMap(s3Result -> {
                if (s3Result.isSuccess() && s3Result.getData().isPresent()) {
                    try {
                        JsonNode s3JsonNode = objectMapper.readTree(s3Result.getData().get());
                        Book bookFromS3 = BookJsonParser.convertJsonToBook(s3JsonNode);
                        if (bookFromS3 != null && bookFromS3.getId() != null) {
                            logger.info("BookDataOrchestrator: Tier 2 S3 HIT for book ID: {}. Title: {}", bookId, bookFromS3.getTitle());
                            // Warm DB for future direct hits
                            saveToDatabase(bookFromS3, s3JsonNode);
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

        return dbFetchBookMono
            .switchIfEmpty(s3FetchBookMono)
            .switchIfEmpty(Mono.<Book>defer(() -> fetchFromApisAndAggregate(bookId)))
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

    public Mono<Book> getBookBySlugTiered(String slug) {
        if (slug == null || slug.isBlank()) {
            return Mono.empty();
        }
        return Mono.fromCallable(() -> findInDatabaseBySlug(slug).map(Book::getId).orElse(null))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(canonicalId -> canonicalId == null ? Mono.empty() : getBookByIdTiered(canonicalId));
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
    
    public Mono<List<Book>> searchBooksTiered(String query, String langCode, int desiredTotalResults, String orderBy) {
        logger.debug("BookDataOrchestrator: Starting tiered search for query: '{}', lang: {}, total: {}, order: {}", query, langCode, desiredTotalResults, orderBy);

        // Check PostgreSQL first - if we have results, return immediately without creating Google API Monos
        List<Book> postgresHits = searchPostgresFirst(query, desiredTotalResults);
        if (!postgresHits.isEmpty()) {
            logger.info("BookDataOrchestrator: Postgres search satisfied query '{}' with {} results.", query, postgresHits.size());
            return Mono.just(postgresHits);
        }

        // Only create Google API Monos if PostgreSQL returned no results
        final Map<String, Object> queryQualifiers = BookJsonParser.extractQualifiersFromSearchQuery(query);
        boolean googleFallbackEnabled = googleApiFetcher.isGoogleFallbackEnabled();
        boolean apiKeyAvailable = googleApiFetcher.isApiKeyAvailable(); 

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

        if (!googleFallbackEnabled) {
            logger.info("BookDataOrchestrator: Google fallback disabled for query '{}'. Returning OpenLibrary results only.", query);
            return openLibrarySearchMono;
        }

        Mono<List<Book>> primarySearchMono = apiKeyAvailable ?
            executePagedSearch(query, langCode, desiredTotalResults, orderBy, true, queryQualifiers) :
            executePagedSearch(query, langCode, desiredTotalResults, orderBy, false, queryQualifiers);

        Mono<List<Book>> fallbackSearchMono = apiKeyAvailable ?
            executePagedSearch(query, langCode, desiredTotalResults, orderBy, false, queryQualifiers) :
            Mono.just(Collections.emptyList());

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

    public Mono<List<BookSearchService.AuthorResult>> searchAuthors(String query, int desiredTotalResults) {
        if (bookSearchService == null) {
            return Mono.just(List.of());
        }
        int safeLimit = desiredTotalResults <= 0 ? 20 : PagingUtils.clamp(desiredTotalResults, 1, 100);
        return Mono.fromCallable(() -> bookSearchService.searchAuthors(query, safeLimit))
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(ex -> {
                    logger.error("BookDataOrchestrator: Author search failed for '{}': {}", query, ex.getMessage(), ex);
                    return Mono.just(List.of());
                });
    }

    private List<Book> searchPostgresFirst(String query, int desiredTotalResults) {
        if (bookSearchService == null || jdbcTemplate == null) {
            return List.of();
        }
        int safeTotal = desiredTotalResults <= 0 ? 20 : PagingUtils.atLeast(desiredTotalResults, 1);
        List<BookSearchService.SearchResult> hits = bookSearchService.searchBooks(query, safeTotal);
        if (hits == null || hits.isEmpty()) {
            return List.of();
        }
        List<Book> resolved = new ArrayList<>(hits.size());
        for (BookSearchService.SearchResult hit : hits) {
            Optional<Book> book = findInDatabaseById(hit.bookId().toString());
            book.ifPresent(resolvedBook -> {
                resolvedBook.addQualifier("search.matchType", hit.matchTypeNormalised());
                resolvedBook.addQualifier("search.relevanceScore", hit.relevanceScore());
                resolved.add(resolvedBook);
            });
        }
        return resolved;
    }

    private Mono<List<Book>> executePagedSearch(String query, String langCode, int desiredTotalResults, String orderBy, boolean authenticated, Map<String, Object> queryQualifiers) {
        final int maxResultsPerPage = 40;
        final int maxTotalResultsToFetch = desiredTotalResults <= 0
            ? 40
            : PagingUtils.clamp(desiredTotalResults, 1, 200);
        final String effectiveOrderBy = (orderBy != null && !orderBy.trim().isEmpty()) ? orderBy : "relevance";
        String authType = authenticated ? "Authenticated" : "Unauthenticated";

        logger.debug("BookDataOrchestrator: Executing {} paged search for query: '{}', lang: {}, total_requested: {}, order: {}", authType, query, langCode, maxTotalResultsToFetch, effectiveOrderBy);

        return googleApiFetcher.streamSearchItems(query, maxTotalResultsToFetch, effectiveOrderBy, langCode, authenticated)
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

    // --- Bulk migration from S3 helpers ---
    /**
     * Bulk migrates previously cached book JSON files from S3 into the database.
     * Enriches existing rows by matching on id/ISBNs/external ids; never creates duplicates.
     *
     * Triggered manually via CLI flags. This method is idempotent and safe to re-run.
     *
     * @param prefix S3 prefix to scan (e.g., "books/v1/")
     * @param maxRecords Maximum number of records to process (<= 0 means no limit)
     * @param skipRecords Number of objects to skip from the beginning (for manual batching)
     */
    public void migrateBooksFromS3(String prefix, int maxRecords, int skipRecords) {
        if (jdbcTemplate == null) {
            logger.warn("S3→DB migration skipped: Database is not configured (JdbcTemplate is null).");
            return;
        }
        if (s3StorageService == null) {
            logger.warn("S3→DB migration skipped: S3 is not configured (S3StorageService is null).");
            return;
        }

        final String effectivePrefix = S3Paths.ensureTrailingSlash(prefix);
        logger.info("Starting S3→DB migration: prefix='{}', max={}, skip={}", effectivePrefix, maxRecords, skipRecords);

        List<String> keysToProcess = prepareJsonKeysForMigration(
            effectivePrefix,
            maxRecords,
            skipRecords,
            "S3→DB migration",
            " Nothing to do.",
            " after filtering."
        );

        if (keysToProcess.isEmpty()) {
            return;
        }

        processS3JsonKeys(
            keysToProcess,
            "S3→DB migration",
            50,
            -1,
            (key, rawJson) -> {
                List<JsonNode> candidates = parseBookJsonPayload(rawJson, key);
                if (candidates.isEmpty()) {
                    logger.debug("S3→DB migration: No usable JSON objects extracted for key {}.", key);
                    return 0;
                }

                int processedCount = 0;
                for (JsonNode jsonNode : candidates) {
                    Book book = BookJsonParser.convertJsonToBook(jsonNode);
                    if (book == null || book.getId() == null) {
                        logger.debug("S3→DB migration: Parsed null/invalid book for key {}. Skipping fragment.", key);
                        continue;
                    }

                    saveToDatabaseEnrichOnMatch(book, jsonNode);
                    processedCount++;
                }
                return processedCount;
            }
        );
    }
    private List<String> prepareJsonKeysForMigration(String effectivePrefix,
                                                    int maxRecords,
                                                    int skipRecords,
                                                    String contextLabel,
                                                    String noObjectsSuffix,
                                                    String totalSuffix) {
        List<S3Object> objects = s3StorageService.listObjects(effectivePrefix);
        if (objects == null || objects.isEmpty()) {
            logger.info("{}: No objects found under prefix '{}'{}", contextLabel, effectivePrefix, noObjectsSuffix);
            return Collections.emptyList();
        }

        List<String> jsonKeys = new ArrayList<>();
        for (S3Object object : objects) {
            if (object == null) {
                continue;
            }
            String key = object.key();
            if (key != null && key.endsWith(".json")) {
                jsonKeys.add(key);
            }
        }

        if (skipRecords > 0 && skipRecords < jsonKeys.size()) {
            jsonKeys = new ArrayList<>(jsonKeys.subList(skipRecords, jsonKeys.size()));
        } else if (skipRecords >= jsonKeys.size()) {
            logger.info("{}: skip={} >= total JSON objects ({}). Nothing to do.", contextLabel, skipRecords, jsonKeys.size());
            return Collections.emptyList();
        }

        int totalToProcess = (maxRecords > 0) ? Math.min(maxRecords, jsonKeys.size()) : jsonKeys.size();
        logger.info("{}: {} JSON object(s) to process{}", contextLabel, totalToProcess, totalSuffix);
        return new ArrayList<>(jsonKeys.subList(0, totalToProcess));
    }

    private List<JsonNode> parseBookJsonPayload(String rawPayload, String key) {
        if (rawPayload == null || rawPayload.isBlank()) {
            logger.warn("S3→DB migration: Empty JSON payload for key {}.", key);
            return Collections.emptyList();
        }

        String sanitized = rawPayload.replace("\u0000", "");
        sanitized = CONTROL_CHAR_PATTERN.matcher(sanitized).replaceAll("");
        sanitized = sanitized.trim();

        if (sanitized.isEmpty()) {
            logger.warn("S3→DB migration: Payload for key {} became empty after sanitization.", key);
            return Collections.emptyList();
        }

        if (!sanitized.startsWith("{") && !sanitized.startsWith("[")) {
            int startIndex = findJsonStartIndex(sanitized);
            if (startIndex < 0) {
                logger.warn("S3→DB migration: Unable to locate JSON start for key {}. Skipping payload.", key);
                return Collections.emptyList();
            }
            if (startIndex > 0) {
                logger.warn("S3→DB migration: Stripping {} leading non-JSON bytes for key {}.", startIndex, key);
                sanitized = sanitized.substring(startIndex);
            }
        }

        List<String> fragments = splitConcatenatedJson(sanitized);
        List<JsonNode> parsedNodes = new ArrayList<>();

        for (String fragment : fragments) {
            if (fragment == null || fragment.isBlank()) {
                continue;
            }
            try {
                JsonNode node = objectMapper.readTree(fragment);
                if (node == null) {
                    continue;
                }
                if (node.isArray()) {
                    node.forEach(parsedNodes::add);
                } else {
                    parsedNodes.add(node);
                }
            } catch (Exception ex) {
                logger.warn("S3→DB migration: Failed to parse JSON fragment for key {}: {}", key, ex.getMessage());
                if (logger.isDebugEnabled()) {
                    logger.debug("Fragment preview: {}", fragment.length() > 200 ? fragment.substring(0, 200) : fragment);
                }
            }
        }

        if (parsedNodes.isEmpty()) {
            return Collections.emptyList();
        }

        List<JsonNode> extracted = new ArrayList<>(parsedNodes.size());
        for (JsonNode candidate : parsedNodes) {
            JsonNode normalized = extractFromPreProcessed(candidate, key);
            if (normalized != null) {
                extracted.add(normalized);
            }
        }

        return deduplicateBookNodes(extracted, key);
    }

    private int findJsonStartIndex(String payload) {
        if (payload == null) {
            return -1;
        }

        int firstBrace = payload.indexOf('{');
        int firstBracket = payload.indexOf('[');
        int candidate = -1;

        if (firstBrace >= 0 && (firstBracket == -1 || firstBrace < firstBracket)) {
            candidate = firstBrace;
        } else if (firstBracket >= 0) {
            candidate = firstBracket;
        }

        if (candidate >= 0 && candidate <= MAX_JSON_START_SCAN) {
            return candidate;
        }

        int bestMatch = -1;
        for (String hint : JSON_START_HINTS) {
            int idx = payload.indexOf(hint);
            if (idx >= 0 && (bestMatch == -1 || idx < bestMatch)) {
                bestMatch = idx;
            }
        }

        return bestMatch;
    }

    private List<String> splitConcatenatedJson(String payload) {
        Matcher matcher = CONCATENATED_OBJECT_PATTERN.matcher(payload);
        if (!matcher.find()) {
            return List.of(payload);
        }

        List<String> fragments = new ArrayList<>();
        int depth = 0;
        int start = 0;

        for (int i = 0; i < payload.length(); i++) {
            char ch = payload.charAt(i);
            if (ch == '{') {
                depth++;
            } else if (ch == '}') {
                depth--;
                if (depth == 0) {
                    fragments.add(payload.substring(start, i + 1));
                    start = i + 1;
                    while (start < payload.length() && Character.isWhitespace(payload.charAt(start))) {
                        start++;
                    }
                }
            }
        }

        if (start < payload.length()) {
            String remainder = payload.substring(start).trim();
            if (!remainder.isEmpty()) {
                fragments.add(remainder);
            }
        }

        return fragments.isEmpty() ? List.of(payload) : fragments;
    }

    private JsonNode extractFromPreProcessed(JsonNode node, String key) {
        if (node == null) {
            return null;
        }

        JsonNode rawNode = node.get("rawJsonResponse");
        boolean hasVolumeInfo = node.has("volumeInfo");
        String id = node.path("id").asText(null);
        String title = node.path("title").asText(null);

        boolean isPreProcessed = rawNode != null && !hasVolumeInfo && id != null && id.equals(title);
        if (!isPreProcessed) {
            return node;
        }

        try {
            String raw = rawNode.isTextual() ? rawNode.asText() : rawNode.toString();
            if (raw.startsWith("\"") && raw.endsWith("\"")) {
                raw = objectMapper.readValue(raw, String.class);
            }
            JsonNode inner = objectMapper.readTree(raw);
            if (inner != null && (inner.has("volumeInfo") || "books#volume".equals(inner.path("kind").asText(null)))) {
                return inner;
            }
        } catch (Exception ex) {
            logger.warn("S3→DB migration: Failed to unwrap rawJsonResponse for key {}: {}", key, ex.getMessage());
        }

        return node;
    }

    private List<JsonNode> deduplicateBookNodes(List<JsonNode> candidates, String key) {
        if (candidates == null || candidates.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, JsonNode> unique = new LinkedHashMap<>();
        for (JsonNode candidate : candidates) {
            if (candidate == null) {
                continue;
            }
            String dedupKey = computeDedupKey(candidate);
            if (!unique.containsKey(dedupKey)) {
                unique.put(dedupKey, candidate);
            } else if (logger.isDebugEnabled()) {
                logger.debug("S3→DB migration: Deduplicated book fragment for key {} using key {}.", key, dedupKey);
            }
        }

        return new ArrayList<>(unique.values());
    }

    private String computeDedupKey(JsonNode node) {
        if (node == null) {
            return "";
        }

        String nodeId = node.path("id").asText(null);

        JsonNode volume = node.has("volumeInfo") ? node.get("volumeInfo") : node;
        JsonNode identifiers = volume.get("industryIdentifiers");
        if (identifiers != null && identifiers.isArray()) {
            for (JsonNode identifierNode : identifiers) {
                String type = identifierNode.path("type").asText("");
                String value = identifierNode.path("identifier").asText("");
                if (!value.isEmpty() && ("ISBN_13".equalsIgnoreCase(type) || "ISBN_10".equalsIgnoreCase(type))) {
                    return (type + ":" + value).toLowerCase(Locale.ROOT);
                }
            }
        }

        if (nodeId != null && !nodeId.isBlank()) {
            return ("id:" + nodeId).toLowerCase(Locale.ROOT);
        }

        String title = volume.path("title").asText("");
        String author = "";
        JsonNode authors = volume.get("authors");
        if (authors != null && authors.isArray() && authors.size() > 0) {
            author = authors.get(0).asText("");
        }

        return (title + ":" + author).toLowerCase(Locale.ROOT);
    }

    private Optional<String> fetchJsonFromS3Key(String key) {
        if (s3StorageService == null) {
            return Optional.empty();
        }

        try {
            S3FetchResult<String> result = s3StorageService.fetchGenericJsonAsync(key).join();
            if (result.isSuccess()) {
                return result.getData();
            }

            if (result.isNotFound()) {
                logger.debug("S3→DB migration: JSON not found for key {}. Skipping.", key);
            } else if (result.isDisabled()) {
                logger.warn("S3→DB migration: S3 disabled while fetching key {}. Skipping.", key);
            } else {
                String errorMessage = result.getErrorMessage().orElse("Unknown error");
                logger.warn("S3→DB migration: Failed to fetch key {} (status {}): {}", key, result.getStatus(), errorMessage);
            }
        } catch (CompletionException ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            logger.warn("S3→DB migration: Exception fetching key {}: {}", key, cause.getMessage());
        }

        return Optional.empty();
    }

    private void processS3JsonKeys(List<String> keys,
                                   String contextLabel,
                                   int progressInterval,
                                   int expectedTotal,
                                   ToIntBiFunction<String, String> handler) {
        if (keys == null || keys.isEmpty()) {
            logger.info("{}: No S3 objects to process.", contextLabel);
            return;
        }

        AtomicInteger processed = new AtomicInteger(0);
        int totalTargets = expectedTotal > 0 ? expectedTotal : keys.size();

        for (String key : keys) {
            Optional<String> jsonOptional = fetchJsonFromS3Key(key);
            if (jsonOptional.isEmpty()) {
                continue;
            }

            try {
                int processedForKey = handler.applyAsInt(key, jsonOptional.get());
                if (processedForKey <= 0) {
                    continue;
                }

                int totalProcessed = processed.addAndGet(processedForKey);
                if (progressInterval > 0 && (totalProcessed % progressInterval == 0 || (expectedTotal > 0 && totalProcessed >= expectedTotal))) {
                    if (expectedTotal > 0) {
                        logger.info("{} progress: {}/{} processed.", contextLabel, Math.min(totalProcessed, expectedTotal), expectedTotal);
                    } else {
                        logger.info("{} progress: {} processed so far (latest key: {}).", contextLabel, totalProcessed, key);
                    }
                }
            } catch (Exception ex) {
                logger.warn("{}: Error processing key {}: {}", contextLabel, key, ex.getMessage());
            }
        }

        if (expectedTotal > 0) {
            logger.info("{} completed. Processed {}/{} record(s).", contextLabel, Math.min(processed.get(), expectedTotal), expectedTotal);
        } else {
            logger.info("{} completed. Processed {} record(s).", contextLabel, processed.get());
        }
    }

    private void saveToDatabaseEnrichOnMatch(Book incoming, JsonNode sourceJson) {
        if (jdbcTemplate == null || incoming == null) {
            return;
        }

        try {
            String googleId = extractGoogleId(sourceJson, incoming);
            String sanitizedIsbn13 = IsbnUtils.sanitize(incoming.getIsbn13());
            String sanitizedIsbn10 = IsbnUtils.sanitize(incoming.getIsbn10());

            String canonicalId = resolveCanonicalBookId(incoming, googleId, sanitizedIsbn13, sanitizedIsbn10);
            if (canonicalId == null) {
                String incomingId = incoming.getId();
                canonicalId = (incomingId != null && !incomingId.isBlank()) ? incomingId : IdGenerator.uuidV7();
            }

            incoming.setId(canonicalId);
            if (sanitizedIsbn13 != null) {
                incoming.setIsbn13(sanitizedIsbn13);
            }
            if (sanitizedIsbn10 != null) {
                incoming.setIsbn10(sanitizedIsbn10);
            }

            saveToDatabase(incoming, sourceJson);
        } catch (Exception e) {
            logger.warn("DB enrich-upsert failed for incoming book {}: {}", incoming.getId(), e.getMessage());
        }
    }

    // --- DB helpers ---

    // --- Lists upsert helpers ---
    /**
     * Bulk migrates S3-stored list JSONs into the neutral book_lists and book_lists_join tables.
     * Example provider: "NYT", prefix: "lists/nyt/".
     */
    public void migrateListsFromS3(String provider, String prefix, int maxRecords, int skipRecords) {
        if (jdbcTemplate == null) {
            logger.warn("S3→DB list migration skipped: Database is not configured (JdbcTemplate is null).");
            return;
        }
        if (s3StorageService == null) {
            logger.warn("S3→DB list migration skipped: S3 is not configured (S3StorageService is null).");
            return;
        }
        String defaultPrefix = "lists/" + (provider != null ? provider.toLowerCase(Locale.ROOT) : "") + "/";
        final String effectivePrefix = S3Paths.ensureTrailingSlash(prefix, defaultPrefix);
        logger.info("Starting S3→DB list migration: provider='{}', prefix='{}', max={}, skip={}", provider, effectivePrefix, maxRecords, skipRecords);

        List<String> keysToProcess = prepareJsonKeysForMigration(
            effectivePrefix,
            maxRecords,
            skipRecords,
            "S3→DB list migration",
            "",
            "."
        );

        if (keysToProcess.isEmpty()) {
            return;
        }

        int totalToProcess = keysToProcess.size();

        processS3JsonKeys(
            keysToProcess,
            "S3→DB list migration",
            20,
            totalToProcess,
            (key, rawJson) -> {
                try {
                    JsonNode root = objectMapper.readTree(rawJson);
                    if (!(root instanceof com.fasterxml.jackson.databind.node.ObjectNode)) {
                        return 0;
                    }
                    com.fasterxml.jackson.databind.node.ObjectNode listJson = (com.fasterxml.jackson.databind.node.ObjectNode) root;

                    String displayName = listJson.path("display_name").asText(null);
                    String listNameEncoded = listJson.path("list_name_encoded").asText(null);
                    String updatedFrequency = listJson.path("updated_frequency").asText(null);
                    java.time.LocalDate bestsellersDate = null;
                    java.time.LocalDate publishedDate = null;
                    try { String s = listJson.path("bestsellers_date").asText(null); if (s != null) bestsellersDate = java.time.LocalDate.parse(s); } catch (Exception ignored) {}
                    try { String s = listJson.path("published_date").asText(null); if (s != null) publishedDate = java.time.LocalDate.parse(s); } catch (Exception ignored) {}
                    if (publishedDate == null || listNameEncoded == null || provider == null) {
                        logger.warn("Skipping list key {}: missing required fields provider/list_name_encoded/published_date.", key);
                        return 0;
                    }

                    String listId = collectionPersistenceService
                        .upsertList(provider, listNameEncoded, publishedDate, displayName, bestsellersDate, updatedFrequency, null, listJson)
                        .orElse(null);
                    if (listId == null) {
                        return 0;
                    }

                    JsonNode booksArray = listJson.path("books");
                    if (booksArray != null && booksArray.isArray()) {
                        for (JsonNode item : booksArray) {
                            Integer rank = item.path("rank").isNumber() ? item.path("rank").asInt() : null;
                            Integer weeksOnList = item.path("weeks_on_list").isNumber() ? item.path("weeks_on_list").asInt() : null;
                            String isbn13 = item.path("primary_isbn13").asText(null);
                            String isbn10 = item.path("primary_isbn10").asText(null);
                            String providerRef = item.path("amazon_product_url").asText(null);

                            String canonicalId = null;
                            if (isbn13 != null) canonicalId = findInDatabaseByIsbn13(isbn13).map(Book::getId).orElse(null);
                            if (canonicalId == null && isbn10 != null) canonicalId = findInDatabaseByIsbn10(isbn10).map(Book::getId).orElse(null);
                            if (canonicalId == null && isbn13 != null) canonicalId = findInDatabaseByAnyExternalId(isbn13).map(Book::getId).orElse(null);
                            if (canonicalId == null && isbn10 != null) canonicalId = findInDatabaseByAnyExternalId(isbn10).map(Book::getId).orElse(null);

                            if (canonicalId == null) {
                                Book minimal = new Book();
                                minimal.setId(isbn13 != null ? isbn13 : (isbn10 != null ? isbn10 : IdGenerator.uuidV7()));
                                minimal.setIsbn13(isbn13);
                                minimal.setIsbn10(isbn10);
                                minimal.setTitle(item.path("title").asText(null));
                                minimal.setPublisher(item.path("publisher").asText(null));
                                minimal.setExternalImageUrl(item.path("book_image").asText(null));
                                saveToDatabase(minimal, null);
                                canonicalId = minimal.getId();
                            }

                            collectionPersistenceService.upsertListMembership(listId, canonicalId, rank, weeksOnList, isbn13, isbn10, providerRef, item);
                        }
                    }

                    return 1;
                } catch (Exception ex) {
                    logger.warn("S3→DB list migration: Error processing key {}: {}", key, ex.getMessage());
                    return 0;
                }
            }
        );
    }
    private Optional<Book> findInDatabaseById(String id) {
        if (postgresBookReader == null || id == null) {
            return Optional.empty();
        }
        return postgresBookReader.fetchByCanonicalId(id);
    }

    private Optional<Book> findInDatabaseBySlug(String slug) {
        if (postgresBookReader == null || slug == null) {
            return Optional.empty();
        }
        return postgresBookReader.fetchBySlug(slug);
    }

    private Optional<Book> findInDatabaseByIsbn13(String isbn13) {
        if (postgresBookReader == null) {
            return Optional.empty();
        }
        String sanitized = IsbnUtils.sanitize(isbn13);
        if (sanitized == null) {
            return Optional.empty();
        }
        return postgresBookReader.fetchByIsbn13(sanitized);
    }

    private Optional<Book> findInDatabaseByIsbn10(String isbn10) {
        if (postgresBookReader == null) {
            return Optional.empty();
        }
        String sanitized = IsbnUtils.sanitize(isbn10);
        if (sanitized == null) {
            return Optional.empty();
        }
        return postgresBookReader.fetchByIsbn10(sanitized);
    }

    private Optional<Book> findInDatabaseByAnyExternalId(String externalId) {
        if (postgresBookReader == null || externalId == null) {
            return Optional.empty();
        }
        return postgresBookReader.fetchByExternalId(externalId);
    }

    private void saveToDatabase(Book book, JsonNode sourceJson) {
        if (jdbcTemplate == null || book == null) {
            return;
        }

        Runnable work = () -> persistCanonicalBook(book, sourceJson);

        if (transactionTemplate != null) {
            transactionTemplate.executeWithoutResult(status -> work.run());
        } else {
            work.run();
        }
    }

    private void persistCanonicalBook(Book book, JsonNode sourceJson) {
        String googleId = extractGoogleId(sourceJson, book);
        String isbn13 = IsbnUtils.sanitize(book.getIsbn13());
        String isbn10 = IsbnUtils.sanitize(book.getIsbn10());

        if (isbn13 != null) {
            book.setIsbn13(isbn13);
        }
        if (isbn10 != null) {
            book.setIsbn10(isbn10);
        }

        String canonicalId = resolveCanonicalBookId(book, googleId, isbn13, isbn10);
        boolean isNew = false;

        if (canonicalId == null) {
            canonicalId = IdGenerator.uuidV7();
            isNew = true;
        }

        book.setId(canonicalId);

        String slug = resolveSlug(canonicalId, book, isNew);
        upsertBookRecord(book, slug);
        persistDimensions(canonicalId, book);

        if (googleId != null) {
            upsertExternalMetadata(canonicalId, "GOOGLE_BOOKS", googleId, book, sourceJson);
        }

        if (isbn13 != null) {
            upsertExternalMetadata(canonicalId, "ISBN13", isbn13, book, null);
        }
        if (isbn10 != null) {
            upsertExternalMetadata(canonicalId, "ISBN10", isbn10, book, null);
        }

        if (sourceJson != null && sourceJson.size() > 0) {
            persistRawJson(canonicalId, sourceJson, determineSource(sourceJson));
        }

        supplementalPersistenceService.persistAuthors(canonicalId, book.getAuthors());
        supplementalPersistenceService.persistCategories(canonicalId, book.getCategories());
        persistImageLinks(canonicalId, book);
        supplementalPersistenceService.assignQualifierTags(canonicalId, book.getQualifiers());
        synchronizeEditionRelationships(canonicalId, book);
        triggerSearchViewRefresh(false);
    }

    private String extractGoogleId(JsonNode sourceJson, Book book) {
        if (sourceJson != null && sourceJson.hasNonNull("id")) {
            return sourceJson.get("id").asText();
        }
        String rawId = book.getId();
        if (rawId != null && !looksLikeUuid(rawId)) {
            return rawId;
        }
        return null;
    }

    private String resolveCanonicalBookId(Book book, String googleId, String isbn13, String isbn10) {
        String existing = null;

        if (googleId != null) {
            existing = queryForId("SELECT book_id FROM book_external_ids WHERE source = ? AND external_id = ? LIMIT 1", "GOOGLE_BOOKS", googleId);
            if (existing != null) return existing;
        }

        String potential = book.getId();
        if (potential != null && looksLikeUuid(potential)) {
            existing = queryForId("SELECT id FROM books WHERE id = ? LIMIT 1", potential);
            if (existing != null) return existing;
        }

        if (isbn13 != null) {
            existing = queryForId("SELECT id FROM books WHERE isbn13 = ? LIMIT 1", isbn13);
            if (existing != null) return existing;

            existing = queryForId("SELECT book_id FROM book_external_ids WHERE provider_isbn13 = ? LIMIT 1", isbn13);
            if (existing != null) return existing;
        }

        if (isbn10 != null) {
            existing = queryForId("SELECT id FROM books WHERE isbn10 = ? LIMIT 1", isbn10);
            if (existing != null) return existing;

            existing = queryForId("SELECT book_id FROM book_external_ids WHERE provider_isbn10 = ? LIMIT 1", isbn10);
            if (existing != null) return existing;
        }

        return null;
    }

    private String queryForId(String sql, Object... params) {
        return JdbcUtils.optionalString(
                jdbcTemplate,
                sql,
                ex -> logger.debug("Query failed: {}", ex.getMessage()),
                params
        ).orElse(null);
    }

    private String resolveSlug(String bookId, Book book, boolean isNew) {
        if (!isNew) {
            String existing = queryForId("SELECT slug FROM books WHERE id = ?", bookId);
            if (existing != null && !existing.isBlank()) {
                return existing;
            }
        }

        String desired = SlugGenerator.generateBookSlug(book.getTitle(), book.getAuthors());
        if (desired == null || desired.isBlank() || jdbcTemplate == null) {
            return null;
        }

        try {
            return jdbcTemplate.queryForObject("SELECT ensure_unique_slug(?)", String.class, desired);
        } catch (DataAccessException ex) {
            return desired;
        }
    }

    private void upsertBookRecord(Book book, String slug) {
        java.util.Date published = book.getPublishedDate();
        Date sqlDate = published != null ? new Date(published.getTime()) : null;
        // editionNumber and editionGroupKey are used directly below from the book instance

        jdbcTemplate.update(
            "INSERT INTO books (id, title, subtitle, description, isbn10, isbn13, published_date, language, publisher, page_count, edition_number, edition_group_key, slug, s3_image_path, created_at, updated_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
            "ON CONFLICT (id) DO UPDATE SET " +
            "title = EXCLUDED.title, " +
            "subtitle = COALESCE(EXCLUDED.subtitle, books.subtitle), " +
            "description = COALESCE(EXCLUDED.description, books.description), " +
            "isbn10 = COALESCE(EXCLUDED.isbn10, books.isbn10), " +
            "isbn13 = COALESCE(EXCLUDED.isbn13, books.isbn13), " +
            "published_date = COALESCE(EXCLUDED.published_date, books.published_date), " +
            "language = COALESCE(EXCLUDED.language, books.language), " +
            "publisher = COALESCE(EXCLUDED.publisher, books.publisher), " +
            "page_count = COALESCE(EXCLUDED.page_count, books.page_count), " +
            "edition_number = COALESCE(EXCLUDED.edition_number, books.edition_number), " +
            "edition_group_key = COALESCE(EXCLUDED.edition_group_key, books.edition_group_key), " +
            "slug = COALESCE(EXCLUDED.slug, books.slug), " +
            "s3_image_path = COALESCE(EXCLUDED.s3_image_path, books.s3_image_path), " +
            "updated_at = NOW()",
            book.getId(),
            book.getTitle(),
            null,
            book.getDescription(),
            book.getIsbn10(),
            book.getIsbn13(),
            sqlDate,
            book.getLanguage(),
            book.getPublisher(),
            book.getPageCount(),
            book.getEditionNumber(),
            book.getEditionGroupKey(),
            slug,
            book.getS3ImagePath()
        );
    }

    private void upsertExternalMetadata(String bookId, String source, String externalId, Book book, JsonNode sourceJson) {
        if (externalId == null || externalId.isBlank()) {
            return;
        }

        // Check if another external ID already has these ISBNs
        // If so, we'll store NULL for the ISBNs to avoid duplicate constraint violations
        String providerIsbn10 = book.getIsbn10();
        String providerIsbn13 = book.getIsbn13();

        // Check for existing ISBN13 in book_external_ids
        if (providerIsbn13 != null && !providerIsbn13.isBlank()) {
            try {
                List<String> existingIds = jdbcTemplate.query(
                    "SELECT external_id FROM book_external_ids WHERE source = ? AND provider_isbn13 = ? LIMIT 1",
                    (rs, rowNum) -> rs.getString("external_id"),
                    source, providerIsbn13
                );

                if (!existingIds.isEmpty() && !existingIds.get(0).equals(externalId)) {
                    logger.info("[INFO] ISBN13 {} already linked via external ID {}, clearing it for new external ID {}",
                        providerIsbn13, existingIds.get(0), externalId);
                    providerIsbn13 = null; // Don't duplicate the ISBN in external_ids table
                }
            } catch (DataAccessException ex) {
                logger.debug("Error checking existing ISBN13: {}", ex.getMessage());
            }
        }

        // Check for existing ISBN10 in book_external_ids
        if (providerIsbn10 != null && !providerIsbn10.isBlank()) {
            try {
                List<String> existingIds = jdbcTemplate.query(
                    "SELECT external_id FROM book_external_ids WHERE source = ? AND provider_isbn10 = ? LIMIT 1",
                    (rs, rowNum) -> rs.getString("external_id"),
                    source, providerIsbn10
                );

                if (!existingIds.isEmpty() && !existingIds.get(0).equals(externalId)) {
                    logger.info("[INFO] ISBN10 {} already linked via external ID {}, clearing it for new external ID {}",
                        providerIsbn10, existingIds.get(0), externalId);
                    providerIsbn10 = null; // Don't duplicate the ISBN in external_ids table
                }
            } catch (DataAccessException ex) {
                logger.debug("Error checking existing ISBN10: {}", ex.getMessage());
            }
        }

        Double listPrice = book.getListPrice();
        String currency = book.getCurrencyCode();
        Double averageRating = book.getAverageRating();
        Integer ratingsCount = book.getRatingsCount();

        jdbcTemplate.update(
            "INSERT INTO book_external_ids (id, book_id, source, external_id, provider_isbn10, provider_isbn13, info_link, preview_link, purchase_link, web_reader_link, average_rating, ratings_count, pdf_available, epub_available, list_price, currency_code, created_at, last_updated) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
            "ON CONFLICT (source, external_id) DO UPDATE SET " +
            "book_id = EXCLUDED.book_id, " +
            "info_link = COALESCE(EXCLUDED.info_link, book_external_ids.info_link), " +
            "preview_link = COALESCE(EXCLUDED.preview_link, book_external_ids.preview_link), " +
            "purchase_link = COALESCE(EXCLUDED.purchase_link, book_external_ids.purchase_link), " +
            "web_reader_link = COALESCE(EXCLUDED.web_reader_link, book_external_ids.web_reader_link), " +
            "average_rating = COALESCE(EXCLUDED.average_rating, book_external_ids.average_rating), " +
            "ratings_count = COALESCE(EXCLUDED.ratings_count, book_external_ids.ratings_count), " +
            "pdf_available = COALESCE(EXCLUDED.pdf_available, book_external_ids.pdf_available), " +
            "epub_available = COALESCE(EXCLUDED.epub_available, book_external_ids.epub_available), " +
            "list_price = COALESCE(EXCLUDED.list_price, book_external_ids.list_price), " +
            "currency_code = COALESCE(EXCLUDED.currency_code, book_external_ids.currency_code), " +
            "last_updated = NOW()",
            IdGenerator.generate(),
            bookId,
            source,
            externalId,
            providerIsbn10,  // Use the potentially cleared ISBN10
            providerIsbn13,  // Use the potentially cleared ISBN13
            book.getInfoLink(),
            book.getPreviewLink(),
            book.getPurchaseLink(),
            book.getWebReaderLink(),
            averageRating,
            ratingsCount,
            book.getPdfAvailable(),
            book.getEpubAvailable(),
            listPrice,
            currency
        );
    }

    private void persistRawJson(String bookId, JsonNode sourceJson, String source) {
        if (sourceJson == null || sourceJson.isEmpty()) {
            return;
        }
        try {
            String payload = objectMapper.writeValueAsString(sourceJson);
            jdbcTemplate.update(
                "INSERT INTO book_raw_data (id, book_id, raw_json_response, source, fetched_at, contributed_at, created_at) " +
                "VALUES (?, ?, ?::jsonb, ?, NOW(), NOW(), NOW()) " +
                "ON CONFLICT (book_id, source) DO UPDATE SET raw_json_response = EXCLUDED.raw_json_response, fetched_at = NOW(), contributed_at = NOW(), updated_at = NOW()",
                IdGenerator.generate(),
                bookId,
                payload,
                source
            );
        } catch (Exception e) {
            logger.warn("Failed to persist raw JSON for book {}: {}", bookId, e.getMessage());
        }
    }

    private void persistDimensions(String bookId, Book book) {
        if (jdbcTemplate == null || bookId == null || !looksLikeUuid(bookId)) {
            return;
        }

        Double height = book.getHeightCm();
        Double width = book.getWidthCm();
        Double thickness = book.getThicknessCm();
        Double weight = book.getWeightGrams();

        java.util.UUID canonicalUuid;
        try {
            canonicalUuid = java.util.UUID.fromString(bookId);
        } catch (IllegalArgumentException ex) {
            logger.debug("Skipping dimension persistence for non-UUID id {}", bookId);
            return;
        }

        if (height == null && width == null && thickness == null && weight == null) {
            try {
                jdbcTemplate.update("DELETE FROM book_dimensions WHERE book_id = ?", canonicalUuid);
            } catch (DataAccessException ex) {
                logger.debug("Failed to delete empty dimensions for {}: {}", bookId, ex.getMessage());
            }
            return;
        }

        try {
            jdbcTemplate.update(
                "INSERT INTO book_dimensions (id, book_id, height_cm, width_cm, thickness_cm, weight_grams, created_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, NOW()) " +
                "ON CONFLICT (book_id) DO UPDATE SET " +
                "height_cm = COALESCE(EXCLUDED.height_cm, book_dimensions.height_cm), " +
                "width_cm = COALESCE(EXCLUDED.width_cm, book_dimensions.width_cm), " +
                "thickness_cm = COALESCE(EXCLUDED.thickness_cm, book_dimensions.thickness_cm), " +
                "weight_grams = COALESCE(EXCLUDED.weight_grams, book_dimensions.weight_grams)",
                IdGenerator.generateShort(),
                canonicalUuid,
                height,
                width,
                thickness,
                weight
            );
        } catch (DataAccessException ex) {
            logger.debug("Failed to persist dimensions for {}: {}", bookId, ex.getMessage());
        }
    }

    private void triggerSearchViewRefresh(boolean force) {
        if (bookSearchService == null) {
            return;
        }
        long now = System.currentTimeMillis();
        long previous;
        if (force) {
            previous = lastSearchViewRefresh.getAndSet(now);
        } else {
            previous = lastSearchViewRefresh.get();
            if (now - previous < SEARCH_VIEW_REFRESH_INTERVAL_MS) {
                return;
            }
            if (!lastSearchViewRefresh.compareAndSet(previous, now)) {
                return;
            }
        }
        long lastValid = previous;
        try {
            bookSearchService.refreshMaterializedView();
        } catch (RuntimeException ex) {
            logger.debug("Failed to refresh book search view: {}", ex.getMessage());
            lastSearchViewRefresh.set(lastValid);
        }
    }

    private String determineSource(JsonNode sourceJson) {
        if (sourceJson == null) {
            return "AGGREGATED";
        }
        if (sourceJson.has("source")) {
            return sourceJson.get("source").asText("AGGREGATED");
        }
        return "AGGREGATED";
    }


    private void persistImageLinks(String bookId, Book book) {
        CoverImages images = book.getCoverImages();
        if (images == null) {
            images = new CoverImages();
        }

        if (images.getPreferredUrl() != null) {
            upsertImageLink(bookId, "preferred", images.getPreferredUrl(), images.getSource() != null ? images.getSource().name() : null);
        }
        if (images.getFallbackUrl() != null) {
            upsertImageLink(bookId, "fallback", images.getFallbackUrl(), images.getSource() != null ? images.getSource().name() : null);
        }
        if (book.getExternalImageUrl() != null) {
            upsertImageLink(bookId, "external", book.getExternalImageUrl(), "EXTERNAL");
        }
        if (book.getS3ImagePath() != null) {
            upsertImageLink(bookId, "s3", book.getS3ImagePath(), "S3");
        }
    }

    private void upsertImageLink(String bookId, String type, String url, String source) {
        jdbcTemplate.update(
            "INSERT INTO book_image_links (id, book_id, image_type, url, source, created_at) VALUES (?, ?, ?, ?, ?, NOW()) " +
            "ON CONFLICT (book_id, image_type) DO UPDATE SET url = EXCLUDED.url, source = EXCLUDED.source, created_at = book_image_links.created_at",
            IdGenerator.generate(),
            bookId,
            type,
            url,
            source
        );
    }

    private void synchronizeEditionRelationships(String bookId, Book book) {
        if (jdbcTemplate == null || bookId == null || bookId.isBlank()) {
            return;
        }

        String groupKey = book.getEditionGroupKey();
        Integer editionNumber = book.getEditionNumber();

        if (groupKey == null) {
            deleteEditionLinksForBooks(Collections.singletonList(bookId));
            return;
        }

        List<EditionLinkRecord> siblings = jdbcTemplate.query(
            "SELECT id, edition_number FROM books WHERE edition_group_key = ?",
            ps -> ps.setString(1, groupKey),
            (rs, rowNum) -> {
                String id = rs.getString("id");
                Integer stored = (Integer) rs.getObject("edition_number");
                int normalized = (stored != null && stored > 0) ? stored : 1;
                return new EditionLinkRecord(id, normalized);
            }
        );

        if (siblings.isEmpty()) {
            return;
        }

        List<EditionLinkRecord> normalized = new ArrayList<>(siblings.size());
        for (EditionLinkRecord record : siblings) {
            int number = record.editionNumber();
            if (record.id().equals(bookId) && editionNumber != null) {
                number = PagingUtils.atLeast(editionNumber, 1);
            }
            normalized.add(new EditionLinkRecord(record.id(), PagingUtils.atLeast(number, 1)));
        }

        if (normalized.size() <= 1) {
            deleteEditionLinksForBooks(Collections.singletonList(bookId));
            return;
        }

        normalized.sort((a, b) -> {
            int diff = Integer.compare(b.editionNumber(), a.editionNumber());
            if (diff != 0) return diff;
            return a.id().compareTo(b.id());
        });

        List<String> groupIds = new ArrayList<>(normalized.size());
        for (EditionLinkRecord record : normalized) {
            groupIds.add(record.id());
        }
        deleteEditionLinksForBooks(groupIds);

        EditionLinkRecord primary = normalized.get(0);
        for (int i = 1; i < normalized.size(); i++) {
            EditionLinkRecord sibling = normalized.get(i);
            jdbcTemplate.update(
                "INSERT INTO book_editions (id, book_id, related_book_id, link_source, relationship_type, created_at, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, NOW(), NOW()) " +
                "ON CONFLICT (book_id, related_book_id) DO UPDATE SET link_source = EXCLUDED.link_source, relationship_type = EXCLUDED.relationship_type, updated_at = NOW()",
                IdGenerator.generateLong(),
                primary.id(),
                sibling.id(),
                "INGESTION",
                "ALTERNATE_EDITION"
            );
        }
    }

    private void deleteEditionLinksForBooks(List<String> ids) {
        if (jdbcTemplate == null || ids == null || ids.isEmpty()) {
            return;
        }
        LinkedHashSet<String> unique = new LinkedHashSet<>();
        for (String id : ids) {
            if (id != null && !id.isBlank()) {
                unique.add(id);
            }
        }
        for (String id : unique) {
            jdbcTemplate.update(
                "DELETE FROM book_editions WHERE book_id = ? OR related_book_id = ?",
                id,
                id
            );
        }
    }

    private record EditionLinkRecord(String id, int editionNumber) {}

    private static final class PostgresBookReader {

        private static final Logger LOG = LoggerFactory.getLogger(PostgresBookReader.class);
        private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

        private final JdbcTemplate jdbcTemplate;
        private final ObjectMapper objectMapper;

        PostgresBookReader(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
            this.jdbcTemplate = jdbcTemplate;
            this.objectMapper = objectMapper;
        }

        Optional<Book> fetchByCanonicalId(String id) {
            if (id == null) {
                return Optional.empty();
            }
            UUID canonicalId;
            try {
                canonicalId = UUID.fromString(id);
            } catch (IllegalArgumentException ex) {
                return Optional.empty();
            }
            return loadAggregate(canonicalId);
        }

        Optional<Book> fetchBySlug(String slug) {
            if (slug == null || slug.isBlank()) {
                return Optional.empty();
            }
            return queryForUuid("SELECT id FROM books WHERE slug = ? LIMIT 1", slug.trim())
                    .flatMap(this::loadAggregate);
        }

        Optional<Book> fetchByIsbn13(String isbn13) {
            if (isbn13 == null || isbn13.isBlank()) {
                return Optional.empty();
            }
            Optional<UUID> canonical = queryForUuid("SELECT id FROM books WHERE isbn13 = ? LIMIT 1", isbn13)
                    .or(() -> queryForUuid("SELECT book_id FROM book_external_ids WHERE provider_isbn13 = ? LIMIT 1", isbn13));
            return canonical.flatMap(this::loadAggregate);
        }

        Optional<Book> fetchByIsbn10(String isbn10) {
            if (isbn10 == null || isbn10.isBlank()) {
                return Optional.empty();
            }
            Optional<UUID> canonical = queryForUuid("SELECT id FROM books WHERE isbn10 = ? LIMIT 1", isbn10)
                    .or(() -> queryForUuid("SELECT book_id FROM book_external_ids WHERE provider_isbn10 = ? LIMIT 1", isbn10));
            return canonical.flatMap(this::loadAggregate);
        }

        Optional<Book> fetchByExternalId(String externalId) {
            if (externalId == null || externalId.isBlank()) {
                return Optional.empty();
            }
            String trimmed = externalId.trim();
            Optional<UUID> canonical = queryForUuid("SELECT book_id FROM book_external_ids WHERE external_id = ? LIMIT 1", trimmed)
                    .or(() -> queryForUuid("SELECT book_id FROM book_external_ids WHERE provider_isbn13 = ? LIMIT 1", trimmed))
                    .or(() -> queryForUuid("SELECT book_id FROM book_external_ids WHERE provider_isbn10 = ? LIMIT 1", trimmed))
                    .or(() -> queryForUuid("SELECT book_id FROM book_external_ids WHERE provider_asin = ? LIMIT 1", trimmed));
            return canonical.flatMap(this::loadAggregate);
        }

        private Optional<Book> loadAggregate(UUID canonicalId) {
            String sql = """
                    SELECT id::text, slug, title, description, isbn10, isbn13, published_date, language, publisher, page_count
                    FROM books
                    WHERE id = ?
                    """;
            try {
                return jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), rs -> {
                    if (!rs.next()) {
                        return Optional.<Book>empty();
                    }
                    Book book = new Book();
                    book.setId(rs.getString("id"));
                    book.setSlug(rs.getString("slug"));
                    book.setTitle(rs.getString("title"));
                    book.setDescription(rs.getString("description"));
                    book.setIsbn10(rs.getString("isbn10"));
                    book.setIsbn13(rs.getString("isbn13"));
                    java.sql.Date published = rs.getDate("published_date");
                    if (published != null) {
                        book.setPublishedDate(new java.util.Date(published.getTime()));
                    }
                    book.setLanguage(rs.getString("language"));
                    book.setPublisher(rs.getString("publisher"));
                    Integer pageCount = (Integer) rs.getObject("page_count");
                    book.setPageCount(pageCount);

                    hydrateAuthors(book, canonicalId);
                    hydrateCategories(book, canonicalId);
                    hydrateCollections(book, canonicalId);
                    hydrateDimensions(book, canonicalId);
                    hydrateRawPayload(book, canonicalId);
                    hydrateTags(book, canonicalId);
                    hydrateEditions(book, canonicalId);
                    hydrateCover(book, canonicalId);
                    hydrateRecommendations(book, canonicalId);
                    hydrateProviderMetadata(book, canonicalId);

                    return Optional.of(book);
                });
            } catch (DataAccessException ex) {
                LOG.debug("Postgres reader failed to load canonical book {}: {}", canonicalId, ex.getMessage());
                return Optional.empty();
            }
        }

        private void hydrateAuthors(Book book, UUID canonicalId) {
            String sql = """
                    SELECT a.name
                    FROM book_authors_join baj
                    JOIN authors a ON a.id = baj.author_id
                    WHERE baj.book_id = ?
                    ORDER BY baj.position
                    """;
            try {
                List<String> authors = jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), (rs, rowNum) -> rs.getString("name"));
                book.setAuthors(authors == null || authors.isEmpty() ? List.of() : authors);
            } catch (DataAccessException ex) {
                LOG.debug("Failed to hydrate authors for {}: {}", canonicalId, ex.getMessage());
                book.setAuthors(List.of());
            }
        }

        private void hydrateCategories(Book book, UUID canonicalId) {
            String sql = """
                    SELECT bc.display_name
                    FROM book_collections_join bcj
                    JOIN book_collections bc ON bc.id = bcj.collection_id
                    WHERE bcj.book_id = ?
                      AND bc.collection_type = 'CATEGORY'
                    ORDER BY COALESCE(bcj.position, 9999), lower(bc.display_name)
                    """;
            try {
                List<String> categories = jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), (rs, rowNum) -> rs.getString("display_name"));
                book.setCategories(categories == null || categories.isEmpty() ? List.of() : categories);
            } catch (DataAccessException ex) {
                LOG.debug("Failed to hydrate categories for {}: {}", canonicalId, ex.getMessage());
                book.setCategories(List.of());
            }
        }

        private void hydrateCollections(Book book, UUID canonicalId) {
            String sql = """
                    SELECT bc.id,
                           bc.display_name,
                           bc.collection_type,
                           bc.source,
                           bcj.position
                    FROM book_collections_join bcj
                    JOIN book_collections bc ON bc.id = bcj.collection_id
                    WHERE bcj.book_id = ?
                    ORDER BY CASE WHEN bc.collection_type = 'CATEGORY' THEN 0 ELSE 1 END,
                             COALESCE(bcj.position, 2147483647),
                             lower(bc.display_name)
                    """;
            try {
                List<Book.CollectionAssignment> assignments = jdbcTemplate.query(
                        sql,
                        ps -> ps.setObject(1, canonicalId),
                        (rs, rowNum) -> {
                            Book.CollectionAssignment assignment = new Book.CollectionAssignment();
                            assignment.setCollectionId(rs.getString("id"));
                            assignment.setName(rs.getString("display_name"));
                            assignment.setCollectionType(rs.getString("collection_type"));
                            Integer rank = (Integer) rs.getObject("position");
                            assignment.setRank(rank);
                            assignment.setSource(rs.getString("source"));
                            return assignment;
                        }
                );
                book.setCollections(assignments);
            } catch (DataAccessException ex) {
                LOG.debug("Failed to hydrate collections for {}: {}", canonicalId, ex.getMessage());
                book.setCollections(List.of());
            }
        }

        private void hydrateDimensions(Book book, UUID canonicalId) {
            String sql = """
                    SELECT height_cm, width_cm, thickness_cm, weight_grams
                    FROM book_dimensions
                    WHERE book_id = ?
                    LIMIT 1
                    """;
            try {
                jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    book.setHeightCm(toDouble(rs.getBigDecimal("height_cm")));
                    book.setWidthCm(toDouble(rs.getBigDecimal("width_cm")));
                    book.setThicknessCm(toDouble(rs.getBigDecimal("thickness_cm")));
                    book.setWeightGrams(toDouble(rs.getBigDecimal("weight_grams")));
                    return null;
                });
            } catch (DataAccessException ex) {
                LOG.debug("Failed to hydrate dimensions for {}: {}", canonicalId, ex.getMessage());
                book.setHeightCm(null);
                book.setWidthCm(null);
                book.setThicknessCm(null);
                book.setWeightGrams(null);
            }
        }

        private void hydrateRawPayload(Book book, UUID canonicalId) {
            String sql = """
                    SELECT raw_json_response::text
                    FROM book_raw_data
                    WHERE book_id = ?
                    ORDER BY contributed_at DESC
                    LIMIT 1
                    """;
            try {
                String rawJson = jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), rs -> rs.next() ? rs.getString(1) : null);
                if (rawJson != null && !rawJson.isBlank()) {
                    book.setRawJsonResponse(rawJson);
                }
            } catch (DataAccessException ex) {
                LOG.debug("Failed to hydrate raw payload for {}: {}", canonicalId, ex.getMessage());
            }
        }

        private void hydrateTags(Book book, UUID canonicalId) {
            String sql = """
                    SELECT bt.key, bt.display_name, bta.source, bta.confidence, bta.metadata
                    FROM book_tag_assignments bta
                    JOIN book_tags bt ON bt.id = bta.tag_id
                    WHERE bta.book_id = ?
                    """;
            try {
                Map<String, Object> tags = jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), rs -> {
                    Map<String, Object> result = new LinkedHashMap<>();
                    while (rs.next()) {
                        String key = rs.getString("key");
                        if (key == null || key.isBlank()) {
                            continue;
                        }
                        Map<String, Object> attributes = new LinkedHashMap<>();
                        String displayName = rs.getString("display_name");
                        if (displayName != null && !displayName.isBlank()) {
                            attributes.put("displayName", displayName);
                        }
                        String source = rs.getString("source");
                        if (source != null && !source.isBlank()) {
                            attributes.put("source", source);
                        }
                        Double confidence = (Double) rs.getObject("confidence");
                        if (confidence != null) {
                            attributes.put("confidence", confidence);
                        }
                        Map<String, Object> metadata = parseJsonAttributes(rs.getObject("metadata"));
                        if (!metadata.isEmpty()) {
                            attributes.put("metadata", metadata);
                        }
                        result.put(key, attributes);
                    }
                    return result;
                });
                book.setQualifiers(tags);
            } catch (DataAccessException ex) {
                LOG.debug("Failed to hydrate tags for {}: {}", canonicalId, ex.getMessage());
                book.setQualifiers(Map.of());
            }
        }

        private void hydrateEditions(Book book, UUID canonicalId) {
            String sql = """
                    SELECT b.id::text as edition_id,
                           b.slug,
                           b.title,
                           b.isbn13,
                           b.isbn10,
                           b.publisher,
                           b.published_date,
                           wc.cluster_method,
                           wcm.confidence,
                           bei.external_id as google_books_id,
                           bil.s3_image_path
                    FROM work_cluster_members wcm1
                    JOIN work_cluster_members wcm ON wcm.cluster_id = wcm1.cluster_id
                    JOIN books b ON b.id = wcm.book_id
                    JOIN work_clusters wc ON wc.id = wcm.cluster_id
                    LEFT JOIN book_external_ids bei
                           ON bei.book_id = b.id AND bei.source = 'GOOGLE_BOOKS'
                    LEFT JOIN book_image_links bil
                           ON bil.book_id = b.id AND bil.is_primary = true
                    WHERE wcm1.book_id = ?
                      AND wcm.book_id <> ?
                    ORDER BY wcm.is_primary DESC,
                             wcm.confidence DESC NULLS LAST,
                             b.published_date DESC NULLS LAST,
                             lower(b.title)
                    """;
            try {
                List<Book.EditionInfo> editions = jdbcTemplate.query(sql,
                        ps -> {
                            ps.setObject(1, canonicalId);
                            ps.setObject(2, canonicalId);
                        },
                        (rs, rowNum) -> {
                            Book.EditionInfo info = new Book.EditionInfo();
                            info.setGoogleBooksId(rs.getString("google_books_id"));
                            info.setType(rs.getString("cluster_method"));
                            String slug = rs.getString("slug");
                            info.setIdentifier(slug != null && !slug.isBlank() ? slug : rs.getString("edition_id"));
                            info.setEditionIsbn13(rs.getString("isbn13"));
                            info.setEditionIsbn10(rs.getString("isbn10"));
                            java.sql.Date published = rs.getDate("published_date");
                            if (published != null) {
                                info.setPublishedDate(new java.util.Date(published.getTime()));
                            }
                            info.setCoverImageUrl(rs.getString("s3_image_path"));
                            return info;
                        });
                book.setOtherEditions(editions.isEmpty() ? List.of() : editions);
            } catch (DataAccessException ex) {
                LOG.debug("Failed to hydrate editions for {}: {}", canonicalId, ex.getMessage());
                book.setOtherEditions(List.of());
            }
        }

        private void hydrateCover(Book book, UUID canonicalId) {
            String sql = """
                    SELECT image_type, url, source, s3_image_path, width, height, is_high_resolution
                    FROM book_image_links
                    WHERE book_id = ?
                    ORDER BY CASE image_type
                        WHEN 'extraLarge' THEN 1
                        WHEN 'large' THEN 2
                        WHEN 'medium' THEN 3
                        WHEN 'small' THEN 4
                        WHEN 'thumbnail' THEN 5
                        WHEN 'smallThumbnail' THEN 6
                        ELSE 7
                    END, created_at DESC
                    LIMIT 2
                    """;
            try {
                List<CoverCandidate> candidates = jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), (rs, rowNum) -> new CoverCandidate(
                        rs.getString("url"),
                        rs.getString("s3_image_path"),
                        rs.getString("source"),
                        rs.getObject("width", Integer.class),
                        rs.getObject("height", Integer.class),
                        rs.getObject("is_high_resolution", Boolean.class)
                ));
                if (candidates.isEmpty()) {
                    return;
                }
                CoverCandidate primary = candidates.get(0);
                book.setExternalImageUrl(primary.url());
                book.setS3ImagePath(primary.s3Path());
                book.setCoverImageWidth(primary.width());
                book.setCoverImageHeight(primary.height());
                book.setIsCoverHighResolution(primary.highRes());
                CoverImages coverImages = new CoverImages(primary.url(),
                        candidates.size() > 1 ? candidates.get(1).url() : primary.url(),
                        toCoverSource(primary.source()));
                book.setCoverImages(coverImages);
            } catch (DataAccessException ex) {
                LOG.debug("Failed to hydrate cover for {}: {}", canonicalId, ex.getMessage());
            }
        }

        private void hydrateRecommendations(Book book, UUID canonicalId) {
            String sql = """
                    SELECT recommended_book_id::text
                    FROM book_recommendations
                    WHERE source_book_id = ?
                    ORDER BY score DESC NULLS LAST, created_at DESC
                    LIMIT 20
                    """;
            try {
                List<String> recommendations = jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), (rs, rowNum) -> rs.getString("recommended_book_id"));
                book.setCachedRecommendationIds(recommendations == null || recommendations.isEmpty() ? List.of() : recommendations);
            } catch (DataAccessException ex) {
                LOG.debug("Failed to hydrate recommendations for {}: {}", canonicalId, ex.getMessage());
                book.setCachedRecommendationIds(List.of());
            }
        }

        private void hydrateProviderMetadata(Book book, UUID canonicalId) {
            String sql = """
                    SELECT info_link, preview_link, web_reader_link, purchase_link,
                           average_rating, ratings_count, list_price, currency_code, provider_asin
                    FROM book_external_ids
                    WHERE book_id = ?
                    ORDER BY CASE WHEN source = 'GOOGLE_BOOKS' THEN 0 ELSE 1 END, created_at DESC
                    LIMIT 1
                    """;
            try {
                jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    book.setInfoLink(rs.getString("info_link"));
                    book.setPreviewLink(rs.getString("preview_link"));
                    book.setWebReaderLink(rs.getString("web_reader_link"));
                    book.setPurchaseLink(rs.getString("purchase_link"));
                    Double averageRating = (Double) rs.getObject("average_rating");
                    if (averageRating != null) {
                        book.setAverageRating(averageRating);
                        book.setHasRatings(Boolean.TRUE);
                    }
                    Integer ratingsCount = (Integer) rs.getObject("ratings_count");
                    if (ratingsCount != null) {
                        book.setRatingsCount(ratingsCount);
                        book.setHasRatings(ratingsCount > 0);
                    }
                    java.math.BigDecimal listPrice = rs.getBigDecimal("list_price");
                    if (listPrice != null) {
                        book.setListPrice(listPrice.doubleValue());
                    }
                    String currency = rs.getString("currency_code");
                    if (currency != null && !currency.isBlank()) {
                        book.setCurrencyCode(currency);
                    }
                    String asin = rs.getString("provider_asin");
                    if (asin != null && !asin.isBlank()) {
                        book.setAsin(asin);
                    }
                    return null;
                });
            } catch (DataAccessException ex) {
                LOG.debug("Failed to hydrate provider metadata for {}: {}", canonicalId, ex.getMessage());
            }
        }

        private Optional<UUID> queryForUuid(String sql, Object param) {
            try {
                UUID result = jdbcTemplate.queryForObject(sql, UUID.class, param);
                return Optional.ofNullable(result);
            } catch (EmptyResultDataAccessException ex) {
                return Optional.empty();
            } catch (DataAccessException ex) {
                LOG.debug("Postgres lookup failed for value {}: {}", param, ex.getMessage());
                return Optional.empty();
            }
        }

        private Map<String, Object> parseJsonAttributes(Object value) {
            if (value == null) {
                return Map.of();
            }
            try {
                String json = null;
                if (value instanceof PGobject pgObject && pgObject.getValue() != null) {
                    json = pgObject.getValue();
                } else if (value instanceof String str) {
                    json = str;
                }
                if (json != null && !json.isBlank()) {
                    return objectMapper.readValue(json, MAP_TYPE);
                }
                if (value instanceof Map<?, ?> mapValue) {
                    Map<String, Object> copy = new LinkedHashMap<>();
                    mapValue.forEach((k, v) -> copy.put(String.valueOf(k), v));
                    return copy;
                }
            } catch (Exception ex) {
                LOG.debug("Failed to parse tag metadata: {}", ex.getMessage());
            }
            return Map.of();
        }

        private CoverImageSource toCoverSource(String raw) {
            if (raw == null || raw.isBlank()) {
                return CoverImageSource.UNDEFINED;
            }
            try {
                return CoverImageSource.valueOf(raw.trim().toUpperCase());
            } catch (IllegalArgumentException ex) {
                return CoverImageSource.UNDEFINED;
            }
        }

        private Double toDouble(java.math.BigDecimal value) {
            return value == null ? null : value.doubleValue();
        }

        private record CoverCandidate(String url,
                                      String s3Path,
                                      String source,
                                      Integer width,
                                      Integer height,
                                      Boolean highRes) {
        }
    }

    private boolean looksLikeUuid(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        try {
            UUID.fromString(value);
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

}
