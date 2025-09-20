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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import com.williamcallahan.book_recommendation_engine.util.IdGenerator;
import com.williamcallahan.book_recommendation_engine.util.SlugGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import software.amazon.awssdk.services.s3.model.S3Object;
import java.sql.Date;
import java.util.UUID;

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
    @Autowired(required = false)
    private JdbcTemplate jdbcTemplate; // Optional DB layer
    @Autowired(required = false)
    private S3StorageService s3StorageService; // Optional S3 layer
    private TransactionTemplate transactionTemplate;

    public BookDataOrchestrator(S3RetryService s3RetryService,
                                GoogleApiFetcher googleApiFetcher,
                                ObjectMapper objectMapper,
                                OpenLibraryBookDataService openLibraryBookDataService,
                                // LongitoodBookDataService longitoodBookDataService, // Removed
                                BookDataAggregatorService bookDataAggregatorService,
                                BookSupplementalPersistenceService supplementalPersistenceService,
                                BookCollectionPersistenceService collectionPersistenceService) {
        this.s3RetryService = s3RetryService;
        this.googleApiFetcher = googleApiFetcher;
        this.objectMapper = objectMapper;
        this.openLibraryBookDataService = openLibraryBookDataService;
        // this.longitoodBookDataService = longitoodBookDataService; // Removed
        this.bookDataAggregatorService = bookDataAggregatorService;
        this.supplementalPersistenceService = supplementalPersistenceService;
        this.collectionPersistenceService = collectionPersistenceService;
    }

    @Autowired(required = false)
    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        if (transactionManager != null) {
            this.transactionTemplate = new TransactionTemplate(transactionManager);
        }
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

        final String effectivePrefix = (prefix != null && !prefix.trim().isEmpty()) ? prefix.trim() : "books/v1/";
        logger.info("Starting S3→DB migration: prefix='{}', max={}, skip={}", effectivePrefix, maxRecords, skipRecords);

        List<S3Object> objects = s3StorageService.listObjects(effectivePrefix);
        if (objects == null || objects.isEmpty()) {
            logger.info("S3→DB migration: No objects found under prefix '{}'. Nothing to do.", effectivePrefix);
            return;
        }

        // Filter for JSON objects and apply skip
        List<S3Object> jsonObjects = new ArrayList<>();
        for (S3Object obj : objects) {
            if (obj == null || obj.key() == null) continue;
            String key = obj.key();
            if (key.endsWith(".json")) {
                jsonObjects.add(obj);
            }
        }

        if (skipRecords > 0 && skipRecords < jsonObjects.size()) {
            jsonObjects = jsonObjects.subList(skipRecords, jsonObjects.size());
        } else if (skipRecords >= jsonObjects.size()) {
            logger.info("S3→DB migration: skip={} >= total JSON objects ({}). Nothing to do.", skipRecords, jsonObjects.size());
            return;
        }

        int totalToProcess = (maxRecords > 0) ? Math.min(maxRecords, jsonObjects.size()) : jsonObjects.size();
        logger.info("S3→DB migration: {} JSON object(s) to process after filtering.", totalToProcess);

        AtomicInteger processed = new AtomicInteger(0);
        for (int i = 0; i < totalToProcess; i++) {
            S3Object obj = jsonObjects.get(i);
            String key = obj.key();
            try {
                byte[] raw = s3StorageService.downloadFileAsBytes(key);
                if (raw == null || raw.length == 0) {
                    logger.debug("S3→DB migration: Empty or missing content for key {}. Skipping.", key);
                    continue;
                }

                String json = tryDecompressAsGzipOrUtf8(raw);
                if (json == null || json.isEmpty()) {
                    logger.debug("S3→DB migration: Failed to read JSON for key {}. Skipping.", key);
                    continue;
                }

                JsonNode jsonNode = objectMapper.readTree(json);
                Book book = BookJsonParser.convertJsonToBook(jsonNode);
                if (book == null || book.getId() == null) {
                    logger.debug("S3→DB migration: Parsed null/invalid book for key {}. Skipping.", key);
                    continue;
                }

                // Enrich on matches, then upsert by canonical id
                saveToDatabaseEnrichOnMatch(book, jsonNode);
                int count = processed.incrementAndGet();
                if (count % 50 == 0 || count == totalToProcess) {
                    logger.info("S3→DB migration progress: {}/{} processed.", count, totalToProcess);
                }
            } catch (Exception e) {
                logger.warn("S3→DB migration: Error processing key {}: {}", key, e.getMessage());
            }
        }

        logger.info("S3→DB migration completed. Processed {} record(s).", processed.get());
    }

    private String tryDecompressAsGzipOrUtf8(byte[] raw) {
        // Attempt GZIP first, fall back to UTF-8 plain text
        try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(raw));
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }
            return baos.toString(StandardCharsets.UTF_8.name());
        } catch (IOException ignored) {
            // Not GZIP or failed to decompress; attempt plain UTF-8 string
            try {
                return new String(raw, StandardCharsets.UTF_8);
            } catch (Exception e) {
                return null;
            }
        }
    }

    private void saveToDatabaseEnrichOnMatch(Book incoming, JsonNode sourceJson) {
        if (jdbcTemplate == null || incoming == null) return;

        try {
            String incomingId = incoming.getId();
            String isbn13 = incoming.getIsbn13();
            String isbn10 = incoming.getIsbn10();
            String externalGoogleId = (sourceJson != null) ? sourceJson.path("id").asText(null) : null;

            String canonicalId = null;
            // Prefer exact id match
            if (incomingId != null) {
                canonicalId = findInDatabaseById(incomingId).map(Book::getId).orElse(null);
            }
            // Then ISBN-13
            if (canonicalId == null && isbn13 != null) {
                canonicalId = findInDatabaseByIsbn13(isbn13).map(Book::getId).orElse(null);
            }
            // Then ISBN-10
            if (canonicalId == null && isbn10 != null) {
                canonicalId = findInDatabaseByIsbn10(isbn10).map(Book::getId).orElse(null);
            }
            // Finally any external id mapping (e.g., GOOGLE_BOOKS, NYT)
            if (canonicalId == null && externalGoogleId != null) {
                canonicalId = findInDatabaseByAnyExternalId(externalGoogleId).map(Book::getId).orElse(null);
            }

            if (canonicalId == null) {
                // No existing record found; use incoming id if present, otherwise generate
                canonicalId = (incomingId != null && !incomingId.isEmpty()) ? incomingId : IdGenerator.uuidV7();
            }

            incoming.setId(canonicalId);
            // Delegate to existing upsert logic (ON CONFLICT (id) DO UPDATE)
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
        final String effectivePrefix = (prefix != null && !prefix.trim().isEmpty()) ? prefix.trim() : "lists/" + (provider != null ? provider.toLowerCase() : "") + "/";
        logger.info("Starting S3→DB list migration: provider='{}', prefix='{}', max={}, skip={}", provider, effectivePrefix, maxRecords, skipRecords);

        List<S3Object> objects = s3StorageService.listObjects(effectivePrefix);
        if (objects == null || objects.isEmpty()) {
            logger.info("S3→DB list migration: No objects found under prefix '{}'.", effectivePrefix);
            return;
        }

        // Filter for list JSON files
        List<S3Object> jsonObjects = new ArrayList<>();
        for (S3Object obj : objects) {
            if (obj == null || obj.key() == null) continue;
            String key = obj.key();
            if (key.endsWith(".json")) {
                jsonObjects.add(obj);
            }
        }

        if (skipRecords > 0 && skipRecords < jsonObjects.size()) {
            jsonObjects = jsonObjects.subList(skipRecords, jsonObjects.size());
        } else if (skipRecords >= jsonObjects.size()) {
            logger.info("S3→DB list migration: skip={} >= total JSON objects ({}). Nothing to do.", skipRecords, jsonObjects.size());
            return;
        }

        int totalToProcess = (maxRecords > 0) ? Math.min(maxRecords, jsonObjects.size()) : jsonObjects.size();
        logger.info("S3→DB list migration: {} JSON object(s) to process.", totalToProcess);

        AtomicInteger processed = new AtomicInteger(0);
        for (int i = 0; i < totalToProcess; i++) {
            S3Object obj = jsonObjects.get(i);
            String key = obj.key();
            try {
                byte[] raw = s3StorageService.downloadFileAsBytes(key);
                if (raw == null || raw.length == 0) continue;
                String json = tryDecompressAsGzipOrUtf8(raw);
                if (json == null || json.isEmpty()) continue;

                JsonNode root = objectMapper.readTree(json);
                if (!(root instanceof com.fasterxml.jackson.databind.node.ObjectNode)) continue;
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
                    continue;
                }

                String listId = collectionPersistenceService
                    .upsertList(provider, listNameEncoded, publishedDate, displayName, bestsellersDate, updatedFrequency, null, listJson)
                    .orElse(null);
                if (listId == null) continue;

                JsonNode booksArray = listJson.path("books");
                if (booksArray != null && booksArray.isArray()) {
                    for (JsonNode item : booksArray) {
                        Integer rank = item.path("rank").isNumber() ? item.path("rank").asInt() : null;
                        Integer weeksOnList = item.path("weeks_on_list").isNumber() ? item.path("weeks_on_list").asInt() : null;
                        String isbn13 = item.path("primary_isbn13").asText(null);
                        String isbn10 = item.path("primary_isbn10").asText(null);
                        String providerRef = item.path("amazon_product_url").asText(null);

                        // Resolve canonical book id (reuse DB finders)
                        String canonicalId = null;
                        if (isbn13 != null) canonicalId = findInDatabaseByIsbn13(isbn13).map(Book::getId).orElse(null);
                        if (canonicalId == null && isbn10 != null) canonicalId = findInDatabaseByIsbn10(isbn10).map(Book::getId).orElse(null);
                        // Also try any mapping
                        if (canonicalId == null && isbn13 != null) canonicalId = findInDatabaseByAnyExternalId(isbn13).map(Book::getId).orElse(null);
                        if (canonicalId == null && isbn10 != null) canonicalId = findInDatabaseByAnyExternalId(isbn10).map(Book::getId).orElse(null);

                        // If still not found, try to upsert a minimal record from item if we have enough data
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

                int count = processed.incrementAndGet();
                if (count % 20 == 0 || count == totalToProcess) {
                    logger.info("S3→DB list migration progress: {}/{} processed.", count, totalToProcess);
                }
            } catch (Exception e) {
                logger.warn("S3→DB list migration: Error processing key {}: {}", key, e.getMessage());
            }
        }

        logger.info("S3→DB list migration completed. Processed {} list file(s).", processed.get());
    }
    private Optional<Book> findInDatabaseById(String id) {
        if (jdbcTemplate == null || id == null) return Optional.empty();
        try {
            return jdbcTemplate.query(
                "SELECT id, title, description, s3_image_path, isbn10, isbn13, published_date, language, publisher, page_count FROM books WHERE id = ?",
                ps -> ps.setString(1, id),
                rs -> rs.next() ? Optional.of(mapRowToBook(rs)) : Optional.empty()
            );
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private Optional<Book> findInDatabaseBySlug(String slug) {
        if (jdbcTemplate == null || slug == null) {
            return Optional.empty();
        }
        try {
            return jdbcTemplate.query(
                    "SELECT id, title, description, s3_image_path, isbn10, isbn13, published_date, language, publisher, page_count FROM books WHERE slug = ?",
                    ps -> ps.setString(1, slug),
                    rs -> rs.next() ? Optional.of(mapRowToBook(rs)) : Optional.empty()
            );
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private Optional<Book> findInDatabaseByIsbn13(String isbn13) {
        if (jdbcTemplate == null || isbn13 == null) return Optional.empty();
        try {
            return jdbcTemplate.query(
                "SELECT id, title, description, s3_image_path, isbn10, isbn13, published_date, language, publisher, page_count FROM books WHERE isbn13 = ?",
                ps -> ps.setString(1, isbn13),
                rs -> rs.next() ? Optional.of(mapRowToBook(rs)) : Optional.empty()
            );
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private Optional<Book> findInDatabaseByIsbn10(String isbn10) {
        if (jdbcTemplate == null || isbn10 == null) return Optional.empty();
        try {
            return jdbcTemplate.query(
                "SELECT id, title, description, s3_image_path, isbn10, isbn13, published_date, language, publisher, page_count FROM books WHERE isbn10 = ?",
                ps -> ps.setString(1, isbn10),
                rs -> rs.next() ? Optional.of(mapRowToBook(rs)) : Optional.empty()
            );
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private Optional<Book> findInDatabaseByAnyExternalId(String externalId) {
        if (jdbcTemplate == null || externalId == null) return Optional.empty();
        try {
            return jdbcTemplate.query(
                "SELECT b.id, b.title, b.description, b.s3_image_path, b.isbn10, b.isbn13, b.published_date, b.language, b.publisher, b.page_count " +
                "FROM book_external_ids e JOIN books b ON b.id = e.book_id WHERE e.external_id = ? LIMIT 1",
                ps -> ps.setString(1, externalId),
                rs -> rs.next() ? Optional.of(mapRowToBook(rs)) : Optional.empty()
            );
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private Book mapRowToBook(ResultSet rs) throws java.sql.SQLException {
        Book b = new Book();
        b.setId(rs.getString("id"));
        b.setTitle(rs.getString("title"));
        b.setDescription(rs.getString("description"));
        b.setS3ImagePath(rs.getString("s3_image_path"));
        b.setIsbn10(rs.getString("isbn10"));
        b.setIsbn13(rs.getString("isbn13"));
        java.sql.Date d = rs.getDate("published_date");
        if (d != null) {
            b.setPublishedDate(new java.util.Date(d.getTime()));
        }
        b.setLanguage(rs.getString("language"));
        b.setPublisher(rs.getString("publisher"));
        Integer pageCount = (Integer) rs.getObject("page_count");
        b.setPageCount(pageCount);
        return b;
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
        String isbn13 = sanitizeIsbn(book.getIsbn13());
        String isbn10 = sanitizeIsbn(book.getIsbn10());

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
        try {
            List<String> ids = jdbcTemplate.query(sql, params, (rs, rowNum) -> rs.getString(1));
            return ids.isEmpty() ? null : ids.get(0);
        } catch (DataAccessException ex) {
            logger.debug("Query failed: {}", ex.getMessage());
            return null;
        }
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
            return jdbcTemplate.queryForObject("SELECT ensure_unique_slug(?)", new Object[]{desired}, String.class);
        } catch (DataAccessException ex) {
            return desired;
        }
    }

    private void upsertBookRecord(Book book, String slug) {
        java.util.Date published = book.getPublishedDate();
        Date sqlDate = published != null ? new Date(published.getTime()) : null;
        Integer editionNumber = book.getEditionNumber();
        String editionGroupKey = book.getEditionGroupKey();

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
            book.getIsbn10(),
            book.getIsbn13(),
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
                number = Math.max(editionNumber, 1);
            }
            normalized.add(new EditionLinkRecord(record.id(), Math.max(number, 1)));
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

    private String sanitizeIsbn(String raw) {
        if (raw == null) {
            return null;
        }
        String cleaned = raw.replaceAll("[^0-9Xx]", "").toUpperCase();
        return cleaned.isBlank() ? null : cleaned;
    }
}
