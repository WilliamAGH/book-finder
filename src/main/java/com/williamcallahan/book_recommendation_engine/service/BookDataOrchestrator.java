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
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import com.williamcallahan.book_recommendation_engine.util.ExternalApiLogger;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.IsbnUtils;
import com.williamcallahan.book_recommendation_engine.util.ReactiveErrorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class BookDataOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(BookDataOrchestrator.class);

    private final S3RetryService s3RetryService;
    private final GoogleApiFetcher googleApiFetcher;
    private final ObjectMapper objectMapper;
    private final OpenLibraryBookDataService openLibraryBookDataService;
    // private final LongitoodBookDataService longitoodBookDataService; // Removed
    private final BookDataAggregatorService bookDataAggregatorService;
    private final BookCollectionPersistenceService collectionPersistenceService;
    private final BookSearchService bookSearchService;
    private final BookS3CacheService bookS3CacheService;
    private final PostgresBookRepository postgresBookRepository;
    private final CanonicalBookPersistenceService canonicalBookPersistenceService;
    private final TieredBookSearchService tieredBookSearchService;
    @Autowired(required = false)
    private S3StorageService s3StorageService; // Optional S3 layer
    private static final long SEARCH_VIEW_REFRESH_INTERVAL_MS = 60_000L;
    private final AtomicLong lastSearchViewRefresh = new AtomicLong(0L);
    private final boolean externalFallbackEnabled;

    public BookDataOrchestrator(S3RetryService s3RetryService,
                                GoogleApiFetcher googleApiFetcher,
                                ObjectMapper objectMapper,
                                OpenLibraryBookDataService openLibraryBookDataService,
                                // LongitoodBookDataService longitoodBookDataService, // Removed
                                BookDataAggregatorService bookDataAggregatorService,
                                BookCollectionPersistenceService collectionPersistenceService,
                                BookSearchService bookSearchService,
                                BookS3CacheService bookS3CacheService,
                                @Nullable PostgresBookRepository postgresBookRepository,
                                @Nullable CanonicalBookPersistenceService canonicalBookPersistenceService,
                                @Lazy @Nullable TieredBookSearchService tieredBookSearchService,
                                @Value("${app.features.external-fallback.enabled:${app.features.google-fallback.enabled:true}}") boolean externalFallbackEnabled) {
        this.s3RetryService = s3RetryService;
        this.googleApiFetcher = googleApiFetcher;
        this.objectMapper = objectMapper;
        this.openLibraryBookDataService = openLibraryBookDataService;
        // this.longitoodBookDataService = longitoodBookDataService; // Removed
        this.bookDataAggregatorService = bookDataAggregatorService;
        this.collectionPersistenceService = collectionPersistenceService;
        this.bookSearchService = bookSearchService;
        this.bookS3CacheService = bookS3CacheService;
        this.postgresBookRepository = postgresBookRepository;
        this.canonicalBookPersistenceService = canonicalBookPersistenceService;
        this.tieredBookSearchService = tieredBookSearchService;
        this.externalFallbackEnabled = externalFallbackEnabled;
    }

    public void refreshSearchView() {
        triggerSearchViewRefresh(false);
    }

    public void refreshSearchViewImmediately() {
        triggerSearchViewRefresh(true);
    }

    /**
     * Fetches a canonical book record directly from Postgres without engaging external fallbacks.
     *
     * @param bookId Canonical UUID string for the book
     * @return Optional containing the hydrated {@link Book} when present in Postgres
     */
    public Optional<Book> getBookFromDatabase(String bookId) {
        return findInDatabaseById(bookId);
    }

    /**
     * Retrieves a canonical book from Postgres using its slug.
     *
     * @param slug The slug to resolve
     * @return Optional containing the hydrated {@link Book} when present in Postgres
     */
    public Optional<Book> getBookFromDatabaseBySlug(String slug) {
        return findInDatabaseBySlug(slug);
    }

    // --- Local DB lookup helpers (delegate to PostgresBookRepository when available) ---
    private Optional<Book> findInDatabaseById(String id) {
        return queryDatabase(repo -> repo.fetchByCanonicalId(id));
    }

    private Optional<Book> findInDatabaseBySlug(String slug) {
        return queryDatabase(repo -> repo.fetchBySlug(slug));
    }

    private Optional<Book> findInDatabaseByIsbn13(String isbn13) {
        return queryDatabase(repo -> repo.fetchByIsbn13(isbn13));
    }

    private Optional<Book> findInDatabaseByIsbn10(String isbn10) {
        return queryDatabase(repo -> repo.fetchByIsbn10(isbn10));
    }

    private Optional<Book> findInDatabaseByAnyExternalId(String externalId) {
        return queryDatabase(repo -> repo.fetchByExternalId(externalId));
    }

    private Optional<Book> queryDatabase(Function<PostgresBookRepository, Optional<Book>> resolver) {
        if (postgresBookRepository == null) {
            return Optional.empty();
        }
        Optional<Book> result = resolver.apply(postgresBookRepository);
        return result != null ? result : Optional.empty();
    }

    /**
     * Fetches book data from external APIs when not found in DB or S3.
     * This is a TRUE FALLBACK - only called when Postgres has no data.
     * 
     * Gracefully degrades across multiple API sources:
     * 1. Google Books authenticated (if API key + circuit breaker allows)
     * 2. Google Books unauthenticated (always available as fallback)
     * 3. Google Books ISBN search (for ISBN lookups)
     * 4. OpenLibrary (final fallback for ISBNs)
     * 
     * All API sources are aggregated to provide the most complete data.
     */
    private Mono<Book> fetchFromApisAndAggregate(String bookId) {
        if (!externalFallbackEnabled) {
            logger.debug("External API fallback disabled. Skipping API fetch for {}", bookId);
            return Mono.empty();
        }
        final boolean looksLikeIsbn13 = IsbnUtils.isValidIsbn13(bookId);
        final boolean looksLikeIsbn10 = IsbnUtils.isValidIsbn10(bookId);
        final boolean looksLikeIsbn = looksLikeIsbn13 || looksLikeIsbn10;

        // This will collect JsonNodes from various API sources
        Mono<List<JsonNode>> apiResponsesMono = Mono.defer(() -> {
            Mono<JsonNode> tier4Mono = looksLikeIsbn
                ? Mono.empty() // volume-by-id won't work for ISBN; prefer search path below
                : googleApiFetcher.fetchVolumeByIdAuthenticated(bookId)
                    .doOnSuccess(json -> { if (json != null) logger.info("BookDataOrchestrator: Tier 4 Google Auth HIT for {}", bookId);})
                    .onErrorResume(e -> { LoggingUtils.warn(logger, e, "Tier 4 Google Auth API error for {}", bookId); return Mono.<JsonNode>empty(); });

            Mono<JsonNode> tier3Mono = looksLikeIsbn
                ? Mono.empty()
                : googleApiFetcher.fetchVolumeByIdUnauthenticated(bookId)
                    .doOnSuccess(json -> { if (json != null) logger.info("BookDataOrchestrator: Tier 3 Google Unauth HIT for {}", bookId);})
                    .onErrorResume(e -> { LoggingUtils.warn(logger, e, "Tier 3 Google Unauth API error for {}", bookId); return Mono.<JsonNode>empty(); });

            // Google Books search by ISBN for better coverage when identifier is an ISBN
            Mono<JsonNode> googleIsbnSearchMono = looksLikeIsbn
                ? googleApiFetcher.searchVolumesUnauthenticated("isbn:" + bookId, 0, "relevance", null)
                    .flatMap(resp -> Mono.justOrEmpty(resp != null && resp.has("items") && resp.get("items").isArray() && resp.get("items").size() > 0
                        ? resp.get("items").get(0)
                        : null))
                    .switchIfEmpty(
                        googleApiFetcher.searchVolumesAuthenticated("isbn:" + bookId, 0, "relevance", null)
                            .flatMap(resp -> Mono.justOrEmpty(resp != null && resp.has("items") && resp.get("items").isArray() && resp.get("items").size() > 0
                                ? resp.get("items").get(0)
                                : null))
                    )
                    .doOnSuccess(json -> { if (json != null) logger.info("BookDataOrchestrator: Google ISBN search HIT for {}", bookId);})
                    .onErrorResume(e -> { LoggingUtils.warn(logger, e, "Google ISBN search error for {}", bookId); return Mono.<JsonNode>empty(); })
                : Mono.empty();

            Mono<JsonNode> olMono = looksLikeIsbn
                ? openLibraryBookDataService.fetchBookByIsbn(bookId)
                    .flatMap(book -> {
                        try {
                            logger.info("BookDataOrchestrator: Tier 5 OpenLibrary HIT for {}. Title: {}", bookId, book.getTitle());
                            return Mono.just(objectMapper.valueToTree(book));
                        } catch (IllegalArgumentException e) {
                            LoggingUtils.error(logger, e, "Error converting OpenLibrary Book to JsonNode for {}", bookId);
                            return Mono.<JsonNode>empty();
                        }
                    })
                    .onErrorResume(e -> { LoggingUtils.warn(logger, e, "Tier 5 OpenLibrary API error for {}", bookId); return Mono.<JsonNode>empty(); })
                : Mono.empty();
            
            return Mono.zip(
                    tier4Mono.defaultIfEmpty(objectMapper.createObjectNode()),
                    tier3Mono.defaultIfEmpty(objectMapper.createObjectNode()),
                    googleIsbnSearchMono.defaultIfEmpty(objectMapper.createObjectNode()),
                    olMono.defaultIfEmpty(objectMapper.createObjectNode())
                )
                .map(tuple -> 
                    java.util.stream.Stream.of(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4())
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
            return Mono.fromRunnable(() -> persistBook(finalBook, aggregatedJson, false))
                .subscribeOn(Schedulers.boundedElastic())
                .then(bookS3CacheService.updateCache(finalBook, aggregatedJson, "Aggregated", s3StorageKey));
        });
    }

    public Mono<Book> getBookByIdTiered(String bookId) {
        logger.debug("BookDataOrchestrator: Starting tiered fetch (DB → S3 → APIs) for book ID: {}", bookId);

        // Tier 1: Database (if configured)
        Mono<Book> dbFetchBookMono = Mono.fromCallable(() -> {
            if (postgresBookRepository == null) return null;
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
        Mono<Book> tier2Mono;
        if (externalFallbackEnabled) {
            tier2Mono = Mono.fromCompletionStage(s3RetryService.fetchJsonWithRetry(bookId))
                .flatMap(s3Result -> {
                    if (s3Result.isSuccess() && s3Result.getData().isPresent()) {
                        try {
                            JsonNode s3JsonNode = objectMapper.readTree(s3Result.getData().get());
                            Book bookFromS3 = BookJsonParser.convertJsonToBook(s3JsonNode);
                            if (bookFromS3 != null && bookFromS3.getId() != null) {
                                logger.info("BookDataOrchestrator: Tier 2 S3 HIT for book ID: {}. Title: {}", bookId, bookFromS3.getTitle());
                                // Warm DB for future direct hits
                            persistBook(bookFromS3, s3JsonNode, false);
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
        } else {
            tier2Mono = Mono.empty();
        }

        Mono<Book> tier3Mono = externalFallbackEnabled ? Mono.defer(() -> fetchFromApisAndAggregate(bookId)) : Mono.empty();

        return dbFetchBookMono
            .switchIfEmpty(tier2Mono)
            .switchIfEmpty(tier3Mono)
            .doOnSuccess(book -> {
                if (book != null) {
                    logger.info("BookDataOrchestrator: Successfully processed book for identifier: {} Title: {}", bookId, book.getTitle());
                } else {
                    logger.info("BookDataOrchestrator: Failed to fetch/process book for identifier: {} from any tier.", bookId);
                }
            })
            .onErrorResume(ReactiveErrorUtils.logAndReturnEmpty("BookDataOrchestrator.getBookByIdTiered(" + bookId + ")"));
    }

    public Mono<Book> getBookBySlugTiered(String slug) {
        if (slug == null || slug.isBlank()) {
            return Mono.empty();
        }
        return Mono.fromCallable(() -> findInDatabaseBySlug(slug).map(Book::getId).orElse(null))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(canonicalId -> canonicalId == null ? Mono.empty() : getBookByIdTiered(canonicalId));
    }

    public Mono<Book> fetchCanonicalBookReactive(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            return Mono.empty();
        }

        // Optimized: Single Postgres query checks all possible lookups
        // Prevents cascading fallbacks that could trigger S3/API calls
        return Mono.fromCallable(() -> {
            if (postgresBookRepository == null) {
                return null;
            }
            
            // Try all lookup methods in one go
            Book result = findInDatabaseBySlug(identifier).orElse(null);
            if (result != null) return result;
            
            result = findInDatabaseById(identifier).orElse(null);
            if (result != null) return result;
            
            result = findInDatabaseByIsbn13(identifier).orElse(null);
            if (result != null) return result;
            
            result = findInDatabaseByIsbn10(identifier).orElse(null);
            if (result != null) return result;
            
            return findInDatabaseByAnyExternalId(identifier).orElse(null);
        })
        .subscribeOn(Schedulers.boundedElastic())
        .flatMap(book -> book != null ? Mono.just(book) : Mono.empty())
        .onErrorResume(e -> {
            logger.warn("fetchCanonicalBookReactive failed for {}: {}", identifier, e.getMessage());
            return Mono.empty();
        });
    }

    public Mono<List<Book>> searchBooksTiered(String query, String langCode, int desiredTotalResults, String orderBy) {
        return searchBooksTiered(query, langCode, desiredTotalResults, orderBy, false);
    }
    
    public Mono<List<Book>> searchBooksTiered(String query, String langCode, int desiredTotalResults, String orderBy, boolean bypassExternalApis) {
        return queryTieredSearch(service -> service.searchBooks(query, langCode, desiredTotalResults, orderBy, bypassExternalApis))
            .doOnSuccess(results -> {
                if (!bypassExternalApis) {
                    triggerBackgroundHydration(results, "SEARCH", query);
                }
            });
    }

    public Mono<List<BookSearchService.AuthorResult>> searchAuthors(String query, int desiredTotalResults) {
        return searchAuthors(query, desiredTotalResults, false);
    }

    public Mono<List<BookSearchService.AuthorResult>> searchAuthors(String query,
                                                                    int desiredTotalResults,
                                                                    boolean bypassExternalApis) {
        return queryTieredSearch(service -> service.searchAuthors(query, desiredTotalResults, bypassExternalApis));
    }

    private <T> Mono<List<T>> queryTieredSearch(Function<TieredBookSearchService, Mono<List<T>>> operation) {
        if (tieredBookSearchService == null) {
            return Mono.just(List.<T>of());
        }
        Mono<List<T>> result = operation.apply(tieredBookSearchService);
        return result != null ? result : Mono.just(List.<T>of());
    }

    public Mono<Void> hydrateBooksReactive(List<Book> books, String context, String correlationId) {
        if (books == null || books.isEmpty()) {
            return Mono.empty();
        }
        Set<String> identifiers = books.stream()
            .map(this::determineBestIdentifier)
            .filter(id -> id != null && !id.isBlank())
            .collect(Collectors.toCollection(LinkedHashSet::new));
        if (identifiers.isEmpty()) {
            return Mono.empty();
        }
        return Flux.fromIterable(identifiers)
            .flatMap(id -> hydrateSingleBook(id, context, correlationId), 4)
            .then();
    }

    public void hydrateBooksAsync(List<Book> books, String context, String correlationId) {
        triggerBackgroundHydration(books, context, correlationId);
    }

    private void triggerBackgroundHydration(List<Book> books, String context, String correlationId) {
        if (books == null || books.isEmpty()) {
            return;
        }
        hydrateBooksReactive(books, context, correlationId)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                ignored -> { },
                error -> logger.warn("Background hydration failed for context {}: {}", context, error.getMessage())
            );
    }

    private Mono<Void> hydrateSingleBook(String identifier, String context, String correlationId) {
        if (identifier == null || identifier.isBlank()) {
            return Mono.empty();
        }
        return Mono.defer(() -> {
            ExternalApiLogger.logHydrationStart(logger, context, identifier, correlationId);
            return getBookByIdTiered(identifier)
                .doOnNext(book -> {
                    if (book != null && book.getId() != null) {
                        ExternalApiLogger.logHydrationSuccess(logger, context, identifier, book.getId(), "TIERED_FLOW");
                    }
                })
                .switchIfEmpty(Mono.fromRunnable(() ->
                    ExternalApiLogger.logHydrationFailure(logger, context, identifier, "NOT_FOUND")
                ))
                .onErrorResume(ex -> {
                    ExternalApiLogger.logHydrationFailure(
                        logger,
                        context,
                        identifier,
                        ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName());
                    return Mono.empty();
                })
                .then();
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private String determineBestIdentifier(Book book) {
        if (book == null) {
            return null;
        }
        if (book.getId() != null && !book.getId().isBlank()) {
            return book.getId();
        }
        if (book.getIsbn13() != null && !book.getIsbn13().isBlank()) {
            return book.getIsbn13();
        }
        if (book.getIsbn10() != null && !book.getIsbn10().isBlank()) {
            return book.getIsbn10();
        }
        return null;
    }


    private void triggerSearchViewRefresh(boolean force) {
        if (bookSearchService == null) {
            return;
        }

        long now = System.currentTimeMillis();
        if (!force) {
            long last = lastSearchViewRefresh.get();
            if (last != 0 && now - last < SEARCH_VIEW_REFRESH_INTERVAL_MS) {
                return;
            }
        }

        try {
            bookSearchService.refreshMaterializedView();
            lastSearchViewRefresh.set(now);
        } catch (Exception ex) {
            logger.warn("BookDataOrchestrator: Failed to refresh search materialized view: {}", ex.getMessage());
        }
    }

    // Private methods expected by tests (invoked via ReflectionTestUtils)
    @SuppressWarnings("unused")
    private String resolveCanonicalBookId(Book book, String googleId, String isbn13, String isbn10) {
        if (canonicalBookPersistenceService == null) {
            return null;
        }
        return canonicalBookPersistenceService.resolveCanonicalBookIdForOrchestrator(book, googleId, isbn13, isbn10);
    }

    @SuppressWarnings("unused")
    private void synchronizeEditionRelationships(String bookId, Book book) {
        if (canonicalBookPersistenceService == null) {
            return;
        }
        canonicalBookPersistenceService.synchronizeEditionRelationshipsForOrchestrator(bookId, book);
    }

    @SuppressWarnings("unused")
    private java.util.List<com.fasterxml.jackson.databind.JsonNode> parseBookJsonPayload(String payload, String sourceName) {
        java.util.List<com.fasterxml.jackson.databind.JsonNode> results = new java.util.ArrayList<>();
        if (payload == null || payload.isBlank()) {
            return results;
        }
        java.util.List<String> objects = splitConcatenatedJsonObjects(payload);
        java.util.Map<String, com.fasterxml.jackson.databind.JsonNode> byId = new java.util.LinkedHashMap<>();
        for (String obj : objects) {
            try {
                com.fasterxml.jackson.databind.JsonNode node = objectMapper.readTree(obj);
                com.fasterxml.jackson.databind.JsonNode effective = node;
                // Unwrap rawJsonResponse if present and textual
                if (node.has("rawJsonResponse") && node.get("rawJsonResponse").isTextual()) {
                    String raw = node.get("rawJsonResponse").asText();
                    if (raw != null && !raw.isBlank()) {
                        try {
                            effective = objectMapper.readTree(raw);
                        } catch (Exception ignored) {
                            // Keep original node if raw cannot be parsed
                        }
                    }
                }
                String id = null;
                if (effective.has("id") && effective.get("id").isTextual()) {
                    id = effective.get("id").asText();
                } else if (node.has("id") && node.get("id").isTextual()) {
                    id = node.get("id").asText();
                }
                if (id == null) {
                    id = java.util.UUID.randomUUID().toString();
                }
                // Deduplicate by id (first wins)
                byId.putIfAbsent(id, effective);
            } catch (Exception e) {
                // skip malformed chunk
            }
        }
        results.addAll(byId.values());
        return results;
    }

    private java.util.List<String> splitConcatenatedJsonObjects(String payload) {
        java.util.List<String> parts = new java.util.ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int braceDepth = 0;
        boolean inString = false;
        boolean escape = false;
        for (int i = 0; i < payload.length(); i++) {
            char c = payload.charAt(i);
            sb.append(c);
            if (escape) {
                escape = false;
                continue;
            }
            if (c == '\\') {
                escape = true;
                continue;
            }
            if (c == '"') {
                inString = !inString;
                continue;
            }
            if (inString) continue;
            if (c == '{') braceDepth++;
            else if (c == '}') braceDepth--;
            if (braceDepth == 0 && sb.length() > 0) {
                String part = sb.toString().trim();
                if (!part.isEmpty()) {
                    parts.add(part);
                }
                sb.setLength(0);
            }
        }
        // Fallback: if nothing split, return whole payload
        if (parts.isEmpty() && !payload.isBlank()) {
            parts.add(payload.trim());
        }
        return parts;
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
        buildS3BookMigrationService("S3→DB migration")
            .ifPresent(service -> service.migrateBooksFromS3(prefix, maxRecords, skipRecords));
    }

    public void migrateListsFromS3(String provider, String prefix, int maxRecords, int skipRecords) {
        buildS3BookMigrationService("S3→DB list migration")
            .ifPresent(service -> {
                service.migrateListsFromS3(provider, prefix, maxRecords, skipRecords);
                logger.info("Migration complete. Work clustering will run automatically via WorkClusterScheduler, or manually run: SELECT * FROM cluster_books_by_isbn(); SELECT * FROM cluster_books_by_google_canonical();");
            });
    }

    private Optional<S3BookMigrationService> buildS3BookMigrationService(String contextLabel) {
        if (canonicalBookPersistenceService == null) {
            logger.warn("{} skipped: Database is not configured (CanonicalBookPersistenceService missing).", contextLabel);
            return Optional.empty();
        }
        if (s3StorageService == null) {
            logger.warn("{} skipped: S3 is not configured (S3StorageService is null).", contextLabel);
            return Optional.empty();
        }
        return Optional.of(new S3BookMigrationService(
            s3StorageService,
            objectMapper,
            collectionPersistenceService,
            (book, json) -> persistBook(book, json, true)
        ));
    }

    private void persistBook(Book book, JsonNode sourceJson, boolean enrich) {
        if (canonicalBookPersistenceService == null) {
            return;
        }
        boolean persisted = enrich
            ? canonicalBookPersistenceService.enrichAndSave(book, sourceJson)
            : canonicalBookPersistenceService.saveBook(book, sourceJson);
        if (persisted) {
            triggerSearchViewRefresh(false);
        }
    }

    @SuppressWarnings("unused")
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
