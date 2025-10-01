/**
 * Orchestrates book data retrieval through a tiered fetch strategy
 *
 * @author William Callahan
 * 
 * Features:
 * - Implements multi-tiered data retrieval from cache and APIs
 * - Manages fetching of individual books by ID or search results  
 * - Coordinates between Postgres persistence and Google Books API
 * - Handles persistence of API responses for performance
 * - Supports both authenticated and unauthenticated API usage
 */
package com.williamcallahan.book_recommendation_engine.service;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.ExternalIdentifierType;
import com.williamcallahan.book_recommendation_engine.util.ExternalApiLogger;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.IsbnUtils;
import com.williamcallahan.book_recommendation_engine.util.IdentifierClassifier;
import com.williamcallahan.book_recommendation_engine.util.ReactiveErrorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class BookDataOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(BookDataOrchestrator.class);

    private final GoogleApiFetcher googleApiFetcher;
    private final ObjectMapper objectMapper;
    // private final LongitoodBookDataService longitoodBookDataService; // Removed
    private final BookSearchService bookSearchService;
    private final PostgresBookRepository postgresBookRepository;
    private final TieredBookSearchService tieredBookSearchService;
    private final BookUpsertService bookUpsertService; // SSOT for all book writes
    private final com.williamcallahan.book_recommendation_engine.mapper.GoogleBooksMapper googleBooksMapper; // For Book->BookAggregate mapping
    private static final long SEARCH_VIEW_REFRESH_INTERVAL_MS = 60_000L;
    private final AtomicLong lastSearchViewRefresh = new AtomicLong(0L);
    private final boolean externalFallbackEnabled;

    public BookDataOrchestrator(GoogleApiFetcher googleApiFetcher,
                                ObjectMapper objectMapper,
                                BookSearchService bookSearchService,
                                @Nullable PostgresBookRepository postgresBookRepository,
                                BookUpsertService bookUpsertService,
                                com.williamcallahan.book_recommendation_engine.mapper.GoogleBooksMapper googleBooksMapper,
                                @Lazy @Nullable TieredBookSearchService tieredBookSearchService,
                                @Value("${app.features.external-fallback.enabled:${app.features.google-fallback.enabled:true}}") boolean externalFallbackEnabled) {
        this.googleApiFetcher = googleApiFetcher;
        this.objectMapper = objectMapper;
        this.bookSearchService = bookSearchService;
        this.postgresBookRepository = postgresBookRepository;
        this.bookUpsertService = bookUpsertService;
        this.googleBooksMapper = googleBooksMapper;
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
     * Fetches book data from external APIs when not found in DB.
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
        
        // Classify identifier to determine appropriate API strategy
        final ExternalIdentifierType identifierType = IdentifierClassifier.classify(bookId);
        final boolean looksLikeIsbn13 = IsbnUtils.isValidIsbn13(bookId);
        final boolean looksLikeIsbn10 = IsbnUtils.isValidIsbn10(bookId);
        final boolean looksLikeIsbn = looksLikeIsbn13 || looksLikeIsbn10;
        final boolean safeForVolumesApi = IdentifierClassifier.isSafeForGoogleBooksVolumesApi(bookId);

        // Log identifier classification for diagnostics
        logger.info("BookDataOrchestrator: Identifier '{}' classified as {}. Safe for volumes API: {}", 
            bookId, identifierType, safeForVolumesApi);

        // This will collect JsonNodes from various API sources
        Mono<List<JsonNode>> apiResponsesMono = Mono.defer(() -> {
            // Only use volumes/{id} endpoint for valid Google Books IDs or ISBNs
            // NEVER send slugs to volumes API
            // Sequential policy: try authenticated once, then fall back to unauthenticated
            Mono<JsonNode> tier4Mono;
            Mono<JsonNode> tier3Mono = Mono.empty();
            if (safeForVolumesApi && !looksLikeIsbn) {
                tier4Mono = googleApiFetcher.fetchVolumeByIdAuthenticated(bookId)
                    .switchIfEmpty(googleApiFetcher.fetchVolumeByIdUnauthenticated(bookId))
                    .doOnSuccess(json -> { if (json != null) logger.info("BookDataOrchestrator: Google volumes HIT for {}", bookId);})
                    .onErrorResume(e -> { LoggingUtils.warn(logger, e, "Google volumes API error for {}", bookId); return Mono.<JsonNode>empty(); });
            } else {
                tier4Mono = Mono.empty();
            }

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

            return Flux.merge(tier4Mono, tier3Mono, googleIsbnSearchMono)
                .filter(jsonNode -> jsonNode != null && jsonNode.size() > 0)
                .collectList();
        });

        return apiResponsesMono.flatMap(jsonList -> {
            if (jsonList.isEmpty()) {
                logger.info("BookDataOrchestrator: No data found from any API source for identifier: {}", bookId);
                return Mono.<Book>empty();
            }
            BookAggregate aggregate = jsonList.stream()
                .map(googleBooksMapper::map)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

            if (aggregate == null) {
                logger.warn("BookDataOrchestrator: Unable to map provider payloads to aggregate for identifier: {}", bookId);
                return Mono.empty();
            }

            return Mono.fromCallable(() -> bookUpsertService.upsert(aggregate))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(result -> Mono.fromCallable(() -> findInDatabaseById(result.getBookId().toString()))
                    .subscribeOn(Schedulers.boundedElastic())
                    .flatMap(optional -> optional.map(Mono::just).orElseGet(Mono::empty)))
                .map(book -> {
                    book.setRetrievedFrom("GOOGLE_BOOKS_API");
                    book.setDataSource(googleBooksMapper.getSourceName());
                    book.setInPostgres(true);
                    return book;
                });
        });
    }

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository#fetchBookDetail(java.util.UUID)}
     * together with controller DTO mappers instead of hydrating legacy {@link Book} instances.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<Book> getBookByIdTiered(String bookId) {
        logger.debug("BookDataOrchestrator: Starting tiered fetch (DB → APIs) for book ID: {}", bookId);

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

        Mono<Book> tier3Mono = externalFallbackEnabled ? Mono.defer(() -> fetchFromApisAndAggregate(bookId)) : Mono.empty();

        return dbFetchBookMono
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

    /**
     * @deprecated Resolve slugs directly via
     * {@link com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository#fetchBookDetailBySlug(String)}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<Book> getBookBySlugTiered(String slug) {
        if (slug == null || slug.isBlank()) {
            return Mono.empty();
        }
        return Mono.fromCallable(() -> {
                    Optional<Book> bookOpt = findInDatabaseBySlug(slug);
                    return bookOpt.map(Book::getId).orElse(null);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(canonicalId -> canonicalId == null ? Mono.empty() : getBookByIdTiered(canonicalId));
    }

    public Mono<Book> fetchCanonicalBookReactive(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            return Mono.empty();
        }

        // Optimized: Single Postgres query checks all possible lookups
        // Prevents cascading fallbacks that could trigger API calls unnecessarily
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

    /**
     * @deprecated Retrieve search results through
     * {@link com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository#fetchBookCards(java.util.List)}
     * fed by {@link BookSearchService} IDs instead of returning legacy {@link Book} entities.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<Book>> searchBooksTiered(String query, String langCode, int desiredTotalResults, String orderBy) {
        return searchBooksTiered(query, langCode, desiredTotalResults, orderBy, false);
    }
    
    /**
     * @deprecated Prefer DTO-centric search using {@link com.williamcallahan.book_recommendation_engine.dto.BookCard}
     * projections from {@link com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<Book>> searchBooksTiered(String query, String langCode, int desiredTotalResults, String orderBy, boolean bypassExternalApis) {
        logger.info("[EXTERNAL-API] [SEARCH] searchBooksTiered CALLED: query='{}', bypass={}, desired={}", query, bypassExternalApis, desiredTotalResults);
        return queryTieredSearch(service -> service.searchBooks(query, langCode, desiredTotalResults, orderBy, bypassExternalApis))
            .doOnSuccess(results -> {
                logger.info("[EXTERNAL-API] [SEARCH] searchBooksTiered doOnSuccess TRIGGERED: bypass={}, results={}", 
                    bypassExternalApis, results != null ? results.size() : "null");
                if (!bypassExternalApis && results != null && !results.isEmpty()) {
                    logger.info("[EXTERNAL-API] [SEARCH] CALLING persistBooksAsync with {} books", results.size());
                    // Persist search results opportunistically to Postgres
                    persistBooksAsync(results, "SEARCH");
                } else {
                    logger.warn("[EXTERNAL-API] [SEARCH] NOT calling persistBooksAsync: bypass={}, resultsNull={}, resultsEmpty={}", 
                        bypassExternalApis, results == null, results != null && results.isEmpty());
                }
            });
    }

    /**
     * @deprecated Query authors via {@link BookSearchService#searchAuthors(String, Integer)} and surface
     * results directly instead of routing through tiered legacy hydration.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<BookSearchService.AuthorResult>> searchAuthors(String query, int desiredTotalResults) {
        return searchAuthors(query, desiredTotalResults, false);
    }

    /**
     * @deprecated See {@link #searchAuthors(String, int)}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
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

    /**
     * @deprecated Trigger DTO hydration via background jobs that operate on
     * {@link com.williamcallahan.book_recommendation_engine.controller.dto.BookDto} projections.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<Void> hydrateBooksReactive(List<Book> books, String context, String correlationId) {
        if (books == null || books.isEmpty()) {
            return Mono.empty();
        }
        Set<String> identifiers = books.stream()
            .filter(Objects::nonNull)
            .map(this::collectIdentifiers)
            .flatMap(Set::stream)
            .collect(Collectors.toCollection(LinkedHashSet::new));
        return hydrateIdentifiersReactive(identifiers, context, correlationId);
    }

    /**
     * @deprecated Replace with DTO-based hydration flows that use `BookQueryRepository` lookups.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<Void> hydrateIdentifiersReactive(Collection<String> identifiers, String context, String correlationId) {
        Set<String> normalized = normalizeIdentifiers(identifiers);
        if (normalized.isEmpty()) {
            return Mono.empty();
        }
        return Flux.fromIterable(normalized)
            .flatMap(id -> hydrateSingleIdentifier(id, context, correlationId), 4)
            .then();
    }

    /**
     * @deprecated Use {@link #hydrateBooksReactive(List, String, String)} or DTO-based background tasks.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public void hydrateBooksAsync(List<Book> books, String context, String correlationId) {
        if (books == null || books.isEmpty()) {
            return;
        }
        Set<String> identifiers = books.stream()
            .filter(Objects::nonNull)
            .map(this::collectIdentifiers)
            .flatMap(Set::stream)
            .collect(Collectors.toCollection(LinkedHashSet::new));
        hydrateIdentifiersAsync(identifiers, context, correlationId);
    }

    /**
     * Persists books that were fetched from external APIs during search/recommendations.
     * This ensures opportunistic upsert: books returned from API calls get saved to Postgres.
     * 
     * @param books List of books to persist
     * @param context Context string for logging (e.g., "SEARCH", "RECOMMENDATION")
     */
    public void persistBooksAsync(List<Book> books, String context) {
        if (books == null || books.isEmpty()) {
            logger.info("[EXTERNAL-API] [{}] persistBooksAsync called but books list is null or empty", context);
            return;
        }
        
        logger.info("[EXTERNAL-API] [{}] persistBooksAsync INVOKED with {} books", context, books.size());
        
        Mono.fromRunnable(() -> {
            logger.info("[EXTERNAL-API] [{}] Persisting {} books to Postgres - RUNNABLE EXECUTING", context, books.size());
            long start = System.currentTimeMillis();
            int successCount = 0;
            int failureCount = 0;
            
            for (Book book : books) {
                if (book == null) {
                    logger.warn("[EXTERNAL-API] [{}] Skipping null book in persistence", context);
                    continue;
                }
                if (book.getId() == null) {
                    logger.warn("[EXTERNAL-API] [{}] Skipping book with null ID: title={}", context, book.getTitle());
                    continue;
                }
                
                try {
                    logger.debug("[EXTERNAL-API] [{}] Attempting to persist book: id={}, title={}", context, book.getId(), book.getTitle());
                    ExternalApiLogger.logHydrationStart(logger, context, book.getId(), context);
                    
                    // Convert book to JSON for storage
                    JsonNode bookJson;
                    if (book.getRawJsonResponse() != null && !book.getRawJsonResponse().isBlank()) {
                        bookJson = objectMapper.readTree(book.getRawJsonResponse());
                    } else {
                        bookJson = objectMapper.valueToTree(book);
                    }
                    
                    logger.debug("[EXTERNAL-API] [{}] Calling persistBook for id={}", context, book.getId());
                    // Persist using the same method as individual fetches
                    boolean ok = persistBook(book, bookJson);
                    
                    if (ok) {
                        ExternalApiLogger.logHydrationSuccess(logger, context, book.getId(), book.getId(), "POSTGRES_UPSERT");
                        successCount++;
                    } else {
                        failureCount++;
                        logger.warn("[EXTERNAL-API] [{}] Persist returned false for id={}", context, book.getId());
                    }
                    logger.debug("[EXTERNAL-API] [{}] Successfully persisted book id={}", context, book.getId());
                } catch (Exception ex) {
                    ExternalApiLogger.logHydrationFailure(logger, context, book.getId(), ex.getMessage());
                    failureCount++;
                    logger.error("[EXTERNAL-API] [{}] Failed to persist book {} from {}: {}", context, book.getId(), context, ex.getMessage(), ex);
                }
            }
            
            long elapsed = System.currentTimeMillis() - start;
            logger.info("[EXTERNAL-API] [{}] Persistence complete: {} succeeded, {} failed ({} ms)", 
                context, successCount, failureCount, elapsed);
        })
        .subscribeOn(Schedulers.boundedElastic())
        .doOnSubscribe(sub -> logger.info("[EXTERNAL-API] [{}] Mono subscribed for persistence", context))
        .subscribe(
            ignored -> logger.info("[EXTERNAL-API] [{}] Persistence Mono completed successfully", context),
            error -> logger.error("[EXTERNAL-API] [{}] Background persistence failed: {}", context, error.getMessage(), error)
        );
        
        logger.info("[EXTERNAL-API] [{}] persistBooksAsync setup complete, async execution scheduled", context);
    }

    /**
     * @deprecated Use {@link #hydrateIdentifiersReactive(Collection, String, String)} with DTO projections.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public void hydrateIdentifiersAsync(Collection<String> identifiers, String context, String correlationId) {
        Set<String> normalized = normalizeIdentifiers(identifiers);
        if (normalized.isEmpty()) {
            return;
        }
        hydrateIdentifiersReactive(normalized, context, correlationId)
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                ignored -> { },
                error -> logger.warn("Background hydration failed for context {}: {}", context, error.getMessage())
            );
    }

    private Mono<Void> hydrateSingleIdentifier(String identifier, String context, String correlationId) {
        if (identifier == null || identifier.isBlank()) {
            return Mono.empty();
        }
        return lookupBookWithLogging(identifier, context, correlationId)
            .subscribeOn(Schedulers.boundedElastic())
            .then();
    }

    private void logHydrationSuccess(String context, String identifier, String tier, Book book) {
        if (book == null) {
            return;
        }
        String canonicalId = book.getId();
        if (canonicalId == null || canonicalId.isBlank()) {
            return;
        }
        ExternalApiLogger.logHydrationSuccess(logger, context, identifier, canonicalId, tier);
    }

    private Mono<Book> lookupBookWithLogging(String identifier, String context, String correlationId) {
        return Mono.defer(() -> {
            ExternalApiLogger.logHydrationStart(logger, context, identifier, correlationId);
            return getBookByIdTiered(identifier)
                .doOnNext(book -> logHydrationSuccess(context, identifier, "TIERED_FLOW", book))
                .switchIfEmpty(Mono.defer(() -> getBookBySlugTiered(identifier)
                    .doOnNext(book -> logHydrationSuccess(context, identifier, "SLUG", book))))
                .switchIfEmpty(Mono.defer(() -> {
                    ExternalApiLogger.logHydrationFailure(logger, context, identifier, "NOT_FOUND");
                    return Mono.empty();
                }))
                .onErrorResume(ex -> {
                    ExternalApiLogger.logHydrationFailure(
                        logger,
                        context,
                        identifier,
                        ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName());
                    return Mono.empty();
                });
        });
    }

    private Set<String> collectIdentifiers(Book book) {
        LinkedHashSet<String> identifiers = new LinkedHashSet<>();
        if (book == null) {
            return identifiers;
        }
        addIdentifier(identifiers, book.getId());
        addIdentifier(identifiers, book.getSlug());
        addIdentifier(identifiers, book.getIsbn13());
        addIdentifier(identifiers, book.getIsbn10());
        return identifiers;
    }

    private void addIdentifier(Set<String> identifiers, String value) {
        if (value == null) {
            return;
        }
        String trimmed = value.trim();
        if (!trimmed.isEmpty()) {
            identifiers.add(trimmed);
        }
    }

    private Set<String> normalizeIdentifiers(Collection<String> identifiers) {
        if (identifiers == null || identifiers.isEmpty()) {
            return Set.of();
        }
        return identifiers.stream()
            .filter(Objects::nonNull)
            .map(String::trim)
            .filter(id -> !id.isEmpty())
            .collect(Collectors.toCollection(LinkedHashSet::new));
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

    // Edition relationships are now handled by work_cluster_members table
    // See schema.sql for clustering logic

    /**
     * Persists book to database using BookUpsertService (SSOT for all writes).
     */
    private boolean persistBook(Book book, JsonNode sourceJson) {
        try {
            // Convert Book + JsonNode to BookAggregate using GoogleBooksMapper
            com.williamcallahan.book_recommendation_engine.dto.BookAggregate aggregate = 
                googleBooksMapper.map(sourceJson);
            
            if (aggregate == null) {
                logger.warn("GoogleBooksMapper returned null for book {}", book.getId());
                return false;
            }
            
            // Use SSOT for writes (UPSERT + outbox events)
            bookUpsertService.upsert(aggregate);
            triggerSearchViewRefresh(false);
            logger.debug("Persisted book via BookUpsertService: {}", book.getId());
            return true;
        } catch (Exception e) {
            logger.error("Error persisting via BookUpsertService for book {}: {}",
                book.getId(), e.getMessage(), e);
            return false;
        }
    }

    // Satisfy linter for private helpers referenced by annotations
    @SuppressWarnings("unused")
    private static boolean looksLikeUuid(String value) {
        if (value == null) return false;
        return value.matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    }

    @SuppressWarnings("unused")
    private static com.fasterxml.jackson.databind.JsonNode parseBookJsonPayload(String payload, String fallbackId) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            try (com.fasterxml.jackson.core.JsonParser parser = mapper.createParser(payload)) {
                com.fasterxml.jackson.databind.JsonNode node = mapper.readTree(parser);
                if (parser.nextToken() != null) {
                    return null;
                }
                return node;
            }
        } catch (Exception e) {
            return null;
        }
    }
}
