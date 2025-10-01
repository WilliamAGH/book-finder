package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.mapper.GoogleBooksMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.util.BookDomainMapper;
import com.williamcallahan.book_recommendation_engine.util.ExternalApiLogger;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import com.williamcallahan.book_recommendation_engine.util.ReactiveErrorUtils;
import com.williamcallahan.book_recommendation_engine.util.SlugGenerator;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import com.williamcallahan.book_recommendation_engine.util.SearchQueryQualifierExtractor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.lang.Nullable;
import com.williamcallahan.book_recommendation_engine.service.event.SearchResultsUpdatedEvent;
import com.williamcallahan.book_recommendation_engine.service.event.SearchProgressEvent;
import com.williamcallahan.book_recommendation_engine.util.SearchQueryUtils;
import java.time.temporal.ChronoUnit;

/**
 * Extracted tiered search orchestration from {@link BookDataOrchestrator}. Handles DB-first search
 * with Google and OpenLibrary fallbacks while keeping the orchestrator slim.
 * 
 * ARCHITECTURE:
 * 1. PRIMARY: Postgres search (always tried first, never replaced)
 * 2. SUPPLEMENT: If Postgres returns insufficient results, external APIs supplement via server-side streaming
 * 3. GRACEFUL DEGRADATION: Authenticated → Unauthenticated → OpenLibrary fallbacks
 * 
 * Circuit breaker protects authenticated calls but allows unauthenticated fallbacks to continue.
 * Uses BookQueryRepository as THE SINGLE SOURCE OF TRUTH for optimized queries.
 */
@Component
@ConditionalOnBean(BookSearchService.class)
public class TieredBookSearchService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TieredBookSearchService.class);
    private static final String EXTERNAL_AUTHOR_ID_PREFIX = "external:author:";

    private final BookSearchService bookSearchService;
    private final GoogleApiFetcher googleApiFetcher;
    private final OpenLibraryBookDataService openLibraryBookDataService;
    private final BookQueryRepository bookQueryRepository;
    private final boolean externalFallbackEnabled;
    private final @Nullable ApplicationEventPublisher eventPublisher;
    private final GoogleBooksMapper googleBooksMapper;
    public TieredBookSearchService(BookSearchService bookSearchService,
                            GoogleApiFetcher googleApiFetcher,
                            OpenLibraryBookDataService openLibraryBookDataService,
                            GoogleBooksMapper googleBooksMapper,
                            @Nullable BookQueryRepository bookQueryRepository,
                            @Value("${app.features.external-fallback.enabled:${app.features.google-fallback.enabled:true}}") boolean externalFallbackEnabled,
                            @Nullable ApplicationEventPublisher eventPublisher) {
        this.bookSearchService = bookSearchService;
        this.googleApiFetcher = googleApiFetcher;
        this.openLibraryBookDataService = openLibraryBookDataService;
        this.googleBooksMapper = googleBooksMapper;
        this.bookQueryRepository = bookQueryRepository;
        this.externalFallbackEnabled = externalFallbackEnabled;
        this.eventPublisher = eventPublisher;
    }

    /**
     * @deprecated Redirect callers to repository-backed DTO searches instead of
     * legacy {@link Book} projections. Prefer {@link BookQueryRepository#fetchBookCards(List)} or
     * {@link BookQueryRepository#searchCards(String, String, int, String)} depending on context.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    Mono<List<Book>> searchBooks(String query, String langCode, int desiredTotalResults, String orderBy) {
        return searchBooks(query, langCode, desiredTotalResults, orderBy, false);
    }

    /**
     * @deprecated Prefer repository-backed DTO projections (e.g. {@link com.williamcallahan.book_recommendation_engine.dto.BookCard})
     * combined with explicit fallbacks instead of legacy tiered {@link Book} entities.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    Mono<List<Book>> searchBooks(String query, String langCode, int desiredTotalResults, String orderBy, boolean bypassExternalApis) {
        return streamSearch(query, langCode, desiredTotalResults, orderBy, bypassExternalApis)
            .collectList();
    }

    /**
     * @deprecated Stream DTOs via repository-backed queries rather than legacy {@link Book} flows.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    Flux<Book> streamSearch(String query,
                             String langCode,
                             int desiredTotalResults,
                             String orderBy,
                             boolean bypassExternalApis) {
        LOGGER.debug("TieredBookSearch: Starting stream for query='{}', lang={}, total={}, order={}, bypassExternal={}",
            query, langCode, desiredTotalResults, orderBy, bypassExternalApis);

        final String queryHash = computeQueryHash(query);
        // Notify start
        safePublish(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.STARTING, "Starting search", queryHash));
        return searchPostgresFirstReactive(query, desiredTotalResults)
            .onErrorResume(postgresError -> {
                String message = postgresError != null && postgresError.getMessage() != null
                    ? postgresError.getMessage()
                    : String.valueOf(postgresError);
                LOGGER.warn("TieredBookSearch: Postgres search failed for query '{}': {}", query, message, postgresError);
                if (!externalFallbackEnabled || bypassExternalApis) {
                    LOGGER.error("TieredBookSearch: No external fallback allowed for '{}' after Postgres failure; streaming empty results.", query);
                    safePublish(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.ERROR, message, queryHash));
                    return Mono.just(List.<Book>of());
                }
                ExternalApiLogger.logTieredSearchStart(LOGGER, query, 0, desiredTotalResults, desiredTotalResults);
                return Mono.just(List.<Book>of());
            })
            .flatMapMany(postgresHits -> {
                List<Book> baseline = postgresHits == null ? List.of() : postgresHits;
                Flux<Book> postgresFlux = Flux.fromIterable(baseline);

                if (!baseline.isEmpty()) {
                    // Always publish baseline results so WebSocket subscribers render immediately
                    safePublish(new SearchResultsUpdatedEvent(query, baseline, "POSTGRES", baseline.size(), queryHash, false));
                }

                if (!externalFallbackEnabled || bypassExternalApis) {
                    if (bypassExternalApis) {
                        LOGGER.info("TieredBookSearch: External APIs bypassed for '{}' — streaming {} Postgres result(s) only.", query, baseline.size());
                    } else {
                        LOGGER.info("TieredBookSearch: External fallback disabled; streaming {} Postgres result(s) for '{}'", baseline.size(), query);
                    }
                    safePublish(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.COMPLETE, "Search complete (Postgres only)", queryHash));
                    return postgresFlux.take(desiredTotalResults);
                }

                boolean satisfied = !baseline.isEmpty() && baseline.size() >= desiredTotalResults;
                if (satisfied) {
                    LOGGER.info("TieredBookSearch: Postgres fully satisfied '{}' with {} result(s); external fallback skipped.", query, baseline.size());
                    if (!baseline.isEmpty()) {
                        safePublish(new SearchResultsUpdatedEvent(query, baseline, "POSTGRES", baseline.size(), queryHash, true));
                    }
                    safePublish(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.COMPLETE, "Search complete (satisfied by Postgres)", queryHash));
                    return postgresFlux.take(desiredTotalResults);
                }

                int missing = Math.max(desiredTotalResults - baseline.size(), 0);
                int externalTarget = baseline.isEmpty() ? desiredTotalResults : missing;
                ExternalApiLogger.logTieredSearchStart(LOGGER, query, baseline.size(), desiredTotalResults, externalTarget);

                AtomicBoolean externalErrored = new AtomicBoolean(false);
                AtomicBoolean externalProducedResults = new AtomicBoolean(false);

                Flux<Book> externalFlux = performExternalSearchStream(query, langCode, externalTarget, orderBy, baseline.isEmpty())
                    .onErrorResume(error -> {
                        externalErrored.set(true);
                        String message = error != null && error.getMessage() != null ? error.getMessage() : String.valueOf(error);
                        LOGGER.warn("TieredBookSearch: External fallback failed for '{}': {}", query, message);
                        safePublish(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.ERROR, message, queryHash));
                        return Flux.empty();
                    })
                    .doOnNext(book -> externalProducedResults.set(true));

                // Track totals for event payloads
                AtomicInteger totalSoFar = new AtomicInteger(baseline.size());

                // Buffer external results and publish events + persist
                Flux<Book> instrumentedExternal = externalFlux
                    .bufferTimeout(5, Duration.of(400, ChronoUnit.MILLIS))
                    .doOnNext(batch -> {
                        if (batch == null || batch.isEmpty()) return;
                        int newTotal = totalSoFar.addAndGet(batch.size());
                        safePublish(new SearchResultsUpdatedEvent(query, batch, "GOOGLE_OR_OL", newTotal, queryHash, false));
                    })
                    .flatMap(Flux::fromIterable)
                    .doOnComplete(() -> {
                        if (externalErrored.get()) {
                            return; // Error event already published
                        }
                        String message = externalProducedResults.get()
                            ? "External supplementation complete"
                            : "External supplementation finished (no additional matches)";
                        safePublish(new SearchProgressEvent(query, SearchProgressEvent.SearchStatus.COMPLETE, message, queryHash));
                    });

                // CRITICAL: Use Flux.concat to ensure Postgres results are emitted FIRST,
                // then external results fill gaps. This guarantees Postgres-first ordering.
                return Flux.concat(postgresFlux, instrumentedExternal)
                    .take(desiredTotalResults);
            })
            .doOnComplete(() -> LOGGER.debug("TieredBookSearch: Completed stream for '{}'", query));
    }
    
    private Flux<Book> performExternalSearchStream(String query,
                                                   String langCode,
                                                   int desiredTotalResults,
                                                   String orderBy,
                                                   boolean postgresWasEmpty) {

        if (desiredTotalResults <= 0) {
            return Flux.empty();
        }

        final Map<String, Object> queryQualifiers = SearchQueryQualifierExtractor.extract(query);
        boolean googleFallbackEnabled = googleApiFetcher.isGoogleFallbackEnabled();
        boolean shouldTryAuthorSearch = postgresWasEmpty && looksLikeAuthorName(query);

        Flux<Book> openLibraryFlux = openLibraryBookDataService
            .searchBooks(query, shouldTryAuthorSearch)
            .map(book -> {
                if (!queryQualifiers.isEmpty()) {
                    queryQualifiers.forEach(book::addQualifier);
                }
                return book;
            })
            .onErrorResume(ReactiveErrorUtils.logAndReturnEmptyFlux("TieredBookSearchService.openLibrarySearch(" + query + ")"));

        if (!googleFallbackEnabled) {
            LOGGER.info("TieredBookSearch: Google fallback disabled for '{}'; using OpenLibrary only.", query);
            ExternalApiLogger.logFallbackDisabled(LOGGER, "GoogleBooks", query);
            return openLibraryFlux.take(desiredTotalResults);
        }

        String effectiveQuery = shouldTryAuthorSearch ? "inauthor:" + query : query;
        if (shouldTryAuthorSearch) {
            LOGGER.info("TieredBookSearch: '{}' detected as author query. Using inauthor qualifier for Google.", query);
        }

        ExternalApiLogger.logApiCallAttempt(LOGGER, "GoogleBooks", "STREAM_SEARCH", effectiveQuery, googleApiFetcher.isApiKeyAvailable());

        Flux<Book> googleFlux = buildGoogleStream(effectiveQuery, langCode, desiredTotalResults, orderBy, queryQualifiers)
            .onErrorResume(err -> {
                LOGGER.warn("TieredBookSearch: Google Books stream failed for '{}': {}", query, err.getMessage());
                ExternalApiLogger.logApiCallFailure(LOGGER, "GoogleBooks", "STREAM_SEARCH", effectiveQuery, err.getMessage());
                return Flux.empty();
            });

        AtomicInteger emittedCount = new AtomicInteger(0);

        Flux<Book> combined = Flux.merge(googleFlux, openLibraryFlux);

        return combined
            .distinct(Book::getId)
            .take(desiredTotalResults)
            .doOnNext(book -> emittedCount.incrementAndGet())
            .doOnComplete(() -> ExternalApiLogger.logApiCallSuccess(
                LOGGER,
                "GoogleBooks",
                "STREAM_SEARCH",
                effectiveQuery,
                emittedCount.get()));
    }

    private Flux<Book> buildGoogleStream(String query,
                                         String langCode,
                                         int desiredTotalResults,
                                         String orderBy,
                                         Map<String, Object> queryQualifiers) {
        final int maxTotalResultsToFetch = desiredTotalResults > 0 ? desiredTotalResults : 200;
        final String effectiveOrderBy = (orderBy != null && !orderBy.trim().isEmpty()) ? orderBy : "newest";
        final boolean apiKeyAvailable = googleApiFetcher.isApiKeyAvailable();

        Flux<JsonNode> jsonFlux;
        
        if (apiKeyAvailable) {
            // SEQUENTIAL fallback: try authenticated FIRST, only fall back to unauth on failure
            jsonFlux = googleApiFetcher.streamSearchItems(query, maxTotalResultsToFetch, effectiveOrderBy, langCode, true)
                .doOnSubscribe(sub -> ExternalApiLogger.logApiCallAttempt(
                    LOGGER,
                    "GoogleBooks",
                    "STREAM_AUTH",
                    query,
                    true))
                .onErrorResume(authErr -> {
                    String message = authErr != null ? authErr.getMessage() : "unknown error";
                    LOGGER.warn("TieredBookSearch: Authenticated Google Books failed for '{}': {}. Falling back to unauthenticated.", 
                        query, message);
                    ExternalApiLogger.logApiCallFailure(
                        LOGGER,
                        "GoogleBooks",
                        "STREAM_AUTH",
                        query,
                        message);
                    
                    // Fall back to unauthenticated on ANY auth error (401, 403, 429, 5xx)
                    return googleApiFetcher.streamSearchItems(query, maxTotalResultsToFetch, effectiveOrderBy, langCode, false)
                        .doOnSubscribe(sub -> {
                            LOGGER.info("TieredBookSearch: Attempting unauthenticated Google Books for '{}' after auth failure", query);
                            ExternalApiLogger.logApiCallAttempt(
                                LOGGER,
                                "GoogleBooks",
                                "STREAM_UNAUTH_FALLBACK",
                                query,
                                false);
                        })
                        .onErrorResume(unauthErr -> {
                            String unauthMessage = unauthErr != null ? unauthErr.getMessage() : "unknown error";
                            LOGGER.warn("TieredBookSearch: Unauthenticated fallback also failed for '{}': {}", query, unauthMessage);
                            ExternalApiLogger.logApiCallFailure(
                                LOGGER,
                                "GoogleBooks",
                                "STREAM_UNAUTH_FALLBACK",
                                query,
                                unauthMessage);
                            return Flux.empty();
                        });
                });
        } else {
            // No API key, go straight to unauthenticated
            jsonFlux = googleApiFetcher.streamSearchItems(query, maxTotalResultsToFetch, effectiveOrderBy, langCode, false)
                .doOnSubscribe(sub -> ExternalApiLogger.logApiCallAttempt(
                    LOGGER,
                    "GoogleBooks",
                    "STREAM_UNAUTH",
                    query,
                    false))
                .onErrorResume(err -> {
                    String message = err != null ? err.getMessage() : "unknown error";
                    ExternalApiLogger.logApiCallFailure(
                        LOGGER,
                        "GoogleBooks",
                        "STREAM_UNAUTH",
                        query,
                        message);
                    return Flux.empty();
                });
        }

        return jsonFlux
            .map(this::convertJsonToBook)
            .filter(Objects::nonNull)
            .map(book -> {
                if (!queryQualifiers.isEmpty()) {
                    queryQualifiers.forEach(book::addQualifier);
                }
                return book;
            });
    }

    private Book convertJsonToBook(JsonNode node) {
        if (node == null) {
            return null;
        }

        try {
            BookAggregate aggregate = googleBooksMapper.map(node);
            Book book = BookDomainMapper.fromAggregate(aggregate);
            if (book != null) {
                book.setRawJsonResponse(node.toString());
            }
            return book;
        } catch (Exception ex) {
            LOGGER.debug("Failed to map Google Books JSON node: {}", ex.getMessage());
            return null;
        }
    }

    /**
     * Heuristic to detect if a query looks like an author name.
     * Author names typically:
     * - Contain 2-4 words (first/middle/last names)
     * - Start with capital letters
     * - Don't contain special search operators or common book-related words
     * - May contain "and" or "&" for co-authors
     */
    private boolean looksLikeAuthorName(String query) {
        if (query == null || query.isBlank()) {
            return false;
        }
        
        String normalized = query.trim();
        
        // Skip if it contains search operators or qualifiers
        if (normalized.contains("intitle:") || normalized.contains("inauthor:") || 
            normalized.contains("isbn:") || normalized.contains("subject:") ||
            normalized.contains("publisher:")) {
            return false;
        }
        
        // Remove common co-author separators for word count
        String withoutConjunctions = normalized.replaceAll("\\s+and\\s+", " ")
                                               .replaceAll("\\s*&\\s*", " ")
                                               .replaceAll("\\s+", " ")
                                               .trim();
        
        // Count words (author names typically have 2-6 words including co-authors)
        String[] words = withoutConjunctions.split("\\s+");
        if (words.length < 2 || words.length > 6) {
            return false;
        }
        
        // Check if words start with capital letters (typical for names)
        int capitalizedWords = 0;
        for (String word : words) {
            if (!word.isEmpty() && Character.isUpperCase(word.charAt(0))) {
                capitalizedWords++;
            }
        }
        
        // At least half the words should be capitalized
        return capitalizedWords >= (words.length / 2.0);
    }

    /**
     * @deprecated Call {@link BookSearchService#searchAuthors(String, Integer)} directly and surface DTOs.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    Mono<List<BookSearchService.AuthorResult>> searchAuthors(String query, int desiredTotalResults) {
        return searchAuthors(query, desiredTotalResults, false);
    }

    /**
     * @deprecated See {@link #searchAuthors(String, int)}.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    Mono<List<BookSearchService.AuthorResult>> searchAuthors(String query,
                                                            int desiredTotalResults,
                                                            boolean bypassExternalApis) {
        if (bookSearchService == null) {
            return Mono.just(List.of());
        }

        int safeLimit = desiredTotalResults <= 0 ? 20 : PagingUtils.clamp(desiredTotalResults, 1, 100);

        Mono<List<BookSearchService.AuthorResult>> postgresMono = Mono.fromCallable(() -> bookSearchService.searchAuthors(query, safeLimit))
            .subscribeOn(Schedulers.boundedElastic())
            .map(results -> results == null ? List.<BookSearchService.AuthorResult>of() : results);

        return postgresMono
            .onErrorResume(postgresError -> {
                String message = postgresError != null && postgresError.getMessage() != null
                    ? postgresError.getMessage()
                    : String.valueOf(postgresError);
                LOGGER.warn("TieredBookSearch: Postgres author search failed for '{}': {}", query, message, postgresError);
                if (!externalFallbackEnabled || bypassExternalApis) {
                    LOGGER.error("TieredBookSearch: Author fallback disabled or bypassed after Postgres error for '{}'. Returning empty set.", query);
                    return Mono.just(List.<BookSearchService.AuthorResult>of());
                }
                return Mono.just(List.<BookSearchService.AuthorResult>of());
            })
            .flatMap(postgresResults -> {
                List<BookSearchService.AuthorResult> baseline = postgresResults == null ? List.of() : postgresResults;

                if (!externalFallbackEnabled || bypassExternalApis) {
                    if (bypassExternalApis) {
                        LOGGER.info("TieredBookSearch: Returning {} Postgres author result(s) for '{}' (external bypass)", baseline.size(), query);
                    } else {
                        LOGGER.info("TieredBookSearch: Returning {} Postgres author result(s) for '{}' (external fallback disabled)", baseline.size(), query);
                    }
                    return Mono.just(baseline);
                }

                if (!baseline.isEmpty() && baseline.size() >= safeLimit) {
                    LOGGER.info("TieredBookSearch: Postgres author search satisfied '{}' with {} result(s).", query, baseline.size());
                    return Mono.just(baseline);
                }

                int remaining = Math.max(safeLimit - baseline.size(), 1);
                return performExternalAuthorSearch(query, safeLimit, remaining, baseline.isEmpty())
                    .map(external -> mergeAuthorResults(baseline, external, safeLimit))
                    .defaultIfEmpty(baseline)
                    .onErrorResume(externalError -> {
                        LOGGER.warn("TieredBookSearch: External author fallback failed for '{}': {}", query, externalError.getMessage());
                        return Mono.just(baseline);
                    });
            })
            .defaultIfEmpty(List.of());
    }

    private Mono<List<BookSearchService.AuthorResult>> performExternalAuthorSearch(String query,
                                                                                   int safeLimit,
                                                                                   int remaining,
                                                                                   boolean postgresWasEmpty) {
        int fetchTarget = Math.max(Math.max(remaining * 2, safeLimit * 2), 5);

        return performExternalSearchStream(query, null, fetchTarget, "relevance", postgresWasEmpty)
            .collectList()
            .map(books -> aggregateBooksToAuthorResults(books, safeLimit))
            .doOnSuccess(results -> {
                if (!results.isEmpty()) {
                    LOGGER.info("TieredBookSearch: External author fallback found {} candidate(s) for '{}'", results.size(), query);
                } else {
                    LOGGER.info("TieredBookSearch: External author fallback returned no candidates for '{}'", query);
                }
            });
    }

    private List<BookSearchService.AuthorResult> aggregateBooksToAuthorResults(List<Book> books, int limit) {
        if (books == null || books.isEmpty()) {
            return List.of();
        }

        Map<String, AuthorAggregation> byNormalized = new LinkedHashMap<>();

        for (Book book : books) {
            if (book == null || ValidationUtils.isNullOrEmpty(book.getAuthors())) {
                continue;
            }

            double relevance = extractRelevanceScore(book);

            for (String authorName : book.getAuthors()) {
                String normalized = normalizeAuthorName(authorName);
                if (normalized.isEmpty()) {
                    continue;
                }

                AuthorAggregation aggregation = byNormalized.computeIfAbsent(
                    normalized,
                    key -> new AuthorAggregation(authorName, generateFallbackAuthorId(authorName, normalized))
                );
                aggregation.incrementBookCount();
                aggregation.updateRelevance(relevance);
            }
        }

        if (byNormalized.isEmpty()) {
            return List.of();
        }

        return byNormalized.values().stream()
            .sorted(Comparator.comparingDouble(AuthorAggregation::topRelevance).reversed()
                .thenComparing(AuthorAggregation::displayName))
            .limit(Math.max(limit, 1))
            .map(AuthorAggregation::toResult)
            .collect(Collectors.toList());
    }

    private List<BookSearchService.AuthorResult> mergeAuthorResults(List<BookSearchService.AuthorResult> postgresResults,
                                                                    List<BookSearchService.AuthorResult> externalResults,
                                                                    int maxTotal) {
        if ((postgresResults == null || postgresResults.isEmpty()) && (externalResults == null || externalResults.isEmpty())) {
            return List.of();
        }

        LinkedHashMap<String, BookSearchService.AuthorResult> merged = new LinkedHashMap<>();

        if (postgresResults != null) {
            for (BookSearchService.AuthorResult result : postgresResults) {
                if (result == null) {
                    continue;
                }
                String key = dedupeKeyForAuthor(result.authorName(), result.authorId());
                if (!key.isEmpty()) {
                    merged.putIfAbsent(key, result);
                }
            }
        }

        if (externalResults != null) {
            for (BookSearchService.AuthorResult result : externalResults) {
                if (result == null) {
                    continue;
                }
                String key = dedupeKeyForAuthor(result.authorName(), result.authorId());
                if (key.isEmpty() || merged.containsKey(key)) {
                    continue;
                }
                merged.put(key, result);
                if (merged.size() >= maxTotal) {
                    break;
                }
            }
        }

        return merged.values().stream()
            .limit(Math.max(maxTotal, 1))
            .collect(Collectors.toList());
    }

    private double extractRelevanceScore(Book book) {
        if (book == null || book.getQualifiers() == null) {
            return 0.0;
        }
        Object qualifier = book.getQualifiers().get("search.relevanceScore");
        if (qualifier instanceof Number number) {
            return number.doubleValue();
        }
        if (qualifier != null) {
            try {
                return Double.parseDouble(qualifier.toString());
            } catch (NumberFormatException ignored) {
                return 0.0;
            }
        }
        return 0.0;
    }

    private String normalizeAuthorName(String name) {
        if (!ValidationUtils.hasText(name)) {
            return "";
        }
        String normalized = name.trim().toLowerCase(Locale.ROOT);
        normalized = normalized.replaceAll("[^a-z0-9\\s]", "");
        normalized = normalized.replaceAll("\\s+", " ").trim();
        return normalized;
    }

    private String generateFallbackAuthorId(String displayName, String normalized) {
        String slug = SlugGenerator.slugify(displayName);
        String suffix = ValidationUtils.hasText(slug)
            ? slug
            : normalized.replace(' ', '-');
        if (!ValidationUtils.hasText(suffix)) {
            suffix = UUID.randomUUID().toString();
        }
        return EXTERNAL_AUTHOR_ID_PREFIX + suffix;
    }

    private String dedupeKeyForAuthor(String authorName, String authorId) {
        String normalized = normalizeAuthorName(authorName);
        if (!normalized.isEmpty()) {
            return normalized;
        }
        if (ValidationUtils.hasText(authorId)) {
            return authorId.trim();
        }
        return "";
    }

    private static final class AuthorAggregation {
        private final String displayName;
        private final String authorId;
        private long bookCount;
        private double topRelevance;

        AuthorAggregation(String displayName, String authorId) {
            this.displayName = displayName;
            this.authorId = authorId;
            this.bookCount = 0L;
            this.topRelevance = 0.0;
        }

        void incrementBookCount() {
            this.bookCount++;
            double baseline = Math.min(1.0, this.bookCount * 0.1);
            if (baseline > this.topRelevance) {
                this.topRelevance = baseline;
            }
        }

        void updateRelevance(double candidate) {
            if (Double.isFinite(candidate) && candidate > this.topRelevance) {
                this.topRelevance = candidate;
            }
        }

        double topRelevance() {
            return topRelevance;
        }

        String displayName() {
            return displayName;
        }

        BookSearchService.AuthorResult toResult() {
            return new BookSearchService.AuthorResult(authorId, displayName, bookCount, topRelevance);
        }
    }

    /**
     * Searches Postgres reactively without blocking.
     * Uses BookQueryRepository for SINGLE OPTIMIZED QUERY instead of N+1 hydration queries.
     * 
     * Performance: Single query to fetch all book cards vs 5+ queries per book.
     */
    private Mono<List<Book>> searchPostgresFirstReactive(String query, int desiredTotalResults) {
        if (bookSearchService == null || bookQueryRepository == null) {
            return Mono.just(List.of());
        }
        
        int safeTotal = desiredTotalResults <= 0 ? 20 : PagingUtils.atLeast(desiredTotalResults, 1);
        
        return Mono.fromCallable(() -> bookSearchService.searchBooks(query, safeTotal))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(hits -> {
                if (hits == null || hits.isEmpty()) {
                    return Mono.just(List.<Book>of());
                }
                
                // Extract book IDs for optimized fetch
                List<UUID> bookIds = hits.stream()
                    .map(hit -> hit.bookId())
                    .collect(Collectors.toList());
                
                if (bookIds.isEmpty()) {
                    return Mono.just(List.<Book>of());
                }
                
                // SINGLE optimized query using BookQueryRepository (THE SINGLE SOURCE OF TRUTH)
                return Mono.fromCallable(() -> bookQueryRepository.fetchBookCards(bookIds))
                    .subscribeOn(Schedulers.boundedElastic())
                    .map(cards -> {
                        // Convert BookCard DTOs to Book entities (temporary bridge)
                        List<Book> books = cards.stream()
                            .map(BookDomainMapper::fromCard)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
                        
                        // Create map for fast lookup
                        Map<String, Book> bookMap = books.stream()
                            .collect(Collectors.toMap(Book::getId, book -> book));
                        
                        // Apply search qualifiers and maintain order
                        List<Book> orderedResults = new ArrayList<>();
                        for (BookSearchService.SearchResult hit : hits) {
                            Book book = bookMap.get(hit.bookId().toString());
                            if (book != null) {
                                book.addQualifier("search.matchType", hit.matchTypeNormalised());
                                book.addQualifier("search.relevanceScore", hit.relevanceScore());
                                orderedResults.add(book);
                            }
                        }
                        return orderedResults;
                    });
            })
            .defaultIfEmpty(List.of())
            .doOnSuccess(results -> {
                if (!results.isEmpty()) {
                    LOGGER.debug("Postgres search returned {} books for query '{}'", results.size(), query);
                }
            });
    }

    private String computeQueryHash(String query) {
        String canonical = SearchQueryUtils.canonicalize(query);
        if (canonical == null) return "";
        return canonical.replaceAll("[^a-z0-9-_]", "_");
    }

    private void safePublish(Object event) {
        try {
            if (eventPublisher != null && event != null) {
                eventPublisher.publishEvent(event);
            }
        } catch (Exception ex) {
            String eventName = event != null ? event.getClass().getSimpleName() : "unknown";
            LOGGER.debug("TieredBookSearch: Failed to publish event {}: {}", eventName, ex.getMessage());
        }
    }
}
