package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.dto.DtoToBookMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import com.williamcallahan.book_recommendation_engine.util.ExternalApiLogger;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import com.williamcallahan.book_recommendation_engine.util.ReactiveErrorUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

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

    private final BookSearchService bookSearchService;
    private final GoogleApiFetcher googleApiFetcher;
    private final GoogleBooksService googleBooksService;
    private final OpenLibraryBookDataService openLibraryBookDataService;
    
    /**
     * THE SINGLE SOURCE OF TRUTH for book queries - uses optimized SQL functions.
     */
    private final BookQueryRepository bookQueryRepository;
    
    private final boolean externalFallbackEnabled;

    TieredBookSearchService(BookSearchService bookSearchService,
                            GoogleApiFetcher googleApiFetcher,
                            GoogleBooksService googleBooksService,
                            OpenLibraryBookDataService openLibraryBookDataService,
                            @Nullable BookQueryRepository bookQueryRepository,
                            @Value("${app.features.external-fallback.enabled:${app.features.google-fallback.enabled:true}}") boolean externalFallbackEnabled) {
        this.bookSearchService = bookSearchService;
        this.googleApiFetcher = googleApiFetcher;
        this.googleBooksService = googleBooksService;
        this.openLibraryBookDataService = openLibraryBookDataService;
        this.bookQueryRepository = bookQueryRepository;
        this.externalFallbackEnabled = externalFallbackEnabled;
    }

    Mono<List<Book>> searchBooks(String query, String langCode, int desiredTotalResults, String orderBy) {
        return searchBooks(query, langCode, desiredTotalResults, orderBy, false);
    }
    
    Mono<List<Book>> searchBooks(String query, String langCode, int desiredTotalResults, String orderBy, boolean bypassExternalApis) {
        return streamSearch(query, langCode, desiredTotalResults, orderBy, bypassExternalApis)
            .collectList();
    }

    Flux<Book> streamSearch(String query,
                             String langCode,
                             int desiredTotalResults,
                             String orderBy,
                             boolean bypassExternalApis) {
        LOGGER.debug("TieredBookSearch: Starting stream for query='{}', lang={}, total={}, order={}, bypassExternal={}",
            query, langCode, desiredTotalResults, orderBy, bypassExternalApis);

        return searchPostgresFirstReactive(query, desiredTotalResults)
            .onErrorResume(postgresError -> {
                LOGGER.warn("TieredBookSearch: Postgres search failed for query '{}': {}", query, postgresError.getMessage());
                if (!externalFallbackEnabled || bypassExternalApis) {
                    LOGGER.error("TieredBookSearch: No external fallback allowed for '{}' after Postgres failure; streaming empty results.", query);
                    return Mono.just(List.<Book>of());
                }
                ExternalApiLogger.logTieredSearchStart(LOGGER, query, 0, desiredTotalResults);
                return Mono.just(List.<Book>of());
            })
            .flatMapMany(postgresHits -> {
                List<Book> baseline = postgresHits == null ? List.of() : postgresHits;
                Flux<Book> postgresFlux = Flux.fromIterable(baseline);

                if (!externalFallbackEnabled || bypassExternalApis) {
                    if (bypassExternalApis) {
                        LOGGER.info("TieredBookSearch: External APIs bypassed for '{}' — streaming {} Postgres result(s) only.", query, baseline.size());
                    } else {
                        LOGGER.info("TieredBookSearch: External fallback disabled; streaming {} Postgres result(s) for '{}'", baseline.size(), query);
                    }
                    return postgresFlux.take(desiredTotalResults);
                }

                boolean satisfied = !baseline.isEmpty() && baseline.size() >= desiredTotalResults;
                if (satisfied) {
                    LOGGER.info("TieredBookSearch: Postgres fully satisfied '{}' with {} result(s); external fallback skipped.", query, baseline.size());
                    return postgresFlux.take(desiredTotalResults);
                }

                int remaining = Math.max(desiredTotalResults - baseline.size(), desiredTotalResults);
                ExternalApiLogger.logTieredSearchStart(LOGGER, query, baseline.size(), desiredTotalResults);

                Duration externalTimeout = Duration.ofMillis(1200);

                Flux<Book> externalFlux = performExternalSearchStream(query, langCode, remaining, orderBy, baseline.isEmpty())
                    .timeout(externalTimeout)
                    .onErrorResume(error -> {
                        LOGGER.warn("TieredBookSearch: External fallback failed for '{}': {}", query, error.getMessage());
                        return Flux.empty();
                    });

                return Flux.concat(postgresFlux, externalFlux)
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

        final Map<String, Object> queryQualifiers = BookJsonParser.extractQualifiersFromSearchQuery(query);
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

        Flux<Book> googleFlux = googleBooksService.streamBooksReactive(effectiveQuery, langCode, desiredTotalResults, orderBy)
            .map(book -> {
                if (!queryQualifiers.isEmpty()) {
                    queryQualifiers.forEach(book::addQualifier);
                }
                return book;
            })
            .onErrorResume(err -> {
                LOGGER.warn("TieredBookSearch: Google Books stream failed for '{}': {}", query, err.getMessage());
                ExternalApiLogger.logApiCallFailure(LOGGER, "GoogleBooks", "STREAM_SEARCH", effectiveQuery, err.getMessage());
                return Flux.empty();
            });

        AtomicInteger emittedCount = new AtomicInteger(0);

        Flux<Book> combined = Flux.concat(googleFlux, openLibraryFlux);

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

    Mono<List<BookSearchService.AuthorResult>> searchAuthors(String query, int desiredTotalResults) {
        if (bookSearchService == null) {
            return Mono.just(List.of());
        }
        int safeLimit = desiredTotalResults <= 0 ? 20 : PagingUtils.clamp(desiredTotalResults, 1, 100);
        return Mono.fromCallable(() -> bookSearchService.searchAuthors(query, safeLimit))
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(ReactiveErrorUtils.logAndReturnEmptyList("TieredBookSearchService.searchAuthors(" + query + ")"));
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
                        List<Book> books = DtoToBookMapper.toBooks(cards);
                        
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

}
