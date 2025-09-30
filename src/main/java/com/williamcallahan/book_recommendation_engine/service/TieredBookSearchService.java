package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.dto.DtoToBookMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import com.williamcallahan.book_recommendation_engine.util.ReactiveErrorUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.LinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
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
    private final OpenLibraryBookDataService openLibraryBookDataService;
    
    /**
     * THE SINGLE SOURCE OF TRUTH for book queries - uses optimized SQL functions.
     */
    private final BookQueryRepository bookQueryRepository;
    
    private final boolean externalFallbackEnabled;

    TieredBookSearchService(BookSearchService bookSearchService,
                            GoogleApiFetcher googleApiFetcher,
                            OpenLibraryBookDataService openLibraryBookDataService,
                            @Nullable BookQueryRepository bookQueryRepository,
                            @Value("${app.features.external-fallback.enabled:${app.features.google-fallback.enabled:true}}") boolean externalFallbackEnabled) {
        this.bookSearchService = bookSearchService;
        this.googleApiFetcher = googleApiFetcher;
        this.openLibraryBookDataService = openLibraryBookDataService;
        this.bookQueryRepository = bookQueryRepository;
        this.externalFallbackEnabled = externalFallbackEnabled;
    }

    Mono<List<Book>> searchBooks(String query, String langCode, int desiredTotalResults, String orderBy) {
        return searchBooks(query, langCode, desiredTotalResults, orderBy, false);
    }
    
    Mono<List<Book>> searchBooks(String query, String langCode, int desiredTotalResults, String orderBy, boolean bypassExternalApis) {
        LOGGER.debug("TieredBookSearch: Starting search for query: '{}', lang: {}, total: {}, order: {}, bypassExternal: {}",
            query, langCode, desiredTotalResults, orderBy, bypassExternalApis);

        // ALWAYS try Postgres first - baseline results
        // FIX BUG #1: Handle Postgres errors separately so they don't prevent external fallback
        return searchPostgresFirstReactive(query, desiredTotalResults)
            .onErrorResume(postgresError -> {
                // Log Postgres failure but continue to external search if enabled
                LOGGER.warn("TieredBookSearch: Postgres search failed for query '{}': {}. Attempting external fallback.",
                    query, postgresError.getMessage());

                // If external fallback disabled/bypassed, fail now
                if (!externalFallbackEnabled || bypassExternalApis) {
                    LOGGER.error("TieredBookSearch: Postgres failed and external fallback disabled/bypassed for query '{}'.", query);
                    return Mono.just(List.<Book>of());
                }

                // Otherwise, try external search as if Postgres returned empty
                LOGGER.info("TieredBookSearch: Proceeding to external search after Postgres failure for query '{}'", query);
                return Mono.just(List.<Book>of()); // Empty list to trigger external search in flatMap
            })
            .flatMap(postgresHits -> {
                LOGGER.info("TieredBookSearch: Postgres returned {} results for query '{}'", postgresHits.size(), query);

                // If we have enough results from Postgres, return them immediately
                if (!postgresHits.isEmpty() && postgresHits.size() >= desiredTotalResults) {
                    LOGGER.info("TieredBookSearch: Postgres fully satisfied query '{}' with {} results.", query, postgresHits.size());
                    return Mono.just(postgresHits);
                }

                // If external fallback disabled OR explicitly bypassed (e.g., for homepage), return what Postgres gave us
                if (!externalFallbackEnabled || bypassExternalApis) {
                    if (bypassExternalApis) {
                        LOGGER.info("TieredBookSearch: External APIs bypassed for query '{}'; returning {} Postgres-only results.", query, postgresHits.size());
                    } else {
                        LOGGER.info("TieredBookSearch: External fallbacks disabled; returning {} Postgres results for query '{}'.", postgresHits.size(), query);
                    }
                    return Mono.just(postgresHits);
                }

                // Otherwise, augment Postgres results with external APIs
                int needed = Math.max(0, desiredTotalResults - postgresHits.size());
                LOGGER.debug("TieredBookSearch: Augmenting {} Postgres results with up to {} external results", postgresHits.size(), needed);

                // FIX BUG #2 & #3: Handle external search errors gracefully, preserve Postgres results
                return performExternalSearch(query, langCode, needed, orderBy)
                    .map(externalHits -> {
                        List<Book> merged = mergeResults(postgresHits, externalHits, desiredTotalResults);
                        LOGGER.info("TieredBookSearch: Merged {} Postgres + {} external = {} total results for query '{}'",
                            postgresHits.size(), externalHits.size(), merged.size(), query);
                        return merged;
                    })
                    .onErrorResume(externalError -> {
                        // If external search fails, return Postgres results we already have
                        LOGGER.warn("TieredBookSearch: External search failed for query '{}': {}. Returning {} Postgres results.",
                            query, externalError.getMessage(), postgresHits.size());
                        return Mono.just(postgresHits);
                    })
                    .defaultIfEmpty(postgresHits); // If external returns empty (not error), still return Postgres results
            })
            .defaultIfEmpty(List.of()) // Final safety net
            .onErrorResume(unexpectedError -> {
                // This should rarely be hit now that we handle errors at each tier
                LOGGER.error("TieredBookSearch: Unexpected error during search for '{}': {}", query, unexpectedError.getMessage());
                return Mono.just(List.of());
            });
    }
    
    private Mono<List<Book>> performExternalSearch(String query, String langCode, int desiredTotalResults, String orderBy) {

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
            .onErrorResume(ReactiveErrorUtils.logAndReturnEmptyList("TieredBookSearchService.openLibrarySearch(" + query + ")"));

        if (!googleFallbackEnabled) {
            LOGGER.info("TieredBookSearch: Google fallback disabled for query '{}'. Returning OpenLibrary results only.", query);
            return openLibrarySearchMono;
        }

        Mono<List<Book>> primarySearchMono = apiKeyAvailable
            ? executePagedSearch(query, langCode, desiredTotalResults, orderBy, true, queryQualifiers)
            : executePagedSearch(query, langCode, desiredTotalResults, orderBy, false, queryQualifiers);

        Mono<List<Book>> fallbackSearchMono = apiKeyAvailable
            ? executePagedSearch(query, langCode, desiredTotalResults, orderBy, false, queryQualifiers)
            : Mono.just(Collections.emptyList());

        // FIX BUG #4: Ensure errors in primary search don't prevent fallback attempts
        return primarySearchMono
            .onErrorResume(primaryError -> {
                // If primary search errors (not just empty), log and proceed to fallback
                LOGGER.warn("TieredBookSearch: Primary Google search ({}) error for query '{}': {}. Attempting fallback.",
                    (apiKeyAvailable ? "Authenticated" : "Unauthenticated"), query, primaryError.getMessage());
                return Mono.just(Collections.<Book>emptyList()); // Treat error as empty to trigger fallback
            })
            .flatMap(googleResults1 -> {
                if (!googleResults1.isEmpty()) {
                    LOGGER.info("TieredBookSearch: Primary Google search ({}) successful for query '{}', found {} books.", (apiKeyAvailable ? "Authenticated" : "Unauthenticated"), query, googleResults1.size());
                    return Mono.just(googleResults1);
                }
                LOGGER.info("TieredBookSearch: Primary Google search ({}) for query '{}' yielded no results. Proceeding to fallback Google search.", (apiKeyAvailable ? "Authenticated" : "Unauthenticated"), query);

                return fallbackSearchMono
                    .onErrorResume(fallbackError -> {
                        // If fallback search errors, log and proceed to OpenLibrary
                        LOGGER.warn("TieredBookSearch: Fallback Google search error for query '{}': {}. Attempting OpenLibrary.",
                            query, fallbackError.getMessage());
                        return Mono.just(Collections.<Book>emptyList());
                    })
                    .flatMap(googleResults2 -> {
                        if (!googleResults2.isEmpty()) {
                            LOGGER.info("TieredBookSearch: Fallback Google search successful for query '{}', found {} books.", query, googleResults2.size());
                            return Mono.just(googleResults2);
                        }
                        LOGGER.info("TieredBookSearch: Fallback Google search for query '{}' yielded no results. Proceeding to OpenLibrary search.", query);
                        return openLibrarySearchMono;
                    });
            })
            .doOnSuccess(books -> {
                if (!books.isEmpty()) {
                    LOGGER.info("TieredBookSearch: Successfully searched books for query '{}'. Found {} books.", query, books.size());
                } else {
                    LOGGER.info("TieredBookSearch: Search for query '{}' yielded no results from any tier.", query);
                }
            });
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
     * Merges Postgres baseline results with external API results.
     * Postgres results take priority; external results fill remaining slots.
     * Deduplicates by book ID.
     */
    private List<Book> mergeResults(List<Book> postgresResults, List<Book> externalResults, int maxTotal) {
        LinkedHashMap<String, Book> merged = new LinkedHashMap<>();
        
        // Add Postgres results first (highest priority)
        for (Book book : postgresResults) {
            if (book != null && book.getId() != null) {
                merged.putIfAbsent(book.getId(), book);
            }
        }
        
        // Add external results to fill remaining slots
        for (Book book : externalResults) {
            if (book != null && book.getId() != null && merged.size() < maxTotal) {
                merged.putIfAbsent(book.getId(), book);
            }
        }
        
        List<Book> result = merged.values().stream().limit(maxTotal).collect(Collectors.toList());
        LOGGER.debug("Merged {} Postgres + {} external = {} total results (max: {})", 
            postgresResults.size(), externalResults.size(), result.size(), maxTotal);
        return result;
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

    private Mono<List<Book>> executePagedSearch(String query,
                                                String langCode,
                                                int desiredTotalResults,
                                                String orderBy,
                                                boolean authenticated,
                                                Map<String, Object> queryQualifiers) {
        final int maxTotalResultsToFetch = desiredTotalResults <= 0
            ? 40
            : PagingUtils.clamp(desiredTotalResults, 1, 200);
        final String effectiveOrderBy = (orderBy != null && !orderBy.trim().isEmpty()) ? orderBy : "relevance";
        String authType = authenticated ? "Authenticated" : "Unauthenticated";

        LOGGER.debug("TieredBookSearch: Executing {} paged search for query: '{}', lang: {}, total_requested: {}, order: {}", authType, query, langCode, maxTotalResultsToFetch, effectiveOrderBy);

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
            .map(books -> books.size() > maxTotalResultsToFetch ? books.subList(0, maxTotalResultsToFetch) : books)
            .doOnSuccess(finalList -> LOGGER.info("TieredBookSearch: {} paged search for query '{}' completed. Aggregated {} books.", authType, query, finalList.size()))
            .onErrorResume(ReactiveErrorUtils.logAndReturnEmptyList("TieredBookSearchService.executePagedSearch auth=" + authType + " query=" + query));
    }
}
