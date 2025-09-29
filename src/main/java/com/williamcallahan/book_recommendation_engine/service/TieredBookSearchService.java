package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import com.williamcallahan.book_recommendation_engine.util.ReactiveErrorUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import java.time.Duration;

/**
 * Extracted tiered search orchestration from {@link BookDataOrchestrator}. Handles DB-first search
 * with Google and OpenLibrary fallbacks while keeping the orchestrator slim.
 */
@Component
@ConditionalOnBean({BookSearchService.class, PostgresBookRepository.class})
public class TieredBookSearchService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TieredBookSearchService.class);

    private final BookSearchService bookSearchService;
    private final GoogleApiFetcher googleApiFetcher;
    private final OpenLibraryBookDataService openLibraryBookDataService;
    private final PostgresBookRepository postgresBookRepository;
    private final boolean externalFallbackEnabled;

    TieredBookSearchService(BookSearchService bookSearchService,
                            GoogleApiFetcher googleApiFetcher,
                            OpenLibraryBookDataService openLibraryBookDataService,
                            PostgresBookRepository postgresBookRepository,
                            @Value("${app.features.external-fallback.enabled:${app.features.google-fallback.enabled:true}}") boolean externalFallbackEnabled) {
        this.bookSearchService = bookSearchService;
        this.googleApiFetcher = googleApiFetcher;
        this.openLibraryBookDataService = openLibraryBookDataService;
        this.postgresBookRepository = postgresBookRepository;
        this.externalFallbackEnabled = externalFallbackEnabled;
    }

    Mono<List<Book>> searchBooks(String query, String langCode, int desiredTotalResults, String orderBy) {
        LOGGER.debug("TieredBookSearch: Starting search for query: '{}', lang: {}, total: {}, order: {}", query, langCode, desiredTotalResults, orderBy);

        // Try Postgres first with timeout
        List<Book> postgresHits = searchPostgresFirst(query, desiredTotalResults);
        if (!postgresHits.isEmpty()) {
            LOGGER.info("TieredBookSearch: Postgres satisfied query '{}' with {} results.", query, postgresHits.size());
            return Mono.just(postgresHits);
        }
        
        LOGGER.debug("TieredBookSearch: Postgres returned empty for '{}', checking external fallback", query);

        if (!externalFallbackEnabled) {
            LOGGER.info("TieredBookSearch: External fallbacks disabled; returning empty result set for query '{}' after Postgres miss.", query);
            return Mono.just(postgresHits);
        }

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

        return primarySearchMono
            .flatMap(googleResults1 -> {
                if (!googleResults1.isEmpty()) {
                    LOGGER.info("TieredBookSearch: Primary Google search ({}) successful for query '{}', found {} books.", (apiKeyAvailable ? "Authenticated" : "Unauthenticated"), query, googleResults1.size());
                    return Mono.just(googleResults1);
                }
                LOGGER.info("TieredBookSearch: Primary Google search ({}) for query '{}' yielded no results. Proceeding to fallback Google search.", (apiKeyAvailable ? "Authenticated" : "Unauthenticated"), query);
                return fallbackSearchMono.flatMap(googleResults2 -> {
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
            })
            .onErrorResume(ReactiveErrorUtils.logAndReturnEmptyList("TieredBookSearchService.searchBooks(" + query + ")"));
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

    private List<Book> searchPostgresFirst(String query, int desiredTotalResults) {
        if (bookSearchService == null || postgresBookRepository == null) {
            return List.of();
        }
        int safeTotal = desiredTotalResults <= 0 ? 20 : PagingUtils.atLeast(desiredTotalResults, 1);
        List<BookSearchService.SearchResult> hits = bookSearchService.searchBooks(query, safeTotal);
        if (hits == null || hits.isEmpty()) {
            return List.of();
        }
        List<Book> resolved = new ArrayList<>(hits.size());
        for (BookSearchService.SearchResult hit : hits) {
            Optional<Book> book = postgresBookRepository.fetchByCanonicalId(hit.bookId().toString());
            book.ifPresent(resolvedBook -> {
                resolvedBook.addQualifier("search.matchType", hit.matchTypeNormalised());
                resolvedBook.addQualifier("search.relevanceScore", hit.relevanceScore());
                resolved.add(resolvedBook);
            });
        }
        return resolved;
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
