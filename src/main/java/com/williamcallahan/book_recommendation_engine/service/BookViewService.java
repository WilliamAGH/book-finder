package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.dto.BookListItem;
import com.williamcallahan.book_recommendation_engine.util.BookDomainMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.util.UuidUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Provides Postgres-first read access for legacy {@link Book} projections while the
 * application migrates toward DTO-centric controllers.
 * <p>
 * Centralises all lookups so callers no longer need to depend on the deprecated
 * tiered orchestrator methods.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BookViewService {

    private final BookLookupService bookLookupService;
    private final BookSearchService bookSearchService;
    private final BookQueryRepository bookQueryRepository;

    /**
     * Search for books using the Postgres-backed full text index.
     * <p>
     * Results are ordered according to the underlying database function which already
     * sorts by relevance. The optional {@code orderBy} flag is preserved for API
     * compatibility but currently acts as a hint only; callers should migrate to
     * DTO-based projections for richer sorting semantics.
     *
     * @deprecated Consume DTO projections directly via
     * {@link BookQueryRepository#searchBookCards(String, String, int, String)}
     * or use {@link BookSearchService#searchBooks(String, Integer)} combined with
     * {@link BookQueryRepository#fetchBookListItems(java.util.List)} to return DTO projections
     * instead of rematerialising legacy {@link Book} entities.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<Book>> searchBooks(String query,
                                        String langCode,
                                        int desiredTotalResults,
                                        String orderBy) {
        if (!ValidationUtils.hasText(query) || bookSearchService == null || bookQueryRepository == null) {
            return Mono.just(List.of());
        }

        int safeLimit = desiredTotalResults > 0 ? desiredTotalResults : 20;

        return Mono.fromCallable(() -> bookSearchService.searchBooks(query, safeLimit))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(results -> {
                if (results == null || results.isEmpty()) {
                    return Mono.just(List.<Book>of());
                }

                List<UUID> bookIds = results.stream()
                    .map(BookSearchService.SearchResult::bookId)
                    .filter(Objects::nonNull)
                    .limit(safeLimit)
                    .toList();

                if (bookIds.isEmpty()) {
                    return Mono.just(List.<Book>of());
                }

                return Mono.fromCallable(() -> bookQueryRepository.fetchBookListItems(bookIds))
                    .subscribeOn(Schedulers.boundedElastic())
                    .map(items -> assembleSearchResults(results, items));
            })
            .defaultIfEmpty(List.of())
            .doOnError(ex -> log.warn("Postgres search failed for query '{}': {}", query, ex.getMessage(), ex))
            .onErrorReturn(List.of());
    }

    /**
     * Fetch a single book by slug, UUID, ISBN, or external identifier.
     * <p>
     * Slug lookups execute first to preserve existing routing semantics. If no
     * slug match is found, the service attempts canonical ID resolution via
     * {@link BookLookupService} before falling back to ISBN / external identifier
     * searches.
     *
     * @deprecated Prefer {@link BookQueryRepository#fetchBookDetailBySlug(String)} or
     * {@link BookQueryRepository#fetchBookDetail(UUID)} and consume DTOs instead of mapping to
     * legacy {@link Book} instances.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<Book> fetchBook(String identifier) {
        if (!ValidationUtils.hasText(identifier) || bookQueryRepository == null) {
            return Mono.empty();
        }

        String trimmed = identifier.trim();

        return Mono.fromCallable(() -> resolveDetail(trimmed))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(optional -> Mono.justOrEmpty(optional.map(BookDomainMapper::fromDetail)))
            .doOnError(ex -> log.warn("Failed to resolve book '{}': {}", trimmed, ex.getMessage(), ex))
            .onErrorResume(ex -> Mono.empty());
    }

    /**
     * Fetch multiple books by canonical IDs. Intended for cache warming flows that
     * previously relied on tiered orchestrator lookups.
     *
     * @deprecated Retrieve list items via {@link BookQueryRepository#fetchBookListItems(List)}
     * and operate on DTOs rather than hydrating {@link Book} objects.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public Mono<List<Book>> fetchBooksByIds(List<String> identifiers) {
        if (identifiers == null || identifiers.isEmpty() || bookQueryRepository == null) {
            return Mono.just(Collections.emptyList());
        }

        List<UUID> uuids = identifiers.stream()
            .map(UuidUtils::parseUuidOrNull)
            .filter(Objects::nonNull)
            .toList();

        if (uuids.isEmpty()) {
            return Mono.just(Collections.emptyList());
        }

        return Mono.fromCallable(() -> bookQueryRepository.fetchBookListItems(uuids))
            .subscribeOn(Schedulers.boundedElastic())
            .map(BookDomainMapper::fromListItems)
            .onErrorResume(ex -> {
                log.warn("Failed to fetch books by ids {}: {}", identifiers, ex.getMessage(), ex);
                return Mono.just(Collections.emptyList());
            });
    }

    /**
     * @deprecated Call {@link BookQueryRepository#fetchBookDetailBySlug(String)} or
     * {@link BookQueryRepository#fetchBookDetail(UUID)} directly and return DTOs.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private Optional<BookDetail> resolveDetail(String identifier) {
        // 1. Try slug
        Optional<BookDetail> bySlug = bookQueryRepository.fetchBookDetailBySlug(identifier);
        if (bySlug.isPresent()) {
            return bySlug;
        }

        // 2. Try canonical UUID
        UUID uuid = UuidUtils.parseUuidOrNull(identifier);
        if (uuid != null) {
            Optional<BookDetail> byId = bookQueryRepository.fetchBookDetail(uuid);
            if (byId.isPresent()) {
                return byId;
            }
        }

        // 3. Try ISBN lookups via BookLookupService
        Optional<String> canonicalId = bookLookupService != null
            ? bookLookupService.findBookIdByIsbn(identifier)
            : Optional.empty();

        if (canonicalId.isEmpty() && bookLookupService != null) {
            canonicalId = bookLookupService.findBookIdByExternalIdentifier(identifier);
        }

        return canonicalId
            .map(UuidUtils::parseUuidOrNull)
            .filter(Objects::nonNull)
            .map(bookQueryRepository::fetchBookDetail)
            .flatMap(optional -> optional);
    }

    /**
     * @deprecated Use DTO search results from {@link BookQueryRepository#searchBookCards}
     * without mapping back to legacy {@link Book} instances.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private List<Book> assembleSearchResults(List<BookSearchService.SearchResult> results,
                                             List<BookListItem> items) {
        if (items == null || items.isEmpty()) {
            return List.of();
        }

        Map<String, BookListItem> itemById = items.stream()
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(BookListItem::id, item -> item, (first, second) -> first, LinkedHashMap::new));

        return results.stream()
            .map(result -> {
                UUID bookId = result.bookId();
                if (bookId == null) {
                    return null;
                }
                BookListItem item = itemById.get(bookId.toString());
                if (item == null) {
                    return null;
                }
                Book book = BookDomainMapper.fromListItem(item);
                if (book == null) {
                    return null;
                }
                book.addQualifier("search.matchType", result.matchTypeNormalised());
                book.addQualifier("search.relevanceScore", result.relevanceScore());
                return book;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }
}
