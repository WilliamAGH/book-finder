package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.dto.BookListItem;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.util.BookDomainMapper;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Coordinates paginated search across Postgres + external sources and emits
 * cursor-friendly slices with deterministic ordering and deduplication.
 */
@Service
@Slf4j
public class SearchPaginationService {

    private final @Nullable TieredBookSearchService tieredBookSearchService;
    private final @Nullable BookDataOrchestrator bookDataOrchestrator;
    private final BookSearchService bookSearchService;
    private final BookQueryRepository bookQueryRepository;

    public SearchPaginationService(@Nullable TieredBookSearchService tieredBookSearchService,
                                   @Nullable BookDataOrchestrator bookDataOrchestrator,
                                   BookSearchService bookSearchService,
                                   BookQueryRepository bookQueryRepository) {
        this.tieredBookSearchService = tieredBookSearchService;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.bookSearchService = bookSearchService;
        this.bookQueryRepository = bookQueryRepository;
    }

    public Mono<SearchPage> search(SearchRequest request) {
        PagingUtils.Window window = PagingUtils.window(
            request.startIndex(),
            request.maxResults(),
            ApplicationConstants.Paging.DEFAULT_SEARCH_LIMIT,
            ApplicationConstants.Paging.MIN_SEARCH_LIMIT,
            ApplicationConstants.Paging.MAX_SEARCH_LIMIT,
            ApplicationConstants.Paging.MAX_TIERED_LIMIT
        );

        if (tieredBookSearchService == null) {
            return performPostgresOnlySearch(request, window);
        }

        long start = System.nanoTime();
        return tieredBookSearchService.streamSearch(request.query(), null, window.totalRequested(), request.orderBy(), false)
            .collectList()
            .defaultIfEmpty(List.of())
            .map(rawResults -> dedupeAndSlice(rawResults, window, request))
            .doOnNext(page -> persistExternalResults(page.uniqueResults()))
            .doOnNext(page -> logPageMetrics(request, window, page, start));
    }

    private Mono<SearchPage> performPostgresOnlySearch(SearchRequest request, PagingUtils.Window window) {
        long start = System.nanoTime();
        return Mono.fromCallable(() -> bookSearchService.searchBooks(request.query(), window.totalRequested()))
            .subscribeOn(Schedulers.boundedElastic())
            .map(results -> results == null ? List.<BookSearchService.SearchResult>of() : results)
            .map(results -> mapPostgresResults(results))
            .map(list -> dedupeAndSlice(list, window, request))
            .doOnNext(page -> logPageMetrics(request, window, page, start));
    }

    private List<Book> mapPostgresResults(List<BookSearchService.SearchResult> results) {
        if (results.isEmpty()) {
            return List.of();
        }
        List<UUID> bookIds = results.stream()
            .map(BookSearchService.SearchResult::bookId)
            .filter(Objects::nonNull)
            .toList();
        if (bookIds.isEmpty()) {
            return List.of();
        }
        List<BookListItem> items = bookQueryRepository.fetchBookListItems(bookIds);
        Map<String, BookListItem> itemsById = items.stream()
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(BookListItem::id, item -> item, (first, second) -> first, LinkedHashMap::new));
        List<Book> ordered = new ArrayList<>(bookIds.size());
        for (BookSearchService.SearchResult result : results) {
            if (result.bookId() == null) {
                continue;
            }
            BookListItem item = itemsById.get(result.bookId().toString());
            if (item == null) {
                continue;
            }
            Book book = BookDomainMapper.fromListItem(item);
            if (book == null) {
                continue;
            }
            book.addQualifier("search.matchType", result.matchTypeNormalised());
            book.addQualifier("search.relevanceScore", result.relevanceScore());
            ordered.add(book);
        }
        return ordered;
    }

    private SearchPage dedupeAndSlice(List<Book> rawResults,
                                      PagingUtils.Window window,
                                      SearchRequest request) {
        LinkedHashMap<String, Book> ordered = new LinkedHashMap<>();
        for (Book book : rawResults) {
            if (book == null) {
                continue;
            }
            String id = book.getId();
            if (!ValidationUtils.hasText(id)) {
                continue;
            }
            ordered.putIfAbsent(id, book);
        }

        List<Book> postgresFirst = new ArrayList<>();
        List<Book> external = new ArrayList<>();
        for (Book candidate : ordered.values()) {
            if (Boolean.TRUE.equals(candidate.getInPostgres())) {
                postgresFirst.add(candidate);
            } else {
                external.add(candidate);
            }
        }

        Comparator<Book> stableExternalOrder = Comparator
            .comparing((Book b) -> Optional.ofNullable(b.getTitle()).orElse("").toLowerCase())
            .thenComparing(b -> Optional.ofNullable(b.getId()).orElse("").toLowerCase());
        external.sort(stableExternalOrder);

        List<Book> uniqueResults = new ArrayList<>(postgresFirst.size() + external.size());
        uniqueResults.addAll(postgresFirst);
        uniqueResults.addAll(external);
        List<Book> pageItems = PagingUtils.slice(uniqueResults, window.startIndex(), window.limit());
        int totalUnique = uniqueResults.size();
        boolean hasMore = PagingUtils.hasMore(totalUnique, window.startIndex(), window.limit());
        int prefetched = PagingUtils.prefetchedCount(totalUnique, window.startIndex(), window.limit());
        int nextStartIndex = hasMore ? window.startIndex() + window.limit() : window.startIndex();

        return new SearchPage(
            request.query(),
            window.startIndex(),
            window.limit(),
            window.totalRequested(),
            totalUnique,
            pageItems,
            uniqueResults,
            hasMore,
            nextStartIndex,
            prefetched,
            request.orderBy()
        );
    }

    private void persistExternalResults(List<Book> candidates) {
        if (bookDataOrchestrator == null || candidates == null || candidates.isEmpty()) {
            return;
        }
        try {
            List<Book> externalOnly = candidates.stream()
                .filter(Objects::nonNull)
                .filter(book -> !Boolean.TRUE.equals(book.getInPostgres()))
                .toList();
            if (!externalOnly.isEmpty()) {
                bookDataOrchestrator.persistBooksAsync(externalOnly, "SEARCH");
            }
        } catch (Exception ex) {
            log.debug("Failed to schedule persistence for search results: {}", ex.getMessage());
        }
    }

    private void logPageMetrics(SearchRequest request,
                                PagingUtils.Window window,
                                SearchPage page,
                                long startNanos) {
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
        log.info(
            "Paginated search '{}' start={} size={} fetched={} hasMore={} prefetched={} ({} ms)",
            request.query(),
            window.startIndex(),
            window.limit(),
            page.totalUnique(),
            page.hasMore(),
            page.prefetchedCount(),
            elapsedMillis
        );
    }

    public record SearchRequest(String query, int startIndex, int maxResults, String orderBy) {
        public SearchRequest {
            Objects.requireNonNull(query, "query");
            orderBy = Optional.ofNullable(orderBy).orElse("newest");
        }
    }

    public record SearchPage(String query,
                             int startIndex,
                             int maxResults,
                             int totalRequested,
                             int totalUnique,
                             List<Book> pageItems,
                             List<Book> uniqueResults,
                             boolean hasMore,
                             int nextStartIndex,
                             int prefetchedCount,
                             String orderBy) {
    }
}
