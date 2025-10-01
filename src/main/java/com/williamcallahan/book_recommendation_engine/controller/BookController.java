/**
 * REST controller exposing Postgres-first book APIs.
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.williamcallahan.book_recommendation_engine.controller.dto.BookDto;
import com.williamcallahan.book_recommendation_engine.controller.dto.BookDtoMapper;
import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.dto.BookListItem;
import com.williamcallahan.book_recommendation_engine.dto.RecommendationCard;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.service.BookIdentifierResolver;
import com.williamcallahan.book_recommendation_engine.service.BookSearchService;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import com.williamcallahan.book_recommendation_engine.util.ReactiveControllerUtils;
import com.williamcallahan.book_recommendation_engine.util.SearchQueryUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import com.williamcallahan.book_recommendation_engine.util.SlugGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Comparator;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.williamcallahan.book_recommendation_engine.util.UuidUtils;

@RestController
@RequestMapping("/api/books")
@Slf4j
public class BookController {
    private final BookSearchService bookSearchService;
    private final BookQueryRepository bookQueryRepository;
    private final BookIdentifierResolver bookIdentifierResolver;

    public BookController(BookSearchService bookSearchService,
                          BookQueryRepository bookQueryRepository,
                          BookIdentifierResolver bookIdentifierResolver) {
        this.bookSearchService = bookSearchService;
        this.bookQueryRepository = bookQueryRepository;
        this.bookIdentifierResolver = bookIdentifierResolver;
    }

    @GetMapping("/search")
    public Mono<ResponseEntity<SearchResponse>> searchBooks(@RequestParam String query,
                                                            @RequestParam(name = "startIndex", defaultValue = "0") int startIndex,
                                                            @RequestParam(name = "maxResults", defaultValue = "10") int maxResults,
                                                            @RequestParam(name = "orderBy", defaultValue = "newest") String orderBy) {
        String normalizedQuery = SearchQueryUtils.normalize(query);
        PagingUtils.Window window = PagingUtils.window(
            startIndex,
            maxResults,
            ApplicationConstants.Paging.DEFAULT_SEARCH_LIMIT,
            ApplicationConstants.Paging.MIN_SEARCH_LIMIT,
            ApplicationConstants.Paging.MAX_SEARCH_LIMIT,
            ApplicationConstants.Paging.MAX_TIERED_LIMIT
        );

        return Mono.fromCallable(() -> searchHits(normalizedQuery, window.totalRequested()))
            .subscribeOn(Schedulers.boundedElastic())
            .map(hits -> buildSearchResponse(normalizedQuery, window.startIndex(), window.limit(), hits))
            .map(ResponseEntity::ok)
            .onErrorResume(ex -> {
                log.error("Failed to search books for query '{}': {}", normalizedQuery, ex.getMessage(), ex);
                return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
            });
    }

    @GetMapping("/authors/search")
    public Mono<ResponseEntity<AuthorSearchResponse>> searchAuthors(@RequestParam String query,
                                                                    @RequestParam(name = "limit", defaultValue = "10") int limit) {
        String normalizedQuery = SearchQueryUtils.normalize(query);
        int safeLimit = PagingUtils.safeLimit(
            limit,
            ApplicationConstants.Paging.DEFAULT_AUTHOR_LIMIT,
            ApplicationConstants.Paging.MIN_AUTHOR_LIMIT,
            ApplicationConstants.Paging.MAX_AUTHOR_LIMIT
        );

        return Mono.fromCallable(() -> bookSearchService.searchAuthors(normalizedQuery, safeLimit))
                .subscribeOn(Schedulers.boundedElastic())
                .map(results -> results == null ? List.<BookSearchService.AuthorResult>of() : results)
                .map(results -> buildAuthorResponse(normalizedQuery, safeLimit, results))
                .map(ResponseEntity::ok)
                .onErrorResume(ex -> {
                    log.error("Failed to search authors for query '{}': {}", normalizedQuery, ex.getMessage(), ex);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
                });
    }

    @GetMapping("/{identifier}")
    public Mono<ResponseEntity<BookDto>> getBookByIdentifier(@PathVariable String identifier) {
        return ReactiveControllerUtils.withErrorHandling(
            findBookDto(identifier),
            String.format("Failed to fetch book '%s'", identifier)
        );
    }

    /**
     * Alias route for explicitly slug-based lookups.
     * Delegates to the same fetchBook logic that handles slugs, IDs, ISBNs, etc.
     */
    @GetMapping("/slug/{slug}")
    public Mono<ResponseEntity<BookDto>> getBookBySlug(@PathVariable String slug) {
        return ReactiveControllerUtils.withErrorHandling(
            findBookDto(slug),
            String.format("Failed to fetch book by slug '%s'", slug)
        );
    }

    @GetMapping("/{identifier}/similar")
    public Mono<ResponseEntity<List<BookDto>>> getSimilarBooks(@PathVariable String identifier,
                                                               @RequestParam(name = "limit", defaultValue = "5") int limit) {
        int safeLimit = PagingUtils.safeLimit(
            limit,
            ApplicationConstants.Paging.DEFAULT_SIMILAR_LIMIT,
            ApplicationConstants.Paging.MIN_SEARCH_LIMIT,
            ApplicationConstants.Paging.MAX_SIMILAR_LIMIT
        );
        Mono<List<BookDto>> similarBooks = Mono.defer(() -> {
            Optional<UUID> maybeUuid = bookIdentifierResolver.resolveToUuid(identifier);
            if (maybeUuid.isEmpty()) {
                return Mono.empty();
            }
            return Mono.fromCallable(() -> bookQueryRepository.fetchRecommendationCards(maybeUuid.get(), safeLimit))
                .subscribeOn(Schedulers.boundedElastic())
                .map(cards -> cards.isEmpty() ? List.<BookDto>of() : cards.stream()
                    .map(this::toRecommendationDto)
                    .filter(Objects::nonNull)
                    .toList());
        });

        return ReactiveControllerUtils.withErrorHandling(
            similarBooks,
            String.format("Failed to load similar books for '%s'", identifier)
        );
    }

    private SearchResponse buildSearchResponse(String query,
                                               int startIndex,
                                               int maxResults,
                                               List<SearchHitDto> hits) {
        List<SearchHitDto> safeHits = hits == null ? List.of() : hits;
        List<SearchHitDto> page = PagingUtils.slice(safeHits, startIndex, maxResults);
        return new SearchResponse(query, startIndex, maxResults, safeHits.size(), page);
    }

    private AuthorSearchResponse buildAuthorResponse(String query,
                                                     int limit,
                                                     List<BookSearchService.AuthorResult> results) {
        List<BookSearchService.AuthorResult> safeResults = results == null ? List.of() : results;
        List<AuthorHitDto> hits = safeResults.stream()
                .sorted(Comparator.comparingDouble(BookSearchService.AuthorResult::relevanceScore).reversed())
                .limit(Math.max(0, limit))
                .map(this::toAuthorHit)
                .toList();
        return new AuthorSearchResponse(query, limit, hits);
    }

    private AuthorHitDto toAuthorHit(BookSearchService.AuthorResult authorResult) {
        String effectiveId = authorResult.authorId();
        if (!ValidationUtils.hasText(effectiveId)) {
            String slug = SlugGenerator.slugify(authorResult.authorName());
            if (slug == null || slug.isBlank()) {
                slug = "unknown";
            }
            effectiveId = "external-author-" + slug;
        }
        return new AuthorHitDto(
                effectiveId,
                authorResult.authorName(),
                authorResult.bookCount(),
                authorResult.relevanceScore()
        );
    }

    private List<SearchHitDto> searchHits(String query, int totalRequested) {
        List<BookSearchService.SearchResult> results = bookSearchService.searchBooks(query, totalRequested);
        if (results == null || results.isEmpty()) {
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
            .collect(Collectors.toMap(BookListItem::id, Function.identity(), (first, second) -> first, LinkedHashMap::new));

        List<SearchHitDto> hits = new ArrayList<>(results.size());
        for (BookSearchService.SearchResult result : results) {
            UUID bookId = result.bookId();
            if (bookId == null) {
                continue;
            }
            BookListItem item = itemsById.get(bookId.toString());
            if (item == null) {
                continue;
            }
            Map<String, Object> extras = new LinkedHashMap<>();
            extras.put("search.matchType", result.matchTypeNormalised());
            extras.put("search.relevanceScore", result.relevanceScore());
            BookDto dto = BookDtoMapper.fromListItem(item, extras);
            hits.add(new SearchHitDto(dto, result.matchTypeNormalised(), result.relevanceScore()));
        }

        return hits;
    }

    private Mono<BookDto> findBookDto(String identifier) {
        if (!ValidationUtils.hasText(identifier)) {
            return Mono.empty();
        }

        String trimmed = identifier.trim();
        return Mono.fromCallable(() -> locateBookDto(trimmed))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(dto -> dto == null ? Mono.empty() : Mono.just(dto));
    }

    private BookDto locateBookDto(String identifier) {
        Optional<BookDetail> bySlug = bookQueryRepository.fetchBookDetailBySlug(identifier);
        if (bySlug.isPresent()) {
            return BookDtoMapper.fromDetail(bySlug.get());
        }

        Optional<String> canonicalId = bookIdentifierResolver.resolveCanonicalId(identifier);
        if (canonicalId.isEmpty()) {
            return null;
        }

        UUID uuid = UuidUtils.parseUuidOrNull(canonicalId.get());
        if (uuid == null) {
            return null;
        }

        return bookQueryRepository.fetchBookDetail(uuid)
            .map(BookDtoMapper::fromDetail)
            .orElse(null);
    }

    private BookDto toRecommendationDto(RecommendationCard card) {
        if (card == null || card.card() == null) {
            return null;
        }
        Map<String, Object> extras = new LinkedHashMap<>();
        if (card.score() != null) {
            extras.put("recommendation.score", card.score());
        }
        if (ValidationUtils.hasText(card.reason())) {
            extras.put("recommendation.reason", card.reason());
        }
        return BookDtoMapper.fromCard(card.card(), extras);
    }

    private record SearchResponse(String query,
                                  int startIndex,
                                  int maxResults,
                                  int totalResults,
                                  List<SearchHitDto> results) {
    }

    private record SearchHitDto(@JsonUnwrapped BookDto book, String matchType, Double relevanceScore) {
    }

    private record AuthorSearchResponse(String query,
                                        int limit,
                                        List<AuthorHitDto> results) {
    }

    private record AuthorHitDto(String id,
                                String name,
                                long bookCount,
                                double relevanceScore) {
    }
}
