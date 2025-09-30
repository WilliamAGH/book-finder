/**
 * REST controller exposing Postgres-first book APIs.
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.williamcallahan.book_recommendation_engine.controller.dto.BookDto;
import com.williamcallahan.book_recommendation_engine.controller.dto.BookDtoMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.BookSearchService;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.util.ExternalApiLogger;
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

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Map;

@RestController
@RequestMapping("/api/books")
@Slf4j
public class BookController {


    private final BookDataOrchestrator bookDataOrchestrator;
    private final RecommendationService recommendationService;

    public BookController(BookDataOrchestrator bookDataOrchestrator,
                          RecommendationService recommendationService) {
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.recommendationService = recommendationService;
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

        // Pass orderBy to orchestrator for server-side sorting
        return bookDataOrchestrator.searchBooksTiered(normalizedQuery, null, window.totalRequested(), orderBy)
                .defaultIfEmpty(List.of())
                .map(results -> buildSearchResponse(normalizedQuery, window.startIndex(), window.limit(), results))
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

        return bookDataOrchestrator.searchAuthors(normalizedQuery, safeLimit)
                .defaultIfEmpty(List.of())
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
            fetchBook(identifier).map(BookDtoMapper::toDto),
            String.format("Failed to fetch book '%s'", identifier)
        );
    }

    @GetMapping("/{identifier}/similar")
    public Mono<ResponseEntity<List<BookDto>>> getSimilarBooks(@PathVariable String identifier,
                                                               @RequestParam(name = "limit", defaultValue = "5") int limit) {
        int safeLimit = PagingUtils.safeLimit(
            limit,
            ApplicationConstants.Paging.DEFAULT_SIMILAR_LIMIT,
            ApplicationConstants.Paging.MIN_AUTHOR_LIMIT,
            ApplicationConstants.Paging.MAX_SIMILAR_LIMIT
        );
        Mono<List<BookDto>> similarBooks = fetchBook(identifier)
            .flatMap(book -> recommendationService.getSimilarBooks(book.getId(), safeLimit)
                .defaultIfEmpty(List.of())
                .map(similar -> similar.stream().map(BookDtoMapper::toDto).toList()));

        return ReactiveControllerUtils.withErrorHandling(
            similarBooks,
            String.format("Failed to load similar books for '%s'", identifier)
        );
    }

    private Mono<Book> fetchBook(String identifier) {
        if (ValidationUtils.isNullOrBlank(identifier)) {
            return Mono.empty();
        }

        Mono<Book> canonical = Mono.defer(() -> {
            Mono<Book> lookup = bookDataOrchestrator.fetchCanonicalBookReactive(identifier);
            return lookup != null ? lookup : Mono.empty();
        })
            .doOnNext(book -> {
                if (book != null && ValidationUtils.hasText(book.getId())) {
                    ExternalApiLogger.logHydrationSuccess(log, "DETAIL_CANONICAL", identifier, book.getId(), "POSTGRES");
                }
            });

        Mono<Book> tieredById = Mono.defer(() -> bookDataOrchestrator.getBookByIdTiered(identifier))
            .doOnSubscribe(sub -> ExternalApiLogger.logHydrationStart(log, "DETAIL", identifier, null))
            .doOnNext(book -> {
                if (book != null && ValidationUtils.hasText(book.getId())) {
                    ExternalApiLogger.logHydrationSuccess(log, "DETAIL", identifier, book.getId(), "TIERED_FLOW");
                }
            });

        Mono<Book> tieredBySlug = Mono.defer(() -> bookDataOrchestrator.getBookBySlugTiered(identifier))
            .doOnSubscribe(sub -> ExternalApiLogger.logHydrationStart(log, "DETAIL_SLUG", identifier, null))
            .doOnNext(book -> {
                if (book != null && ValidationUtils.hasText(book.getId())) {
                    ExternalApiLogger.logHydrationSuccess(log, "DETAIL_SLUG", identifier, book.getId(), "TIERED_FLOW");
                }
            });

        return canonical
            .switchIfEmpty(tieredById.switchIfEmpty(tieredBySlug))
            .switchIfEmpty(Mono.defer(() -> {
                ExternalApiLogger.logHydrationFailure(log, "DETAIL", identifier, "NOT_FOUND");
                return Mono.empty();
            }));
    }

    private SearchResponse buildSearchResponse(String query,
                                               int startIndex,
                                               int maxResults,
                                               List<Book> results) {
        List<Book> safeResults = Objects.requireNonNullElse(results, List.of());
        List<Book> windowed = PagingUtils.slice(safeResults, startIndex, maxResults);
        List<SearchHitDto> page = windowed.stream()
                .map(this::toSearchHit)
                .toList();
        return new SearchResponse(query, startIndex, maxResults, safeResults.size(), page);
    }

    private SearchHitDto toSearchHit(Book book) {
        BookDto dto = BookDtoMapper.toDto(book);
        Map<String, Object> extras = dto.extras();
        String matchType = null;
        Double relevance = null;
        if (extras != null && !extras.isEmpty()) {
            Object matchValue = extras.get("search.matchType");
            if (matchValue != null) {
                matchType = matchValue.toString();
            }
            Object scoreValue = extras.get("search.relevanceScore");
            if (scoreValue instanceof Number number) {
                relevance = number.doubleValue();
            } else if (scoreValue != null) {
                try {
                    relevance = Double.parseDouble(scoreValue.toString());
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return new SearchHitDto(dto, matchType, relevance);
    }

    private AuthorSearchResponse buildAuthorResponse(String query,
                                                     int limit,
                                                     List<BookSearchService.AuthorResult> results) {
        List<BookSearchService.AuthorResult> safeResults = results == null ? List.of() : results;
        List<AuthorHitDto> hits = safeResults.stream()
                .sorted(Comparator.comparingDouble(BookSearchService.AuthorResult::relevanceScore).reversed())
                .map(this::toAuthorHit)
                .toList();
        return new AuthorSearchResponse(query, limit, hits);
    }

    private AuthorHitDto toAuthorHit(BookSearchService.AuthorResult authorResult) {
        String effectiveId = authorResult.authorId();
        if (ValidationUtils.isNullOrBlank(effectiveId)) {
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
