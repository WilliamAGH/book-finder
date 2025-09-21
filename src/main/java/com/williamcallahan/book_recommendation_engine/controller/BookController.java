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
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import com.williamcallahan.book_recommendation_engine.util.SearchQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class BookController {

    private static final Logger logger = LoggerFactory.getLogger(BookController.class);

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
                                                            @RequestParam(name = "maxResults", defaultValue = "10") int maxResults) {
        String normalizedQuery = SearchQueryUtils.normalize(query);
        PagingUtils.Window window = PagingUtils.window(startIndex, maxResults, 10, 1, 50, 200);

        return bookDataOrchestrator.searchBooksTiered(normalizedQuery, null, window.totalRequested(), null)
                .defaultIfEmpty(List.of())
                .map(results -> buildSearchResponse(normalizedQuery, window.startIndex(), window.limit(), results))
                .map(ResponseEntity::ok)
                .onErrorResume(ex -> {
                    logger.error("Failed to search books for query '{}': {}", normalizedQuery, ex.getMessage(), ex);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
                });
    }

    @GetMapping("/authors/search")
    public Mono<ResponseEntity<AuthorSearchResponse>> searchAuthors(@RequestParam String query,
                                                                    @RequestParam(name = "limit", defaultValue = "10") int limit) {
        String normalizedQuery = SearchQueryUtils.normalize(query);
        int safeLimit = PagingUtils.safeLimit(limit, 10, 1, 100);

        return bookDataOrchestrator.searchAuthors(normalizedQuery, safeLimit)
                .defaultIfEmpty(List.of())
                .map(results -> buildAuthorResponse(normalizedQuery, safeLimit, results))
                .map(ResponseEntity::ok)
                .onErrorResume(ex -> {
                    logger.error("Failed to search authors for query '{}': {}", normalizedQuery, ex.getMessage(), ex);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
                });
    }

    @GetMapping("/{identifier}")
    public Mono<ResponseEntity<BookDto>> getBookByIdentifier(@PathVariable String identifier) {
        return fetchBook(identifier)
                .map(BookDtoMapper::toDto)
                .map(ResponseEntity::ok)
                .switchIfEmpty(Mono.just(ResponseEntity.notFound().build()))
                .onErrorResume(ex -> {
                    logger.error("Failed to fetch book '{}': {}", identifier, ex.getMessage(), ex);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
                });
    }

    @GetMapping("/{identifier}/similar")
    public Mono<ResponseEntity<List<BookDto>>> getSimilarBooks(@PathVariable String identifier,
                                                               @RequestParam(name = "limit", defaultValue = "5") int limit) {
        int safeLimit = PagingUtils.safeLimit(limit, 5, 1, 20);
        return fetchBook(identifier)
                .flatMap(book -> recommendationService.getSimilarBooks(book.getId(), safeLimit)
                        .defaultIfEmpty(List.of())
                        .map(similar -> similar.stream().map(BookDtoMapper::toDto).toList())
                        .map(ResponseEntity::ok))
                .switchIfEmpty(Mono.just(ResponseEntity.notFound().build()))
                .onErrorResume(ex -> {
                    logger.error("Failed to load similar books for '{}': {}", identifier, ex.getMessage(), ex);
                    return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
                });
    }

    private Mono<Book> fetchBook(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            return Mono.empty();
        }
        Mono<Book> byId = bookDataOrchestrator.getBookByIdTiered(identifier);
        Mono<Book> bySlug = Mono.defer(() -> {
            Mono<Book> lookup = bookDataOrchestrator.getBookBySlugTiered(identifier);
            return lookup == null ? Mono.empty() : lookup;
        });
        return byId.switchIfEmpty(bySlug);
    }

    private SearchResponse buildSearchResponse(String query,
                                               int startIndex,
                                               int maxResults,
                                               List<Book> results) {
        List<Book> safeResults = Objects.requireNonNullElse(results, List.of());
        int total = safeResults.size();
        int fromIndex = Math.min(startIndex, total);
        int toIndex = Math.min(fromIndex + maxResults, total);
        List<SearchHitDto> page = safeResults.subList(fromIndex, toIndex).stream()
                .map(this::toSearchHit)
                .toList();
        return new SearchResponse(query, startIndex, maxResults, total, page);
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
        return new AuthorHitDto(
                authorResult.authorId(),
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
