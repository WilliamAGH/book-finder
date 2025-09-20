/**
 * REST controller exposing Postgres-first book APIs.
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.controller.dto.BookDto;
import com.williamcallahan.book_recommendation_engine.controller.dto.BookDtoMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
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

import java.util.List;
import java.util.Objects;

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
        String normalizedQuery = normalizeQuery(query);
        int safeStartIndex = Math.max(0, startIndex);
        int safeMaxResults = Math.min(Math.max(maxResults, 1), 50);
        int desiredTotalResults = Math.min(200, safeStartIndex + safeMaxResults);

        return bookDataOrchestrator.searchBooksTiered(normalizedQuery, null, desiredTotalResults, null)
                .defaultIfEmpty(List.of())
                .map(results -> buildSearchResponse(normalizedQuery, safeStartIndex, safeMaxResults, results))
                .map(ResponseEntity::ok)
                .onErrorResume(ex -> {
                    logger.error("Failed to search books for query '{}': {}", normalizedQuery, ex.getMessage(), ex);
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
        int safeLimit = Math.min(Math.max(limit, 1), 20);
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
        List<BookDto> page = safeResults.subList(fromIndex, toIndex).stream()
                .map(BookDtoMapper::toDto)
                .toList();
        return new SearchResponse(query, startIndex, maxResults, total, page);
    }

    private String normalizeQuery(String query) {
        if (query == null) {
            return "*";
        }
        String trimmed = query.trim();
        return trimmed.isEmpty() ? "*" : trimmed;
    }

    private record SearchResponse(String query,
                                  int startIndex,
                                  int maxResults,
                                  int totalResults,
                                  List<BookDto> results) {
    }
}
