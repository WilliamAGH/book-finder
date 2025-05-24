/**
 * Service for asynchronous book data enrichment using reactive streams
 * Provides non-blocking enrichment of book metadata for improved UI responsiveness
 * Orchestrates multiple data sources to enhance book information in parallel
 *
 * @author William Callahan
 *
 * Features:
 * - Reactive stream-based enrichment for non-blocking operations
 * - Parallel processing of covers, editions, affiliate links, and recommendations
 * - Optimized for homepage data enrichment with bestsellers and recent views
 * - Event-driven architecture for real-time UI updates via WebSocket/SSE
 */

package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.image.BookCoverManagementService;
import com.williamcallahan.book_recommendation_engine.types.EnrichmentEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import org.slf4j.Logger; // Added
import org.slf4j.LoggerFactory; // Added
import org.springframework.stereotype.Component;
import java.util.Collections; // Added
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Background enrichment service for asynchronous book data processing
 */
@Component
public class BackgroundEnrichmentService {

    private static final Logger logger = LoggerFactory.getLogger(BackgroundEnrichmentService.class); // Added logger
    private final NewYorkTimesService newYorkTimesService;
    private final RecentlyViewedService recentlyViewedService;
    private final BookCoverManagementService bookCoverManagementService;
    private final DuplicateBookService duplicateBookService;
    private final AffiliateLinkService affiliateLinkService;
    private final RecommendationService recommendationService;

    /** Maximum number of bestsellers to enrich */
    private static final int MAX_BESTSELLERS = 12;
    
    /** Maximum number of recent books to enrich */
    private static final int MAX_RECENTS = 12;
    
    /** Default number of similar books to fetch */
    private static final int DEFAULT_SIMILAR_COUNT = 6;

    /**
     * Constructs enrichment service with required dependencies
     *
     * @param newYorkTimesService service for bestseller data
     * @param recentlyViewedService service for user's recent book views
     * @param bookCoverManagementService service for cover image management
     * @param duplicateBookService service for finding book editions
     * @param affiliateLinkService service for affiliate link generation
     * @param recommendationService service for book recommendations
     */
    public BackgroundEnrichmentService(NewYorkTimesService newYorkTimesService,
                                       RecentlyViewedService recentlyViewedService,
                                       BookCoverManagementService bookCoverManagementService,
                                       DuplicateBookService duplicateBookService,
                                       AffiliateLinkService affiliateLinkService,
                                       RecommendationService recommendationService) {
        this.newYorkTimesService = newYorkTimesService;
        this.recentlyViewedService = recentlyViewedService;
        this.bookCoverManagementService = bookCoverManagementService;
        this.duplicateBookService = duplicateBookService;
        this.affiliateLinkService = affiliateLinkService;
        this.recommendationService = recommendationService;
    }

    /**
     * Creates reactive stream of enrichment events for homepage book data
     * Enriches both bestsellers and recently viewed books with additional metadata
     * 
     * @return Flux of EnrichmentEvent containing covers, editions, affiliate links, and similar books
     */
    public Flux<EnrichmentEvent> homeEnrichmentStream() {
        Mono<List<Book>> bestsellersMono = newYorkTimesService.getCurrentBestSellers("hardcover-fiction", MAX_BESTSELLERS)
            .onErrorReturn(Collections.emptyList());
        Mono<List<Book>> recentsMono = Mono.fromFuture(recentlyViewedService.getRecentlyViewedBooksAsync())
            .map(list -> list.stream().limit(MAX_RECENTS).collect(Collectors.toList()))
            .onErrorReturn(Collections.emptyList());

        return Mono.zip(bestsellersMono, recentsMono)
            .flatMapMany(tuple -> {
                List<Book> bestsellers = tuple.getT1();
                List<Book> recents = tuple.getT2();
                
                Flux<EnrichmentEvent> bestsellerEvents = Flux.fromIterable(bestsellers)
                    .flatMap(book -> {
                        String bookId = book.getId();
                        Flux<EnrichmentEvent> coverFlux = bookCoverManagementService
                            .getInitialCoverUrlAndTriggerBackgroundUpdate(book, true)
                            .map(ci -> new EnrichmentEvent("cover", bookId, ci))
                            .flux()
                            .onErrorResume(e -> {
                                logger.warn("Error fetching cover for book {}: {}", bookId, e.getMessage());
                                return Flux.empty();
                            });
                        Flux<EnrichmentEvent> editionsFlux = duplicateBookService
                            .populateDuplicateEditionsReactive(book)
                            .then(Mono.fromCallable(() -> book.getOtherEditions()))
                            .map(editions -> new EnrichmentEvent("editions", bookId, editions))
                            .flux()
                            .onErrorResume(e -> {
                                logger.warn("Error fetching editions for book {}: {}", bookId, e.getMessage());
                                return Flux.empty();
                            });
                        Flux<EnrichmentEvent> affiliateLinksFlux = Flux.defer(() ->
                            Mono.zip(
                                Mono.fromFuture(affiliateLinkService.generateBarnesAndNobleLink(book.getIsbn13(), null, null)).onErrorReturn(""),
                                Mono.fromFuture(affiliateLinkService.generateBookshopLink(book.getIsbn13(), null)).onErrorReturn(""),
                                Mono.fromFuture(affiliateLinkService.generateAmazonLink(book.getIsbn13(), book.getTitle(), null)).onErrorReturn("")
                            ).map(tup -> Map.of(
                                "barnesandnoble", tup.getT1(),
                                "bookshop", tup.getT2(),
                                "amazon", tup.getT3()
                            )).map(payload -> new EnrichmentEvent("affiliateLinks", bookId, payload))
                        ).onErrorResume(e -> {
                            logger.warn("Error fetching affiliate links for book {}: {}", bookId, e.getMessage());
                            return Mono.empty();
                        });
                        Flux<EnrichmentEvent> similarBooksFlux = recommendationService
                            .getSimilarBooks(bookId, DEFAULT_SIMILAR_COUNT)
                            .map(list -> new EnrichmentEvent("similar", bookId, list))
                            .flux()
                            .onErrorResume(e -> {
                                logger.warn("Error fetching similar books for book {}: {}", bookId, e.getMessage());
                                return Flux.empty();
                            });
                        return Flux.merge(coverFlux, editionsFlux, affiliateLinksFlux, similarBooksFlux);
                    });

                Flux<EnrichmentEvent> recentEvents = Flux.fromIterable(recents)
                    .flatMap(book -> {
                        String bookId = book.getId();
                        Flux<EnrichmentEvent> coverFlux = bookCoverManagementService
                            .getInitialCoverUrlAndTriggerBackgroundUpdate(book, true)
                            .map(ci -> new EnrichmentEvent("cover", bookId, ci))
                            .flux()
                            .onErrorResume(e -> {
                                logger.warn("Error fetching cover for recent book {}: {}", bookId, e.getMessage());
                                return Flux.empty();
                            });
                        Flux<EnrichmentEvent> editionsFlux = duplicateBookService
                            .populateDuplicateEditionsReactive(book)
                            .then(Mono.fromCallable(() -> book.getOtherEditions()))
                            .map(editions -> new EnrichmentEvent("editions", bookId, editions))
                            .flux()
                            .onErrorResume(e -> {
                                logger.warn("Error fetching editions for recent book {}: {}", bookId, e.getMessage());
                                return Flux.empty();
                            });
                        Flux<EnrichmentEvent> affiliateLinksFlux = Flux.defer(() ->
                            Mono.zip(
                                Mono.fromFuture(affiliateLinkService.generateBarnesAndNobleLink(book.getIsbn13(), null, null)).onErrorReturn(""),
                                Mono.fromFuture(affiliateLinkService.generateBookshopLink(book.getIsbn13(), null)).onErrorReturn(""),
                                Mono.fromFuture(affiliateLinkService.generateAmazonLink(book.getIsbn13(), book.getTitle(), null)).onErrorReturn("")
                            ).map(tup -> Map.of(
                                "barnesandnoble", tup.getT1(),
                                "bookshop", tup.getT2(),
                                "amazon", tup.getT3()
                            )).map(payload -> new EnrichmentEvent("affiliateLinks", bookId, payload))
                        ).onErrorResume(e -> {
                            logger.warn("Error fetching affiliate links for recent book {}: {}", bookId, e.getMessage());
                            return Mono.empty();
                        });
                        Flux<EnrichmentEvent> similarBooksFlux = recommendationService
                            .getSimilarBooks(bookId, DEFAULT_SIMILAR_COUNT)
                            .map(list -> new EnrichmentEvent("similar", bookId, list))
                            .flux()
                            .onErrorResume(e -> {
                                logger.warn("Error fetching similar books for recent book {}: {}", bookId, e.getMessage());
                                return Flux.empty();
                            });
                        return Flux.merge(coverFlux, editionsFlux, affiliateLinksFlux, similarBooksFlux);
                    });

                return Flux.merge(bestsellerEvents, recentEvents);
            })
            .subscribeOn(Schedulers.boundedElastic());
    }
}
