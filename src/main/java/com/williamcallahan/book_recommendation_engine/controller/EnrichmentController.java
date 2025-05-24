/**
 * REST controller for Server-Sent Events (SSE) streaming of book enrichment data
 * Provides real-time updates for book metadata enrichment to frontend clients
 * Enables non-blocking UI updates with progressive data loading
 *
 * @author William Callahan
 *
 * Features:
 * - Server-Sent Events endpoint for homepage enrichment
 * - Reactive streaming of book covers, editions, and recommendations
 * - Non-blocking data delivery for improved user experience
 * - Event-based architecture for real-time UI updates
 */

package com.williamcallahan.book_recommendation_engine.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import com.williamcallahan.book_recommendation_engine.service.BackgroundEnrichmentService;
import com.williamcallahan.book_recommendation_engine.types.EnrichmentEvent;

/**
 * Controller for streaming book enrichment events
 */
@RestController
public class EnrichmentController {

    private final BackgroundEnrichmentService backgroundEnrichmentService;

    /**
     * Constructs enrichment controller with required service dependency
     *
     * @param backgroundEnrichmentService service for book data enrichment
     */
    public EnrichmentController(BackgroundEnrichmentService backgroundEnrichmentService) {
        this.backgroundEnrichmentService = backgroundEnrichmentService;
    }

    /**
     * Streams enrichment events for homepage book data via Server-Sent Events
     * Provides real-time updates for covers, editions, affiliate links, and similar books
     *
     * @return Flux of ServerSentEvent containing EnrichmentEvent data
     */
    @GetMapping(path = "/sse/home", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<EnrichmentEvent>> streamHomeEnrichment() {
        return backgroundEnrichmentService.homeEnrichmentStream()
            .map(event -> ServerSentEvent.<EnrichmentEvent>builder()
                .id(event.getId())
                .event(event.getType())
                .data(event)
                .build()
            );
    }
} 