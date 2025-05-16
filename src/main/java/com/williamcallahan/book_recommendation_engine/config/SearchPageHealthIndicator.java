package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component("searchPageHealthIndicator")
class SearchPageHealthIndicator implements ReactiveHealthIndicator {
    private final WebPageHealthIndicator delegate;
    private static final String HEALTHCHECK_QUERY = "healthcheck"; // Fixed query for the health check

    public SearchPageHealthIndicator(WebClient.Builder webClientBuilder, @Value("${server.port:8081}") int serverPort) {
        String baseUrl = "http://localhost:" + serverPort;
        // Check the search page with a predefined query
        this.delegate = new WebPageHealthIndicator(webClientBuilder, baseUrl, "/search?query=" + HEALTHCHECK_QUERY, "search_page");
    }

    @Override
    public Mono<Health> health() {
        return delegate.checkPage();
    }
} 