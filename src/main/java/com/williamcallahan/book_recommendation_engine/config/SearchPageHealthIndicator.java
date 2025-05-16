package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * Health indicator for checking search page availability
 *
 * @author William Callahan
 * 
 * Verifies the search functionality is operational by checking the search page
 * with a predefined query term
 */
@Component("searchPageHealthIndicator")
class SearchPageHealthIndicator implements ReactiveHealthIndicator {
    private final WebPageHealthIndicator delegate;
    private static final String HEALTHCHECK_QUERY = "healthcheck"; // Fixed query for the health check

    public SearchPageHealthIndicator(WebClient.Builder webClientBuilder, ApplicationContext ctx,
                                   @Value("${healthcheck.report-errors-as-down:true}") boolean reportErrorsAsDown) {
        int port = ((ReactiveWebServerApplicationContext) ctx).getWebServer().getPort();
        String baseUrl = "http://localhost:" + port;
        // Check the search page with a predefined query
        this.delegate = new WebPageHealthIndicator(webClientBuilder, baseUrl, "/search?query=" + HEALTHCHECK_QUERY, "search_page", reportErrorsAsDown);
    }

    @Override
    public Mono<Health> health() {
        return delegate.checkPage();
    }
} 