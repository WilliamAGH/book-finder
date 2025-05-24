/**
 * Health indicator for checking book detail page availability
 * Provides reactive up/down status for the /books/{id} endpoint
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * Health indicator for checking book detail page availability
 */

@Component("bookDetailPageHealthIndicator")
public class BookDetailPageHealthIndicator implements ReactiveHealthIndicator {
    private final WebPageHealthIndicator delegate;
    private final String testBookId;
    private final boolean isConfigured;

    public BookDetailPageHealthIndicator(
            WebClient.Builder webClientBuilder,
            @Value("${server.port:8081}") int serverPort,
            @Value("${healthcheck.test-book-id:}") String testBookId,
            @Value("${healthcheck.report-errors-as-down:true}") boolean reportErrorsAsDown) {
        this.testBookId = testBookId;
        this.isConfigured = this.testBookId != null && !this.testBookId.trim().isEmpty();
        if (isConfigured) {
            String baseUrl = "http://localhost:" + serverPort;
            this.delegate = new WebPageHealthIndicator(webClientBuilder, baseUrl, "/books/" + this.testBookId, "book_detail_page", reportErrorsAsDown, true);
        } else {
            this.delegate = null;
        }
    }

    @Override
    /**
     * Check health of the configured book detail page
     */
    public Mono<Health> health() {
        if (!isConfigured) {
            return Mono.just(Health.up()
                .withDetail("book_detail_page_status", "not_configured")
                .withDetail("detail", "Property 'healthcheck.test-book-id' is not set.")
                .build());
        }
        return delegate.checkPage();
    }
} 