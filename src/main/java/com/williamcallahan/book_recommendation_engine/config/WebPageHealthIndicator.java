package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Helper class for implementing health check indicators for web pages
 * 
 * @author William Callahan
 * 
 * Provides a reusable mechanism for checking the health of web pages
 * Used by specific health indicator components for different page types
 */
public class WebPageHealthIndicator {

    private final WebClient webClient;
    private final String healthCheckName;
    private final String path;
    private final boolean reportErrorsAsDown;
    private static final Duration PAGE_TIMEOUT = Duration.ofSeconds(5);
    private final boolean isParentConfigured; // New field

    public WebPageHealthIndicator(WebClient.Builder webClientBuilder, String baseUrl, String path, String healthCheckName, boolean reportErrorsAsDown, boolean isParentConfigured) {
        this.isParentConfigured = isParentConfigured;
        if (this.isParentConfigured && baseUrl != null) {
            this.webClient = webClientBuilder.baseUrl(baseUrl).build();
        } else {
            this.webClient = null; // Or a dummy client if preferred, but null is fine if checkPage handles it
        }
        this.path = path;
        this.healthCheckName = healthCheckName;
        this.reportErrorsAsDown = reportErrorsAsDown;
    }

    public Mono<Health> checkPage() {
        if (!this.isParentConfigured || this.webClient == null) {
            return Mono.just(Health.unknown()
                    .withDetail(healthCheckName + "_status", "not_checked_due_to_missing_config")
                    .withDetail("reason", "Base URL or port not available from parent indicator")
                    .build());
        }
        return webClient.get()
                .uri(path)
                .retrieve()
                .toBodilessEntity()
                .map(responseEntity -> {
                    if (responseEntity.getStatusCode().is2xxSuccessful()) {
                        return Health.up()
                                .withDetail(healthCheckName + "_status", "available")
                                .withDetail("path", path)
                                .withDetail("http_status", responseEntity.getStatusCode().value())
                                .build();
                    } else {
                        // Handle 4xx/5xx errors based on the reportErrorsAsDown flag
                        Health.Builder healthBuilder = reportErrorsAsDown ? Health.down() : Health.up();
                        return healthBuilder
                                .withDetail(healthCheckName + "_status", "error_status")
                                .withDetail("path", path)
                                .withDetail("http_status", responseEntity.getStatusCode().value())
                                .build();
                    }
                })
                .timeout(PAGE_TIMEOUT)
                .onErrorResume(WebClientResponseException.class, ex -> {
                    // For client errors (4xx) that throw WebClientResponseException
                    Health.Builder healthBuilder = reportErrorsAsDown ? Health.down() : Health.up();
                    return Mono.just(healthBuilder
                            .withDetail(healthCheckName + "_status", "client_error")
                            .withDetail("path", path)
                            .withDetail("http_status", ex.getStatusCode().value())
                            .withDetail("error_body", ex.getResponseBodyAsString())
                            .build());
                })
                .onErrorResume(Exception.class, ex -> Mono.just(Health.down() // Always DOWN for other exceptions like connection errors or timeouts
                        .withDetail(healthCheckName + "_status", "unavailable_or_timeout")
                        .withDetail("path", path)
                        .withDetail("error", ex.getClass().getName())
                        .withDetail("message", ex.getMessage())
                        .build()));
    }
}

/**
 * Health indicator for checking the homepage availability
 * 
 * @author William Callahan
 */
@Component("homepageHealthIndicator")
class HomepageHealthIndicator implements ReactiveHealthIndicator {
    private final WebPageHealthIndicator delegate;

    public HomepageHealthIndicator(WebClient.Builder webClientBuilder,
                                   @Value("${server.port:8081}") int serverPort,
                                   @Value("${healthcheck.report-errors-as-down:true}") boolean reportErrorsAsDown) {
        String baseUrl = "http://localhost:" + serverPort;
        // Assuming HomepageHealthIndicator is always configured as it gets port directly or defaults
        this.delegate = new WebPageHealthIndicator(webClientBuilder, baseUrl, "/", "homepage", reportErrorsAsDown, true);
    }

    @Override
    public Mono<Health> health() {
        return delegate.checkPage();
    }
}

/**
 * Health indicator for checking book detail page availability
 *
 * @author William Callahan
 */
@Component("bookDetailPageHealthIndicator")
class BookDetailPageHealthIndicator implements ReactiveHealthIndicator {
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
            // Assuming BookDetailPageHealthIndicator is configured if testBookId is present
            this.delegate = new WebPageHealthIndicator(webClientBuilder, baseUrl, "/books/" + this.testBookId, "book_detail_page", reportErrorsAsDown, true);
        } else {
            this.delegate = null; // Delegate remains null if not configured
        }
    }

    @Override
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
