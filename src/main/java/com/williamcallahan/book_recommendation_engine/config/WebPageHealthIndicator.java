/**
 * Health monitoring system components for web page availability in the book recommendation engine
 * 
 * This file provides:
 * - A reusable WebPageHealthIndicator class for checking web page availability
 * - HomepageHealthIndicator for monitoring the application's homepage
 * - BookDetailPageHealthIndicator for checking book detail pages using a test book ID
 * 
 * These health indicators are used by Spring Boot Actuator to provide health status
 * information through the /actuator/health endpoint, helping with monitoring and
 * diagnostics of the application's frontend availability
 * 
 * @author William Callahan
 */

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

    /**
     * Constructs a WebPageHealthIndicator with all required parameters
     * 
     * @param webClientBuilder The Spring WebClient builder for making HTTP requests
     * @param baseUrl The base URL for the web application (e.g., http://localhost:8081)
     * @param path The path to check (e.g., "/" for homepage or "/books/123" for a book detail page)
     * @param healthCheckName A descriptive name for this health check (used in health status reporting)
     * @param reportErrorsAsDown If true, HTTP errors (4xx, 5xx) will be reported as DOWN; if false as UP with details
     * @param isParentConfigured Indicates if the parent health indicator is properly configured
     * 
     * @implNote Creates a WebClient with the provided base URL if the parent is configured
     */
    public WebPageHealthIndicator(WebClient.Builder webClientBuilder, String baseUrl, String path, String healthCheckName, boolean reportErrorsAsDown, boolean isParentConfigured) {
        this.isParentConfigured = isParentConfigured;
        if (this.isParentConfigured && baseUrl != null) {
            this.webClient = webClientBuilder.baseUrl(baseUrl).build();
        } else {
            this.webClient = null;
        }
        this.path = path;
        this.healthCheckName = healthCheckName;
        this.reportErrorsAsDown = reportErrorsAsDown;
    }

    /**
     * Performs the health check by making an HTTP request to the configured page
     * 
     * @return Mono emitting a Health object with the check result and details
     * 
     * @implNote Checks if the page is accessible with a successful HTTP status (2xx)
     * Handles different error scenarios (4xx, 5xx, timeouts, connection issues)
     * Returns appropriate health status based on configuration and response
     */
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
 */
@Component("homepageHealthIndicator")
class HomepageHealthIndicator implements ReactiveHealthIndicator {
    private final WebPageHealthIndicator delegate;

    /**
     * Constructs a HomepageHealthIndicator with required dependencies
     * 
     * @param webClientBuilder The Spring WebClient builder for making HTTP requests
     * @param serverPort The port on which the server is running (defaults to 8081)
     * @param reportErrorsAsDown If true, HTTP errors will be reported as DOWN status
     * 
     * @implNote Creates a WebPageHealthIndicator delegate to check the homepage (root path)
     */
    public HomepageHealthIndicator(WebClient.Builder webClientBuilder,
                                   @Value("${server.port:8081}") int serverPort,
                                   @Value("${healthcheck.report-errors-as-down:true}") boolean reportErrorsAsDown) {
        String baseUrl = "http://localhost:" + serverPort;
        this.delegate = new WebPageHealthIndicator(webClientBuilder, baseUrl, "/", "homepage", reportErrorsAsDown, true);
    }

    /**
     * Implements the health() method from ReactiveHealthIndicator interface
     * 
     * @return Mono emitting a Health object with the homepage availability status
     * 
     * @implNote Delegates to the WebPageHealthIndicator instance to perform the actual check
     */
    @Override
    public Mono<Health> health() {
        return delegate.checkPage();
    }
}
