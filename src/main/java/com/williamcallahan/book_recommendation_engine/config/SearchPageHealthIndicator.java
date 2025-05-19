/**
 * Health indicator for checking search page availability
 *
 * @author William Callahan
 *
 * Features:
 * - Verifies search functionality is operational by checking search page
 * - Uses a predefined query term for consistent health checks
 * - Dynamically configures itself when application server starts
 * - Reports search page status to Spring Boot Actuator health endpoint
 */
package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
@Component("searchPageHealthIndicator")
class SearchPageHealthIndicator implements ReactiveHealthIndicator, ApplicationListener<WebServerInitializedEvent> {
    private volatile WebPageHealthIndicator delegate;
    private final WebClient.Builder webClientBuilder;
    private final boolean reportErrorsAsDown;
    private static final String HEALTHCHECK_QUERY = "healthcheck";
    private volatile boolean isConfigured = false;
    private static final Logger logger = LoggerFactory.getLogger(SearchPageHealthIndicator.class);

    /**
     * Constructs SearchPageHealthIndicator with configurable error handling
     *
     * @param webClientBuilder builder for creating WebClient instances
     * @param reportErrorsAsDown whether to report errors as DOWN instead of UNKNOWN
     */
    public SearchPageHealthIndicator(WebClient.Builder webClientBuilder,
                                   @Value("${healthcheck.report-errors-as-down:true}") boolean reportErrorsAsDown) {
        this.webClientBuilder = webClientBuilder;
        this.reportErrorsAsDown = reportErrorsAsDown;
    }

    /**
     * Configures health indicator when web server initializes
     * 
     * @param event web server initialization event containing server port
     */
    @Override
    public void onApplicationEvent(@NonNull WebServerInitializedEvent event) {
        try {
            int port = event.getWebServer().getPort();
            if (port <= 0) {
                logger.warn("WebServerInitializedEvent reported an invalid port: {}. SearchPageHealthIndicator will remain unconfigured.", port);
                this.isConfigured = false;
                this.delegate = new WebPageHealthIndicator(this.webClientBuilder, null, "/search?query=" + HEALTHCHECK_QUERY, "search_page", this.reportErrorsAsDown, false);
                return;
            }
            String baseUrl = "http://localhost:" + port;
            this.delegate = new WebPageHealthIndicator(this.webClientBuilder, baseUrl, "/search?query=" + HEALTHCHECK_QUERY, "search_page", this.reportErrorsAsDown, true);
            this.isConfigured = true;
            logger.info("SearchPageHealthIndicator configured with port: {}", port);
        } catch (Exception e) {
            logger.error("Error configuring SearchPageHealthIndicator from WebServerInitializedEvent: {}", e.getMessage(), e);
            this.isConfigured = false;
             this.delegate = new WebPageHealthIndicator(this.webClientBuilder, null, "/search?query=" + HEALTHCHECK_QUERY, "search_page", this.reportErrorsAsDown, false);
        }
    }

    /**
     * Checks search page health status
     * 
     * @return health status with details about search page availability
     */
    @Override
    public Mono<Health> health() {
        if (!isConfigured || this.delegate == null) {
            return Mono.just(Health.unknown().withDetail("reason", "Server port not available or health indicator not fully configured for health check").build());
        }
        return delegate.checkPage();
    }
}
