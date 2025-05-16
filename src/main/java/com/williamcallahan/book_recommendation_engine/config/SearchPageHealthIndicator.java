package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private boolean isConfigured = true;
    private static final Logger logger = LoggerFactory.getLogger(SearchPageHealthIndicator.class);


    public SearchPageHealthIndicator(WebClient.Builder webClientBuilder, ApplicationContext ctx, Environment environment,
                                   @Value("${healthcheck.report-errors-as-down:true}") boolean reportErrorsAsDown) {
        Integer port = environment.getProperty("local.server.port", Integer.class);
        if (port == null && ctx instanceof ReactiveWebServerApplicationContext) {
            try {
                port = ((ReactiveWebServerApplicationContext) ctx).getWebServer().getPort();
            } catch (Exception e) {
                logger.warn("Could not get port from ReactiveWebServerApplicationContext: {}", e.getMessage());
            }
        }

        if (port == null) {
            logger.warn("Server port could not be determined for SearchPageHealthIndicator. Health check will be disabled or report UNKNOWN.");
            this.isConfigured = false;
            // Initialize delegate with null or placeholder values that WebPageHealthIndicator can handle
            this.delegate = new WebPageHealthIndicator(webClientBuilder, null, "/search?query=" + HEALTHCHECK_QUERY, "search_page", reportErrorsAsDown, false);
        } else {
            String baseUrl = "http://localhost:" + port;
            this.delegate = new WebPageHealthIndicator(webClientBuilder, baseUrl, "/search?query=" + HEALTHCHECK_QUERY, "search_page", reportErrorsAsDown, true);
        }
    }

    @Override
    public Mono<Health> health() {
        if (!isConfigured) {
            return Mono.just(Health.unknown().withDetail("reason", "Server port not available for health check").build());
        }
        return delegate.checkPage();
    }
}
