package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.time.Duration;

// Not a @Component itself, but a helper class to be used by specific health indicators
public class WebPageHealthIndicator {

    private final WebClient webClient;
    private final String healthCheckName;
    private final String path;
    private static final Duration PAGE_TIMEOUT = Duration.ofSeconds(5);

    public WebPageHealthIndicator(WebClient.Builder webClientBuilder, String baseUrl, String path, String healthCheckName) {
        this.webClient = webClientBuilder.baseUrl(baseUrl).build();
        this.path = path;
        this.healthCheckName = healthCheckName;
    }

    public Mono<Health> checkPage() {
        return webClient.get()
                .uri(path)
                .retrieve()
                .toBodilessEntity() // We only care about the status code
                .map(responseEntity -> {
                    if (responseEntity.getStatusCode().is2xxSuccessful()) {
                        return Health.up()
                                .withDetail(healthCheckName + "_status", "available")
                                .withDetail("path", path)
                                .withDetail("http_status", responseEntity.getStatusCode().value())
                                .build();
                    } else {
                        return Health.up() // Still UP for overall app health
                                .withDetail(healthCheckName + "_status", "error_status")
                                .withDetail("path", path)
                                .withDetail("http_status", responseEntity.getStatusCode().value())
                                .build();
                    }
                })
                .timeout(PAGE_TIMEOUT)
                .onErrorResume(WebClientResponseException.class, ex -> Mono.just(Health.up()
                        .withDetail(healthCheckName + "_status", "client_error")
                        .withDetail("path", path)
                        .withDetail("http_status", ex.getStatusCode().value())
                        .withDetail("error_body", ex.getResponseBodyAsString())
                        .build()))
                .onErrorResume(Exception.class, ex -> Mono.just(Health.up()
                        .withDetail(healthCheckName + "_status", "unavailable_or_timeout")
                        .withDetail("path", path)
                        .withDetail("error", ex.getClass().getName())
                        .withDetail("message", ex.getMessage())
                        .build()));
    }
}

@Component("homepageHealthIndicator")
class HomepageHealthIndicator implements ReactiveHealthIndicator {
    private final WebPageHealthIndicator delegate;

    public HomepageHealthIndicator(WebClient.Builder webClientBuilder, @Value("${server.port:8081}") int serverPort) {
        String baseUrl = "http://localhost:" + serverPort;
        this.delegate = new WebPageHealthIndicator(webClientBuilder, baseUrl, "/", "homepage");
    }

    @Override
    public Mono<Health> health() {
        return delegate.checkPage();
    }
}

@Component("bookDetailPageHealthIndicator")
class BookDetailPageHealthIndicator implements ReactiveHealthIndicator {
    private final WebPageHealthIndicator delegate;
    private final String testBookId;
    private final boolean isConfigured;

    public BookDetailPageHealthIndicator(
            WebClient.Builder webClientBuilder, 
            @Value("${server.port:8081}") int serverPort,
            @Value("${healthcheck.test-book-id:}") String testBookId) {
        this.testBookId = testBookId;
        this.isConfigured = this.testBookId != null && !this.testBookId.trim().isEmpty();
        if (isConfigured) {
            String baseUrl = "http://localhost:" + serverPort;
            this.delegate = new WebPageHealthIndicator(webClientBuilder, baseUrl, "/books/" + this.testBookId, "book_detail_page");
        } else {
            this.delegate = null; // Will be handled in health()
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