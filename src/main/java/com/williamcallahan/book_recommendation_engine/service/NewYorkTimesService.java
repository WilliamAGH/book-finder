/**
 * Service for interacting with the New York Times Books API
 * This class is responsible for fetching data from the New York Times Best Sellers API
 * It handles:
 * - Constructing and executing API requests to retrieve current bestseller lists
 * - Converting JSON responses from the NYT API into {@link Book} domain objects
 * - Error handling for API communication issues
 * - Transforming NYT book data format into the application's Book model
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

// import java.util.ArrayList; // Unused
import java.util.Collections;
import java.util.List;

import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;

@Service
@Slf4j
public class NewYorkTimesService {
    
    // S3 no longer used as read source for NYT bestsellers
    private final WebClient webClient;
    private final String nytApiBaseUrl;

    private final String nytApiKey;
    private final PostgresBookRepository postgresBookRepository;

    public NewYorkTimesService(WebClient.Builder webClientBuilder,
                               @Value("${nyt.api.base-url:https://api.nytimes.com/svc/books/v3}") String nytApiBaseUrl,
                               @Value("${nyt.api.key}") String nytApiKey,
                               @Nullable PostgresBookRepository postgresBookRepository) {
        this.nytApiBaseUrl = nytApiBaseUrl;
        this.nytApiKey = nytApiKey;
        this.webClient = webClientBuilder.baseUrl(nytApiBaseUrl).build();
        this.postgresBookRepository = postgresBookRepository;
    }

    /**
     * Fetches the full bestseller list overview directly from the New York Times API
     * This is intended for use by the scheduler
     *
     * @return Mono<JsonNode> representing the API response, or empty on error
     */
    public Mono<JsonNode> fetchBestsellerListOverview() {
        String overviewUrl = "/lists/overview.json?api-key=" + nytApiKey;
        log.info("Fetching NYT bestseller list overview from API: {}", nytApiBaseUrl + overviewUrl);
        return webClient.mutate()
                .baseUrl(nytApiBaseUrl)
                .build()
                .get()
                .uri(overviewUrl)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(e -> {
                    LoggingUtils.error(log, e, "Error fetching NYT bestseller list overview from API");
                    return Mono.empty();
                });
    }


    /**
     * Fetch the latest published list's books for the given provider list code from Postgres.
     */
    @Cacheable(value = "nytBestsellersCurrent", key = "#listNameEncoded + '-' + T(com.williamcallahan.book_recommendation_engine.util.PagingUtils).clamp(#limit, 1, 100)")
    public Mono<List<Book>> getCurrentBestSellers(String listNameEncoded, int limit) {
        // Validate and clamp limit to reasonable range
        final int effectiveLimit = PagingUtils.clamp(limit, 1, 100);

        if (postgresBookRepository == null) {
            log.warn("PostgresBookRepository not available; returning empty bestsellers list.");
            return Mono.just(Collections.emptyList());
        }

        return Mono.fromCallable(() ->
            postgresBookRepository.fetchLatestBestsellerBooks(listNameEncoded, effectiveLimit)
        )
        .map(list -> {
            log.info("NYT repository returned {} books for list '{}'", list.size(), listNameEncoded);
            return list;
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(e -> {
            LoggingUtils.error(log, e, "DB error fetching current bestsellers for list '{}'", listNameEncoded);
            return Mono.just(Collections.emptyList());
        });
    }

}
