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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

// import java.util.ArrayList; // Unused
import java.util.Collections;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;

import com.williamcallahan.book_recommendation_engine.util.PagingUtils;

@Service
public class NewYorkTimesService {
    private static final Logger logger = LoggerFactory.getLogger(NewYorkTimesService.class);

    // S3 no longer used as read source for NYT bestsellers
    private final WebClient webClient;
    private final String nytApiBaseUrl;

    private final String nytApiKey;
    private final JdbcTemplate jdbcTemplate;

    public NewYorkTimesService(WebClient.Builder webClientBuilder,
                               @Value("${nyt.api.base-url:https://api.nytimes.com/svc/books/v3}") String nytApiBaseUrl,
                               @Value("${nyt.api.key}") String nytApiKey,
                               JdbcTemplate jdbcTemplate) {
        this.nytApiBaseUrl = nytApiBaseUrl;
        this.nytApiKey = nytApiKey;
        this.webClient = webClientBuilder.baseUrl(nytApiBaseUrl).build();
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Fetches the full bestseller list overview directly from the New York Times API
     * This is intended for use by the scheduler
     *
     * @return Mono<JsonNode> representing the API response, or empty on error
     */
    public Mono<JsonNode> fetchBestsellerListOverview() {
        String overviewUrl = "/lists/overview.json?api-key=" + nytApiKey;
        logger.info("Fetching NYT bestseller list overview from API: {}", nytApiBaseUrl + overviewUrl);
        return webClient.mutate()
                .baseUrl(nytApiBaseUrl)
                .build()
                .get()
                .uri(overviewUrl)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .onErrorResume(e -> {
                    logger.error("Error fetching NYT bestseller list overview from API: {}", e.getMessage(), e);
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

        if (jdbcTemplate == null) {
            logger.warn("JdbcTemplate not available; returning empty bestsellers list.");
            return Mono.just(Collections.emptyList());
        }

        final String sql =
            "SELECT b.id, b.title, b.description, b.s3_image_path, b.isbn10, b.isbn13, b.published_date, " +
            "       b.language, b.publisher, b.page_count " +
            "FROM book_collections bc " +
            "JOIN book_collections_join bcj ON bc.id = bcj.collection_id " +
            "JOIN books b ON b.id = bcj.book_id " +
            "WHERE bc.collection_type = 'BESTSELLER_LIST' AND bc.source = 'NYT' AND bc.provider_list_code = ? " +
            "  AND bc.published_date = (SELECT max(published_date) FROM book_collections WHERE collection_type = 'BESTSELLER_LIST' AND source = 'NYT' AND provider_list_code = ?) " +
            "ORDER BY bcj.position NULLS LAST, b.title ASC " +
            "LIMIT ?";

        return Mono.fromCallable(() ->
            jdbcTemplate.query(sql, (rs, rowNum) -> {
                Book b = new Book();
                b.setId(rs.getString("id"));
                b.setTitle(rs.getString("title"));
                b.setDescription(rs.getString("description"));
                b.setS3ImagePath(rs.getString("s3_image_path"));
                b.setIsbn10(rs.getString("isbn10"));
                b.setIsbn13(rs.getString("isbn13"));
                java.sql.Date published = rs.getDate("published_date");
                if (published != null) {
                    b.setPublishedDate(new java.util.Date(published.getTime()));
                }
                b.setLanguage(rs.getString("language"));
                b.setPublisher(rs.getString("publisher"));
                b.setPageCount((Integer) rs.getObject("page_count"));
                return b;
            }, listNameEncoded, listNameEncoded, effectiveLimit)
        )
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(e -> {
            logger.error("DB error fetching current bestsellers for list '{}': {}", listNameEncoded, e.getMessage(), e);
            return Mono.just(Collections.emptyList());
        });
    }

}
