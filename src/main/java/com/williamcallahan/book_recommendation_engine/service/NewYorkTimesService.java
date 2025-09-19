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

import com.fasterxml.jackson.annotation.JsonProperty;
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
    @Cacheable(value = "nytBestsellersCurrent", key = "#listNameEncoded + '-' + T(java.lang.Math).min(T(java.lang.Math).max(#limit,1),100)")
    public Mono<List<Book>> getCurrentBestSellers(String listNameEncoded, int limit) {
        // Validate and clamp limit to reasonable range
        final int effectiveLimit = Math.max(1, Math.min(limit, 100));

        if (jdbcTemplate == null) {
            logger.warn("JdbcTemplate not available; returning empty bestsellers list.");
            return Mono.just(Collections.emptyList());
        }

        final String sql =
            "SELECT b.id, b.title, b.description, b.\"s3-image-path\" as s3_image_path, b.external_image_url, b.isbn10, b.isbn13, b.published_date, " +
            "       b.language, b.publisher, b.info_link, b.preview_link, b.purchase_link, b.list_price, b.currency_code, " +
            "       b.average_rating, b.ratings_count, b.page_count " +
            "FROM book_lists bl " +
            "JOIN book_lists_join blj ON bl.list_id = blj.list_id " +
            "JOIN books b ON b.id = blj.book_id " +
            "WHERE bl.source = 'NYT' AND bl.provider_list_code = ? " +
            "  AND bl.published_date = (SELECT max(published_date) FROM book_lists WHERE source = 'NYT' AND provider_list_code = ?) " +
            "ORDER BY blj.position NULLS LAST, b.title ASC " +
            "LIMIT ?";

        return Mono.fromCallable(() ->
            jdbcTemplate.query(sql, (rs, rowNum) -> {
                com.williamcallahan.book_recommendation_engine.model.Book b = new com.williamcallahan.book_recommendation_engine.model.Book();
                b.setId(rs.getString("id"));
                b.setTitle(rs.getString("title"));
                b.setDescription(rs.getString("description"));
                try { b.setS3ImagePath(rs.getString("s3_image_path")); } catch (Exception ignored) {}
                try { b.setExternalImageUrl(rs.getString("external_image_url")); } catch (Exception ignored) {}
                b.setIsbn10(rs.getString("isbn10"));
                b.setIsbn13(rs.getString("isbn13"));
                try { java.sql.Date d = rs.getDate("published_date"); b.setPublishedDate(d); } catch (Exception ignored) {}
                b.setLanguage(rs.getString("language"));
                b.setPublisher(rs.getString("publisher"));
                b.setInfoLink(rs.getString("info_link"));
                b.setPreviewLink(rs.getString("preview_link"));
                b.setPurchaseLink(rs.getString("purchase_link"));
                try { java.math.BigDecimal lp = (java.math.BigDecimal) rs.getObject("list_price"); b.setListPrice(lp != null ? lp.doubleValue() : null); } catch (Exception ignored) {}
                b.setCurrencyCode(rs.getString("currency_code"));
                try { java.math.BigDecimal ar = (java.math.BigDecimal) rs.getObject("average_rating"); b.setAverageRating(ar != null ? ar.doubleValue() : null); } catch (Exception ignored) {}
                try { Integer rc = (Integer) rs.getObject("ratings_count"); b.setRatingsCount(rc); } catch (Exception ignored) {}
                try { Integer pc = (Integer) rs.getObject("page_count"); b.setPageCount(pc); } catch (Exception ignored) {}
                return b;
            }, listNameEncoded, listNameEncoded, effectiveLimit)
        )
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(e -> {
            logger.error("DB error fetching current bestsellers for list '{}': {}", listNameEncoded, e.getMessage(), e);
            return Mono.just(Collections.emptyList());
        });
    }

    @SuppressWarnings("unused")
    private Book toBook(NytBookPayload bp) {
        Book book = new Book();
        String id = bp.primaryIsbn13() != null ? bp.primaryIsbn13() : bp.primaryIsbn10();
        if (id == null || id.trim().isEmpty()) {
            // Attempt to find a valid ISBN from the isbns list if primary ones are missing
            if (bp.isbns() != null) {
                for (IsbnPayload isbnEntry : bp.isbns()) {
                    if (isbnEntry.isbn13() != null && !isbnEntry.isbn13().trim().isEmpty()) {
                        id = isbnEntry.isbn13();
                        break;
                    }
                    if (isbnEntry.isbn10() != null && !isbnEntry.isbn10().trim().isEmpty()) {
                        id = isbnEntry.isbn10();
                        break;
                    }
                }
            }
            if (id == null || id.trim().isEmpty()) {
                 logger.warn("Book with title '{}' by {} has no valid ISBNs. Using placeholder ID.", bp.title(), bp.author());
                 id = "MISSING_ID_" + (bp.title() != null ? bp.title().replaceAll("[^a-zA-Z0-9]+", "_") : "UNTITLED") + "_" + (bp.author() != null ? bp.author().replaceAll("[^a-zA-Z0-9]+", "_") : "UNKNOWN");
            }
        }
        book.setId(id);
        book.setTitle(bp.title());
        book.setAuthors(List.of(bp.author() != null ? bp.author() : "Unknown Author"));
        book.setDescription(bp.description());
        book.setPublisher(bp.publisher());
        book.setIsbn10(bp.primaryIsbn10()); // Keep this for consistency, even if ID might be derived from isbns list
        book.setIsbn13(bp.primaryIsbn13());// Keep this for consistency
        book.setExternalImageUrl(bp.bookImage());
        book.setInfoLink(bp.amazonProductUrl() != null ? bp.amazonProductUrl() : "");
        // Potentially map other fields like rank, weeks_on_list if added to Book model
        // book.setRank(bp.rank());
        // book.setWeeksOnList(bp.weeksOnList());
        return book;
    }

    /**
     * Represents the structure of an individual bestseller list details,
     * as it would be stored in S3 by the scheduler
     */
    // Removed unused record NytApiListDetails

    /**
     * Represents an individual book as returned by the NYT API within a list
     */
    private static record NytBookPayload(
        @JsonProperty("rank") int rank,
        @JsonProperty("weeks_on_list") int weeksOnList,
        @JsonProperty("publisher") String publisher,
        @JsonProperty("description") String description,
        @JsonProperty("title") String title,
        @JsonProperty("author") String author,
        @JsonProperty("contributor") String contributor,
        @JsonProperty("contributor_note") String contributorNote,
        @JsonProperty("book_image") String bookImage,
        @JsonProperty("book_image_width") int bookImageWidth,
        @JsonProperty("book_image_height") int bookImageHeight,
        @JsonProperty("amazon_product_url") String amazonProductUrl,
        @JsonProperty("age_group") String ageGroup,
        @JsonProperty("book_review_link") String bookReviewLink,
        @JsonProperty("first_chapter_link") String firstChapterLink,
        @JsonProperty("sunday_review_link") String sundayReviewLink,
        @JsonProperty("article_chapter_link") String articleChapterLink,
        @JsonProperty("isbns") List<IsbnPayload> isbns,
        @JsonProperty("primary_isbn13") String primaryIsbn13,
        @JsonProperty("primary_isbn10") String primaryIsbn10
    ) {}

    private static record IsbnPayload(
        @JsonProperty("isbn10") String isbn10,
        @JsonProperty("isbn13") String isbn13
    ) {}

}
