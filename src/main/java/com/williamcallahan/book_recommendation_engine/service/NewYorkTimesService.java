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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.config.S3EnvironmentCondition;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
// import java.util.ArrayList; // Unused
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


@Service
@Conditional(S3EnvironmentCondition.class)
public class NewYorkTimesService {
    private static final Logger logger = LoggerFactory.getLogger(NewYorkTimesService.class);

    private final S3StorageService s3StorageService;
    private final ObjectMapper objectMapper;
    private final WebClient webClient;
    private final String nytApiBaseUrl;
    private final Environment environment;
    private final String nytApiKey;

    public NewYorkTimesService(S3StorageService s3StorageService,
                               ObjectMapper objectMapper,
                               WebClient.Builder webClientBuilder,
                               @Value("${nyt.api.base-url:https://api.nytimes.com/svc/books/v3}") String nytApiBaseUrl,
                               @Value("${nyt.api.key}") String nytApiKey,
                               Environment environment) {
        this.s3StorageService = s3StorageService;
        this.objectMapper = objectMapper;
        this.nytApiBaseUrl = nytApiBaseUrl;
        this.nytApiKey = nytApiKey;
        this.webClient = webClientBuilder.baseUrl(nytApiBaseUrl).build();
        this.environment = environment;
    }

    /**
     * Fetches the full bestseller list overview directly from the New York Times API
     * This is intended for use by the scheduler
     *
     * @return Mono<JsonNode> representing the API response, or empty on error
     */
    public Mono<JsonNode> fetchBestsellerListOverview() {
        String overviewUrl = "/lists/overview.json?api-key=" + nytApiKey;
        if (environment.acceptsProfiles(Profiles.of("dev"))) {
            logger.info("[ASYNC_WEBC] Fetching NYT bestseller list overview from API: {}", nytApiBaseUrl + overviewUrl);
        } else {
            logger.info("Fetching NYT bestseller list overview from API: {}", nytApiBaseUrl + overviewUrl);
        }
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
     * Fetches the "current" NYT bestsellers list for a given list name from cache or S3
     * Data is expected to be populated by a scheduled job
     *
     * @param listNameEncoded The URL-encoded name of the bestseller list (e.g., "hardcover-fiction")
     * @param limit maximum number of books to retrieve
     * @return Mono of list of Book. Returns empty list if not found
     */
    @Cacheable(value = "nytBestsellersCurrent", key = "#listNameEncoded + '-' + #limit")
    public Mono<List<Book>> getCurrentBestSellers(String listNameEncoded, int limit) {
        String s3Prefix = "lists/nyt/" + listNameEncoded + "/";
        logger.debug("Attempting to fetch latest NYT list for category '{}' (limit {}) from S3 prefix: {}", listNameEncoded, limit, s3Prefix);

        return Mono.fromFuture(s3StorageService.listObjectsAsync(s3Prefix)) // Use async method
            .flatMap(s3ObjectList -> { // s3ObjectList is List<S3Object>
                if (s3ObjectList == null || s3ObjectList.isEmpty()) {
                    logger.warn("No S3 objects found for prefix '{}'. Returning empty list.", s3Prefix);
                    return Mono.just(Collections.<Book>emptyList());
                }

                Optional<String> latestS3KeyOpt = s3ObjectList.stream()
                    .map(s3Object -> {
                        String key = s3Object.key();
                        // Extract filename, e.g., "2023-10-26.json" from "lists/nyt/hardcover-fiction/2023-10-26.json"
                        String[] parts = key.split("/");
                        return parts.length > 0 ? parts[parts.length - 1] : null;
                    })
                    .filter(filename -> filename != null && filename.endsWith(".json"))
                    .map(filename -> filename.substring(0, filename.length() - 5)) // Remove ".json"
                    .map(dateStr -> {
                        try {
                            return LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
                        } catch (DateTimeParseException e) {
                            logger.trace("Could not parse date from filename '{}' for key prefix '{}'", dateStr, s3Prefix);
                            return null;
                        }
                    })
                    .filter(java.util.Objects::nonNull)
                    .max(Comparator.naturalOrder())
                    .map(latestDate -> s3Prefix + latestDate.format(DateTimeFormatter.ISO_LOCAL_DATE) + ".json");

                if (latestS3KeyOpt.isEmpty()) {
                    logger.warn("No valid dated JSON files found for NYT list category '{}' under S3 prefix '{}'. Returning empty list.", listNameEncoded, s3Prefix);
                    return Mono.just(Collections.<Book>emptyList());
                }

                String latestS3Key = latestS3KeyOpt.get();
                logger.info("Latest S3 key identified for NYT list '{}': {}", listNameEncoded, latestS3Key);

                return Mono.fromFuture(s3StorageService.fetchGenericJsonAsync(latestS3Key))
                    .flatMap(s3Result -> {
                        if (s3Result.isSuccess()) {
                            try {
                                String jsonContent = s3Result.getData().orElseThrow(() ->
                                    new RuntimeException("S3 success result with no data for key: " + latestS3Key));

                                logger.info("Successfully fetched NYT list '{}' from S3 key: {}", listNameEncoded, latestS3Key);
                                NytApiListDetails listDetails = objectMapper.readValue(jsonContent, NytApiListDetails.class);

                                List<Book> books = listDetails.books().stream()
                                    .limit(limit)
                                    .map(this::toBook)
                                    .collect(Collectors.toList());
                                return Mono.just(books);
                            } catch (Exception e) {
                                logger.error("Error deserializing NYT list '{}' from S3 (key: {}): {}. Returning empty list.",
                                             listNameEncoded, latestS3Key, e.getMessage(), e);
                                return Mono.just(Collections.<Book>emptyList());
                            }
                        } else {
                            logger.warn("NYT list '{}' not found or error fetching from S3 (key: {}). Status: {}. Returning empty list.",
                                        listNameEncoded, latestS3Key, s3Result.getStatus());
                            return Mono.just(Collections.<Book>emptyList());
                        }
                    })
                    .onErrorResume(e -> {
                        logger.error("Error during S3 fetch for NYT list '{}' (key: {}): {}. Returning empty list.",
                                     listNameEncoded, latestS3Key, e.getMessage(), e);
                        return Mono.just(Collections.<Book>emptyList());
                    });
            })
            .onErrorResume(e -> { // Handles errors from the Mono.fromCallable or subsequent flatMap
                logger.error("Error processing S3 objects for NYT list prefix '{}': {}. Returning empty list.",
                             s3Prefix, e.getMessage(), e);
                return Mono.just(Collections.<Book>emptyList());
            });
    }

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
        book.setCoverImageUrl(bp.bookImage());
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
    private static record NytApiListDetails(
        @JsonProperty("list_id") int listId,
        @JsonProperty("list_name") String listName,
        @JsonProperty("list_name_encoded") String listNameEncoded,
        @JsonProperty("display_name") String displayName,
        @JsonProperty("updated") String updated, // e.g., "WEEKLY", "MONTHLY"
        @JsonProperty("bestsellers_date") String bestsellersDate, // Date FOR this list data
        @JsonProperty("published_date") String publishedDate, // Date this list was published by NYT
        @JsonProperty("books") List<NytBookPayload> books
        // Other fields from the list overview like 'normal_list_ends_at', 'corrections' can be added if needed
    ) {}

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
