package com.williamcallahan.book_recommendation_engine.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.BookCollectionPersistenceService;
import com.williamcallahan.book_recommendation_engine.service.BookSupplementalPersistenceService;
import com.williamcallahan.book_recommendation_engine.service.NewYorkTimesService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Locale;

import com.williamcallahan.book_recommendation_engine.util.IsbnUtils;
import com.williamcallahan.book_recommendation_engine.util.JdbcUtils;

/**
 * Scheduler that ingests New York Times bestseller data directly into Postgres.
 * <p>
 * Responsibilities:
 * <ul>
 *     <li>Fetch overview data from the NYT API.</li>
 *     <li>Persist list metadata in {@code book_collections}.</li>
 *     <li>Persist list membership in {@code book_collections_join}.</li>
 *     <li>Ensure canonical books exist (delegating to {@link BookDataOrchestrator}).</li>
 *     <li>Tag books with NYT specific qualifiers via {@code book_tag_assignments}.</li>
 * </ul>
 */
@Component
public class NewYorkTimesBestsellerScheduler {

    private static final Logger logger = LoggerFactory.getLogger(NewYorkTimesBestsellerScheduler.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private final NewYorkTimesService newYorkTimesService;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final BookCollectionPersistenceService collectionPersistenceService;
    private final BookSupplementalPersistenceService supplementalPersistenceService;

    @Value("${app.nyt.scheduler.cron:0 0 4 * * SUN}")
    private String cronExpression;

    @Value("${app.nyt.scheduler.enabled:true}")
    private boolean schedulerEnabled;

    public NewYorkTimesBestsellerScheduler(NewYorkTimesService newYorkTimesService,
                                           BookDataOrchestrator bookDataOrchestrator,
                                           ObjectMapper objectMapper,
                                           JdbcTemplate jdbcTemplate,
                                           BookCollectionPersistenceService collectionPersistenceService,
                                           BookSupplementalPersistenceService supplementalPersistenceService) {
        this.newYorkTimesService = newYorkTimesService;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.collectionPersistenceService = collectionPersistenceService;
        this.supplementalPersistenceService = supplementalPersistenceService;
    }

    @Scheduled(cron = "${app.nyt.scheduler.cron:0 0 4 * * SUN}")
    public void processNewYorkTimesBestsellers() {
        if (!schedulerEnabled) {
            logger.info("NYT bestseller scheduler disabled via configuration.");
            return;
        }
        if (jdbcTemplate == null) {
            logger.warn("JdbcTemplate unavailable; NYT bestseller ingest skipped.");
            return;
        }

        logger.info("Starting NYT bestseller ingest.");
        JsonNode overview = newYorkTimesService.fetchBestsellerListOverview()
                .onErrorResume(e -> {
                    logger.error("Unable to fetch NYT bestseller overview: {}", e.getMessage(), e);
                    return Mono.empty();
                })
                .block(Duration.ofMinutes(2));

        if (overview == null || overview.isEmpty()) {
            logger.info("NYT overview returned no data. Job complete.");
            return;
        }

        JsonNode results = overview.path("results");
        LocalDate bestsellersDate = parseDate(results.path("bestsellers_date").asText(null));
        LocalDate publishedDate = parseDate(results.path("published_date").asText(null));
        ArrayNode lists = results.has("lists") && results.get("lists").isArray() ? (ArrayNode) results.get("lists") : null;

        if (lists == null || lists.isEmpty()) {
            logger.info("NYT overview contained no lists. Job complete.");
            return;
        }

        lists.forEach(listNode -> persistList(listNode, bestsellersDate, publishedDate));
        logger.info("NYT bestseller ingest completed successfully.");
    }

    private void persistList(JsonNode listNode, LocalDate bestsellersDate, LocalDate publishedDate) {
        String displayName = listNode.path("display_name").asText(null);
        String listCode = listNode.path("list_name_encoded").asText(null);
        if (listCode == null || listCode.isBlank()) {
            logger.warn("Skipping NYT list without list_name_encoded.");
            return;
        }

        String providerListId = listNode.path("list_id").asText(null);
        String description = listNode.path("list_name").asText(null);
        String normalized = listCode.toLowerCase(Locale.ROOT);

        LocalDate listPublishedDate = parseDate(listNode.path("published_date").asText(null));
        if (listPublishedDate == null) {
            listPublishedDate = publishedDate;
        }
        String collectionId = collectionPersistenceService
            .upsertBestsellerCollection(providerListId, listCode, displayName, normalized, description, bestsellersDate, listPublishedDate, listNode)
            .orElse(null);

        if (collectionId == null) {
            logger.warn("Failed to upsert NYT collection for list code {}", listCode);
            return;
        }

        ArrayNode booksNode = listNode.has("books") && listNode.get("books").isArray() ? (ArrayNode) listNode.get("books") : null;
        if (booksNode == null || booksNode.isEmpty()) {
            logger.info("NYT list '{}' contained no books.", listCode);
            return;
        }

        booksNode.forEach(bookNode -> persistListEntry(collectionId, listCode, bookNode));
    }

    private void persistListEntry(String collectionId, String listCode, JsonNode bookNode) {
        String isbn13 = IsbnUtils.sanitize(bookNode.path("primary_isbn13").asText(null));
        String isbn10 = IsbnUtils.sanitize(bookNode.path("primary_isbn10").asText(null));

        String canonicalId = resolveCanonicalBookId(isbn13, isbn10);
        if (canonicalId == null) {
            canonicalId = hydrateAndResolve(isbn13, isbn10);
        }

        if (canonicalId == null) {
            logger.warn("Unable to locate or create canonical book for NYT list entry (ISBN13: {}, ISBN10: {}).", isbn13, isbn10);
            return;
        }

        Integer rank = bookNode.path("rank").isInt() ? bookNode.get("rank").asInt() : null;
        Integer weeksOnList = bookNode.path("weeks_on_list").isInt() ? bookNode.get("weeks_on_list").asInt() : null;
        Integer rankLastWeek = bookNode.path("rank_last_week").isInt() ? bookNode.get("rank_last_week").asInt() : null;
        Integer peakPosition = bookNode.path("audiobook_rank").isInt() ? bookNode.get("audiobook_rank").asInt() : null;
        String providerRef = bookNode.path("amazon_product_url").asText(null);

        String rawItem;
        try {
            rawItem = objectMapper.writeValueAsString(bookNode);
        } catch (Exception e) {
            rawItem = null;
        }

        collectionPersistenceService.upsertBestsellerMembership(
            collectionId,
            canonicalId,
            rank,
            weeksOnList,
            rankLastWeek,
            peakPosition,
            isbn13,
            isbn10,
            providerRef,
            rawItem
        );

        assignCoreTags(canonicalId, listCode, rank);
    }

    private String hydrateAndResolve(String isbn13, String isbn10) {
        if (bookDataOrchestrator == null) {
            return null;
        }
        Book hydrated = null;
        try {
            if (isbn13 != null) {
                hydrated = bookDataOrchestrator.getBookByIdTiered(isbn13)
                        .blockOptional(Duration.ofSeconds(10))
                        .orElse(null);
            }
            if (hydrated == null && isbn10 != null) {
                hydrated = bookDataOrchestrator.getBookByIdTiered(isbn10)
                        .blockOptional(Duration.ofSeconds(10))
                        .orElse(null);
            }
        } catch (Exception e) {
            logger.warn("Error hydrating book for NYT ingest (ISBN13: {}, ISBN10: {}): {}", isbn13, isbn10, e.getMessage());
        }
        return hydrated != null ? hydrated.getId() : resolveCanonicalBookId(isbn13, isbn10);
    }

    private String resolveCanonicalBookId(String isbn13, String isbn10) {
        String id = null;
        if (isbn13 != null) {
            id = queryForId("SELECT id FROM books WHERE isbn13 = ? LIMIT 1", isbn13);
            if (id == null) {
                id = queryForId("SELECT book_id FROM book_external_ids WHERE provider_isbn13 = ? LIMIT 1", isbn13);
            }
            if (id != null) {
                return id;
            }
        }
        if (isbn10 != null) {
            id = queryForId("SELECT id FROM books WHERE isbn10 = ? LIMIT 1", isbn10);
            if (id == null) {
                id = queryForId("SELECT book_id FROM book_external_ids WHERE provider_isbn10 = ? LIMIT 1", isbn10);
            }
        }
        return id;
    }

    private String queryForId(String sql, String value) {
        if (value == null) {
            return null;
        }
        return JdbcUtils.optionalString(jdbcTemplate, sql, value).orElse(null);
    }

    private void assignCoreTags(String bookId, String listCode, Integer rank) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("list_code", listCode);
        if (rank != null) {
            metadata.put("rank", rank);
        }
        supplementalPersistenceService.assignTag(bookId, "nyt_bestseller", "NYT Bestseller", "NYT", 1.0, metadata);
        supplementalPersistenceService.assignTag(bookId, "nyt_list_" + listCode.replaceAll("[^a-z0-9]", "_"), "NYT List: " + listCode, "NYT", 1.0, metadata);
    }

    private LocalDate parseDate(String isoDate) {
        if (isoDate == null || isoDate.isBlank()) {
            return null;
        }
        try {
            return LocalDate.parse(isoDate, DATE_FORMATTER);
        } catch (Exception e) {
            return null;
        }
    }

}
