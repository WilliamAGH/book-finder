package com.williamcallahan.book_recommendation_engine.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.BookCollectionPersistenceService;
import com.williamcallahan.book_recommendation_engine.service.BookLookupService;
import com.williamcallahan.book_recommendation_engine.service.BookSupplementalPersistenceService;
import com.williamcallahan.book_recommendation_engine.service.NewYorkTimesService;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.DateParsingUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Locale;

import com.williamcallahan.book_recommendation_engine.util.IsbnUtils;

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
@Slf4j
public class NewYorkTimesBestsellerScheduler {

    private final NewYorkTimesService newYorkTimesService;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final BookLookupService bookLookupService;
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
                                           BookLookupService bookLookupService,
                                           ObjectMapper objectMapper,
                                           JdbcTemplate jdbcTemplate,
                                           BookCollectionPersistenceService collectionPersistenceService,
                                           BookSupplementalPersistenceService supplementalPersistenceService) {
        this.newYorkTimesService = newYorkTimesService;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.bookLookupService = bookLookupService;
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.collectionPersistenceService = collectionPersistenceService;
        this.supplementalPersistenceService = supplementalPersistenceService;
    }

    @Scheduled(cron = "${app.nyt.scheduler.cron:0 0 4 * * SUN}")
    public void processNewYorkTimesBestsellers() {
        if (!schedulerEnabled) {
            log.info("NYT bestseller scheduler disabled via configuration.");
            return;
        }
        if (jdbcTemplate == null) {
            log.warn("JdbcTemplate unavailable; NYT bestseller ingest skipped.");
            return;
        }

        log.info("Starting NYT bestseller ingest.");
        JsonNode overview = newYorkTimesService.fetchBestsellerListOverview()
                .onErrorResume(e -> {
                    LoggingUtils.error(log, e, "Unable to fetch NYT bestseller overview");
                    return Mono.empty();
                })
                .block(Duration.ofMinutes(2));

        if (overview == null || overview.isEmpty()) {
            log.info("NYT overview returned no data. Job complete.");
            return;
        }

        JsonNode results = overview.path("results");
        LocalDate bestsellersDate = parseDate(results.path("bestsellers_date").asText(null));
        LocalDate publishedDate = parseDate(results.path("published_date").asText(null));
        ArrayNode lists = results.has("lists") && results.get("lists").isArray() ? (ArrayNode) results.get("lists") : null;

        if (lists == null || lists.isEmpty()) {
            log.info("NYT overview contained no lists. Job complete.");
            return;
        }

        lists.forEach(listNode -> persistList(listNode, bestsellersDate, publishedDate));
        log.info("NYT bestseller ingest completed successfully.");
    }

    private void persistList(JsonNode listNode, LocalDate bestsellersDate, LocalDate publishedDate) {
        String displayName = listNode.path("display_name").asText(null);
        String listCode = listNode.path("list_name_encoded").asText(null);
        if (listCode == null || listCode.isBlank()) {
            log.warn("Skipping NYT list without list_name_encoded.");
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
            log.warn("Failed to upsert NYT collection for list code {}", listCode);
            return;
        }

        ArrayNode booksNode = listNode.has("books") && listNode.get("books").isArray() ? (ArrayNode) listNode.get("books") : null;
        if (booksNode == null || booksNode.isEmpty()) {
            log.info("NYT list '{}' contained no books.", listCode);
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
            log.warn("Unable to locate or create canonical book for NYT list entry (ISBN13: {}, ISBN10: {}).", isbn13, isbn10);
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
            LoggingUtils.warn(log, e, "Error hydrating book for NYT ingest (ISBN13: {}, ISBN10: {})", isbn13, isbn10);
        }
        return hydrated != null ? hydrated.getId() : resolveCanonicalBookId(isbn13, isbn10);
    }

    private String resolveCanonicalBookId(String isbn13, String isbn10) {
        return bookLookupService.resolveCanonicalBookId(isbn13, isbn10);
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

    private LocalDate parseDate(String date) {
        if (date == null || date.isBlank()) {
            return null;
        }
        LocalDate parsed = DateParsingUtils.parseBestsellerDate(date);
        return parsed;
    }

}
