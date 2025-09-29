package com.williamcallahan.book_recommendation_engine.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.service.BookCollectionPersistenceService;
import com.williamcallahan.book_recommendation_engine.service.BookLookupService;
import com.williamcallahan.book_recommendation_engine.service.BookSupplementalPersistenceService;
import com.williamcallahan.book_recommendation_engine.service.CanonicalBookPersistenceService;
import com.williamcallahan.book_recommendation_engine.service.NewYorkTimesService;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.DateParsingUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
    private final BookLookupService bookLookupService;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final BookCollectionPersistenceService collectionPersistenceService;
    private final BookSupplementalPersistenceService supplementalPersistenceService;
    private final CanonicalBookPersistenceService canonicalBookPersistenceService;

    @Value("${app.nyt.scheduler.cron:0 0 4 * * SUN}")
    private String cronExpression;

    @Value("${app.nyt.scheduler.enabled:true}")
    private boolean schedulerEnabled;

    @Value("${app.nyt.scheduler.nyt-only:true}")
    private boolean nytOnly;

    public NewYorkTimesBestsellerScheduler(NewYorkTimesService newYorkTimesService,
                                           BookLookupService bookLookupService,
                                           ObjectMapper objectMapper,
                                           JdbcTemplate jdbcTemplate,
                                           BookCollectionPersistenceService collectionPersistenceService,
                                           BookSupplementalPersistenceService supplementalPersistenceService,
                                           @Nullable CanonicalBookPersistenceService canonicalBookPersistenceService) {
        this.newYorkTimesService = newYorkTimesService;
        this.bookLookupService = bookLookupService;
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.collectionPersistenceService = collectionPersistenceService;
        this.supplementalPersistenceService = supplementalPersistenceService;
        this.canonicalBookPersistenceService = canonicalBookPersistenceService;
    }

    @Scheduled(cron = "${app.nyt.scheduler.cron:0 0 4 * * SUN}")
    public void processNewYorkTimesBestsellers() {
        processNewYorkTimesBestsellers(null, false);
    }

    public void processNewYorkTimesBestsellers(@Nullable LocalDate requestedDate) {
        processNewYorkTimesBestsellers(requestedDate, false);
    }

    public void processNewYorkTimesBestsellers(@Nullable LocalDate requestedDate, boolean forceExecution) {
        if (!forceExecution && !schedulerEnabled) {
            log.info("NYT bestseller scheduler disabled via configuration.");
            return;
        }
        assertNytOnly();
        if (jdbcTemplate == null) {
            log.warn("JdbcTemplate unavailable; NYT bestseller ingest skipped.");
            return;
        }

        log.info("Starting NYT bestseller ingest{}.",
            requestedDate != null ? " for " + requestedDate : "");
        JsonNode overview = newYorkTimesService.fetchBestsellerListOverview(requestedDate)
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
        log.info("NYT bestseller ingest completed successfully{}.",
            requestedDate != null ? " for " + requestedDate : "");
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

        // Validate ISBN formats; discard non-ISBN vendor codes (e.g., X0234484)
        if (isbn13 != null && !IsbnUtils.isValidIsbn13(isbn13)) {
            isbn13 = null;
        }
        if (isbn10 != null && !IsbnUtils.isValidIsbn10(isbn10)) {
            isbn10 = null;
        }

        String canonicalId = resolveCanonicalBookId(isbn13, isbn10);
        // IMPORTANT: For NYT ingestion, we DO NOT consult any external sources (Google/OpenLibrary) at all.
        // If the book is not already present in Postgres, create a minimal canonical record directly from NYT data.
        if (canonicalId == null) {
            canonicalId = createCanonicalFromNyt(bookNode, listCode, isbn13, isbn10);
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

    // Removed external hydration path to enforce NYT-only ingestion.

    private String resolveCanonicalBookId(String isbn13, String isbn10) {
        return bookLookupService.resolveCanonicalBookId(isbn13, isbn10);
    }

    private String firstAuthor(JsonNode bookNode) {
        String author = bookNode.path("author").asText(null);
        if (author != null && !author.isBlank()) {
            return author;
        }
        // Some NYT feeds use 'contributor'
        String contributor = bookNode.path("contributor").asText(null);
        if (contributor != null && !contributor.isBlank()) {
            return contributor;
        }
        return null;
    }

    private String createCanonicalFromNyt(JsonNode bookNode, String listCode, String isbn13, String isbn10) {
        if (canonicalBookPersistenceService == null) {
            return null;
        }

        Book stub = buildBookFromNyt(bookNode, listCode, isbn13, isbn10);
        if (stub == null) {
            return null;
        }

        boolean saved = canonicalBookPersistenceService.saveMinimalBook(stub, bookNode);
        if (!saved) {
            return null;
        }
        return stub.getId();
    }

    private Book buildBookFromNyt(JsonNode bookNode, String listCode, String isbn13, String isbn10) {
        String title = firstNonEmptyText(bookNode, "book_title", "title");
        if (!ValidationUtils.hasText(title)) {
            return null;
        }

        Book book = new Book();
        // Normalize title to proper case
        book.setTitle(com.williamcallahan.book_recommendation_engine.util.TextUtils.normalizeBookTitle(title));

        String description = firstNonEmptyText(bookNode, "description", "summary");
        if (ValidationUtils.hasText(description)) {
            book.setDescription(description);
        }

        String publisher = firstNonEmptyText(bookNode, "publisher");
        if (ValidationUtils.hasText(publisher)) {
            book.setPublisher(publisher);
        }

        if (ValidationUtils.hasText(isbn13)) {
            book.setIsbn13(isbn13);
        }
        if (ValidationUtils.hasText(isbn10)) {
            book.setIsbn10(isbn10);
        }

        Date published = parsePublishedDate(bookNode);
        if (published != null) {
            book.setPublishedDate(published);
        }

        List<String> authors = extractAuthors(bookNode);
        if (!authors.isEmpty()) {
            // Normalize author names to proper case
            List<String> normalizedAuthors = authors.stream()
                .map(com.williamcallahan.book_recommendation_engine.util.TextUtils::normalizeAuthorName)
                .collect(java.util.stream.Collectors.toList());
            book.setAuthors(normalizedAuthors);
        }

        String imageUrl = firstNonEmptyText(bookNode, "book_image", "book_image_url");
        if (ValidationUtils.hasText(imageUrl)) {
            book.setExternalImageUrl(imageUrl);
            book.setS3ImagePath(imageUrl);
            CoverImages coverImages = new CoverImages();
            coverImages.setPreferredUrl(imageUrl);
            coverImages.setFallbackUrl(imageUrl);
            coverImages.setSource(CoverImageSource.UNDEFINED);
            book.setCoverImages(coverImages);
        }

        String purchaseUrl = firstNonEmptyText(bookNode, "amazon_product_url");
        if (ValidationUtils.hasText(purchaseUrl)) {
            book.setPurchaseLink(purchaseUrl);
        }

        Map<String, Object> qualifiers = new HashMap<>();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("list_code", listCode);
        if (bookNode.has("rank") && bookNode.get("rank").canConvertToInt()) {
            metadata.put("rank", bookNode.get("rank").asInt());
        }
        qualifiers.put("nytBestseller", metadata);
        book.setQualifiers(qualifiers);

        if (ValidationUtils.hasText(listCode)) {
            List<String> categories = new ArrayList<>();
            categories.add("NYT " + listCode.replace('-', ' '));
            book.setCategories(categories);
        }

        return book;
    }

    private Date parsePublishedDate(JsonNode bookNode) {
        String dateStr = firstNonEmptyText(bookNode, "published_date", "publication_dt", "created_date");
        if (!ValidationUtils.hasText(dateStr)) {
            return null;
        }
        return DateParsingUtils.parseFlexibleDate(dateStr);
    }

    private List<String> extractAuthors(JsonNode bookNode) {
        List<String> authors = new ArrayList<>();
        addAuthors(authors, bookNode.path("author").asText(null));
        addAuthors(authors, bookNode.path("contributor").asText(null));
        addAuthors(authors, bookNode.path("contributor_note").asText(null));
        if (authors.isEmpty()) {
            return List.of();
        }
        LinkedHashSet<String> deduped = new LinkedHashSet<>(authors);
        return new ArrayList<>(deduped);
    }

    private void addAuthors(List<String> authors, @Nullable String raw) {
        if (!ValidationUtils.hasText(raw)) {
            return;
        }
        String normalized = raw.replace(" and ", ",");
        for (String part : normalized.split("[,;&]")) {
            String cleaned = part.trim();
            if (ValidationUtils.hasText(cleaned)) {
                authors.add(cleaned);
            }
        }
    }

    private String firstNonEmptyText(JsonNode node, String... fieldNames) {
        for (String field : fieldNames) {
            if (node.hasNonNull(field)) {
                String value = node.get(field).asText(null);
                if (ValidationUtils.hasText(value)) {
                    return value.trim();
                }
            }
        }
        return null;
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

    private void assertNytOnly() {
        if (!nytOnly) {
            throw new IllegalStateException("NYT-only enforcement is active: set app.nyt.scheduler.nyt-only=true to run this job.");
        }
    }

    private LocalDate parseDate(String date) {
        if (date == null || date.isBlank()) {
            return null;
        }
        LocalDate parsed = DateParsingUtils.parseBestsellerDate(date);
        return parsed;
    }

}
