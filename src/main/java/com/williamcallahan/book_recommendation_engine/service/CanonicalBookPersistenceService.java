package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.util.IdGenerator;
import com.williamcallahan.book_recommendation_engine.util.IsbnUtils;
import com.williamcallahan.book_recommendation_engine.util.JdbcUtils;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import com.williamcallahan.book_recommendation_engine.util.SlugGenerator;
import java.sql.Date;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Handles canonical {@link Book} persistence into Postgres, extracted from
 * {@link BookDataOrchestrator} to keep orchestration logic lightweight and reuseable.
 */
@Component
@ConditionalOnBean(JdbcTemplate.class)
public class CanonicalBookPersistenceService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanonicalBookPersistenceService.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final BookSupplementalPersistenceService supplementalPersistenceService;
    private final BookLookupService bookLookupService;
    private TransactionTemplate transactionTemplate;

    public CanonicalBookPersistenceService(JdbcTemplate jdbcTemplate,
                                           ObjectMapper objectMapper,
                                           BookSupplementalPersistenceService supplementalPersistenceService,
                                           BookLookupService bookLookupService) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.supplementalPersistenceService = supplementalPersistenceService;
        this.bookLookupService = bookLookupService;
    }

    @Autowired
    void setTransactionManager(@Nullable PlatformTransactionManager transactionManager) {
        if (transactionManager != null) {
            this.transactionTemplate = new TransactionTemplate(transactionManager);
        }
    }

    boolean enrichAndSave(Book incoming, JsonNode sourceJson) {
        if (jdbcTemplate == null || incoming == null) {
            return false;
        }

        try {
            String googleId = extractGoogleId(sourceJson, incoming);
            String sanitizedIsbn13 = IsbnUtils.sanitize(incoming.getIsbn13());
            String sanitizedIsbn10 = IsbnUtils.sanitize(incoming.getIsbn10());

            String canonicalId = resolveCanonicalBookId(incoming, googleId, sanitizedIsbn13, sanitizedIsbn10);
            if (canonicalId == null) {
                String incomingId = incoming.getId();
                canonicalId = (incomingId != null && !incomingId.isBlank()) ? incomingId : IdGenerator.uuidV7();
            }

            incoming.setId(canonicalId);
            if (sanitizedIsbn13 != null) {
                incoming.setIsbn13(sanitizedIsbn13);
            }
            if (sanitizedIsbn10 != null) {
                incoming.setIsbn10(sanitizedIsbn10);
            }

            return saveBook(incoming, sourceJson);
        } catch (Exception ex) {
            LoggingUtils.warn(LOGGER, ex, "DB enrich-upsert failed for incoming book {}", incoming != null ? incoming.getId() : null);
            return false;
        }
    }

    boolean saveBook(Book book, JsonNode sourceJson) {
        if (jdbcTemplate == null || book == null) {
            return false;
        }

        Runnable work = () -> persistCanonicalBook(book, sourceJson);

        if (transactionTemplate != null) {
            transactionTemplate.executeWithoutResult(status -> work.run());
        } else {
            work.run();
        }
        return true;
    }

    private void persistCanonicalBook(Book book, JsonNode sourceJson) {
        String googleId = extractGoogleId(sourceJson, book);
        String isbn13 = IsbnUtils.sanitize(book.getIsbn13());
        String isbn10 = IsbnUtils.sanitize(book.getIsbn10());

        if (isbn13 != null) {
            book.setIsbn13(isbn13);
        }
        if (isbn10 != null) {
            book.setIsbn10(isbn10);
        }

        String canonicalId = resolveCanonicalBookId(book, googleId, isbn13, isbn10);
        boolean isNew = false;

        if (canonicalId == null) {
            canonicalId = IdGenerator.uuidV7();
            isNew = true;
        }

        book.setId(canonicalId);

        String slug = resolveSlug(canonicalId, book, isNew);
        upsertBookRecord(book, slug);
        persistDimensions(canonicalId, book);

        if (googleId != null) {
            upsertExternalMetadata(
                canonicalId,
                ApplicationConstants.Provider.GOOGLE_BOOKS,
                googleId,
                book,
                sourceJson
            );
        }

        if (isbn13 != null) {
            upsertExternalMetadata(canonicalId, "ISBN13", isbn13, book, null);
        }
        if (isbn10 != null) {
            upsertExternalMetadata(canonicalId, "ISBN10", isbn10, book, null);
        }

        if (sourceJson != null && sourceJson.size() > 0) {
            persistRawJson(canonicalId, sourceJson, determineSource(sourceJson));
        }

        supplementalPersistenceService.persistAuthors(canonicalId, book.getAuthors());
        supplementalPersistenceService.persistCategories(canonicalId, book.getCategories());
        persistImageLinks(canonicalId, book);
        supplementalPersistenceService.assignQualifierTags(canonicalId, book.getQualifiers());
        synchronizeEditionRelationships(canonicalId, book);
    }

    private String extractGoogleId(JsonNode sourceJson, Book book) {
        if (sourceJson != null && sourceJson.hasNonNull("id")) {
            return sourceJson.get("id").asText();
        }
        String rawId = book.getId();
        if (rawId != null && !looksLikeUuid(rawId)) {
            return rawId;
        }
        return null;
    }

    private String resolveCanonicalBookId(Book book, String googleId, String isbn13, String isbn10) {
        // Use centralized BookLookupService for all ID resolution
        if (bookLookupService == null) {
            LOGGER.warn("BookLookupService is not available, cannot resolve canonical ID");
            return null;
        }

        // Try Google Books external ID first if available
        if (googleId != null) {
            String existing = bookLookupService
                .findBookIdByExternalId(ApplicationConstants.Provider.GOOGLE_BOOKS, googleId)
                .orElse(null);
            if (existing != null) {
                return existing;
            }
        }

        // Try the book's ID if it looks like a UUID
        String potential = book.getId();
        if (potential != null && looksLikeUuid(potential)) {
            String existing = bookLookupService.findBookById(potential).orElse(null);
            if (existing != null) {
                return existing;
            }
        }

        // Try ISBN resolution (ISBN13 first, then ISBN10)
        return bookLookupService.resolveCanonicalBookId(isbn13, isbn10);
    }

    private String queryForId(String sql, Object... params) {
        return JdbcUtils.optionalString(
                jdbcTemplate,
                sql,
                ex -> LOGGER.debug("Query failed: {}", ex.getMessage()),
                params
        ).orElse(null);
    }

    private String resolveSlug(String bookId, Book book, boolean isNew) {
        if (!isNew) {
            String existing = queryForId("SELECT slug FROM books WHERE id = ?", bookId);
            if (existing != null && !existing.isBlank()) {
                return existing;
            }
        }

        String desired = SlugGenerator.generateBookSlug(book.getTitle(), book.getAuthors());
        if (desired == null || desired.isBlank() || jdbcTemplate == null) {
            return null;
        }

        try {
            return jdbcTemplate.queryForObject("SELECT ensure_unique_slug(?)", String.class, desired);
        } catch (DataAccessException ex) {
            return desired;
        }
    }

    private void upsertBookRecord(Book book, String slug) {
        java.util.Date published = book.getPublishedDate();
        Date sqlDate = published != null ? new Date(published.getTime()) : null;

        jdbcTemplate.update(
            "INSERT INTO books (id, title, subtitle, description, isbn10, isbn13, published_date, language, publisher, page_count, edition_number, edition_group_key, slug, s3_image_path, created_at, updated_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
            "ON CONFLICT (id) DO UPDATE SET " +
            "title = EXCLUDED.title, " +
            "subtitle = COALESCE(EXCLUDED.subtitle, books.subtitle), " +
            "description = COALESCE(EXCLUDED.description, books.description), " +
            "isbn10 = COALESCE(EXCLUDED.isbn10, books.isbn10), " +
            "isbn13 = COALESCE(EXCLUDED.isbn13, books.isbn13), " +
            "published_date = COALESCE(EXCLUDED.published_date, books.published_date), " +
            "language = COALESCE(EXCLUDED.language, books.language), " +
            "publisher = COALESCE(EXCLUDED.publisher, books.publisher), " +
            "page_count = COALESCE(EXCLUDED.page_count, books.page_count), " +
            "edition_number = COALESCE(EXCLUDED.edition_number, books.edition_number), " +
            "edition_group_key = COALESCE(EXCLUDED.edition_group_key, books.edition_group_key), " +
            "slug = COALESCE(EXCLUDED.slug, books.slug), " +
            "s3_image_path = COALESCE(EXCLUDED.s3_image_path, books.s3_image_path), " +
            "updated_at = NOW()",
            book.getId(),
            book.getTitle(),
            null,
            book.getDescription(),
            book.getIsbn10(),
            book.getIsbn13(),
            sqlDate,
            book.getLanguage(),
            book.getPublisher(),
            book.getPageCount(),
            book.getEditionNumber(),
            book.getEditionGroupKey(),
            slug,
            book.getS3ImagePath()
        );
    }

    private void upsertExternalMetadata(String bookId, String source, String externalId, Book book, JsonNode sourceJson) {
        if (externalId == null || externalId.isBlank()) {
            return;
        }

        String providerIsbn10 = book.getIsbn10();
        String providerIsbn13 = book.getIsbn13();

        if (providerIsbn13 != null && !providerIsbn13.isBlank()) {
            try {
                List<String> existingIds = jdbcTemplate.query(
                    "SELECT external_id FROM book_external_ids WHERE source = ? AND provider_isbn13 = ? LIMIT 1",
                    (rs, rowNum) -> rs.getString("external_id"),
                    source, providerIsbn13
                );

                if (!existingIds.isEmpty() && !Objects.equals(existingIds.get(0), externalId)) {
                    LOGGER.info("ISBN13 {} already linked via external ID {}, clearing it for new external ID {}",
                        providerIsbn13, existingIds.get(0), externalId);
                    providerIsbn13 = null;
                }
            } catch (DataAccessException ex) {
                LOGGER.debug("Error checking existing ISBN13: {}", ex.getMessage());
            }
        }

        if (providerIsbn10 != null && !providerIsbn10.isBlank()) {
            try {
                List<String> existingIds = jdbcTemplate.query(
                    "SELECT external_id FROM book_external_ids WHERE source = ? AND provider_isbn10 = ? LIMIT 1",
                    (rs, rowNum) -> rs.getString("external_id"),
                    source, providerIsbn10
                );

                if (!existingIds.isEmpty() && !Objects.equals(existingIds.get(0), externalId)) {
                    LOGGER.info("ISBN10 {} already linked via external ID {}, clearing it for new external ID {}",
                        providerIsbn10, existingIds.get(0), externalId);
                    providerIsbn10 = null;
                }
            } catch (DataAccessException ex) {
                LOGGER.debug("Error checking existing ISBN10: {}", ex.getMessage());
            }
        }

        Double listPrice = book.getListPrice();
        String currency = book.getCurrencyCode();
        Double averageRating = book.getAverageRating();
        Integer ratingsCount = book.getRatingsCount();

        jdbcTemplate.update(
            "INSERT INTO book_external_ids (id, book_id, source, external_id, provider_isbn10, provider_isbn13, info_link, preview_link, purchase_link, web_reader_link, average_rating, ratings_count, pdf_available, epub_available, list_price, currency_code, created_at, last_updated) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
            "ON CONFLICT (source, external_id) DO UPDATE SET " +
            "book_id = EXCLUDED.book_id, " +
            "info_link = COALESCE(EXCLUDED.info_link, book_external_ids.info_link), " +
            "preview_link = COALESCE(EXCLUDED.preview_link, book_external_ids.preview_link), " +
            "purchase_link = COALESCE(EXCLUDED.purchase_link, book_external_ids.purchase_link), " +
            "web_reader_link = COALESCE(EXCLUDED.web_reader_link, book_external_ids.web_reader_link), " +
            "average_rating = COALESCE(EXCLUDED.average_rating, book_external_ids.average_rating), " +
            "ratings_count = COALESCE(EXCLUDED.ratings_count, book_external_ids.ratings_count), " +
            "pdf_available = COALESCE(EXCLUDED.pdf_available, book_external_ids.pdf_available), " +
            "epub_available = COALESCE(EXCLUDED.epub_available, book_external_ids.epub_available), " +
            "list_price = COALESCE(EXCLUDED.list_price, book_external_ids.list_price), " +
            "currency_code = COALESCE(EXCLUDED.currency_code, book_external_ids.currency_code), " +
            "last_updated = NOW()",
            IdGenerator.generate(),
            bookId,
            source,
            externalId,
            providerIsbn10,
            providerIsbn13,
            book.getInfoLink(),
            book.getPreviewLink(),
            book.getPurchaseLink(),
            book.getWebReaderLink(),
            averageRating,
            ratingsCount,
            book.getPdfAvailable(),
            book.getEpubAvailable(),
            listPrice,
            currency
        );
    }

    private void persistRawJson(String bookId, JsonNode sourceJson, String source) {
        if (sourceJson == null || sourceJson.isEmpty()) {
            return;
        }
        try {
            String payload = objectMapper.writeValueAsString(sourceJson);
            jdbcTemplate.update(
                "INSERT INTO book_raw_data (id, book_id, raw_json_response, source, fetched_at, contributed_at, created_at) " +
                "VALUES (?, ?, ?::jsonb, ?, NOW(), NOW(), NOW()) " +
                "ON CONFLICT (book_id, source) DO UPDATE SET raw_json_response = EXCLUDED.raw_json_response, fetched_at = NOW(), contributed_at = NOW(), updated_at = NOW()",
                IdGenerator.generate(),
                bookId,
                payload,
                source
            );
        } catch (Exception e) {
            LoggingUtils.warn(LOGGER, e, "Failed to persist raw JSON for book {}", bookId);
        }
    }

    private void persistDimensions(String bookId, Book book) {
        if (jdbcTemplate == null || bookId == null || !looksLikeUuid(bookId)) {
            return;
        }

        Double height = book.getHeightCm();
        Double width = book.getWidthCm();
        Double thickness = book.getThicknessCm();
        Double weight = book.getWeightGrams();

        java.util.UUID canonicalUuid;
        try {
            canonicalUuid = java.util.UUID.fromString(bookId);
        } catch (IllegalArgumentException ex) {
            LOGGER.debug("Skipping dimension persistence for non-UUID id {}", bookId);
            return;
        }

        if (height == null && width == null && thickness == null && weight == null) {
            try {
                jdbcTemplate.update("DELETE FROM book_dimensions WHERE book_id = ?", canonicalUuid);
            } catch (DataAccessException ex) {
                LOGGER.debug("Failed to delete empty dimensions for {}: {}", bookId, ex.getMessage());
            }
            return;
        }

        try {
            jdbcTemplate.update(
                "INSERT INTO book_dimensions (id, book_id, height_cm, width_cm, thickness_cm, weight_grams, created_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, NOW()) " +
                "ON CONFLICT (book_id) DO UPDATE SET " +
                "height_cm = COALESCE(EXCLUDED.height_cm, book_dimensions.height_cm), " +
                "width_cm = COALESCE(EXCLUDED.width_cm, book_dimensions.width_cm), " +
                "thickness_cm = COALESCE(EXCLUDED.thickness_cm, book_dimensions.thickness_cm), " +
                "weight_grams = COALESCE(EXCLUDED.weight_grams, book_dimensions.weight_grams)",
                IdGenerator.generateShort(),
                canonicalUuid,
                height,
                width,
                thickness,
                weight
            );
        } catch (DataAccessException ex) {
            LOGGER.debug("Failed to persist dimensions for {}: {}", bookId, ex.getMessage());
        }
    }

    private void persistImageLinks(String bookId, Book book) {
        CoverImages images = book.getCoverImages();
        if (images == null) {
            images = new CoverImages();
        }

        if (images.getPreferredUrl() != null) {
            upsertImageLink(bookId, "preferred", images.getPreferredUrl(), images.getSource() != null ? images.getSource().name() : null);
        }
        if (images.getFallbackUrl() != null) {
            upsertImageLink(bookId, "fallback", images.getFallbackUrl(), images.getSource() != null ? images.getSource().name() : null);
        }
        if (book.getExternalImageUrl() != null) {
            upsertImageLink(bookId, "external", book.getExternalImageUrl(), "EXTERNAL");
        }
        if (book.getS3ImagePath() != null) {
            upsertImageLink(bookId, "s3", book.getS3ImagePath(), "S3");
        }
    }

    private void upsertImageLink(String bookId, String type, String url, String source) {
        jdbcTemplate.update(
            "INSERT INTO book_image_links (id, book_id, image_type, url, source, created_at) VALUES (?, ?, ?, ?, ?, NOW()) " +
            "ON CONFLICT (book_id, image_type) DO UPDATE SET url = EXCLUDED.url, source = EXCLUDED.source, created_at = book_image_links.created_at",
            IdGenerator.generate(),
            bookId,
            type,
            url,
            source
        );
    }

    private void synchronizeEditionRelationships(String bookId, Book book) {
        if (jdbcTemplate == null || bookId == null || bookId.isBlank()) {
            return;
        }

        String groupKey = book.getEditionGroupKey();
        Integer editionNumber = book.getEditionNumber();

        if (groupKey == null) {
            deleteEditionLinksForBooks(List.of(bookId));
            return;
        }

        List<EditionLinkRecord> siblings = jdbcTemplate.query(
            "SELECT id, edition_number FROM books WHERE edition_group_key = ?",
            ps -> ps.setString(1, groupKey),
            (rs, rowNum) -> {
                String id = rs.getString("id");
                Integer stored = (Integer) rs.getObject("edition_number");
                int normalized = (stored != null && stored > 0) ? stored : 1;
                return new EditionLinkRecord(id, normalized);
            }
        );

        if (siblings.isEmpty()) {
            return;
        }

        List<EditionLinkRecord> normalized = new ArrayList<>(siblings.size());
        for (EditionLinkRecord record : siblings) {
            int number = record.editionNumber();
            if (record.id().equals(bookId) && editionNumber != null) {
                number = PagingUtils.atLeast(editionNumber, 1);
            }
            normalized.add(new EditionLinkRecord(record.id(), PagingUtils.atLeast(number, 1)));
        }

        if (normalized.size() <= 1) {
            deleteEditionLinksForBooks(List.of(bookId));
            return;
        }

        normalized.sort((a, b) -> {
            int diff = Integer.compare(b.editionNumber(), a.editionNumber());
            if (diff != 0) {
                return diff;
            }
            return a.id().compareTo(b.id());
        });

        List<String> groupIds = new ArrayList<>(normalized.size());
        for (EditionLinkRecord record : normalized) {
            groupIds.add(record.id());
        }
        deleteEditionLinksForBooks(groupIds);

        EditionLinkRecord primary = normalized.get(0);
        for (int i = 1; i < normalized.size(); i++) {
            EditionLinkRecord sibling = normalized.get(i);
            jdbcTemplate.update(
                "INSERT INTO book_editions (id, book_id, related_book_id, link_source, relationship_type, created_at, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, NOW(), NOW()) " +
                "ON CONFLICT (book_id, related_book_id) DO UPDATE SET link_source = EXCLUDED.link_source, relationship_type = EXCLUDED.relationship_type, updated_at = NOW()",
                IdGenerator.generateLong(),
                primary.id(),
                sibling.id(),
                "INGESTION",
                "ALTERNATE_EDITION"
            );
        }
    }

    private void deleteEditionLinksForBooks(List<String> ids) {
        if (jdbcTemplate == null || ids == null || ids.isEmpty()) {
            return;
        }
        LinkedHashSet<String> unique = new LinkedHashSet<>();
        for (String id : ids) {
            if (id != null && !id.isBlank()) {
                unique.add(id);
            }
        }
        for (String id : unique) {
            jdbcTemplate.update(
                "DELETE FROM book_editions WHERE book_id = ? OR related_book_id = ?",
                id,
                id
            );
        }
    }

    private String determineSource(JsonNode sourceJson) {
        if (sourceJson == null) {
            return "AGGREGATED";
        }
        if (sourceJson.has("source")) {
            return sourceJson.get("source").asText("AGGREGATED");
        }
        return "AGGREGATED";
    }

    private boolean looksLikeUuid(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        try {
            java.util.UUID.fromString(value);
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    private record EditionLinkRecord(String id, int editionNumber) {}

    // Public wrappers for orchestrator tests
    public String resolveCanonicalBookIdForOrchestrator(Book book, String googleId, String isbn13, String isbn10) {
        return resolveCanonicalBookId(book, googleId, isbn13, isbn10);
    }

    public void synchronizeEditionRelationshipsForOrchestrator(String bookId, Book book) {
        synchronizeEditionRelationships(bookId, book);
    }
}
