package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.util.ExternalApiLogger;
import com.williamcallahan.book_recommendation_engine.util.IdGenerator;
import com.williamcallahan.book_recommendation_engine.util.IsbnUtils;
import com.williamcallahan.book_recommendation_engine.util.JdbcUtils;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.SlugGenerator;
import com.williamcallahan.book_recommendation_engine.util.UrlUtils;
import com.williamcallahan.book_recommendation_engine.util.UuidUtils;
import java.sql.Date;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.context.ApplicationEventPublisher;
import com.williamcallahan.book_recommendation_engine.service.event.BookUpsertEvent;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Handles canonical {@link Book} persistence into Postgres, extracted from
 * {@link BookDataOrchestrator} to keep orchestration logic lightweight and reuseable.
 * 
 * @deprecated This service is being replaced by {@link BookUpsertService} + {@link com.williamcallahan.book_recommendation_engine.service.image.CoverImageService}
 *             for a cleaner, more maintainable persistence layer. New code should use BookUpsertService.
 *             This class will be removed in a future version after full migration.
 */
@Deprecated(since = "2025-01-30", forRemoval = true)
@Component
@ConditionalOnBean(JdbcTemplate.class)
public class CanonicalBookPersistenceService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CanonicalBookPersistenceService.class);

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final BookSupplementalPersistenceService supplementalPersistenceService;
    private final BookLookupService bookLookupService;
    private final ApplicationEventPublisher eventPublisher;
    private TransactionTemplate transactionTemplate;

    public CanonicalBookPersistenceService(JdbcTemplate jdbcTemplate,
                                           ObjectMapper objectMapper,
                                           BookSupplementalPersistenceService supplementalPersistenceService,
                                           BookLookupService bookLookupService,
                                           ApplicationEventPublisher eventPublisher) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.supplementalPersistenceService = supplementalPersistenceService;
        this.bookLookupService = bookLookupService;
        this.eventPublisher = eventPublisher;
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
            ExternalApiLogger.logPersistenceFailure(
                LOGGER,
                "CANONICAL",
                incoming != null ? incoming.getId() : null,
                ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName());
            return false;
        }
    }

    public boolean saveMinimalBook(Book book, JsonNode sourceJson) {
        if (book == null) {
            LOGGER.warn("saveMinimalBook called with null book");
            return false;
        }
        if (book.getTitle() == null || book.getTitle().isBlank()) {
            LOGGER.warn("saveMinimalBook called with book missing title: {}", book.getId());
            return false;
        }
        LOGGER.info("Attempting to save minimal book: title='{}', isbn13='{}', isbn10='{}', id='{}'",
            book.getTitle(), book.getIsbn13(), book.getIsbn10(), book.getId());
        return saveBook(book, sourceJson);
    }

    boolean saveBook(Book book, JsonNode sourceJson) {
        if (jdbcTemplate == null || book == null) {
            LOGGER.warn("saveBook failed: jdbcTemplate={}, book={}", jdbcTemplate != null, book != null);
            return false;
        }

        try {
            Runnable work = () -> persistCanonicalBook(book, sourceJson);

            if (transactionTemplate != null) {
                transactionTemplate.executeWithoutResult(status -> work.run());
            } else {
                work.run();
            }
            LOGGER.info("Successfully persisted book: id='{}', title='{}'", book.getId(), book.getTitle());
            return true;
        } catch (Exception ex) {
            LoggingUtils.error(LOGGER, ex, "Failed to persist book: id='{}', title='{}', isbn13='{}'",
                book.getId(), book.getTitle(), book.getIsbn13());
            ExternalApiLogger.logPersistenceFailure(
                LOGGER,
                "CANONICAL",
                book != null ? book.getId() : null,
                ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName());
            return false;
        }
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
        ExternalApiLogger.logPersistenceSuccess(LOGGER, "CANONICAL", canonicalId, isNew, book.getTitle());
        try {
            if (eventPublisher != null) {
                eventPublisher.publishEvent(new BookUpsertEvent(canonicalId, slug, book.getTitle(), isNew, "CANONICAL"));
            }
        } catch (Exception ignored) {}
    }

    private String extractGoogleId(JsonNode sourceJson, Book book) {
        if (sourceJson != null && sourceJson.hasNonNull("id")) {
            return sourceJson.get("id").asText();
        }
        String rawId = book.getId();
        if (rawId != null && !UuidUtils.looksLikeUuid(rawId)) {
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
        if (potential != null && UuidUtils.looksLikeUuid(potential)) {
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
            String existing = queryForId("SELECT slug FROM books WHERE id = ?::uuid", bookId);
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

        String isbn10 = book.getIsbn10();
        String isbn13 = book.getIsbn13();
        // Convert empty strings to NULL to avoid unique constraint violations on partial indexes
        if (isbn10 != null && isbn10.isBlank()) isbn10 = null;
        if (isbn13 != null && isbn13.isBlank()) isbn13 = null;
        
        jdbcTemplate.update(
            "INSERT INTO books (id, title, subtitle, description, isbn10, isbn13, published_date, language, publisher, page_count, slug, created_at, updated_at) " +
            "VALUES (?::uuid, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
            "ON CONFLICT (id) DO UPDATE SET " +
            "title = EXCLUDED.title, " +
            "subtitle = COALESCE(NULLIF(EXCLUDED.subtitle, ''), books.subtitle), " +
            "description = COALESCE(NULLIF(EXCLUDED.description, ''), books.description), " +
            "isbn10 = COALESCE(NULLIF(EXCLUDED.isbn10, ''), books.isbn10), " +
            "isbn13 = COALESCE(NULLIF(EXCLUDED.isbn13, ''), books.isbn13), " +
            "published_date = COALESCE(EXCLUDED.published_date, books.published_date), " +
            "language = COALESCE(NULLIF(EXCLUDED.language, ''), books.language), " +
            "publisher = COALESCE(NULLIF(EXCLUDED.publisher, ''), books.publisher), " +
            "page_count = COALESCE(EXCLUDED.page_count, books.page_count), " +
            "slug = COALESCE(NULLIF(EXCLUDED.slug, ''), books.slug), " +
            "updated_at = NOW()",
            book.getId(),
            book.getTitle(),
            null,
            book.getDescription(),
            isbn10,
            isbn13,
            sqlDate,
            book.getLanguage(),
            book.getPublisher(),
            book.getPageCount(),
            slug
        );
    }

    private void upsertExternalMetadata(String bookId, String source, String externalId, Book book, JsonNode sourceJson) {
        if (externalId == null || externalId.isBlank()) {
            return;
        }

        String providerIsbn10 = book.getIsbn10();
        String providerIsbn13 = book.getIsbn13();
        
        // Convert empty strings to NULL for unique constraints
        if (providerIsbn10 != null && providerIsbn10.isBlank()) providerIsbn10 = null;
        if (providerIsbn13 != null && providerIsbn13.isBlank()) providerIsbn13 = null;

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
            "VALUES (?, ?::uuid, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
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
            UrlUtils.normalizeToHttps(book.getInfoLink()),
            UrlUtils.normalizeToHttps(book.getPreviewLink()),
            UrlUtils.normalizeToHttps(book.getPurchaseLink()),
            UrlUtils.normalizeToHttps(book.getWebReaderLink()),
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
                "VALUES (?, ?::uuid, ?::jsonb, ?, NOW(), NOW(), NOW()) " +
                "ON CONFLICT (book_id, source) DO UPDATE SET raw_json_response = EXCLUDED.raw_json_response, fetched_at = NOW(), contributed_at = NOW()",
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
        if (jdbcTemplate == null || bookId == null || !UuidUtils.looksLikeUuid(bookId)) {
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
                "INSERT INTO book_dimensions (book_id, height, width, thickness, weight_grams, created_at, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, NOW(), NOW()) " +
                "ON CONFLICT (book_id) DO UPDATE SET " +
                "height = COALESCE(EXCLUDED.height, book_dimensions.height), " +
                "width = COALESCE(EXCLUDED.width, book_dimensions.width), " +
                "thickness = COALESCE(EXCLUDED.thickness, book_dimensions.thickness), " +
                "weight_grams = COALESCE(EXCLUDED.weight_grams, book_dimensions.weight_grams), " +
                "updated_at = NOW()",
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

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.service.image.CoverImageService#upsertAllAndSetPrimary}
     *             via {@link BookUpsertService} instead. This method only persists basic image URLs without
     *             enhanced metadata (width, height, resolution).
     */
    @Deprecated(since = "2025-01-30", forRemoval = false)
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

    /**
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.service.image.CoverImageService#upsertAllAndSetPrimary}
     *             via {@link BookUpsertService} instead. This method only persists basic image URLs without
     *             enhanced metadata (width, height, resolution).
     */
    @Deprecated(since = "2025-01-30", forRemoval = false)
    private void upsertImageLink(String bookId, String type, String url, String source) {
        // Use shared UrlUtils instead of inline duplication
        String normalizedUrl = UrlUtils.normalizeToHttps(url);
            
        jdbcTemplate.update(
            "INSERT INTO book_image_links (id, book_id, image_type, url, source, created_at) VALUES (?, ?::uuid, ?, ?, ?, NOW()) " +
            "ON CONFLICT (book_id, image_type) DO UPDATE SET url = EXCLUDED.url, source = EXCLUDED.source, created_at = book_image_links.created_at",
            IdGenerator.generate(),
            bookId,
            type,
            normalizedUrl,
            source
        );
    }

    private void synchronizeEditionRelationships(String bookId, Book book) {
        // DISABLED: book_editions table replaced with work_clusters system
        // Edition tracking is now handled by work_cluster_members table
        // See schema.sql comments for migration details
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


    // Public wrappers for orchestrator tests
    public String resolveCanonicalBookIdForOrchestrator(Book book, String googleId, String isbn13, String isbn10) {
        return resolveCanonicalBookId(book, googleId, isbn13, isbn10);
    }

    public void synchronizeEditionRelationshipsForOrchestrator(String bookId, Book book) {
        synchronizeEditionRelationships(bookId, book);
    }
}
