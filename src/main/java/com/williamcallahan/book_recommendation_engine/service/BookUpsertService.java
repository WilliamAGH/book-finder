package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.util.DimensionParser;
import com.williamcallahan.book_recommendation_engine.util.IdGenerator;
import com.williamcallahan.book_recommendation_engine.util.SlugGenerator;
import com.williamcallahan.book_recommendation_engine.util.UrlUtils;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Single Source of Truth (SSOT) for ALL book database writes.
 * <p>
 * This service is THE ONLY place that writes to:
 * - books table
 * - authors table
 * - book_authors_join table
 * - book_external_ids table
 * - book_image_links table
 * - book_dimensions table (if dimensions provided)
 * - events_outbox table (transactional event publishing)
 * <p>
 * Key principles:
 * - UPSERT everywhere (INSERT ... ON CONFLICT DO UPDATE)
 * - Update guards: only update if new data is fresher
 * - Transactional: all writes in single transaction
 * - Emits events via outbox pattern for WebSocket delivery
 * - Generates stable, unique slugs
 * <p>
 * Replaces logic from:
 * - CanonicalBookPersistenceService (entire class)
 * - BookSupplementalPersistenceService (persistAuthors, persistCategories)
 * <p>
 * Usage:
 * <pre>
 * BookAggregate aggregate = googleBooksMapper.map(json);
 * UpsertResult result = bookUpsertService.upsert(aggregate);
 * UUID bookId = result.getBookId();
 * String slug = result.getSlug();
 * </pre>
 */
@Service
@Slf4j
public class BookUpsertService {
    
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final BookCollectionPersistenceService collectionPersistenceService;
    
    public BookUpsertService(
        JdbcTemplate jdbcTemplate,
        ObjectMapper objectMapper,
        BookCollectionPersistenceService collectionPersistenceService
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.collectionPersistenceService = collectionPersistenceService;
    }
    
    /**
     * Upserts a book from normalized external data.
     * <p>
     * This is the primary entry point for all book persistence.
     * ALL writes happen in a single transaction.
     * <p>
     * Process:
     * 1. Determine book_id (existing or new UUID v7)
     * 2. Generate unique slug
     * 3. UPSERT books table
     * 4. UPSERT authors and book_authors_join
     * 5. UPSERT book_external_ids
     * 6. UPSERT book_image_links
     * 7. UPSERT book_dimensions (if provided)
     * 8. Emit outbox event (same transaction)
     * <p>
     * Thread-safe and can be called concurrently.
     *
     * @param aggregate Normalized book data from external source
     * @return UpsertResult with bookId, slug, and isNew flag
     */
    @Transactional
    public UpsertResult upsert(BookAggregate aggregate) {
        if (aggregate == null) {
            throw new IllegalArgumentException("BookAggregate cannot be null");
        }
        
        if (aggregate.getTitle() == null || aggregate.getTitle().isBlank()) {
            throw new IllegalArgumentException("Book title is required");
        }
        
        log.debug("Upserting book: title='{}', isbn13='{}', source='{}'", 
            aggregate.getTitle(), 
            aggregate.getIsbn13(),
            aggregate.getIdentifiers() != null ? aggregate.getIdentifiers().getSource() : "UNKNOWN"
        );
        
        // 1. Determine book_id (check if book already exists)
        UUID bookId = findOrCreateBookId(aggregate);
        boolean isNew = bookId == null;
        
        if (isNew) {
            bookId = UUID.fromString(IdGenerator.uuidV7());
            log.info("Creating new book: id={}, title='{}'", bookId, aggregate.getTitle());
        } else {
            log.info("Updating existing book: id={}, title='{}'", bookId, aggregate.getTitle());
        }
        
        // 2. Generate unique slug
        String slug = ensureUniqueSlug(aggregate.getSlugBase(), bookId, isNew);
        
        // 3. UPSERT books table
        upsertBookRecord(bookId, aggregate, slug);
        
        // 4. UPSERT authors
        if (aggregate.getAuthors() != null && !aggregate.getAuthors().isEmpty()) {
            upsertAuthors(bookId, aggregate.getAuthors());
        }
        
        // 5. UPSERT external IDs
        if (aggregate.getIdentifiers() != null) {
            upsertExternalIds(bookId, aggregate.getIdentifiers());
        }
        
        // 6. UPSERT image links
        if (aggregate.getIdentifiers() != null && aggregate.getIdentifiers().getImageLinks() != null) {
            upsertImageLinks(bookId, aggregate.getIdentifiers().getImageLinks());
        }
        
        // 7. UPSERT categories
        if (aggregate.getCategories() != null && !aggregate.getCategories().isEmpty()) {
            upsertCategories(bookId, aggregate.getCategories());
        }
        
        // 8. UPSERT dimensions (if provided)
        if (aggregate.getDimensions() != null) {
            upsertDimensions(bookId, aggregate.getDimensions());
        }
        
        // 9. Emit outbox event (transactional)
        emitOutboxEvent(bookId, slug, aggregate.getTitle(), isNew);
        
        log.info("Successfully upserted book: id={}, slug='{}', isNew={}", bookId, slug, isNew);
        
        return UpsertResult.builder()
            .bookId(bookId)
            .slug(slug)
            .isNew(isNew)
            .build();
    }
    
    /**
     * Find existing book ID or return null for new book.
     * Lookup strategy (in order):
     * 1. External ID (source + externalId)
     * 2. ISBN-13
     * 3. ISBN-10
     */
    private UUID findOrCreateBookId(BookAggregate aggregate) {
        // Try external ID first
        if (aggregate.getIdentifiers() != null) {
            String source = aggregate.getIdentifiers().getSource();
            String externalId = aggregate.getIdentifiers().getExternalId();
            
            if (source != null && externalId != null) {
                UUID existing = findBookByExternalId(source, externalId);
                if (existing != null) {
                    return existing;
                }
            }
        }
        
        // Try ISBN-13
        if (aggregate.getIsbn13() != null) {
            UUID existing = findBookByIsbn13(aggregate.getIsbn13());
            if (existing != null) {
                return existing;
            }
        }
        
        // Try ISBN-10
        if (aggregate.getIsbn10() != null) {
            UUID existing = findBookByIsbn10(aggregate.getIsbn10());
            if (existing != null) {
                return existing;
            }
        }
        
        return null; // Book doesn't exist
    }
    
    /**
     * Find book by external ID.
     */
    private UUID findBookByExternalId(String source, String externalId) {
        try {
            return jdbcTemplate.query(
                "SELECT book_id FROM book_external_ids WHERE source = ? AND external_id = ? LIMIT 1",
                rs -> rs.next() ? (UUID) rs.getObject("book_id") : null,
                source, externalId
            );
        } catch (Exception e) {
            log.debug("Error finding book by external ID: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Find book by ISBN-13.
     */
    private UUID findBookByIsbn13(String isbn13) {
        try {
            return jdbcTemplate.query(
                "SELECT id FROM books WHERE isbn13 = ? LIMIT 1",
                rs -> rs.next() ? (UUID) rs.getObject("id") : null,
                isbn13
            );
        } catch (Exception e) {
            log.debug("Error finding book by ISBN-13: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Find book by ISBN-10.
     */
    private UUID findBookByIsbn10(String isbn10) {
        try {
            return jdbcTemplate.query(
                "SELECT id FROM books WHERE isbn10 = ? LIMIT 1",
                rs -> rs.next() ? (UUID) rs.getObject("id") : null,
                isbn10
            );
        } catch (Exception e) {
            log.debug("Error finding book by ISBN-10: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Generate unique slug for book.
     * If book exists, keeps existing slug (stability).
     * If new or slug collision, generates unique slug.
     */
    private String ensureUniqueSlug(String slugBase, UUID bookId, boolean isNew) {
        // For existing books, keep current slug
        if (!isNew) {
            try {
                String existing = jdbcTemplate.query(
                    "SELECT slug FROM books WHERE id = ?",
                    rs -> rs.next() ? rs.getString("slug") : null,
                    bookId
                );
                if (existing != null && !existing.isBlank()) {
                    return existing;
                }
            } catch (Exception e) {
                log.debug("Error fetching existing slug: {}", e.getMessage());
            }
        }
        
        // Generate new slug
        if (slugBase == null || slugBase.isBlank()) {
            return null;
        }
        
        // Use database function to ensure uniqueness
        try {
            return jdbcTemplate.queryForObject(
                "SELECT ensure_unique_slug(?)",
                String.class,
                slugBase
            );
        } catch (Exception e) {
            log.warn("Error generating unique slug, using base: {}", e.getMessage());
            return slugBase;
        }
    }
    
    /**
     * UPSERT books table with COALESCE guards.
     * Only updates if new data is present (COALESCE keeps existing if new is null).
     */
    private void upsertBookRecord(UUID bookId, BookAggregate aggregate, String slug) {
        Date sqlDate = aggregate.getPublishedDate() != null 
            ? Date.valueOf(aggregate.getPublishedDate()) 
            : null;
        
        // Convert empty strings to null for unique constraints
        String isbn10 = nullIfBlank(aggregate.getIsbn10());
        String isbn13 = nullIfBlank(aggregate.getIsbn13());
        
        jdbcTemplate.update(
            """
            INSERT INTO books (
                id, title, subtitle, description, isbn10, isbn13, 
                published_date, language, publisher, page_count, slug, 
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                subtitle = COALESCE(NULLIF(EXCLUDED.subtitle, ''), books.subtitle),
                description = COALESCE(NULLIF(EXCLUDED.description, ''), books.description),
                isbn10 = COALESCE(NULLIF(EXCLUDED.isbn10, ''), books.isbn10),
                isbn13 = COALESCE(NULLIF(EXCLUDED.isbn13, ''), books.isbn13),
                published_date = COALESCE(EXCLUDED.published_date, books.published_date),
                language = COALESCE(NULLIF(EXCLUDED.language, ''), books.language),
                publisher = COALESCE(NULLIF(EXCLUDED.publisher, ''), books.publisher),
                page_count = COALESCE(EXCLUDED.page_count, books.page_count),
                slug = COALESCE(NULLIF(EXCLUDED.slug, ''), books.slug),
                updated_at = NOW()
            """,
            bookId,
            aggregate.getTitle(),
            aggregate.getSubtitle(),
            aggregate.getDescription(),
            isbn10,
            isbn13,
            sqlDate,
            aggregate.getLanguage(),
            aggregate.getPublisher(),
            aggregate.getPageCount(),
            slug
        );
    }
    
    /**
     * UPSERT authors and book_authors_join table.
     * Authors are deduplicated by name (case-sensitive).
     * Position is preserved for author ordering.
     */
    private void upsertAuthors(UUID bookId, List<String> authors) {
        int position = 0;
        for (String authorName : authors) {
            if (authorName == null || authorName.isBlank()) {
                continue;
            }
            
            String normalized = authorName.toLowerCase()
                .replaceAll("[^a-z0-9\\s]", "")
                .trim();
            
            String authorId = upsertAuthor(authorName, normalized);
            
            // Link book to author
            jdbcTemplate.update(
                """
                INSERT INTO book_authors_join (id, book_id, author_id, position, created_at)
                VALUES (?, ?, ?, ?, NOW())
                ON CONFLICT (book_id, author_id) DO UPDATE SET
                    position = EXCLUDED.position
                """,
                IdGenerator.generateLong(),
                bookId,
                authorId,
                position++
            );
        }
    }
    
    /**
     * UPSERT single author.
     * Returns authorId (existing or new).
     */
    private String upsertAuthor(String name, String normalized) {
        try {
            return jdbcTemplate.queryForObject(
                """
                INSERT INTO authors (id, name, normalized_name, created_at, updated_at)
                VALUES (?, ?, ?, NOW(), NOW())
                ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
                RETURNING id
                """,
                (rs, rowNum) -> rs.getString("id"),
                IdGenerator.generate(),
                name,
                normalized
            );
        } catch (Exception e) {
            // Fallback: fetch existing
            log.debug("Error upserting author, fetching existing: {}", e.getMessage());
            return jdbcTemplate.query(
                "SELECT id FROM authors WHERE name = ? LIMIT 1",
                rs -> rs.next() ? rs.getString("id") : null,
                name
            );
        }
    }
    
    /**
     * UPSERT book_external_ids table.
     * Links book to external provider identifiers and metadata.
     */
    private void upsertExternalIds(UUID bookId, BookAggregate.ExternalIdentifiers identifiers) {
        String source = identifiers.getSource();
        String externalId = identifiers.getExternalId();
        
        if (source == null || externalId == null) {
            log.warn("Cannot upsert external ID without source and externalId");
            return;
        }
        
        // Convert empty strings to null
        String providerIsbn10 = nullIfBlank(identifiers.getProviderIsbn10());
        String providerIsbn13 = nullIfBlank(identifiers.getProviderIsbn13());
        
        jdbcTemplate.update(
            """
            INSERT INTO book_external_ids (
                id, book_id, source, external_id,
                provider_isbn10, provider_isbn13,
                info_link, preview_link, web_reader_link, purchase_link, canonical_volume_link,
                average_rating, ratings_count, review_count,
                is_ebook, pdf_available, epub_available, embeddable, public_domain,
                viewability, text_readable, image_readable,
                print_type, maturity_rating, content_version, text_to_speech_permission,
                saleability, country_code, is_ebook_for_sale,
                list_price, retail_price, currency_code,
                oclc_work_id, openlibrary_work_id, goodreads_work_id, google_canonical_id,
                created_at, last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
            ON CONFLICT (source, external_id) DO UPDATE SET
                book_id = EXCLUDED.book_id,
                info_link = COALESCE(EXCLUDED.info_link, book_external_ids.info_link),
                preview_link = COALESCE(EXCLUDED.preview_link, book_external_ids.preview_link),
                web_reader_link = COALESCE(EXCLUDED.web_reader_link, book_external_ids.web_reader_link),
                purchase_link = COALESCE(EXCLUDED.purchase_link, book_external_ids.purchase_link),
                canonical_volume_link = COALESCE(EXCLUDED.canonical_volume_link, book_external_ids.canonical_volume_link),
                average_rating = COALESCE(EXCLUDED.average_rating, book_external_ids.average_rating),
                ratings_count = COALESCE(EXCLUDED.ratings_count, book_external_ids.ratings_count),
                review_count = COALESCE(EXCLUDED.review_count, book_external_ids.review_count),
                is_ebook = COALESCE(EXCLUDED.is_ebook, book_external_ids.is_ebook),
                pdf_available = COALESCE(EXCLUDED.pdf_available, book_external_ids.pdf_available),
                epub_available = COALESCE(EXCLUDED.epub_available, book_external_ids.epub_available),
                embeddable = COALESCE(EXCLUDED.embeddable, book_external_ids.embeddable),
                public_domain = COALESCE(EXCLUDED.public_domain, book_external_ids.public_domain),
                viewability = COALESCE(EXCLUDED.viewability, book_external_ids.viewability),
                text_readable = COALESCE(EXCLUDED.text_readable, book_external_ids.text_readable),
                image_readable = COALESCE(EXCLUDED.image_readable, book_external_ids.image_readable),
                print_type = COALESCE(EXCLUDED.print_type, book_external_ids.print_type),
                maturity_rating = COALESCE(EXCLUDED.maturity_rating, book_external_ids.maturity_rating),
                content_version = COALESCE(EXCLUDED.content_version, book_external_ids.content_version),
                text_to_speech_permission = COALESCE(EXCLUDED.text_to_speech_permission, book_external_ids.text_to_speech_permission),
                saleability = COALESCE(EXCLUDED.saleability, book_external_ids.saleability),
                country_code = COALESCE(EXCLUDED.country_code, book_external_ids.country_code),
                is_ebook_for_sale = COALESCE(EXCLUDED.is_ebook_for_sale, book_external_ids.is_ebook_for_sale),
                list_price = COALESCE(EXCLUDED.list_price, book_external_ids.list_price),
                retail_price = COALESCE(EXCLUDED.retail_price, book_external_ids.retail_price),
                currency_code = COALESCE(EXCLUDED.currency_code, book_external_ids.currency_code),
                oclc_work_id = COALESCE(EXCLUDED.oclc_work_id, book_external_ids.oclc_work_id),
                openlibrary_work_id = COALESCE(EXCLUDED.openlibrary_work_id, book_external_ids.openlibrary_work_id),
                goodreads_work_id = COALESCE(EXCLUDED.goodreads_work_id, book_external_ids.goodreads_work_id),
                google_canonical_id = COALESCE(EXCLUDED.google_canonical_id, book_external_ids.google_canonical_id),
                last_updated = NOW()
            """,
            IdGenerator.generate(),
            bookId,
            source,
            externalId,
            providerIsbn10,
            providerIsbn13,
            normalizeToHttps(identifiers.getInfoLink()),
            normalizeToHttps(identifiers.getPreviewLink()),
            normalizeToHttps(identifiers.getWebReaderLink()),
            normalizeToHttps(identifiers.getPurchaseLink()),
            normalizeToHttps(identifiers.getCanonicalVolumeLink()),
            identifiers.getAverageRating(),
            identifiers.getRatingsCount(),
            identifiers.getReviewCount(),
            identifiers.getIsEbook(),
            identifiers.getPdfAvailable(),
            identifiers.getEpubAvailable(),
            identifiers.getEmbeddable(),
            identifiers.getPublicDomain(),
            identifiers.getViewability(),
            identifiers.getTextReadable(),
            identifiers.getImageReadable(),
            identifiers.getPrintType(),
            identifiers.getMaturityRating(),
            identifiers.getContentVersion(),
            identifiers.getTextToSpeechPermission(),
            identifiers.getSaleability(),
            identifiers.getCountryCode(),
            identifiers.getIsEbookForSale(),
            identifiers.getListPrice(),
            identifiers.getRetailPrice(),
            identifiers.getCurrencyCode(),
            identifiers.getOclcWorkId(),
            identifiers.getOpenlibraryWorkId(),
            identifiers.getGoodreadsWorkId(),
            identifiers.getGoogleCanonicalId()
        );
    }
    
    /**
     * UPSERT book_image_links table.
     */
    private void upsertImageLinks(UUID bookId, Map<String, String> imageLinks) {
        for (Map.Entry<String, String> entry : imageLinks.entrySet()) {
            String imageType = entry.getKey();
            String url = entry.getValue();
            
            if (url == null || url.isBlank()) {
                continue;
            }
            
            jdbcTemplate.update(
                """
                INSERT INTO book_image_links (id, book_id, image_type, url, source, created_at)
                VALUES (?, ?, ?, ?, ?, NOW())
                ON CONFLICT (book_id, image_type) DO UPDATE SET
                    url = EXCLUDED.url,
                    source = EXCLUDED.source
                """,
                IdGenerator.generate(),
                bookId,
                imageType,
                normalizeToHttps(url),
                "GOOGLE_BOOKS"
            );
        }
    }
    
    /**
     * UPSERT book_dimensions table.
     * Stores physical dimensions (height, width, thickness) from Google Books API.
     * Uses DimensionParser to convert strings like "24.00 cm" to numeric values.
     */
    private void upsertDimensions(UUID bookId, BookAggregate.Dimensions dimensions) {
        // Parse dimension strings to numeric values in centimeters
        DimensionParser.ParsedDimensions parsed = DimensionParser.parseAll(
            dimensions.getHeight(),
            dimensions.getWidth(),
            dimensions.getThickness()
        );
        
        // Only insert if at least one dimension is valid
        if (!parsed.hasAnyDimension()) {
            log.debug("No valid dimensions to upsert for book {}", bookId);
            return;
        }
        
        jdbcTemplate.update(
            """
            INSERT INTO book_dimensions (book_id, height, width, thickness, created_at, updated_at)
            VALUES (?, ?, ?, ?, NOW(), NOW())
            ON CONFLICT (book_id) DO UPDATE SET
                height = COALESCE(EXCLUDED.height, book_dimensions.height),
                width = COALESCE(EXCLUDED.width, book_dimensions.width),
                thickness = COALESCE(EXCLUDED.thickness, book_dimensions.thickness),
                updated_at = NOW()
            """,
            bookId,
            parsed.height(),
            parsed.width(),
            parsed.thickness()
        );
    }
    
    /**
     * UPSERT categories via BookCollectionPersistenceService.
     */
    private void upsertCategories(UUID bookId, List<String> categories) {
        for (String category : categories) {
            if (category == null || category.isBlank()) {
                continue;
            }
            
            collectionPersistenceService.upsertCategory(category)
                .ifPresent(collectionId -> 
                    collectionPersistenceService.addBookToCategory(collectionId, bookId.toString())
                );
        }
    }
    
    /**
     * Emit event to outbox table (transactional).
     * WebSocket relay will poll this table and publish to clients.
     */
    private void emitOutboxEvent(UUID bookId, String slug, String title, boolean isNew) {
        try {
            String topic = "/topic/book." + bookId;
            
            Map<String, Object> payload = Map.of(
                "bookId", bookId.toString(),
                "slug", slug != null ? slug : "",
                "title", title != null ? title : "",
                "isNew", isNew,
                "timestamp", System.currentTimeMillis()
            );
            
            String payloadJson = objectMapper.writeValueAsString(payload);
            
            jdbcTemplate.update(
                "INSERT INTO events_outbox (topic, payload, created_at) VALUES (?, ?::jsonb, NOW())",
                topic,
                payloadJson
            );
            
            log.debug("Emitted outbox event for book {}", bookId);
        } catch (Exception e) {
            log.warn("Failed to emit outbox event for book {}: {}", bookId, e.getMessage());
            // Don't fail the transaction - event is optional
        }
    }
    
    // Helper methods
    
    private String nullIfBlank(String value) {
        return (value != null && !value.isBlank()) ? value : null;
    }
    
    private String normalizeToHttps(String url) {
        // Use shared utility instead of duplicate implementation
        return UrlUtils.normalizeToHttps(url);
    }
    
    /**
     * Result of upsert operation.
     */
    @Value
    @Builder
    public static class UpsertResult {
        UUID bookId;
        String slug;
        boolean isNew;
    }
}
