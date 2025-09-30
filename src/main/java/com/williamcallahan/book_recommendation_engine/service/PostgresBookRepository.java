package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import static com.williamcallahan.book_recommendation_engine.util.ApplicationConstants.Database.Queries.BOOK_BY_SLUG;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Repository responsible for hydrating full {@link Book} aggregates directly from Postgres.
 *
 * <p>This is extracted from {@link BookDataOrchestrator} so that the orchestrator can delegate
 * complex JDBC logic while keeping a slim facade. All methods remain optional-aware to preserve
 * existing behaviour.</p>
 */
@Component
@ConditionalOnClass(JdbcTemplate.class)
public class PostgresBookRepository {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresBookRepository.class);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final String PROVIDER_GOOGLE_BOOKS = ApplicationConstants.Provider.GOOGLE_BOOKS;

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final BookLookupService bookLookupService;

    @Autowired
    public PostgresBookRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, BookLookupService bookLookupService) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.bookLookupService = bookLookupService;
    }

    /**
     * Backward-compatible constructor for tests and legacy callers.
     * Creates a BookLookupService using the provided JdbcTemplate.
     */
    public PostgresBookRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this(jdbcTemplate, objectMapper, new BookLookupService(jdbcTemplate));
    }

    Optional<Book> fetchByCanonicalId(String id) {
        if (id == null) {
            return Optional.empty();
        }
        UUID canonicalId;
        try {
            canonicalId = UUID.fromString(id);
        } catch (IllegalArgumentException ex) {
            return Optional.empty();
        }
        return loadAggregate(canonicalId);
    }

    Optional<Book> fetchBySlug(String slug) {
        if (slug == null || slug.isBlank()) {
            return Optional.empty();
        }
        return queryForUuid(BOOK_BY_SLUG, slug.trim())
                .flatMap(this::loadAggregate);
    }

    Optional<Book> fetchByIsbn13(String isbn13) {
        return lookupAndHydrate(bookLookupService.findBookIdByIsbn(isbn13));
    }

    Optional<Book> fetchByIsbn10(String isbn10) {
        return lookupAndHydrate(bookLookupService.findBookIdByIsbn(isbn10));
    }

    Optional<Book> fetchByExternalId(String externalId) {
        return lookupAndHydrate(bookLookupService.findBookIdByExternalIdentifier(externalId));
    }

    private Optional<Book> lookupAndHydrate(Optional<String> idOptional) {
        return idOptional.flatMap(this::loadAggregate);
    }








    private Optional<Book> loadAggregate(String canonicalId) {
        if (canonicalId == null || canonicalId.isBlank()) {
            return Optional.empty();
        }
        try {
            return loadAggregate(UUID.fromString(canonicalId));
        } catch (IllegalArgumentException ex) {
            LOG.debug("Value {} is not a valid UUID", canonicalId);
            return Optional.empty();
        }
    }

    private Optional<Book> loadAggregate(UUID canonicalId) {
        String sql = """
                SELECT id::text, slug, title, description, isbn10, isbn13, published_date, language, publisher, page_count
                FROM books
                WHERE id = ?
                """;
        try {
            return jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), rs -> {
                if (!rs.next()) {
                    return Optional.<Book>empty();
                }
                Book book = new Book();
                book.setId(rs.getString("id"));
                book.setSlug(rs.getString("slug"));
                book.setTitle(rs.getString("title"));
                book.setDescription(rs.getString("description"));
                book.setIsbn10(rs.getString("isbn10"));
                book.setIsbn13(rs.getString("isbn13"));
                java.sql.Date published = rs.getDate("published_date");
                if (published != null) {
                    book.setPublishedDate(new java.util.Date(published.getTime()));
                }
                book.setLanguage(rs.getString("language"));
                book.setPublisher(rs.getString("publisher"));
                Integer pageCount = (Integer) rs.getObject("page_count");
                book.setPageCount(pageCount);

                hydrateAuthors(book, canonicalId);
                hydrateCategories(book, canonicalId);
                hydrateCollections(book, canonicalId);
                hydrateDimensions(book, canonicalId);
                hydrateRawPayload(book, canonicalId);
                hydrateTags(book, canonicalId);
                hydrateEditions(book, canonicalId);
                hydrateCover(book, canonicalId);
                hydrateRecommendations(book, canonicalId);
                hydrateProviderMetadata(book, canonicalId);
                
                // Set retrieval metadata for development mode tracking
                book.setRetrievedFrom("POSTGRES");
                book.setInPostgres(true);
                // Data source will be determined from collections/provider metadata
                hydrateDataSource(book, canonicalId);

                return Optional.of(book);
            });
        } catch (DataAccessException ex) {
            LOG.debug("Postgres reader failed to load canonical book {}: {}", canonicalId, ex.getMessage());
            return Optional.empty();
        }
    }

    private void hydrateAuthors(Book book, UUID canonicalId) {
        String sql = """
                SELECT a.name
                FROM book_authors_join baj
                JOIN authors a ON a.id = baj.author_id
                WHERE baj.book_id = ?::uuid
                ORDER BY COALESCE(baj.position, 2147483647), lower(a.name)
                """;
        try {
            List<String> authors = jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), (rs, rowNum) -> rs.getString("name"));
            book.setAuthors(authors == null || authors.isEmpty() ? List.of() : authors);
        } catch (DataAccessException ex) {
            LOG.debug("Failed to hydrate authors for {}: {}", canonicalId, ex.getMessage());
            book.setAuthors(List.of());
        }
    }

    private void hydrateCategories(Book book, UUID canonicalId) {
        String sql = """
                SELECT bc.display_name
                FROM book_collections_join bcj
                JOIN book_collections bc ON bc.id = bcj.collection_id
                WHERE bcj.book_id = ?::uuid
                  AND bc.collection_type = 'CATEGORY'
                ORDER BY COALESCE(bcj.position, 9999), lower(bc.display_name)
                """;
        try {
            List<String> categories = jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), (rs, rowNum) -> rs.getString("display_name"));
            book.setCategories(categories == null || categories.isEmpty() ? List.of() : categories);
        } catch (DataAccessException ex) {
            LOG.debug("Failed to hydrate categories for {}: {}", canonicalId, ex.getMessage());
            book.setCategories(List.of());
        }
    }

    private void hydrateCollections(Book book, UUID canonicalId) {
        String sql = """
                SELECT bc.id,
                       bc.display_name,
                       bc.collection_type,
                       bc.source,
                       bcj.position
                FROM book_collections_join bcj
                JOIN book_collections bc ON bc.id = bcj.collection_id
                WHERE bcj.book_id = ?::uuid
                ORDER BY CASE WHEN bc.collection_type = 'CATEGORY' THEN 0 ELSE 1 END,
                         COALESCE(bcj.position, 2147483647),
                         lower(bc.display_name)
                """;
        try {
            List<Book.CollectionAssignment> assignments = jdbcTemplate.query(
                    sql,
                    ps -> ps.setObject(1, canonicalId),
                    (rs, rowNum) -> {
                        Book.CollectionAssignment assignment = new Book.CollectionAssignment();
                        assignment.setCollectionId(rs.getString("id"));
                        assignment.setName(rs.getString("display_name"));
                        assignment.setCollectionType(rs.getString("collection_type"));
                        Integer rank = (Integer) rs.getObject("position");
                        assignment.setRank(rank);
                        assignment.setSource(rs.getString("source"));
                        return assignment;
                    }
            );
            book.setCollections(assignments);
        } catch (DataAccessException ex) {
            LOG.debug("Failed to hydrate collections for {}: {}", canonicalId, ex.getMessage());
            book.setCollections(List.of());
        }
    }

    private void hydrateDimensions(Book book, UUID canonicalId) {
        String sql = """
                SELECT height, width, thickness, weight_grams
                FROM book_dimensions
                WHERE book_id = ?::uuid
                LIMIT 1
                """;
        try {
            jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), rs -> {
                if (!rs.next()) {
                    return null;
                }
                book.setHeightCm(toDouble(rs.getBigDecimal("height")));
                book.setWidthCm(toDouble(rs.getBigDecimal("width")));
                book.setThicknessCm(toDouble(rs.getBigDecimal("thickness")));
                book.setWeightGrams(toDouble(rs.getBigDecimal("weight_grams")));
                return null;
            });
        } catch (DataAccessException ex) {
            LOG.debug("Failed to hydrate dimensions for {}: {}", canonicalId, ex.getMessage());
            book.setHeightCm(null);
            book.setWidthCm(null);
            book.setThicknessCm(null);
            book.setWeightGrams(null);
        }
    }

    private void hydrateRawPayload(Book book, UUID canonicalId) {
        String sql = """
                SELECT raw_json_response::text
                FROM book_raw_data
                WHERE book_id = ?::uuid
                ORDER BY contributed_at DESC
                LIMIT 1
                """;
        try {
            String rawJson = jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), rs -> rs.next() ? rs.getString(1) : null);
            if (rawJson != null && !rawJson.isBlank()) {
                book.setRawJsonResponse(rawJson);
            }
        } catch (DataAccessException ex) {
            LOG.debug("Failed to hydrate raw payload for {}: {}", canonicalId, ex.getMessage());
        }
    }

    private void hydrateTags(Book book, UUID canonicalId) {
        String sql = """
                SELECT bt.key, bt.display_name, bta.source, bta.confidence, bta.metadata
                FROM book_tag_assignments bta
                JOIN book_tags bt ON bt.id = bta.tag_id
                WHERE bta.book_id = ?::uuid
                """;
        try {
            Map<String, Object> tags = jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), rs -> {
                Map<String, Object> result = new LinkedHashMap<>();
                while (rs.next()) {
                    String key = rs.getString("key");
                    if (key == null || key.isBlank()) {
                        continue;
                    }
                    // Convert snake_case to camelCase for frontend compatibility
                    String camelCaseKey = snakeToCamelCase(key);
                    
                    Map<String, Object> attributes = new LinkedHashMap<>();
                    String displayName = rs.getString("display_name");
                    if (displayName != null && !displayName.isBlank()) {
                        attributes.put("displayName", displayName);
                    }
                    String source = rs.getString("source");
                    if (source != null && !source.isBlank()) {
                        attributes.put("source", source);
                    }
                    Double confidence = (Double) rs.getObject("confidence");
                    if (confidence != null) {
                        attributes.put("confidence", confidence);
                    }
                    Map<String, Object> metadata = parseJsonAttributes(rs.getObject("metadata"));
                    if (!metadata.isEmpty()) {
                        attributes.put("metadata", metadata);
                    }
                    result.put(camelCaseKey, attributes);
                }
                return result;
            });
            book.setQualifiers(tags);
        } catch (DataAccessException ex) {
            LOG.debug("Failed to hydrate tags for {}: {}", canonicalId, ex.getMessage());
            book.setQualifiers(Map.of());
        }
    }
    
    /**
     * Converts snake_case to camelCase.
     * Example: nyt_bestseller -> nytBestseller
     */
    private String snakeToCamelCase(String snakeCase) {
        if (snakeCase == null || snakeCase.isBlank()) {
            return snakeCase;
        }
        String[] parts = snakeCase.split("_");
        if (parts.length == 1) {
            return snakeCase; // No underscores, return as-is
        }
        StringBuilder camelCase = new StringBuilder(parts[0].toLowerCase());
        for (int i = 1; i < parts.length; i++) {
            String part = parts[i];
            if (!part.isEmpty()) {
                camelCase.append(Character.toUpperCase(part.charAt(0)));
                if (part.length() > 1) {
                    camelCase.append(part.substring(1).toLowerCase());
                }
            }
        }
        return camelCase.toString();
    }

    private void hydrateEditions(Book book, UUID canonicalId) {
        String sql = """
                SELECT b.id::text as edition_id,
                       b.slug,
                       b.title,
                       b.isbn13,
                       b.isbn10,
                       b.publisher,
                       b.published_date,
                       wc.cluster_method,
                       wcm.confidence,
                       bei.external_id as google_books_id,
                       bil.s3_image_path
                FROM work_cluster_members wcm1
                JOIN work_cluster_members wcm ON wcm.cluster_id = wcm1.cluster_id
                JOIN books b ON b.id = wcm.book_id
                JOIN work_clusters wc ON wc.id = wcm.cluster_id
                LEFT JOIN book_external_ids bei
                       ON bei.book_id = b.id AND bei.source = '%s'
                LEFT JOIN LATERAL (
                       SELECT s3_image_path, url, source, width, height, is_high_resolution
                       FROM book_image_links
                       WHERE book_id = b.id
                       ORDER BY COALESCE(is_high_resolution, false) DESC,
                                COALESCE(width, 0) DESC,
                                created_at DESC
                       LIMIT 1
                ) bil ON TRUE
                WHERE wcm1.book_id = ?
                  AND wcm.book_id <> ?
                ORDER BY wcm.is_primary DESC,
                         wcm.confidence DESC NULLS LAST,
                         b.published_date DESC NULLS LAST,
                         lower(b.title)
                """.formatted(PROVIDER_GOOGLE_BOOKS);
        try {
            List<Book.EditionInfo> editions = jdbcTemplate.query(sql,
                    ps -> {
                        ps.setObject(1, canonicalId);
                        ps.setObject(2, canonicalId);
                    },
                    (rs, rowNum) -> {
                        Book.EditionInfo info = new Book.EditionInfo();
                        info.setGoogleBooksId(rs.getString("google_books_id"));
                        info.setType(rs.getString("cluster_method"));
                        String slug = rs.getString("slug");
                        info.setIdentifier(slug != null && !slug.isBlank() ? slug : rs.getString("edition_id"));
                        info.setEditionIsbn13(rs.getString("isbn13"));
                        info.setEditionIsbn10(rs.getString("isbn10"));
                        java.sql.Date published = rs.getDate("published_date");
                        if (published != null) {
                            info.setPublishedDate(new java.util.Date(published.getTime()));
                        }
                        info.setCoverImageUrl(rs.getString("s3_image_path"));
                        return info;
                    });
            book.setOtherEditions(editions.isEmpty() ? List.of() : editions);
        } catch (DataAccessException ex) {
            LOG.debug("Failed to hydrate editions for {}: {}", canonicalId, ex.getMessage());
            book.setOtherEditions(List.of());
        }
    }

    private void hydrateCover(Book book, UUID canonicalId) {
        String sql = """
                SELECT image_type, url, source, s3_image_path, width, height, is_high_resolution
                FROM book_image_links
                WHERE book_id = ?
                ORDER BY CASE image_type
                    WHEN 'extraLarge' THEN 1
                    WHEN 'large' THEN 2
                    WHEN 'medium' THEN 3
                    WHEN 'small' THEN 4
                    WHEN 'thumbnail' THEN 5
                    WHEN 'smallThumbnail' THEN 6
                    ELSE 7
                END, created_at DESC
                LIMIT 2
                """;
        try {
            List<CoverCandidate> candidates = jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), (rs, rowNum) -> new CoverCandidate(
                    rs.getString("url"),
                    rs.getString("s3_image_path"),
                    rs.getString("source"),
                    rs.getObject("width", Integer.class),
                    rs.getObject("height", Integer.class),
                    rs.getObject("is_high_resolution", Boolean.class)
            ));
            if (candidates.isEmpty()) {
                return;
            }
            CoverCandidate primary = candidates.get(0);
            book.setExternalImageUrl(primary.url());
            book.setS3ImagePath(primary.s3Path());
            book.setCoverImageWidth(primary.width());
            book.setCoverImageHeight(primary.height());
            book.setIsCoverHighResolution(primary.highRes());
            CoverImages coverImages = new CoverImages(primary.url(),
                    candidates.size() > 1 ? candidates.get(1).url() : primary.url(),
                    toCoverSource(primary.source()));
            book.setCoverImages(coverImages);
        } catch (DataAccessException ex) {
            LOG.debug("Failed to hydrate cover for {}: {}", canonicalId, ex.getMessage());
        }
    }

    private void hydrateRecommendations(Book book, UUID canonicalId) {
        String sql = """
                SELECT recommended_book_id::text
                FROM book_recommendations
                WHERE source_book_id = ?
                ORDER BY score DESC NULLS LAST, created_at DESC
                LIMIT 20
                """;
        try {
            List<String> recommendations = jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), (rs, rowNum) -> rs.getString("recommended_book_id"));
            book.setCachedRecommendationIds(recommendations == null || recommendations.isEmpty() ? List.of() : recommendations);
        } catch (DataAccessException ex) {
            LOG.debug("Failed to hydrate recommendations for {}: {}", canonicalId, ex.getMessage());
            book.setCachedRecommendationIds(List.of());
        }
    }

    private void hydrateProviderMetadata(Book book, UUID canonicalId) {
        String sql = """
                SELECT info_link, preview_link, web_reader_link, purchase_link,
                       average_rating, ratings_count, list_price, currency_code, provider_asin
                FROM book_external_ids
                WHERE book_id = ?
                ORDER BY CASE WHEN source = '%s' THEN 0 ELSE 1 END, created_at DESC
                LIMIT 1
                """.formatted(PROVIDER_GOOGLE_BOOKS);
        try {
            jdbcTemplate.query(sql, ps -> ps.setObject(1, canonicalId), rs -> {
                if (!rs.next()) {
                    return null;
                }
                book.setInfoLink(rs.getString("info_link"));
                book.setPreviewLink(rs.getString("preview_link"));
                book.setWebReaderLink(rs.getString("web_reader_link"));
                book.setPurchaseLink(rs.getString("purchase_link"));
                java.math.BigDecimal avgDecimal = rs.getBigDecimal("average_rating");
                if (avgDecimal != null) {
                    book.setAverageRating(avgDecimal.doubleValue());
                    book.setHasRatings(Boolean.TRUE);
                }
                Integer ratingsCount = rs.getObject("ratings_count", Integer.class);
                if (ratingsCount != null) {
                    book.setRatingsCount(ratingsCount);
                    book.setHasRatings(ratingsCount > 0);
                }
                java.math.BigDecimal listPrice = rs.getBigDecimal("list_price");
                if (listPrice != null) {
                    book.setListPrice(listPrice.doubleValue());
                }
                String currency = rs.getString("currency_code");
                if (currency != null && !currency.isBlank()) {
                    book.setCurrencyCode(currency);
                }
                String asin = rs.getString("provider_asin");
                if (asin != null && !asin.isBlank()) {
                    book.setAsin(asin);
                }
                return null;
            });
        } catch (DataAccessException ex) {
            LOG.debug("Failed to hydrate provider metadata for {}: {}", canonicalId, ex.getMessage());
        }
    }

    private void hydrateDataSource(Book book, UUID canonicalId) {
        // Check if book is part of NYT bestseller list
        String nytCheck = """
                SELECT 1
                FROM book_collections_join bcj
                JOIN book_collections bc ON bc.id = bcj.collection_id
                WHERE bcj.book_id = ?
                  AND bc.source = 'NYT'
                LIMIT 1
                """;
        try {
            List<Integer> nytResults = jdbcTemplate.query(nytCheck, ps -> ps.setObject(1, canonicalId), (rs, rowNum) -> 1);
            if (!nytResults.isEmpty()) {
                book.setDataSource("NYT");
                return;
            }
        } catch (DataAccessException ex) {
            LOG.debug("Failed to check NYT source for {}: {}", canonicalId, ex.getMessage());
        }
        
        // Check external provider IDs to determine primary data source
        String providerCheck = """
                SELECT source
                FROM book_external_ids
                WHERE book_id = ?
                ORDER BY created_at ASC
                LIMIT 1
                """;
        try {
            List<String> sources = jdbcTemplate.query(providerCheck, ps -> ps.setObject(1, canonicalId), (rs, rowNum) -> rs.getString("source"));
            if (!sources.isEmpty()) {
                book.setDataSource(sources.get(0));
            }
        } catch (DataAccessException ex) {
            LOG.debug("Failed to check provider source for {}: {}", canonicalId, ex.getMessage());
        }
    }
    
    private Optional<UUID> queryForUuid(String sql, Object param) {
        try {
            UUID result = jdbcTemplate.queryForObject(sql, UUID.class, param);
            return Optional.ofNullable(result);
        } catch (EmptyResultDataAccessException ex) {
            return Optional.empty();
        } catch (DataAccessException ex) {
            LOG.debug("Postgres lookup failed for value {}: {}", param, ex.getMessage());
            return Optional.empty();
        }
    }

    private Map<String, Object> parseJsonAttributes(Object value) {
        if (value == null) {
            return Map.of();
        }
        try {
            String json = null;
            Class<?> clazz = value.getClass();
            if ("org.postgresql.util.PGobject".equals(clazz.getName())) {
                try {
                    java.lang.reflect.Method getValue = clazz.getMethod("getValue");
                    Object v = getValue.invoke(value);
                    if (v != null) {
                        json = v.toString();
                    }
                } catch (ReflectiveOperationException ignored) {
                    // Fall back to other parsing paths
                }
            } else if (value instanceof String str) {
                json = str;
            }
            if (json != null && !json.isBlank()) {
                return objectMapper.readValue(json, MAP_TYPE);
            }
            if (value instanceof Map<?, ?> mapValue) {
                Map<String, Object> copy = new LinkedHashMap<>();
                mapValue.forEach((k, v) -> copy.put(String.valueOf(k), v));
                return copy;
            }
        } catch (Exception ex) {
            LOG.debug("Failed to parse tag metadata: {}", ex.getMessage());
        }
        return Map.of();
    }

    private CoverImageSource toCoverSource(String raw) {
        if (raw == null || raw.isBlank()) {
            return CoverImageSource.UNDEFINED;
        }
        try {
            return CoverImageSource.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            return CoverImageSource.UNDEFINED;
        }
    }

    private Double toDouble(java.math.BigDecimal value) {
        return value == null ? null : value.doubleValue();
    }
    private record CoverCandidate(String url,
                                  String s3Path,
                                  String source,
                                  Integer width,
                                  Integer height,
                                  Boolean highRes) {
    }
}
