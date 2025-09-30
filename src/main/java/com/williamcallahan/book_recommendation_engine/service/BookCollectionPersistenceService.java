package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.util.IdGenerator;
import com.williamcallahan.book_recommendation_engine.util.JdbcUtils;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.Optional;
import java.util.Locale;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
@Slf4j
public class BookCollectionPersistenceService {

    private static final Logger log = LoggerFactory.getLogger(BookCollectionPersistenceService.class);

    private final JdbcTemplate jdbcTemplate;

    public BookCollectionPersistenceService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<String> upsertCategory(String displayName) {
        if (jdbcTemplate == null || displayName == null || displayName.isBlank()) {
            return Optional.empty();
        }

        String normalized = displayName.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-");

        try {
            String id = jdbcTemplate.queryForObject(
                "INSERT INTO book_collections (id, collection_type, source, display_name, normalized_name, created_at, updated_at) " +
                "VALUES (?, 'CATEGORY', ?, ?, ?, NOW(), NOW()) " +
                "ON CONFLICT (collection_type, source, normalized_name) WHERE collection_type = 'CATEGORY' AND normalized_name IS NOT NULL " +
                "DO UPDATE SET display_name = EXCLUDED.display_name, updated_at = NOW() RETURNING id",
                (rs, rowNum) -> rs.getString("id"),
                IdGenerator.generateShort(),
                ApplicationConstants.Provider.GOOGLE_BOOKS,
                displayName,
                normalized
            );
            return Optional.ofNullable(id);
        } catch (DataAccessException ex) {
            return JdbcUtils.optionalString(
                jdbcTemplate,
                "SELECT id FROM book_collections WHERE collection_type = 'CATEGORY' AND source = ? AND normalized_name = ?",
                ApplicationConstants.Provider.GOOGLE_BOOKS,
                normalized
            );
        }
    }

    public void addBookToCategory(String collectionId, String bookId) {
        if (jdbcTemplate == null || collectionId == null || bookId == null) {
            return;
        }
        JdbcUtils.executeUpdate(
            jdbcTemplate,
            "INSERT INTO book_collections_join (id, collection_id, book_id, created_at, updated_at) VALUES (?, ?, ?, NOW(), NOW()) " +
            "ON CONFLICT (collection_id, book_id) DO UPDATE SET updated_at = NOW()",
            IdGenerator.generateLong(),
            collectionId,
            UUID.fromString(bookId)
        );
    }

    public Optional<String> upsertBestsellerCollection(String providerListId,
                                                       String listCode,
                                                       String displayName,
                                                       String normalizedName,
                                                       String description,
                                                       LocalDate bestsellersDate,
                                                       LocalDate publishedDate,
                                                       JsonNode rawListJson) {
        if (jdbcTemplate == null || listCode == null || publishedDate == null) {
            return Optional.empty();
        }

        String normalized = normalizedName != null && !normalizedName.isBlank()
            ? normalizedName
            : listCode.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-");

        String safeDisplayName = (displayName == null || displayName.isBlank()) ? listCode : displayName;
        String rawJson = rawListJson != null ? rawListJson.toString() : null;

        try {
            String id = jdbcTemplate.queryForObject(
                "INSERT INTO book_collections (id, collection_type, source, provider_list_id, provider_list_code, display_name, normalized_name, description, bestsellers_date, published_date, raw_data_json, created_at, updated_at) " +
                "VALUES (?, 'BESTSELLER_LIST', 'NYT', ?, ?, ?, ?, ?, ?, ?, ?::jsonb, NOW(), NOW()) " +
                "ON CONFLICT (source, provider_list_code, published_date) WHERE provider_list_code IS NOT NULL AND published_date IS NOT NULL " +
                "DO UPDATE SET display_name = EXCLUDED.display_name, description = EXCLUDED.description, raw_data_json = EXCLUDED.raw_data_json, updated_at = NOW() RETURNING id",
                (rs, rowNum) -> rs.getString("id"),
                IdGenerator.generateShort(),
                providerListId,
                listCode,
                safeDisplayName,
                normalized,
                description,
                bestsellersDate,
                publishedDate,
                rawJson
            );

            return Optional.ofNullable(id);
        } catch (DataAccessException ex) {
            LoggingUtils.error(log, ex, "Failed upserting bestseller collection listCode={} publishedDate={}", listCode, publishedDate);
            return JdbcUtils.optionalString(
                jdbcTemplate,
                "SELECT id FROM book_collections WHERE source = 'NYT' AND provider_list_code = ? AND published_date = ?",
                listCode,
                publishedDate
            );
        }
    }

    public void upsertBestsellerMembership(String collectionId,
                                           String bookId,
                                           Integer position,
                                           Integer weeksOnList,
                                           Integer rankLastWeek,
                                           Integer peakPosition,
                                           String providerIsbn13,
                                           String providerIsbn10,
                                           String providerBookRef,
                                           String rawItemJson) {
        if (jdbcTemplate == null) {
            log.warn("upsertBestsellerMembership: jdbcTemplate is null");
            return;
        }
        if (collectionId == null || bookId == null) {
            log.warn("upsertBestsellerMembership: collectionId or bookId is null - collectionId: {}, bookId: {}", collectionId, bookId);
            return;
        }

        try {
            // Validate that bookId is a valid UUID
            UUID bookUuid = UUID.fromString(bookId);
            
            log.debug("Inserting book into collection: collectionId='{}', bookId='{}', position={}, isbn13='{}'",
                collectionId, bookId, position, providerIsbn13);
            
            JdbcUtils.executeUpdate(
                jdbcTemplate,
                "INSERT INTO book_collections_join (id, collection_id, book_id, position, weeks_on_list, rank_last_week, peak_position, provider_isbn13, provider_isbn10, provider_book_ref, raw_item_json, created_at, updated_at) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, NOW(), NOW()) " +
                "ON CONFLICT (collection_id, book_id) DO UPDATE SET position = EXCLUDED.position, weeks_on_list = COALESCE(EXCLUDED.weeks_on_list, book_collections_join.weeks_on_list), rank_last_week = COALESCE(EXCLUDED.rank_last_week, book_collections_join.rank_last_week), peak_position = COALESCE(EXCLUDED.peak_position, book_collections_join.peak_position), provider_isbn13 = COALESCE(EXCLUDED.provider_isbn13, book_collections_join.provider_isbn13), provider_isbn10 = COALESCE(EXCLUDED.provider_isbn10, book_collections_join.provider_isbn10), provider_book_ref = COALESCE(EXCLUDED.provider_book_ref, book_collections_join.provider_book_ref), raw_item_json = EXCLUDED.raw_item_json, updated_at = NOW()",
                IdGenerator.generateLong(),
                collectionId,
                bookUuid,
                position,
                weeksOnList,
                rankLastWeek,
                peakPosition,
                providerIsbn13,
                providerIsbn10,
                providerBookRef,
                rawItemJson
            );
            
            log.info("Successfully added book to collection: collectionId='{}', bookId='{}', position={}",
                collectionId, bookId, position);
        } catch (IllegalArgumentException ex) {
            LoggingUtils.error(log, ex, "Invalid UUID format for bookId: {}", bookId);
        } catch (DataAccessException ex) {
            LoggingUtils.error(log, ex, "Database error adding book to collection: collectionId='{}', bookId='{}', position={}, isbn13='{}'",
                collectionId, bookId, position, providerIsbn13);
        }
    }

    public Optional<String> upsertList(String source,
                                       String providerListCode,
                                       LocalDate publishedDate,
                                       String displayName,
                                       LocalDate bestsellersDate,
                                       String updatedFrequency,
                                       String providerListId,
                                       JsonNode rawListJson) {
        if (jdbcTemplate == null || source == null || providerListCode == null || publishedDate == null) {
            return Optional.empty();
        }

        String deterministicKey = source + ":" + providerListCode + ":" + publishedDate.toString();
        String listId = UUID.nameUUIDFromBytes(deterministicKey.getBytes(java.nio.charset.StandardCharsets.UTF_8)).toString();

        JdbcUtils.executeUpdate(
            jdbcTemplate,
            "INSERT INTO book_collections (id, collection_type, source, provider_list_id, provider_list_code, display_name, bestsellers_date, published_date, updated_frequency, raw_data_json) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb)) " +
            "ON CONFLICT (source, provider_list_code, published_date) DO UPDATE SET display_name = EXCLUDED.display_name, bestsellers_date = EXCLUDED.bestsellers_date, updated_frequency = EXCLUDED.updated_frequency, raw_data_json = EXCLUDED.raw_data_json, updated_at = now()",
            listId,
            "BESTSELLER_LIST",
            source,
            providerListId,
            providerListCode,
            displayName,
            bestsellersDate,
            publishedDate,
            updatedFrequency,
            rawListJson != null ? rawListJson.toString() : null
        );
        return Optional.of(listId);
    }

    public void upsertListMembership(String listId,
                                      String bookId,
                                      Integer position,
                                      Integer weeksOnList,
                                      String providerIsbn13,
                                      String providerIsbn10,
                                      String providerBookRef,
                                      JsonNode rawItemJson) {
        if (jdbcTemplate == null || listId == null || bookId == null) {
            return;
        }

        JdbcUtils.executeUpdate(
            jdbcTemplate,
            "INSERT INTO book_collections_join (id, collection_id, book_id, position, weeks_on_list, provider_isbn13, provider_isbn10, provider_book_ref, raw_item_json) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb)) " +
            "ON CONFLICT (collection_id, book_id) DO UPDATE SET position = EXCLUDED.position, weeks_on_list = EXCLUDED.weeks_on_list, provider_isbn13 = EXCLUDED.provider_isbn13, provider_isbn10 = EXCLUDED.provider_isbn10, provider_book_ref = EXCLUDED.provider_book_ref, raw_item_json = EXCLUDED.raw_item_json, updated_at = now()",
            com.williamcallahan.book_recommendation_engine.util.IdGenerator.generateLong(),
            listId,
            UUID.fromString(bookId),
            position,
            weeksOnList,
            providerIsbn13,
            providerIsbn10,
            providerBookRef,
            rawItemJson != null ? rawItemJson.toString() : null
        );
    }
}
