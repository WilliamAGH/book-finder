package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.util.IdGenerator;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.Optional;
import java.util.Locale;
import java.util.UUID;

@Service
public class BookCollectionPersistenceService {

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
                "VALUES (?, 'CATEGORY', 'GOOGLE_BOOKS', ?, ?, NOW(), NOW()) " +
                "ON CONFLICT (collection_type, source, normalized_name) DO UPDATE SET display_name = EXCLUDED.display_name, updated_at = NOW() RETURNING id",
                new Object[]{IdGenerator.generateShort(), displayName, normalized},
                (rs, rowNum) -> rs.getString("id")
            );
            return Optional.ofNullable(id);
        } catch (DataAccessException ex) {
            String existing = queryForId(
                "SELECT id FROM book_collections WHERE collection_type = 'CATEGORY' AND source = 'GOOGLE_BOOKS' AND normalized_name = ?",
                normalized
            );
            return Optional.ofNullable(existing);
        }
    }

    public void addBookToCategory(String collectionId, String bookId) {
        if (jdbcTemplate == null || collectionId == null || bookId == null) {
            return;
        }
        jdbcTemplate.update(
            "INSERT INTO book_collections_join (id, collection_id, book_id, created_at, updated_at) VALUES (?, ?, ?, NOW(), NOW()) " +
            "ON CONFLICT (collection_id, book_id) DO UPDATE SET updated_at = NOW()",
            IdGenerator.generateLong(),
            collectionId,
            bookId
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

        String rawJson = rawListJson != null ? rawListJson.toString() : null;

        String id = jdbcTemplate.queryForObject(
            "INSERT INTO book_collections (id, collection_type, source, provider_list_id, provider_list_code, display_name, normalized_name, description, bestsellers_date, published_date, raw_data_json, created_at, updated_at) " +
            "VALUES (?, 'BESTSELLER_LIST', 'NYT', ?, ?, ?, ?, ?, ?, ?, ?::jsonb, NOW(), NOW()) " +
            "ON CONFLICT (source, provider_list_code, published_date) DO UPDATE SET display_name = EXCLUDED.display_name, description = EXCLUDED.description, raw_data_json = EXCLUDED.raw_data_json, updated_at = NOW() RETURNING id",
            new Object[]{
                IdGenerator.generateShort(),
                providerListId,
                listCode,
                displayName,
                normalized,
                description,
                bestsellersDate,
                publishedDate,
                rawJson
            },
            (rs, rowNum) -> rs.getString("id")
        );

        return Optional.ofNullable(id);
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
        if (jdbcTemplate == null || collectionId == null || bookId == null) {
            return;
        }

        jdbcTemplate.update(
            "INSERT INTO book_collections_join (id, collection_id, book_id, position, weeks_on_list, rank_last_week, peak_position, provider_isbn13, provider_isbn10, provider_book_ref, raw_item_json, created_at, updated_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, NOW(), NOW()) " +
            "ON CONFLICT (collection_id, book_id) DO UPDATE SET position = EXCLUDED.position, weeks_on_list = COALESCE(EXCLUDED.weeks_on_list, book_collections_join.weeks_on_list), rank_last_week = COALESCE(EXCLUDED.rank_last_week, book_collections_join.rank_last_week), peak_position = COALESCE(EXCLUDED.peak_position, book_collections_join.peak_position), provider_isbn13 = COALESCE(EXCLUDED.provider_isbn13, book_collections_join.provider_isbn13), provider_isbn10 = COALESCE(EXCLUDED.provider_isbn10, book_collections_join.provider_isbn10), provider_book_ref = COALESCE(EXCLUDED.provider_book_ref, book_collections_join.provider_book_ref), raw_item_json = EXCLUDED.raw_item_json, updated_at = NOW()",
            IdGenerator.generateLong(),
            collectionId,
            bookId,
            position,
            weeksOnList,
            rankLastWeek,
            peakPosition,
            providerIsbn13,
            providerIsbn10,
            providerBookRef,
            rawItemJson
        );
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

        try {
            jdbcTemplate.update(
                "INSERT INTO book_lists (list_id, source, provider_list_id, provider_list_code, display_name, bestsellers_date, published_date, updated_frequency, raw_list_json) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb)) " +
                "ON CONFLICT (source, provider_list_code, published_date) DO UPDATE SET display_name = EXCLUDED.display_name, bestsellers_date = EXCLUDED.bestsellers_date, updated_frequency = EXCLUDED.updated_frequency, raw_list_json = EXCLUDED.raw_list_json, updated_at = now()",
                listId,
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
        } catch (Exception e) {
            return Optional.of(listId);
        }
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

        jdbcTemplate.update(
            "INSERT INTO book_lists_join (list_id, book_id, position, weeks_on_list, provider_isbn13, provider_isbn10, provider_book_ref, raw_item_json) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb)) " +
            "ON CONFLICT (list_id, book_id) DO UPDATE SET position = EXCLUDED.position, weeks_on_list = EXCLUDED.weeks_on_list, provider_isbn13 = EXCLUDED.provider_isbn13, provider_isbn10 = EXCLUDED.provider_isbn10, provider_book_ref = EXCLUDED.provider_book_ref, raw_item_json = EXCLUDED.raw_item_json, updated_at = now()",
            listId,
            bookId,
            position,
            weeksOnList,
            providerIsbn13,
            providerIsbn10,
            providerBookRef,
            rawItemJson != null ? rawItemJson.toString() : null
        );
    }

    private String queryForId(String sql, Object... args) {
        if (jdbcTemplate == null) {
            return null;
        }
        try {
            return jdbcTemplate.queryForObject(sql, args, String.class);
        } catch (DataAccessException ex) {
            return null;
        }
    }
}

