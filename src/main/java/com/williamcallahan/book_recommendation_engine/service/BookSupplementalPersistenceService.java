package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.util.IdGenerator;
import com.williamcallahan.book_recommendation_engine.util.JdbcUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class BookSupplementalPersistenceService {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final BookCollectionPersistenceService collectionPersistenceService;

    public BookSupplementalPersistenceService(JdbcTemplate jdbcTemplate,
                                              ObjectMapper objectMapper,
                                              BookCollectionPersistenceService collectionPersistenceService) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.collectionPersistenceService = collectionPersistenceService;
    }

    public void persistAuthors(String bookId, List<String> authors) {
        if (jdbcTemplate == null || ValidationUtils.isNullOrBlank(bookId) || ValidationUtils.isNullOrEmpty(authors)) {
            return;
        }

        int position = 0;
        for (String author : authors) {
            if (ValidationUtils.isNullOrBlank(author)) {
                continue;
            }
            String normalized = author.toLowerCase().replaceAll("[^a-z0-9\\s]", "").trim();
            String authorId = upsertAuthor(author, normalized);
            JdbcUtils.executeUpdate(
                jdbcTemplate,
                "INSERT INTO book_authors_join (id, book_id, author_id, position, created_at, updated_at) VALUES (?, ?, ?, ?, NOW(), NOW()) " +
                "ON CONFLICT (book_id, author_id) DO UPDATE SET position = EXCLUDED.position, updated_at = NOW()",
                IdGenerator.generateLong(),
                UUID.fromString(bookId),
                authorId,
                position++
            );
        }
    }

    public void persistCategories(String bookId, List<String> categories) {
        if (ValidationUtils.isNullOrBlank(bookId) || ValidationUtils.isNullOrEmpty(categories)) {
            return;
        }

        for (String category : categories) {
            if (ValidationUtils.isNullOrBlank(category)) {
                continue;
            }
            collectionPersistenceService.upsertCategory(category)
                .ifPresent(collectionId -> collectionPersistenceService.addBookToCategory(collectionId, bookId));
        }
    }

    public void assignQualifierTags(String bookId, Map<String, Object> qualifiers) {
        if (ValidationUtils.isNullOrBlank(bookId) || ValidationUtils.isNullOrEmpty(qualifiers)) {
            return;
        }

        qualifiers.forEach((key, value) -> {
            if (ValidationUtils.isNullOrBlank(key)) {
                return;
            }
            Double confidence = (value instanceof Boolean && (Boolean) value) ? 1.0 : null;
            assignTagWithSerializedMetadata(
                bookId,
                key,
                key,
                ApplicationConstants.Tag.QUALIFIER,
                ApplicationConstants.Tag.QUALIFIER,
                confidence,
                serializeQualifierMetadata(value)
            );
        });
    }

    public void assignTag(String bookId,
                           String key,
                           String displayName,
                           String source,
                           Double confidence,
                           Map<String, Object> metadata) {
        if (ValidationUtils.isNullOrBlank(bookId) || ValidationUtils.isNullOrBlank(key)) {
            return;
        }
        String resolvedDisplayName = displayName != null ? displayName : key;
        Map<String, Object> metadataMap = !ValidationUtils.isNullOrEmpty(metadata)
            ? metadata
            : Map.of("value", resolvedDisplayName);
        String metadataJson = serializeMetadata(metadataMap);
        assignTagWithSerializedMetadata(bookId, key, resolvedDisplayName, ApplicationConstants.Tag.QUALIFIER, source, confidence, metadataJson);
    }

    private void assignTagInternal(String bookId,
                                   String tagId,
                                   String source,
                                   Double confidence,
                                   String metadataJson) {
        if (jdbcTemplate == null) {
            return;
        }

        JdbcUtils.executeUpdate(
            jdbcTemplate,
            "INSERT INTO book_tag_assignments (id, book_id, tag_id, source, confidence, metadata, created_at) VALUES (?, ?, ?, ?, ?, ?::jsonb, NOW()) " +
            "ON CONFLICT (book_id, tag_id, source) DO UPDATE SET metadata = EXCLUDED.metadata, confidence = COALESCE(EXCLUDED.confidence, book_tag_assignments.confidence)",
            IdGenerator.generateLong(),
            JdbcUtils.toUuid(bookId),

            tagId,
            source,
            confidence,
            metadataJson
        );
    }

    private String upsertAuthor(String name, String normalized) {
        try {
            return jdbcTemplate.queryForObject(
                "INSERT INTO authors (id, name, normalized_name, created_at, updated_at) VALUES (?, ?, ?, NOW(), NOW()) " +
                "ON CONFLICT (name) DO UPDATE SET updated_at = NOW() RETURNING id",
                (rs, rowNum) -> rs.getString("id"),
                IdGenerator.generate(), name, normalized
            );
        } catch (DataAccessException ex) {
            return JdbcUtils.optionalString(jdbcTemplate, "SELECT id FROM authors WHERE name = ?", name).orElse(null);
        }
    }

    private String upsertTag(String key, String displayName, String tagType) {
        try {
            return jdbcTemplate.queryForObject(
                "INSERT INTO book_tags (id, key, display_name, tag_type, created_at, updated_at) VALUES (?, ?, ?, ?, NOW(), NOW()) " +
                "ON CONFLICT (key) DO UPDATE SET display_name = COALESCE(book_tags.display_name, EXCLUDED.display_name), updated_at = NOW() RETURNING id",
                (rs, rowNum) -> rs.getString("id"),
                IdGenerator.generate(), key, displayName, tagType
            );
        } catch (DataAccessException ex) {
            return JdbcUtils.optionalString(jdbcTemplate, "SELECT id FROM book_tags WHERE key = ?", key).orElse(null);
        }
    }

    private String serializeQualifierMetadata(Object value) {
        try {
            return objectMapper.writeValueAsString(Map.of("value", value));
        } catch (Exception e) {
            return "{\"value\":null}";
        }
    }

    private String serializeMetadata(Map<String, Object> metadata) {
        try {
            return objectMapper.writeValueAsString(metadata);
        } catch (Exception e) {
            return "{}";
        }
    }

    private void assignTagWithSerializedMetadata(String bookId,
                                                 String key,
                                                 String displayName,
                                                 String tagType,
                                                 String source,
                                                 Double confidence,
                                                 String metadataJson) {
        if (ValidationUtils.isNullOrBlank(bookId) || ValidationUtils.isNullOrBlank(key)) {
            return;
        }

        String canonicalKey = key.trim().toLowerCase();
        if (canonicalKey.isEmpty()) {
            return;
        }

        String tagId = upsertTag(canonicalKey, displayName != null ? displayName : key, tagType);
        String resolvedSource = source != null ? source : canonicalKey;
        assignTagInternal(bookId, tagId, resolvedSource, confidence, metadataJson);
    }

}
