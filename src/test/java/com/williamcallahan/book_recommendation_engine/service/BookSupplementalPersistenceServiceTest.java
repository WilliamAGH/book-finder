package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Exercises the tag persistence guardrails noted in docs/task-retrofit-code-for-postgres-schema.md.
 */
@ExtendWith(MockitoExtension.class)
class BookSupplementalPersistenceServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private BookCollectionPersistenceService collectionPersistenceService;

    private BookSupplementalPersistenceService service;

    @BeforeEach
    void setUp() {
        service = new BookSupplementalPersistenceService(jdbcTemplate, new ObjectMapper(), collectionPersistenceService);
    }

    @Test
    void assignQualifierTags_persistsMetadataJson() {
        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), any(RowMapper.class)))
                .thenReturn("tag-001");

        ArgumentCaptor<String> metadataCaptor = ArgumentCaptor.forClass(String.class);

        service.assignQualifierTags(
                "book-123",
                Map.of("nytBestseller", Map.of("list", "hardcover-fiction", "rank", 1))
        );

        verify(jdbcTemplate).update(
                startsWith("INSERT INTO book_tag_assignments"),
                any(),
                eq("book-123"),
                eq("tag-001"),
                eq("QUALIFIER"),
                isNull(),
                metadataCaptor.capture()
        );

        assertThat(metadataCaptor.getValue()).contains("hardcover-fiction");
    }
}
