package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Validates the NYT ingest persistence scenario from docs/task-retrofit-code-for-postgres-schema.md.
 */
@ExtendWith(MockitoExtension.class)
class BookCollectionPersistenceServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private BookCollectionPersistenceService service;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        service = new BookCollectionPersistenceService(jdbcTemplate);
    }

    @Test
    void upsertBestsellerCollection_persistsRawJsonAndReturnsId() {
        ObjectNode raw = objectMapper.createObjectNode();
        raw.put("list_name", "Hardcover Fiction");

        when(jdbcTemplate.queryForObject(
                anyString(),
                org.mockito.ArgumentMatchers.<RowMapper<String>>any(),
                any(), any(), any(), any(), any(), any(), any(), any(), any()
        )).thenReturn("collection-123");

        Optional<String> result = service.upsertBestsellerCollection(
                "nyt-fiction-2024-38",
                "hardcover-fiction",
                "NYT Hardcover Fiction",
                null,
                "Latest weekly update",
                LocalDate.of(2024, 9, 1),
                LocalDate.of(2024, 9, 8),
                raw
        );

        assertThat(result).contains("collection-123");

        ArgumentCaptor<Object> argCaptor = ArgumentCaptor.forClass(Object.class);

        verify(jdbcTemplate).queryForObject(
                startsWith("INSERT INTO book_collections (id, collection_type, source, provider_list_id"),
                org.mockito.ArgumentMatchers.<RowMapper<String>>any(),
                argCaptor.capture(), argCaptor.capture(), argCaptor.capture(), argCaptor.capture(),
                argCaptor.capture(), argCaptor.capture(), argCaptor.capture(), argCaptor.capture(), argCaptor.capture()
        );

        List<Object> captured = argCaptor.getAllValues();
        assertThat(captured.get(1)).isEqualTo("nyt-fiction-2024-38");
        assertThat(captured.get(2)).isEqualTo("hardcover-fiction");
        assertThat(captured.get(3)).isEqualTo("NYT Hardcover Fiction");
        assertThat(captured.get(4)).isEqualTo("hardcover-fiction");
        assertThat(captured.get(8)).isEqualTo(raw.toString());
    }
}
