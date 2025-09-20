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

        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), any(RowMapper.class)))
                .thenReturn("collection-123");

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

        ArgumentCaptor<Object[]> paramsCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(jdbcTemplate).queryForObject(
                startsWith("INSERT INTO book_collections (id, collection_type, source, provider_list_id"),
                paramsCaptor.capture(),
                any(RowMapper.class)
        );

        Object[] params = paramsCaptor.getValue();
        assertThat(params[1]).isEqualTo("nyt-fiction-2024-38");
        assertThat(params[2]).isEqualTo("hardcover-fiction");
        assertThat(params[3]).isEqualTo("NYT Hardcover Fiction");
        assertThat(params[4]).isEqualTo("hardcover-fiction");
        assertThat(params[8]).isEqualTo(raw.toString());
    }
}
