package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers retrofit checklist scenarios for dedupe and edition-chaining behaviour.
 * See docs/task-retrofit-code-for-postgres-schema.md.
 */
@ExtendWith(MockitoExtension.class)
class BookDataOrchestratorPersistenceScenariosTest {

    @Mock
    private S3RetryService s3RetryService;

    @Mock
    private GoogleApiFetcher googleApiFetcher;

    @Mock
    private OpenLibraryBookDataService openLibraryBookDataService;

    @Mock
    private BookDataAggregatorService bookDataAggregatorService;

    @Mock
    private BookSupplementalPersistenceService supplementalPersistenceService;

    @Mock
    private BookCollectionPersistenceService collectionPersistenceService;

    @Mock
    private JdbcTemplate jdbcTemplate;

    private BookDataOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        orchestrator = new BookDataOrchestrator(
                s3RetryService,
                googleApiFetcher,
                new ObjectMapper(),
                openLibraryBookDataService,
                bookDataAggregatorService,
                supplementalPersistenceService,
                collectionPersistenceService
        );
        ReflectionTestUtils.setField(orchestrator, "jdbcTemplate", jdbcTemplate);
    }

    @Test
    void resolveCanonicalBookId_prefersExistingExternalMapping() {
        lenient().when(jdbcTemplate.<String>query(anyString(), ArgumentMatchers.<RowMapper<String>>any(), ArgumentMatchers.<Object[]>any()))
                .thenReturn(Collections.<String>emptyList());
        whenExternalIdLookupReturns("existing-book-id");

        Book incoming = new Book();
        incoming.setId("temporary-id");
        incoming.setIsbn13("9781234567890");
        incoming.setIsbn10("1234567890");

        String resolved = ReflectionTestUtils.invokeMethod(
                orchestrator,
                "resolveCanonicalBookId",
                incoming,
                "google-abc",
                "9781234567890",
                "1234567890"
        );

        assertThat(resolved).isEqualTo("existing-book-id");
    }

    @Test
    void synchronizeEditionRelationships_linksHighestEditionAsPrimary() throws SQLException {
        Book book = new Book();
        book.setEditionGroupKey("group-key");
        book.setEditionNumber(2);

        lenient().when(jdbcTemplate.<String>query(anyString(), ArgumentMatchers.<RowMapper<String>>any(), ArgumentMatchers.<Object[]>any()))
                .thenReturn(Collections.<String>emptyList());

        doAnswer(invocation -> {
            PreparedStatementSetter setter = invocation.getArgument(1);
            RowMapper<Object> rowMapper = invocation.getArgument(2);
            setter.setValues(mock(PreparedStatement.class));

            ResultSet primary = mockResult("primary-id", 2);
            ResultSet sibling = mockResult("sibling-id", 1);
            return List.of(
                    rowMapper.mapRow(primary, 0),
                    rowMapper.mapRow(sibling, 1)
            );
        }).when(jdbcTemplate).query(eq("SELECT id, edition_number FROM books WHERE edition_group_key = ?"),
                any(PreparedStatementSetter.class), ArgumentMatchers.<RowMapper<Object>>any());

        ReflectionTestUtils.invokeMethod(orchestrator, "synchronizeEditionRelationships", "primary-id", book);

        verify(jdbcTemplate).update(
                eq("DELETE FROM book_editions WHERE book_id = ? OR related_book_id = ?"),
                eq("primary-id"),
                eq("primary-id")
        );
        verify(jdbcTemplate).update(
                eq("DELETE FROM book_editions WHERE book_id = ? OR related_book_id = ?"),
                eq("sibling-id"),
                eq("sibling-id")
        );
        verify(jdbcTemplate).update(
                eq("INSERT INTO book_editions (id, book_id, related_book_id, link_source, relationship_type, created_at, updated_at) " +
                        "VALUES (?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON CONFLICT (book_id, related_book_id) DO UPDATE SET link_source = EXCLUDED.link_source, relationship_type = EXCLUDED.relationship_type, updated_at = NOW()"),
                any(),
                eq("primary-id"),
                eq("sibling-id"),
                eq("INGESTION"),
                eq("ALTERNATE_EDITION")
        );
    }

    private void whenExternalIdLookupReturns(String bookId) {
        lenient().when(jdbcTemplate.<String>query(
                eq("SELECT book_id FROM book_external_ids WHERE source = ? AND external_id = ? LIMIT 1"),
                ArgumentMatchers.<RowMapper<String>>any(),
                eq("GOOGLE_BOOKS"),
                any()
        )).thenReturn(List.of(bookId));
    }

    private ResultSet mockResult(String id, Integer editionNumber) throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("id")).thenReturn(id);
        when(resultSet.getObject("edition_number")).thenReturn(editionNumber);
        return resultSet;
    }
}
