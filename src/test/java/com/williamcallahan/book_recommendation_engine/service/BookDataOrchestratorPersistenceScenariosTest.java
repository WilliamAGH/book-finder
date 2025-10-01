package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Objects;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Covers retrofit checklist scenarios for dedupe and edition-chaining behaviour.
 * See docs/task-retrofit-code-for-postgres-schema.md.
 */
@ExtendWith(MockitoExtension.class)
class BookDataOrchestratorPersistenceScenariosTest {

    @Mock
    private GoogleApiFetcher googleApiFetcher;

    @Mock
    private BookSearchService bookSearchService;

    @Mock
    private com.williamcallahan.book_recommendation_engine.mapper.GoogleBooksMapper googleBooksMapper;

    @Mock
    private TieredBookSearchService tieredBookSearchService;

    @Mock
    private BookUpsertService bookUpsertService;

    @Mock
    private JdbcTemplate jdbcTemplate;

    private BookDataOrchestrator orchestrator;

    // Concrete dependencies instantiated in setUp()
    private PostgresBookRepository postgresBookRepository;

    @BeforeEach
    void setUp() {
        ObjectMapper om = new ObjectMapper();
        postgresBookRepository = new PostgresBookRepository(jdbcTemplate, om);

        orchestrator = new BookDataOrchestrator(
                googleApiFetcher,
                om,
                bookSearchService,
                postgresBookRepository,
                bookUpsertService,
                googleBooksMapper,
                tieredBookSearchService,
                false
        );
        lenient().when(bookSearchService.searchBooks(anyString(), any())).thenReturn(List.of());
        lenient().when(bookSearchService.searchByIsbn(anyString())).thenReturn(java.util.Optional.empty());
        lenient().when(bookSearchService.searchAuthors(anyString(), any())).thenReturn(List.of());
        lenient().doNothing().when(bookSearchService).refreshMaterializedView();

        lenient().when(jdbcTemplate.queryForObject(anyString(), eq(String.class), ArgumentMatchers.<Object[]>any()))
                .thenThrow(new EmptyResultDataAccessException(1));
    }

    @Test
    @Disabled("Method resolveCanonicalBookId was moved from BookDataOrchestrator to CanonicalBookPersistenceService (deprecated)")
    void resolveCanonicalBookId_prefersExistingExternalMapping() {
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
    @Disabled("Method synchronizeEditionRelationships was moved to CanonicalBookPersistenceService and is now disabled. Edition relationships are handled by work_cluster_members table.")
    void synchronizeEditionRelationships_linksHighestEditionAsPrimary() {
        Book book = new Book();
        book.setEditionGroupKey("group-key");
        book.setEditionNumber(2);

        ReflectionTestUtils.invokeMethod(orchestrator, "synchronizeEditionRelationships", "primary-id", book);

        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void parseBookJsonPayload_returnsNullForConcatenatedPayloads() {
        String payload = "{\"id\":\"-0UZAAAAYAAJ\",\"title\":\"-0UZAAAAYAAJ\",\"authors\":[\"Ralph Tate\",\"Samuel Peckworth Woodward\"],\"publisher\":\"C. Lockwood and Company\",\"publishedDate\":\"1830\",\"pageCount\":627,\"rawJsonResponse\":\"{\\\"kind\\\":\\\"books#volume\\\",\\\"id\\\":\\\"-0UZAAAAYAAJ\\\",\\\"etag\\\":\\\"PAlEva08Grw\\\",\\\"selfLink\\\":\\\"https://www.googleapis.com/books/v1/volumes/-0UZAAAAYAAJ\\\",\\\"volumeInfo\\\":{\\\"title\\\":\\\"A Manual of the Mollusca\\\",\\\"subtitle\\\":\\\"Being a Treatise on Recent and Fossil Shells\\\",\\\"authors\\\":[\\\"Samuel Peckworth Woodward\\\",\\\"Ralph Tate\\\"],\\\"publisher\\\":\\\"C. Lockwood and Company\\\",\\\"publishedDate\\\":\\\"1830\\\",\\\"pageCount\\\":627}}\",\"rawJsonSource\":\"GoogleBooks\",\"contributingSources\":[\"GoogleBooks\"]}{\"id\":\"-0UZAAAAYAAJ\",\"title\":\"-0UZAAAAYAAJ\",\"authors\":[\"Ralph Tate\",\"Samuel Peckworth Woodward\"],\"publisher\":\"C. Lockwood and Company\",\"publishedDate\":\"1830\",\"pageCount\":627,\"rawJsonResponse\":\"{\\\"kind\\\":\\\"books#volume\\\",\\\"id\\\":\\\"-0UZAAAAYAAJ\\\",\\\"volumeInfo\\\":{\\\"title\\\":\\\"A Manual of the Mollusca\\\"}}\",\"rawJsonSource\":\"GoogleBooks\",\"contributingSources\":[\"GoogleBooks\"]}";

        JsonNode result = ReflectionTestUtils.invokeMethod(orchestrator, "parseBookJsonPayload", payload, "test.json");

        assertThat(result).isNull();
    }

    @Test
    void parseBookJsonPayload_unwrapsRawJsonResponse() {
        String payload = "{\"id\":\"--AMEAAAQBAJ\",\"title\":\"--AMEAAAQBAJ\",\"authors\":[\"Gabriel Gambetta\"],\"description\":\"Computer graphics book\",\"publisher\":\"No Starch Press\",\"publishedDate\":\"2021-05-18\",\"pageCount\":248,\"rawJsonResponse\":\"{\\\"volumeInfo\\\":{\\\"title\\\":\\\"Computer Graphics from Scratch\\\",\\\"authors\\\":[\\\"Gabriel Gambetta\\\"]}}\",\"rawJsonSource\":\"GoogleBooks\"}";

        JsonNode result = ReflectionTestUtils.invokeMethod(orchestrator, "parseBookJsonPayload", payload, "single.json");

        assertThat(result).isNotNull();
        JsonNode safeResult = Objects.requireNonNull(result);
        assertThat(safeResult.path("id").asText()).isEqualTo("--AMEAAAQBAJ");
        assertThat(safeResult.path("rawJsonResponse").isTextual()).isTrue();
    }

    private void whenExternalIdLookupReturns(String bookId) {
        lenient().when(jdbcTemplate.queryForObject(
                eq("SELECT book_id FROM book_external_ids WHERE source = ? AND external_id = ? LIMIT 1"),
                eq(String.class),
                eq(ApplicationConstants.Provider.GOOGLE_BOOKS),
                any()
        )).thenReturn(bookId);
    }
}
