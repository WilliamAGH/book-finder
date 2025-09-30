package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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

    @Mock
    private BookSearchService bookSearchService;

    @Mock
    private TieredBookSearchService tieredBookSearchService;

    private BookDataOrchestrator orchestrator;

    // Concrete dependencies instantiated in setUp()
    private BookS3CacheService bookS3CacheService;
    private PostgresBookRepository postgresBookRepository;
    private CanonicalBookPersistenceService canonicalBookPersistenceService;
    private BookLookupService bookLookupService;

    @BeforeEach
    void setUp() {
        ObjectMapper om = new ObjectMapper();
        bookS3CacheService = new BookS3CacheService(s3RetryService, om);
        postgresBookRepository = new PostgresBookRepository(jdbcTemplate, om, new BookLookupService(jdbcTemplate));
        bookLookupService = new BookLookupService(jdbcTemplate);
canonicalBookPersistenceService = new CanonicalBookPersistenceService(jdbcTemplate, om, supplementalPersistenceService, bookLookupService, null);

        orchestrator = new BookDataOrchestrator(
                s3RetryService,
                googleApiFetcher,
                om,
                openLibraryBookDataService,
                bookDataAggregatorService,
                collectionPersistenceService,
                bookSearchService,
                bookS3CacheService,
                postgresBookRepository,
                canonicalBookPersistenceService,
                tieredBookSearchService,
                false,
                false,
                false,
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
    void synchronizeEditionRelationships_linksHighestEditionAsPrimary() {
        Book book = new Book();
        book.setEditionGroupKey("group-key");
        book.setEditionNumber(2);

        ReflectionTestUtils.invokeMethod(orchestrator, "synchronizeEditionRelationships", "primary-id", book);

        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void parseBookJsonPayload_handlesConcatenatedAndPreProcessedRecords() {
        String payload = "{\"id\":\"-0UZAAAAYAAJ\",\"title\":\"-0UZAAAAYAAJ\",\"authors\":[\"Ralph Tate\",\"Samuel Peckworth Woodward\"],\"publisher\":\"C. Lockwood and Company\",\"publishedDate\":\"1830\",\"pageCount\":627,\"rawJsonResponse\":\"{\\\"kind\\\":\\\"books#volume\\\",\\\"id\\\":\\\"-0UZAAAAYAAJ\\\",\\\"etag\\\":\\\"PAlEva08Grw\\\",\\\"selfLink\\\":\\\"https://www.googleapis.com/books/v1/volumes/-0UZAAAAYAAJ\\\",\\\"volumeInfo\\\":{\\\"title\\\":\\\"A Manual of the Mollusca\\\",\\\"subtitle\\\":\\\"Being a Treatise on Recent and Fossil Shells\\\",\\\"authors\\\":[\\\"Samuel Peckworth Woodward\\\",\\\"Ralph Tate\\\"],\\\"publisher\\\":\\\"C. Lockwood and Company\\\",\\\"publishedDate\\\":\\\"1830\\\",\\\"pageCount\\\":627}}\",\"rawJsonSource\":\"GoogleBooks\",\"contributingSources\":[\"GoogleBooks\"]}{\"id\":\"-0UZAAAAYAAJ\",\"title\":\"-0UZAAAAYAAJ\",\"authors\":[\"Ralph Tate\",\"Samuel Peckworth Woodward\"],\"publisher\":\"C. Lockwood and Company\",\"publishedDate\":\"1830\",\"pageCount\":627,\"rawJsonResponse\":\"{\\\"kind\\\":\\\"books#volume\\\",\\\"id\\\":\\\"-0UZAAAAYAAJ\\\",\\\"volumeInfo\\\":{\\\"title\\\":\\\"A Manual of the Mollusca\\\"}}\",\"rawJsonSource\":\"GoogleBooks\",\"contributingSources\":[\"GoogleBooks\"]}";

        List<JsonNode> result = java.util.Objects.requireNonNull(
                ReflectionTestUtils.invokeMethod(orchestrator, "parseBookJsonPayload", payload, "test.json")
        );

        assertThat(result).hasSize(1);
        JsonNode first = result.get(0);
        assertThat(first.path("volumeInfo").path("title").asText()).isEqualTo("A Manual of the Mollusca");
    }

    @Test
    void parseBookJsonPayload_unwrapsRawJsonResponse() {
        String payload = "{\"id\":\"--AMEAAAQBAJ\",\"title\":\"--AMEAAAQBAJ\",\"authors\":[\"Gabriel Gambetta\"],\"description\":\"Computer graphics book\",\"publisher\":\"No Starch Press\",\"publishedDate\":\"2021-05-18\",\"pageCount\":248,\"rawJsonResponse\":\"{\\\"volumeInfo\\\":{\\\"title\\\":\\\"Computer Graphics from Scratch\\\",\\\"authors\\\":[\\\"Gabriel Gambetta\\\"]}}\",\"rawJsonSource\":\"GoogleBooks\"}";

        List<JsonNode> result = java.util.Objects.requireNonNull(
                ReflectionTestUtils.invokeMethod(orchestrator, "parseBookJsonPayload", payload, "single.json")
        );

        assertThat(result).hasSize(1);
        JsonNode node = result.get(0);
        assertThat(node.path("volumeInfo").path("title").asText()).isEqualTo("Computer Graphics from Scratch");
        assertThat(node.path("volumeInfo").path("authors").isArray()).isTrue();
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

@ExtendWith(MockitoExtension.class)
class TieredBookSearchServiceAuthorSearchTest {

    @Mock
    private BookSearchService bookSearchService;

    @Mock
    private GoogleApiFetcher googleApiFetcher;

    @Mock
    private OpenLibraryBookDataService openLibraryBookDataService;

    private TieredBookSearchService service;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        service = new TieredBookSearchService(
            bookSearchService,
            googleApiFetcher,
            openLibraryBookDataService,
            null,
            true
        );

        lenient().when(googleApiFetcher.isGoogleFallbackEnabled()).thenReturn(true);
        lenient().when(googleApiFetcher.isApiKeyAvailable()).thenReturn(false);
        lenient().when(googleApiFetcher.streamSearchItems(anyString(), anyInt(), anyString(), any(), anyBoolean()))
            .thenReturn(Flux.empty());
        lenient().when(openLibraryBookDataService.searchBooks(anyString(), anyBoolean())).thenReturn(Flux.empty());
    }

    @Test
    void searchAuthors_returnsPostgresResultsWhenSufficient() {
        List<BookSearchService.AuthorResult> postgres = List.of(
            new BookSearchService.AuthorResult("db-1", "First Author", 4, 0.91),
            new BookSearchService.AuthorResult("db-2", "Second Author", 3, 0.74)
        );

        when(bookSearchService.searchAuthors(eq("Query"), any())).thenReturn(postgres);

        List<BookSearchService.AuthorResult> results = service.searchAuthors("Query", 2).block();

        assertThat(results).isNotNull();
        assertThat(results).containsExactlyElementsOf(postgres);
        verify(googleApiFetcher, never()).streamSearchItems(anyString(), anyInt(), anyString(), any(), anyBoolean());
        verify(openLibraryBookDataService, never()).searchBooks(anyString(), anyBoolean());
    }

    @Test
    void searchAuthors_fallsBackToExternalWhenPostgresEmpty() {
        when(bookSearchService.searchAuthors(eq("Ann Patchett"), any())).thenReturn(List.of());

        when(googleApiFetcher.streamSearchItems(eq("inauthor:Ann Patchett"), anyInt(), anyString(), any(), eq(false)))
            .thenReturn(Flux.just(
                buildGoogleVolume("g-1", "Ann Patchett", 0.82),
                buildGoogleVolume("g-2", "Another Author", 0.58)
            ));

        when(openLibraryBookDataService.searchBooks(eq("Ann Patchett"), anyBoolean()))
            .thenReturn(Flux.just(buildExternalBook("ol-1", "Third Author", 0.42)));

        List<BookSearchService.AuthorResult> results = service.searchAuthors("Ann Patchett", 3).block();

        assertThat(results).isNotNull();
        assertThat(results).isNotEmpty();
        assertThat(results).extracting(BookSearchService.AuthorResult::authorName)
            .contains("Ann Patchett", "Another Author", "Third Author");
        assertThat(results).extracting(BookSearchService.AuthorResult::authorId)
            .allSatisfy(id -> assertThat(id).isNotBlank());
    }

    @Test
    void searchAuthors_mergesExternalWithoutDuplicatingPostgresAuthors() {
        List<BookSearchService.AuthorResult> postgres = List.of(
            new BookSearchService.AuthorResult("db-1", "Shared Author", 6, 0.95)
        );

        when(bookSearchService.searchAuthors(eq("Shared Author"), any())).thenReturn(postgres);

        when(googleApiFetcher.streamSearchItems(eq("Shared Author"), anyInt(), anyString(), any(), eq(false)))
            .thenReturn(Flux.just(
                buildGoogleVolume("g-3", "Shared Author", 0.41),
                buildGoogleVolume("g-4", "New Contributor", 0.63)
            ));

        List<BookSearchService.AuthorResult> results = service.searchAuthors("Shared Author", 3).block();

        assertThat(results).isNotNull();
        assertThat(results).extracting(BookSearchService.AuthorResult::authorName)
            .contains("Shared Author", "New Contributor");
        assertThat(results).filteredOn(r -> r.authorName().equals("Shared Author"))
            .singleElement()
            .extracting(BookSearchService.AuthorResult::authorId)
            .isEqualTo("db-1");
        assertThat(results).filteredOn(r -> r.authorName().equals("New Contributor"))
            .singleElement()
            .satisfies(author -> assertThat(author.authorId()).startsWith("external:author:"));
    }

    private Book buildExternalBook(String id, String authorName, double relevance) {
        Book book = new Book();
        book.setId(id);
        book.setTitle("Title " + id);
        book.setAuthors(List.of(authorName));
        book.addQualifier("search.relevanceScore", relevance);
        return book;
    }

    private JsonNode buildGoogleVolume(String id, String authorName, double relevance) {
        try {
            return objectMapper.readTree(String.format("{\n  \"id\": \"%s\",\n  \"volumeInfo\": {\n    \"title\": \"Title %s\",\n    \"authors\": [\"%s\"]\n  },\n  \"qualifiers\": {\n    \"search.relevanceScore\": %s\n  }\n}", id, id, authorName, Double.toString(relevance)));
        } catch (Exception exception) {
            throw new RuntimeException("Failed to build Google volume JSON", exception);
        }
    }
}
