package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.s3.S3FetchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class PostgresBookReaderDedupeTest {

    private static final String PRIMARY_ID = "00000000-0000-0000-0000-000000000111";
    private final JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
    private BookDataOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        S3RetryService s3RetryService = Mockito.mock(S3RetryService.class);
        lenient().when(s3RetryService.fetchJsonWithRetry(anyString()))
                .thenReturn(CompletableFuture.completedFuture(S3FetchResult.disabled()));
        lenient().when(s3RetryService.uploadJsonWithRetry(anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        var om = new com.fasterxml.jackson.databind.ObjectMapper();
        var google = Mockito.mock(GoogleApiFetcher.class);
        var openLibrary = Mockito.mock(OpenLibraryBookDataService.class);
        var aggregator = Mockito.mock(BookDataAggregatorService.class);
        var collection = Mockito.mock(BookCollectionPersistenceService.class);
        var search = createBookSearchServiceMock();

        PostgresBookRepository repo = new PostgresBookRepository(jdbcTemplate, om, new BookLookupService(jdbcTemplate));

        orchestrator = new BookDataOrchestrator(
                s3RetryService,
                google,
                om,
                openLibrary,
                aggregator,
                collection,
                search,
                new BookS3CacheService(s3RetryService, om),
                repo,
                null,
                null,
                false,
                false
        );

        stubDatabaseQueries();
    }

    private BookSearchService createBookSearchServiceMock() {
        BookSearchService searchService = Mockito.mock(BookSearchService.class);
        lenient().when(searchService.searchBooks(anyString(), any())).thenReturn(List.of());
        lenient().when(searchService.searchByIsbn(anyString())).thenReturn(java.util.Optional.empty());
        lenient().doNothing().when(searchService).refreshMaterializedView();
        return searchService;
    }

    @Test
    void returnsCanonicalBookWithEditionChainFromHydratedPostgresRows() {
        StepVerifier.create(orchestrator.getBookByIdTiered(PRIMARY_ID))
                .assertNext(book -> {
                    assertThat(book.getId()).isEqualTo(PRIMARY_ID);
                    assertThat(book.getSlug()).isEqualTo("primary-fixture-hardcover");
                    assertThat(book.getHeightCm()).isEqualTo(23.1);
                    assertThat(book.getWidthCm()).isEqualTo(15.2);
                    assertThat(book.getThicknessCm()).isEqualTo(3.1);
                    assertThat(book.getWeightGrams()).isEqualTo(540.0);
                    assertThat(book.getRawJsonResponse()).contains("google-primary");

                    List<Book.EditionInfo> editions = book.getOtherEditions();
                    assertThat(editions).hasSize(1);
                    Book.EditionInfo edition = editions.getFirst();
                    assertThat(edition.getIdentifier()).isEqualTo("primary-fixture-paperback");
                    assertThat(edition.getGoogleBooksId()).isEqualTo("google-paper");
                    assertThat(edition.getEditionIsbn13()).isEqualTo("9780000000222");
                    assertThat(edition.getType()).isEqualTo("ISBN_PREFIX");
                    assertThat(edition.getCoverImageUrl()).isEqualTo("covers/primary-fixture-paperback.jpg");
                })
                .verifyComplete();
    }

    @SuppressWarnings("unchecked")
    private void stubDatabaseQueries() {
        Book canonical = buildCanonicalFixture();

        lenient().when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(ResultSetExtractor.class)))
                .thenAnswer(invocation -> {
                    String sql = normalizeSql(invocation.getArgument(0));
                    if (sql.startsWith("SELECT id::text, slug, title")) {
                        return Optional.of(copyBook(canonical));
                    }
                    if (sql.startsWith("SELECT raw_json_response::text")) {
                        return canonical.getRawJsonResponse();
                    }
                    return null;
                });

        lenient().when(jdbcTemplate.query(anyString(), any(PreparedStatementSetter.class), any(RowMapper.class)))
                .thenReturn(List.of());

        lenient().when(jdbcTemplate.queryForObject(anyString(), Mockito.eq(java.util.UUID.class), any()))
                .thenThrow(new EmptyResultDataAccessException(1));
    }

    private String normalizeSql(String sql) {
        return sql == null ? "" : sql.replaceAll("\\s+", " ").trim();
    }

    private Book buildCanonicalFixture() {
        Book book = new Book();
        book.setId(PRIMARY_ID);
        book.setSlug("primary-fixture-hardcover");
        book.setTitle("Primary Fixture Hardcover");
        book.setDescription("Primary volume for edition chaining tests.");
        book.setIsbn10("0000000111");
        book.setIsbn13("9780000000111");
        book.setLanguage("en");
        book.setPublisher("Fixture House");
        book.setPageCount(320);
        book.setHeightCm(23.1);
        book.setWidthCm(15.2);
        book.setThicknessCm(3.1);
        book.setWeightGrams(540.0);
        book.setRawJsonResponse("{\"id\":\"google-primary\"}");
        book.setAuthors(List.of("Edition Author"));
        Book.EditionInfo edition = new Book.EditionInfo();
        edition.setIdentifier("primary-fixture-paperback");
        edition.setGoogleBooksId("google-paper");
        edition.setEditionIsbn13("9780000000222");
        edition.setEditionIsbn10("0000000222");
        edition.setType("ISBN_PREFIX");
        edition.setCoverImageUrl("covers/primary-fixture-paperback.jpg");
        book.setOtherEditions(List.of(edition));
        return book;
    }

    private Book copyBook(Book original) {
        Book clone = new Book();
        clone.setId(original.getId());
        clone.setSlug(original.getSlug());
        clone.setTitle(original.getTitle());
        clone.setDescription(original.getDescription());
        clone.setIsbn10(original.getIsbn10());
        clone.setIsbn13(original.getIsbn13());
        clone.setLanguage(original.getLanguage());
        clone.setPublisher(original.getPublisher());
        clone.setPageCount(original.getPageCount());
        clone.setHeightCm(original.getHeightCm());
        clone.setWidthCm(original.getWidthCm());
        clone.setThicknessCm(original.getThicknessCm());
        clone.setWeightGrams(original.getWeightGrams());
        clone.setRawJsonResponse(original.getRawJsonResponse());
        clone.setAuthors(new java.util.ArrayList<>(original.getAuthors()));
        clone.setOtherEditions(new java.util.ArrayList<>(original.getOtherEditions()));
        return clone;
    }

}
