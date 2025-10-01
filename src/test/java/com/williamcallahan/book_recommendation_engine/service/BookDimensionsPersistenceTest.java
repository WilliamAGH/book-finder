package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.test.annotations.DbIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for book_dimensions table persistence.
 * 
 * Tests verify:
 * - New schema with book_id as PRIMARY KEY (no 'id' column)
 * - Column names without '_cm' suffix (height, width, thickness)
 * - updated_at timestamp column
 * - COALESCE logic for partial updates
 */
@DbIntegrationTest
class BookDimensionsPersistenceTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    @Autowired
    private PostgresBookRepository postgresBookRepository;
    
    @Autowired
    private BookUpsertService bookUpsertService;

    private UUID testBookId;

    @BeforeEach
    void setUp() {
        testBookId = createTestBook();
    }

    @Test
    void testSchema_bookIdIsPrimaryKey() {
        // Verify schema structure - book_id should be PRIMARY KEY, not 'id'
        String pkQuery = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name = 'book_dimensions'
            AND tc.constraint_type = 'PRIMARY KEY'
            """;
        
        List<String> pkColumns = jdbcTemplate.query(pkQuery, 
            (rs, rowNum) -> rs.getString("column_name"));
        
        assertThat(pkColumns).containsExactly("book_id");
    }

    @Test
    void testSchema_noIdColumn() {
        // Verify 'id' column does NOT exist in new schema
        String columnQuery = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'book_dimensions' 
            AND column_name = 'id'
            """;
        
        List<String> columns = jdbcTemplate.query(columnQuery, 
            (rs, rowNum) -> rs.getString("column_name"));
        
        assertThat(columns).isEmpty();
    }

    @Test
    void testSchema_columnsWithoutCmSuffix() {
        // Verify columns are named 'height', 'width', 'thickness' (not height_cm, etc.)
        String columnQuery = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'book_dimensions' 
            AND column_name IN ('height', 'width', 'thickness')
            ORDER BY column_name
            """;
        
        List<String> columns = jdbcTemplate.query(columnQuery, 
            (rs, rowNum) -> rs.getString("column_name"));
        
        assertThat(columns).containsExactly("height", "thickness", "width");
    }

    @Test
    void testSchema_hasUpdatedAtColumn() {
        // Verify updated_at column exists
        String columnQuery = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'book_dimensions' 
            AND column_name = 'updated_at'
            """;
        
        Map<String, Object> column = jdbcTemplate.queryForMap(columnQuery);
        
        assertThat(column.get("column_name")).isEqualTo("updated_at");
        assertThat(column.get("data_type")).isEqualTo("timestamp with time zone");
    }

    @Test
    void testInsert_newDimensionRecord() {
        // Insert new dimension record
        jdbcTemplate.update(
            "INSERT INTO book_dimensions (book_id, height, width, thickness, created_at, updated_at) " +
            "VALUES (?, ?, ?, ?, NOW(), NOW())",
            testBookId, 24.0, 16.0, 2.5
        );
        
        // Verify insertion
        Map<String, Object> result = jdbcTemplate.queryForMap(
            "SELECT book_id, height, width, thickness FROM book_dimensions WHERE book_id = ?",
            testBookId
        );
        
        assertThat(result.get("book_id")).isEqualTo(testBookId);
        assertThat(result.get("height")).isEqualTo(new BigDecimal("24.0"));
        assertThat(result.get("width")).isEqualTo(new BigDecimal("16.0"));
        assertThat(result.get("thickness")).isEqualTo(new BigDecimal("2.5"));
    }

    @Test
    void testInsert_withPartialData() {
        // Insert with only height (width/thickness null)
        jdbcTemplate.update(
            "INSERT INTO book_dimensions (book_id, height, created_at, updated_at) " +
            "VALUES (?, ?, NOW(), NOW())",
            testBookId, 24.0
        );
        
        // Verify null handling
        Map<String, Object> result = jdbcTemplate.queryForMap(
            "SELECT height, width, thickness FROM book_dimensions WHERE book_id = ?",
            testBookId
        );
        
        assertThat(result.get("height")).isEqualTo(new BigDecimal("24.0"));
        assertThat(result.get("width")).isNull();
        assertThat(result.get("thickness")).isNull();
    }

    @Test
    void testUpsert_conflictPreservesExistingNonNullValues() {
        // Insert initial dimensions with all values
        jdbcTemplate.update(
            "INSERT INTO book_dimensions (book_id, height, width, thickness, created_at, updated_at) " +
            "VALUES (?, ?, ?, ?, NOW(), NOW())",
            testBookId, 24.0, 16.0, 2.5
        );
        
        // Update with partial data (null width and thickness)
        jdbcTemplate.update(
            """
            INSERT INTO book_dimensions (book_id, height, width, thickness, created_at, updated_at)
            VALUES (?, ?, ?, ?, NOW(), NOW())
            ON CONFLICT (book_id) DO UPDATE SET
                height = COALESCE(EXCLUDED.height, book_dimensions.height),
                width = COALESCE(EXCLUDED.width, book_dimensions.width),
                thickness = COALESCE(EXCLUDED.thickness, book_dimensions.thickness),
                updated_at = NOW()
            """,
            testBookId, 25.0, null, null
        );
        
        // Verify COALESCE preserved existing width and thickness
        Map<String, Object> result = jdbcTemplate.queryForMap(
            "SELECT height, width, thickness FROM book_dimensions WHERE book_id = ?",
            testBookId
        );
        
        assertThat(result.get("height")).isEqualTo(new BigDecimal("25.0")); // Updated
        assertThat(result.get("width")).isEqualTo(new BigDecimal("16.0"));  // Preserved
        assertThat(result.get("thickness")).isEqualTo(new BigDecimal("2.5")); // Preserved
    }

    @Test
    void testUpsert_conflictReplacesWithNewNonNullValues() {
        // Insert initial dimensions
        jdbcTemplate.update(
            "INSERT INTO book_dimensions (book_id, height, width, thickness, created_at, updated_at) " +
            "VALUES (?, ?, ?, ?, NOW(), NOW())",
            testBookId, 24.0, 16.0, 2.5
        );
        
        // Update with all new non-null values
        jdbcTemplate.update(
            """
            INSERT INTO book_dimensions (book_id, height, width, thickness, created_at, updated_at)
            VALUES (?, ?, ?, ?, NOW(), NOW())
            ON CONFLICT (book_id) DO UPDATE SET
                height = COALESCE(EXCLUDED.height, book_dimensions.height),
                width = COALESCE(EXCLUDED.width, book_dimensions.width),
                thickness = COALESCE(EXCLUDED.thickness, book_dimensions.thickness),
                updated_at = NOW()
            """,
            testBookId, 25.0, 17.0, 3.0
        );
        
        // Verify all values updated
        Map<String, Object> result = jdbcTemplate.queryForMap(
            "SELECT height, width, thickness FROM book_dimensions WHERE book_id = ?",
            testBookId
        );
        
        assertThat(result.get("height")).isEqualTo(new BigDecimal("25.0"));
        assertThat(result.get("width")).isEqualTo(new BigDecimal("17.0"));
        assertThat(result.get("thickness")).isEqualTo(new BigDecimal("3.0"));
    }

    @Test
    void testUpdatedAt_changesOnUpdate() throws InterruptedException {
        // Insert initial record
        jdbcTemplate.update(
            "INSERT INTO book_dimensions (book_id, height, created_at, updated_at) " +
            "VALUES (?, ?, NOW(), NOW())",
            testBookId, 24.0
        );
        
        Instant initialUpdatedAt = jdbcTemplate.queryForObject(
            "SELECT updated_at FROM book_dimensions WHERE book_id = ?",
            Instant.class,
            testBookId
        );
        
        // Wait to ensure timestamp difference
        Thread.sleep(100);
        
        // Update record
        jdbcTemplate.update(
            """
            INSERT INTO book_dimensions (book_id, height, created_at, updated_at)
            VALUES (?, ?, NOW(), NOW())
            ON CONFLICT (book_id) DO UPDATE SET
                height = EXCLUDED.height,
                updated_at = NOW()
            """,
            testBookId, 25.0
        );
        
        Instant newUpdatedAt = jdbcTemplate.queryForObject(
            "SELECT updated_at FROM book_dimensions WHERE book_id = ?",
            Instant.class,
            testBookId
        );
        
        assertThat(newUpdatedAt).isAfter(initialUpdatedAt);
    }

    @Test
    void testPostgresBookRepository_hydrateDimensionsSuccess() {
        // Insert dimensions
        jdbcTemplate.update(
            "INSERT INTO book_dimensions (book_id, height, width, thickness, weight_grams, created_at, updated_at) " +
            "VALUES (?, ?, ?, ?, ?, NOW(), NOW())",
            testBookId, 24.0, 16.0, 2.5, 450.0
        );
        
        // Load book via repository
        Optional<Book> bookOpt = postgresBookRepository.fetchByCanonicalId(testBookId.toString());
        
        assertThat(bookOpt).isPresent();
        Book book = bookOpt.get();
        assertThat(book.getHeightCm()).isEqualTo(24.0);
        assertThat(book.getWidthCm()).isEqualTo(16.0);
        assertThat(book.getThicknessCm()).isEqualTo(2.5);
        assertThat(book.getWeightGrams()).isEqualTo(450.0);
    }

    @Test
    void testPostgresBookRepository_hydrateDimensionsWithNulls() {
        // Insert dimensions with only height
        jdbcTemplate.update(
            "INSERT INTO book_dimensions (book_id, height, created_at, updated_at) " +
            "VALUES (?, ?, NOW(), NOW())",
            testBookId, 24.0
        );
        
        // Load book via repository
        Optional<Book> bookOpt = postgresBookRepository.fetchByCanonicalId(testBookId.toString());
        
        assertThat(bookOpt).isPresent();
        Book book = bookOpt.get();
        assertThat(book.getHeightCm()).isEqualTo(24.0);
        assertThat(book.getWidthCm()).isNull();
        assertThat(book.getThicknessCm()).isNull();
        assertThat(book.getWeightGrams()).isNull();
    }

    @Test
    void testPostgresBookRepository_noDimensionsReturnsNulls() {
        // Don't insert any dimensions for this book
        
        // Load book via repository
        Optional<Book> bookOpt = postgresBookRepository.fetchByCanonicalId(testBookId.toString());
        
        assertThat(bookOpt).isPresent();
        Book book = bookOpt.get();
        assertThat(book.getHeightCm()).isNull();
        assertThat(book.getWidthCm()).isNull();
        assertThat(book.getThicknessCm()).isNull();
        assertThat(book.getWeightGrams()).isNull();
    }

    @Test
    void testBookUpsertService_persistDimensions() {
        BookAggregate aggregate = buildAggregate(
            "dim-full",
            BookAggregate.Dimensions.builder()
                .height("24.0 cm")
                .width("16.0 cm")
                .thickness("2.5 cm")
                .build()
        );

        BookUpsertService.UpsertResult result = bookUpsertService.upsert(aggregate);

        Map<String, Object> row = jdbcTemplate.queryForMap(
            "SELECT height, width, thickness, weight_grams FROM book_dimensions WHERE book_id = ?",
            result.getBookId()
        );

        assertThat((BigDecimal) row.get("height")).isEqualByComparingTo("24.0");
        assertThat((BigDecimal) row.get("width")).isEqualByComparingTo("16.0");
        assertThat((BigDecimal) row.get("thickness")).isEqualByComparingTo("2.5");
        assertThat(row.get("weight_grams")).isNull();
    }

    @Test
    void testBookUpsertService_persistDimensionsWithNulls() {
        BookAggregate aggregate = buildAggregate(
            "dim-partial",
            BookAggregate.Dimensions.builder()
                .height("24.0 cm")
                .build()
        );

        BookUpsertService.UpsertResult result = bookUpsertService.upsert(aggregate);

        Map<String, Object> row = jdbcTemplate.queryForMap(
            "SELECT height, width, thickness FROM book_dimensions WHERE book_id = ?",
            result.getBookId()
        );

        assertThat((BigDecimal) row.get("height")).isEqualByComparingTo("24.0");
        assertThat(row.get("width")).isNull();
        assertThat(row.get("thickness")).isNull();
    }

    @Test
    void testBookUpsertService_deletesDimensionsWhenAllNull() {
        String externalId = "dim-delete";
        BookAggregate createAggregate = buildAggregate(
            externalId,
            BookAggregate.Dimensions.builder()
                .height("24.0 cm")
                .width("16.0 cm")
                .thickness("2.5 cm")
                .build()
        );

        BookUpsertService.UpsertResult initial = bookUpsertService.upsert(createAggregate);

        Integer countBefore = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM book_dimensions WHERE book_id = ?",
            Integer.class,
            initial.getBookId()
        );
        assertThat(countBefore).isEqualTo(1);

        BookAggregate clearAggregate = buildAggregate(
            externalId,
            BookAggregate.Dimensions.builder().build()
        );

        bookUpsertService.upsert(clearAggregate);

        Integer countAfter = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM book_dimensions WHERE book_id = ?",
            Integer.class,
            initial.getBookId()
        );
        assertThat(countAfter).isZero();
    }

    @Test
    void testWeightGrams_persistsCorrectly() {
        // Insert dimensions with weight
        jdbcTemplate.update(
            "INSERT INTO book_dimensions (book_id, height, weight_grams, created_at, updated_at) " +
            "VALUES (?, ?, ?, NOW(), NOW())",
            testBookId, 24.0, 450.0
        );
        
        // Verify weight persisted (note: weight_grams keeps its suffix)
        Map<String, Object> result = jdbcTemplate.queryForMap(
            "SELECT weight_grams FROM book_dimensions WHERE book_id = ?",
            testBookId
        );
        
        assertThat(result.get("weight_grams")).isEqualTo(new BigDecimal("450.0"));
    }

    @Test
    void testCascadeDelete_dimensionsDeletedWhenBookDeleted() {
        // Insert dimensions
        jdbcTemplate.update(
            "INSERT INTO book_dimensions (book_id, height, created_at, updated_at) " +
            "VALUES (?, ?, NOW(), NOW())",
            testBookId, 24.0
        );
        
        // Verify dimension exists
        Integer countBefore = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM book_dimensions WHERE book_id = ?",
            Integer.class,
            testBookId
        );
        assertThat(countBefore).isEqualTo(1);
        
        // Delete the book
        jdbcTemplate.update("DELETE FROM books WHERE id = ?", testBookId);
        
        // Verify dimension was cascade deleted
        Integer countAfter = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM book_dimensions WHERE book_id = ?",
            Integer.class,
            testBookId
        );
        assertThat(countAfter).isZero();
    }

    /**
     * Helper method to create a test book in the database.
     */
    private UUID createTestBook() {
        UUID bookId = UUID.randomUUID();
        jdbcTemplate.update(
            "INSERT INTO books (id, title, slug, created_at, updated_at) " +
            "VALUES (?, ?, ?, NOW(), NOW())",
            bookId, 
            "Test Book " + bookId, 
            "test-book-" + bookId
        );
        return bookId;
    }

    private BookAggregate buildAggregate(String externalId, BookAggregate.Dimensions dimensions) {
        return BookAggregate.builder()
            .title("Dimensions Test " + externalId)
            .slugBase("dimensions-test-" + externalId)
            .authors(List.of("Test Author"))
            .categories(List.of())
            .identifiers(BookAggregate.ExternalIdentifiers.builder()
                .source("TEST")
                .externalId(externalId)
                .build())
            .dimensions(dimensions)
            .build();
    }
}
