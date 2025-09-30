package com.williamcallahan.book_recommendation_engine.repository;

import com.williamcallahan.book_recommendation_engine.dto.BookCard;
import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.dto.BookListItem;
import com.williamcallahan.book_recommendation_engine.dto.EditionSummary;
import com.williamcallahan.book_recommendation_engine.test.annotations.DbIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for BookQueryRepository - THE SINGLE SOURCE OF TRUTH for book queries.
 * 
 * These tests verify that optimized SQL functions work correctly and return
 * exactly what each view needs with minimal queries.
 * 
 * Tests only run when database is available (not in CI without DB).
 */
@DbIntegrationTest
class BookQueryRepositoryTest {

    @Autowired(required = false)
    private BookQueryRepository bookQueryRepository;

    @Autowired(required = false)
    private JdbcTemplate jdbcTemplate;

    @Test
    void contextLoads() {
        // Verify beans are wired correctly when DB available
        if (bookQueryRepository != null) {
            assertThat(bookQueryRepository).isNotNull();
            assertThat(jdbcTemplate).isNotNull();
        }
    }

    @Test
    void testFetchBookCards_returnsCardsWithAllRequiredFields() {
        if (bookQueryRepository == null || jdbcTemplate == null) {
            return; // Skip test if no DB
        }

        // Get a few book IDs from database
        List<UUID> bookIds = jdbcTemplate.query(
            "SELECT id FROM books LIMIT 3",
            (rs, rowNum) -> (UUID) rs.getObject("id")
        );

        if (bookIds.isEmpty()) {
            return; // No books in test DB
        }

        // Fetch book cards (SINGLE QUERY)
        List<BookCard> cards = bookQueryRepository.fetchBookCards(bookIds);

        // Verify we got results
        assertThat(cards).isNotEmpty();
        assertThat(cards.size()).isLessThanOrEqualTo(3);

        // Verify each card has required fields
        cards.forEach(card -> {
            assertThat(card.id()).isNotBlank();
            assertThat(card.slug()).isNotBlank();
            assertThat(card.title()).isNotBlank();
            assertThat(card.authors()).isNotNull(); // May be empty but not null
            // coverUrl, rating, tags may be null - that's OK
        });
    }

    @Test
    void testFetchBookCards_withEmptyList_returnsEmpty() {
        if (bookQueryRepository == null) {
            return;
        }

        List<BookCard> cards = bookQueryRepository.fetchBookCards(List.of());
        assertThat(cards).isEmpty();
    }

    @Test
    void testFetchBookCardsByProviderListCode_returnsNYTBestsellers() {
        if (bookQueryRepository == null || jdbcTemplate == null) {
            return;
        }

        // Check if NYT collections exist
        Integer count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM book_collections WHERE collection_type = 'BESTSELLER_LIST' AND source = 'NYT'",
            Integer.class
        );

        if (count == null || count == 0) {
            return; // No NYT collections in test DB
        }

        // Fetch hardcover fiction bestsellers
        List<BookCard> bestsellers = bookQueryRepository.fetchBookCardsByProviderListCode("hardcover-fiction", 8);

        // Should return up to 8 books (or fewer if list is smaller)
        assertThat(bestsellers).isNotNull();
        assertThat(bestsellers.size()).isLessThanOrEqualTo(8);

        if (!bestsellers.isEmpty()) {
            // Verify first book has required fields
            BookCard first = bestsellers.get(0);
            assertThat(first.id()).isNotBlank();
            assertThat(first.title()).isNotBlank();
        }
    }

    @Test
    void testFetchBookListItems_includesDescriptionAndCategories() {
        if (bookQueryRepository == null || jdbcTemplate == null) {
            return;
        }

        List<UUID> bookIds = jdbcTemplate.query(
            "SELECT id FROM books WHERE description IS NOT NULL LIMIT 2",
            (rs, rowNum) -> (UUID) rs.getObject("id")
        );

        if (bookIds.isEmpty()) {
            return;
        }

        List<BookListItem> items = bookQueryRepository.fetchBookListItems(bookIds);

        assertThat(items).isNotEmpty();

        items.forEach(item -> {
            assertThat(item.id()).isNotBlank();
            assertThat(item.title()).isNotBlank();
            // description might be null if not set
            assertThat(item.authors()).isNotNull();
            assertThat(item.categories()).isNotNull();
        });
    }

    @Test
    void testFetchBookDetail_returnsCompleteBookData() {
        if (bookQueryRepository == null || jdbcTemplate == null) {
            return;
        }

        List<UUID> bookIds = jdbcTemplate.query(
            "SELECT id FROM books LIMIT 1",
            (rs, rowNum) -> (UUID) rs.getObject("id")
        );

        if (bookIds.isEmpty()) {
            return;
        }

        Optional<BookDetail> detailOpt = bookQueryRepository.fetchBookDetail(bookIds.get(0));

        assertThat(detailOpt).isPresent();

        BookDetail detail = detailOpt.get();
        assertThat(detail.id()).isNotBlank();
        assertThat(detail.title()).isNotBlank();
        assertThat(detail.authors()).isNotNull();
        assertThat(detail.categories()).isNotNull();
        assertThat(detail.tags()).isNotNull();
        assertThat(detail.editions()).isNotNull(); // Initially empty, loaded separately
    }

    @Test
    void testFetchBookDetailBySlug_findsBookBySlug() {
        if (bookQueryRepository == null || jdbcTemplate == null) {
            return;
        }

        // Get a book with a slug
        List<String> slugs = jdbcTemplate.query(
            "SELECT slug FROM books WHERE slug IS NOT NULL LIMIT 1",
            (rs, rowNum) -> rs.getString("slug")
        );

        if (slugs.isEmpty()) {
            return;
        }

        String slug = slugs.get(0);
        Optional<BookDetail> detailOpt = bookQueryRepository.fetchBookDetailBySlug(slug);

        assertThat(detailOpt).isPresent();
        assertThat(detailOpt.get().slug()).isEqualTo(slug);
    }

    @Test
    void testFetchBookDetailBySlug_withNonexistentSlug_returnsEmpty() {
        if (bookQueryRepository == null) {
            return;
        }

        Optional<BookDetail> detailOpt = bookQueryRepository.fetchBookDetailBySlug("nonexistent-book-slug-12345");

        assertThat(detailOpt).isEmpty();
    }

    @Test
    void testFetchBookEditions_returnsRelatedEditions() {
        if (bookQueryRepository == null || jdbcTemplate == null) {
            return;
        }

        // Find a book that's part of a work cluster with multiple editions
        List<UUID> bookIds = jdbcTemplate.query(
            """
            SELECT DISTINCT b.id
            FROM books b
            JOIN work_cluster_members wcm ON wcm.book_id = b.id
            WHERE wcm.cluster_id IN (
                SELECT cluster_id
                FROM work_cluster_members
                GROUP BY cluster_id
                HAVING COUNT(*) > 1
            )
            LIMIT 1
            """,
            (rs, rowNum) -> (UUID) rs.getObject("id")
        );

        if (bookIds.isEmpty()) {
            return; // No books with editions in test DB
        }

        List<EditionSummary> editions = bookQueryRepository.fetchBookEditions(bookIds.get(0));

        assertThat(editions).isNotNull();
        // May be empty if no other editions, that's OK
        
        if (!editions.isEmpty()) {
            EditionSummary edition = editions.get(0);
            assertThat(edition.id()).isNotBlank();
            assertThat(edition.title()).isNotBlank();
        }
    }

    @Test
    void testPerformance_fetchBookCards_isFast() {
        if (bookQueryRepository == null || jdbcTemplate == null) {
            return;
        }

        List<UUID> bookIds = jdbcTemplate.query(
            "SELECT id FROM books LIMIT 10",
            (rs, rowNum) -> (UUID) rs.getObject("id")
        );

        if (bookIds.size() < 5) {
            return; // Not enough books for performance test
        }

        // Measure time for optimized query
        long start = System.currentTimeMillis();
        List<BookCard> cards = bookQueryRepository.fetchBookCards(bookIds);
        long duration = System.currentTimeMillis() - start;

        assertThat(cards).hasSize(bookIds.size());
        
        // Should complete in under 1 second (very generous, typically <100ms)
        assertThat(duration).isLessThan(1000);
    }

    @Test
    void testFetchBookCards_maintainsDataIntegrity() {
        if (bookQueryRepository == null || jdbcTemplate == null) {
            return;
        }

        // Get a specific book and verify data matches
        List<UUID> bookIds = jdbcTemplate.query(
            "SELECT id FROM books WHERE title IS NOT NULL LIMIT 1",
            (rs, rowNum) -> (UUID) rs.getObject("id")
        );

        if (bookIds.isEmpty()) {
            return;
        }

        UUID bookId = bookIds.get(0);

        // Fetch via new optimized method
        List<BookCard> cards = bookQueryRepository.fetchBookCards(List.of(bookId));
        assertThat(cards).hasSize(1);

        BookCard card = cards.get(0);

        // Verify data matches database
        String titleFromDb = jdbcTemplate.queryForObject(
            "SELECT title FROM books WHERE id = ?",
            String.class,
            bookId
        );

        assertThat(card.title()).isEqualTo(titleFromDb);
        assertThat(card.id()).isEqualTo(bookId.toString());
    }
}