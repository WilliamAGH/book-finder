/**
 * JPA implementation of CachedBookRepository for database persistence
 *
 * @author William Callahan
 *
 * Features:
 * - Provides full book caching with PostgreSQL database
 * - Conditionally activates only when database URL is configured
 * - Implements vector similarity search for book recommendations
 * - Supports full-text search on book titles and categories
 * - Optimizes queries with PostgreSQL-specific vector operations
 * - Includes specialized search methods for titles, authors, and categories
 */
package com.williamcallahan.book_recommendation_engine.repository;

import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.types.PgVector;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.Set;
@Component
@ConditionalOnExpression("'${spring.datasource.url:}'.length() > 0")
public interface JpaCachedBookRepository extends JpaRepository<CachedBook, String>, CachedBookRepository {

    @Override
    default Optional<CachedBook> findByGoogleBooksId(String googleBooksId) {
        return findByGoogleBooksIdInternal(googleBooksId);
    }
    
    @Query("SELECT c FROM CachedBook c WHERE c.googleBooksId = :googleBooksId")
    Optional<CachedBook> findByGoogleBooksIdInternal(@Param("googleBooksId") String googleBooksId);
    
    @Override
    default Optional<CachedBook> findByIsbn10(String isbn10) {
        return findByIsbn10Internal(isbn10);
    }
    
    @Query("SELECT c FROM CachedBook c WHERE c.isbn10 = :isbn10")
    Optional<CachedBook> findByIsbn10Internal(@Param("isbn10") String isbn10);
    
    @Override
    default Optional<CachedBook> findByIsbn13(String isbn13) {
        return findByIsbn13Internal(isbn13);
    }
    
    @Query("SELECT c FROM CachedBook c WHERE c.isbn13 = :isbn13")
    Optional<CachedBook> findByIsbn13Internal(@Param("isbn13") String isbn13);
    
    @Override
    default List<CachedBook> findSimilarBooksById(String bookId, int limit) {
        return findSimilarBooksByIdInternal(bookId, limit);
    }
    
    @Query(value = "SELECT c.* FROM cached_books c " +
                   "WHERE c.id != :bookId " +
                   "ORDER BY vector_similarity(c.embedding, (SELECT embedding FROM cached_books WHERE id = :bookId)) DESC " +
                   "LIMIT :limit", nativeQuery = true)
    List<CachedBook> findSimilarBooksByIdInternal(@Param("bookId") String bookId, @Param("limit") int limit);
    
    
    /**
     * Find books with titles containing the specified query string
     * - Case-insensitive partial match on book titles
     * - Uses JPA query with LIKE operator
     *
     * @param query Search string to match against book titles
     * @return List of books with matching titles
     */
    @Query(value = "SELECT c FROM CachedBook c WHERE c.title ILIKE CONCAT('%', :query, '%')", nativeQuery = true)
    List<CachedBook> findByTitleContainingIgnoreCase(@Param("query") String query);
    
    /**
     * Finds cached books by a specific author.
     * The 'authors' field is stored as a JSONB array in the database.
     * This query uses a native PostgreSQL JSONB operator (@>) to check if the provided author
     * string is an element within the 'authors' JSON array.
     *
     * @param author The author's name to search for.
     * @return A list of CachedBook entities where the author is present in the authors list
     */
    @Query(value = "SELECT * FROM cached_books c WHERE c.authors @> CAST(CONCAT('[\"', :author, '\"]') AS jsonb)", nativeQuery = true)
    List<CachedBook> findByAuthor(@Param("author") String author);

    /**
     * Find a cached book by ID and author
     * - Combines ID lookup with author verification
     * - Uses PostgreSQL JSONB operations for author matching
     * - Returns single result or empty optional
     *
     * @param id The book ID to find
     * @param author The author name that must be present
     * @return Optional containing the book if both conditions are met
     */
    @Query(value = "SELECT * FROM cached_books c WHERE c.id = :id AND c.authors @> CAST(CONCAT('[\"', :author, '\"]') AS jsonb)", nativeQuery = true)
    Optional<CachedBook> findByIdAndAuthor(@Param("id") Long id, @Param("author") String author);
    
    /**
     * Find books by category with partial matching
     * - Case-insensitive partial match on book categories
     * - Searches within JSON array using PostgreSQL UNNEST
     *
     * @param category Category name to search for
     * @return List of books in the specified category
     */
    @Query(value = "SELECT c.* FROM cached_books c "
                + "WHERE EXISTS (SELECT 1 FROM UNNEST(c.categories) AS cat "
                + "WHERE LOWER(cat) LIKE LOWER(CONCAT('%', :category, '%')))",
           nativeQuery = true)
    List<CachedBook> findByCategory(@Param("category") String category);
    
    /**
     * Find similar books using vector similarity
     * - Uses PostgreSQL vector_similarity function on embeddings
     * - Performs semantic similarity search on book content
     * - Excludes input book from results
     *
     * @param embedding Vector embedding representing book content
     * @param limit Maximum number of similar books to return
     * @return List of books sorted by similarity to the input embedding
     */
    @Query(value = "SELECT c.* FROM cached_books c ORDER BY vector_similarity(c.embedding, :embedding) DESC LIMIT :limit", nativeQuery = true)
    List<CachedBook> findSimilarBooks(@Param("embedding") PgVector embedding, @Param("limit") int limit);

    @Override
    @Query("SELECT DISTINCT c.googleBooksId FROM CachedBook c WHERE c.googleBooksId IS NOT NULL")
    Set<String> findAllDistinctGoogleBooksIds();

    /**
     * Internal implementation to find books by title with case insensitivity, excluding a specific ID
     * - Used for duplicate detection in book collections
     * - Performs exact title match ignoring case differences
     * - Excludes a specific book ID from results
     *
     * @param title The book title to search for (case insensitive)
     * @param idToExclude The book ID to exclude from results
     * @return List of books matching the title but not the excluded ID
     */
    @Query("SELECT c FROM CachedBook c WHERE lower(c.title) = lower(:title) AND c.id != :idToExclude")
    List<CachedBook> findByTitleIgnoreCaseAndIdNotInternal(@Param("title") String title, @Param("idToExclude") String idToExclude);

    /**
     * Implements interface method from CachedBookRepository
     * Delegates to internal implementation method
     */
    @Override
    default List<CachedBook> findByTitleIgnoreCaseAndIdNot(String title, String idToExclude) {
        return findByTitleIgnoreCaseAndIdNotInternal(title, idToExclude);
    }
}
