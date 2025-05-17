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
     * Retrieves books whose titles contain the specified query string, ignoring case.
     *
     * @param query the substring to search for within book titles
     * @return a list of books with titles containing the query string, case-insensitive
     */
    @Query(value = "SELECT c FROM CachedBook c WHERE c.title ILIKE CONCAT('%', :query, '%')", nativeQuery = true)
    List<CachedBook> findByTitleContainingIgnoreCase(@Param("query") String query);
    
    /**
     * Retrieves all cached books where the specified author is present in the authors JSONB array.
     *
     * @param author the name of the author to search for within the authors list
     * @return a list of CachedBook entities containing the specified author
     */
    @Query(value = "SELECT * FROM cached_books c WHERE c.authors @> CAST(CONCAT('[\"', :author, '\"]') AS jsonb)", nativeQuery = true)
    List<CachedBook> findByAuthor(@Param("author") String author);

    /**
     * Retrieves a cached book by its ID, ensuring the specified author is present in the authors list.
     *
     * @param id the unique identifier of the book
     * @param author the author name to match within the book's authors
     * @return an Optional containing the matching CachedBook if found, or empty if no match exists
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

    /**
     * Retrieves all distinct non-null Google Books IDs from cached books.
     *
     * @return a set of unique Google Books IDs present in the repository
     */
    @Override
    @Query("SELECT DISTINCT c.googleBooksId FROM CachedBook c WHERE c.googleBooksId IS NOT NULL")
    Set<String> findAllDistinctGoogleBooksIds();

    /**
     * Finds books with an exact title match ignoring case, excluding a specified book ID.
     *
     * @param title the title to match, case-insensitive
     * @param idToExclude the ID of the book to exclude from the results
     * @return a list of books with the given title, excluding the specified ID
     */
    @Query("SELECT c FROM CachedBook c WHERE lower(c.title) = lower(:title) AND c.id != :idToExclude")
    List<CachedBook> findByTitleIgnoreCaseAndIdNotInternal(@Param("title") String title, @Param("idToExclude") String idToExclude);

    /**
     * Finds books with an exact title match (case-insensitive), excluding a specific book ID.
     *
     * @param title the title to match, case-insensitive
     * @param idToExclude the ID of the book to exclude from results
     * @return a list of books matching the title, excluding the specified ID
     */
    @Override
    default List<CachedBook> findByTitleIgnoreCaseAndIdNot(String title, String idToExclude) {
        return findByTitleIgnoreCaseAndIdNotInternal(title, idToExclude);
    }
}
