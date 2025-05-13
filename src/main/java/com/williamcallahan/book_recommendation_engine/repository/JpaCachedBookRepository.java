package com.williamcallahan.book_recommendation_engine.repository;

import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * JPA implementation of CachedBookRepository that is used when a database is available.
 * This provides the full caching functionality with PostgreSQL.
 */
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
    
    // Additional database-specific queries that may be useful
    
    @Query(value = "SELECT c FROM CachedBook c WHERE LOWER(c.title) LIKE LOWER(CONCAT('%', :query, '%'))")
    List<CachedBook> findByTitleContainingIgnoreCase(@Param("query") String query);
    
    @Query(value = "SELECT c FROM CachedBook c WHERE :author MEMBER OF c.authors")
    List<CachedBook> findByAuthor(@Param("author") String author);
    
    @Query(value = "SELECT c FROM CachedBook c WHERE EXISTS (SELECT 1 FROM UNNEST(c.categories) cat WHERE LOWER(cat) LIKE LOWER(CONCAT('%', :category, '%')))")
    List<CachedBook> findByCategory(@Param("category") String category);
    
    @Query(value = "SELECT c.* FROM cached_books c ORDER BY vector_similarity(c.embedding, :embedding) DESC LIMIT :limit", nativeQuery = true)
    List<CachedBook> findSimilarBooks(@Param("embedding") float[] embedding, @Param("limit") int limit);
}
