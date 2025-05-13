package com.williamcallahan.book_recommendation_engine.repository;

import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for CachedBook entities.
 * This is now a plain interface that can be implemented by either JPA or non-JPA implementations.
 */
@Repository
public interface CachedBookRepository {

    /**
     * Find a book by its Google Books ID
     */
    Optional<CachedBook> findByGoogleBooksId(String googleBooksId);
    
    /**
     * Find a book by its ISBN-10
     */
    Optional<CachedBook> findByIsbn10(String isbn10);
    
    /**
     * Find a book by its ISBN-13
     */
    Optional<CachedBook> findByIsbn13(String isbn13);
    
    /**
     * Find similar books by ID
     */
    List<CachedBook> findSimilarBooksById(String bookId, int limit);
    
    /**
     * Save a book to the cache
     */
    <S extends CachedBook> S save(S entity);
    
    /**
     * Save multiple books to the cache
     */
    <S extends CachedBook> Iterable<S> saveAll(Iterable<S> entities);
    
    /**
     * Find a book by its ID
     */
    Optional<CachedBook> findById(String id);
    
    /**
     * Check if a book exists by its ID
     */
    boolean existsById(String id);
    
    /**
     * Find all books in the cache
     */
    Iterable<CachedBook> findAll();
    
    /**
     * Find all books by ID
     */
    Iterable<CachedBook> findAllById(Iterable<String> ids);
    
    /**
     * Count all books in the cache
     */
    long count();
    
    /**
     * Delete a book by its ID
     */
    void deleteById(String id);
    
    /**
     * Delete a book
     */
    void delete(CachedBook entity);
    
    /**
     * Delete books by ID
     */
    void deleteAllById(Iterable<? extends String> ids);
    
    /**
     * Delete multiple books
     */
    void deleteAll(Iterable<? extends CachedBook> entities);
    
    /**
     * Delete all books
     */
    void deleteAll();
}
