package com.williamcallahan.book_recommendation_engine.repository;

import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for managing CachedBook entities in persistent storage
 * 
 * @author William Callahan
 * 
 * Key capabilities:
 * - Provides abstraction over underlying database implementation
 * - Supports both JPA and non-JPA implementations
 * - Enables book retrieval by Google Books ID and ISBN numbers
 * - Manages similar book relationships
 * - Handles standard CRUD operations for cached book data
 * - Tracks unique Google Books IDs for deduplication
 */
@Repository
public interface CachedBookRepository {

    /**
     * Retrieves a cached book by its Google Books API identifier
     * 
     * @param googleBooksId The unique identifier from Google Books API
     * @return Optional containing the cached book if found, empty otherwise
     */
    Optional<CachedBook> findByGoogleBooksId(String googleBooksId);
    
    /**
     * Retrieves a cached book by its ISBN-10 identifier
     * 
     * @param isbn10 The 10-digit ISBN identifier
     * @return Optional containing the cached book if found, empty otherwise
     */
    Optional<CachedBook> findByIsbn10(String isbn10);
    
    /**
     * Retrieves a cached book by its ISBN-13 identifier
     * 
     * @param isbn13 The 13-digit ISBN identifier
     * @return Optional containing the cached book if found, empty otherwise
     */
    Optional<CachedBook> findByIsbn13(String isbn13);
    
    /**
     * Retrieves a list of books that are similar to the specified book
     * 
     * @param bookId The identifier of the source book to find similar books for
     * @param limit Maximum number of similar books to return
     * @return List of cached books similar to the specified book
     */
    List<CachedBook> findSimilarBooksById(String bookId, int limit);
    
    /**
     * Persists a book entity in the cache
     * 
     * @param entity The book entity to save
     * @return The saved book entity with any generated values populated
     * @param <S> Type extending CachedBook
     */
    <S extends CachedBook> S save(S entity);
    
    /**
     * Persists multiple book entities in the cache
     * 
     * @param entities The collection of book entities to save
     * @return The saved book entities with any generated values populated
     * @param <S> Type extending CachedBook
     */
    <S extends CachedBook> Iterable<S> saveAll(Iterable<S> entities);
    
    /**
     * Retrieves a cached book by its primary identifier
     * 
     * @param id The unique identifier of the book
     * @return Optional containing the cached book if found, empty otherwise
     */
    Optional<CachedBook> findById(String id);
    
    /**
     * Checks if a book with the specified identifier exists in the cache
     * 
     * @param id The unique identifier of the book to check
     * @return true if the book exists, false otherwise
     */
    boolean existsById(String id);
    
    /**
     * Retrieves all books stored in the cache
     * 
     * @return Iterable collection of all cached books
     */
    Iterable<CachedBook> findAll();
    
    /**
     * Retrieves multiple books by their identifiers
     * 
     * @param ids Collection of book identifiers to retrieve
     * @return Iterable collection of cached books matching the specified identifiers
     */
    Iterable<CachedBook> findAllById(Iterable<String> ids);
    
    /**
     * Counts the total number of books in the cache
     * 
     * @return Total count of cached books
     */
    long count();
    
    /**
     * Removes a book from the cache by its identifier
     * 
     * @param id The unique identifier of the book to delete
     */
    void deleteById(String id);
    
    /**
     * Removes a specific book entity from the cache
     * 
     * @param entity The book entity to delete
     */
    void delete(CachedBook entity);
    
    /**
     * Removes multiple books from the cache by their identifiers
     * 
     * @param ids Collection of book identifiers to delete
     */
    void deleteAllById(Iterable<? extends String> ids);
    
    /**
     * Removes multiple book entities from the cache
     * 
     * @param entities Collection of book entities to delete
     */
    void deleteAll(Iterable<? extends CachedBook> entities);
    
    /**
     * Removes all books from the cache
     * 
     * @implNote This operation may be expensive and should be used with caution
     */
    void deleteAll();

    /**
     * Retrieves all unique Google Books identifiers present in the cache
     * 
     * @return Set of unique Google Books IDs for deduplication
     */
    java.util.Set<String> findAllDistinctGoogleBooksIds();
}
