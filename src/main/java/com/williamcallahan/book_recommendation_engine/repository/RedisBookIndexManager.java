/**
 * Redis secondary index management for book identifiers
 * Maintains mapping between various book identifiers (ISBN, Google Books ID) and primary CachedBook IDs
 * Provides index lifecycle management with automatic cleanup and TTL support
 *
 * @author William Callahan
 *
 * Features:
 * - Secondary index creation and maintenance for ISBN-10, ISBN-13, and Google Books IDs
 * - Automatic cleanup of obsolete index entries when book identifiers change
 * - TTL support for index entries with configurable expiration times
 * - Safe index operations with error handling and logging
 */

package com.williamcallahan.book_recommendation_engine.repository;

import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisPooled;

import java.util.Optional;
@Service
public class RedisBookIndexManager {

    private static final Logger logger = LoggerFactory.getLogger(RedisBookIndexManager.class);
    private static final String ISBN10_INDEX_PREFIX = "isbn10_idx:";
    private static final String ISBN13_INDEX_PREFIX = "isbn13_idx:";
    private static final String GOOGLE_BOOKS_ID_INDEX_PREFIX = "gbid_idx:";
    private static final long INDEX_TTL_SECONDS = 24 * 60 * 60;

    private final JedisPooled jedisPooled;

    public RedisBookIndexManager(JedisPooled jedisPooled) {
        this.jedisPooled = jedisPooled;
        logger.info("RedisBookIndexManager initialized");
    }

    private String getIsbn10IndexKey(String isbn10) {
        return ISBN10_INDEX_PREFIX + isbn10;
    }

    private String getIsbn13IndexKey(String isbn13) {
        return ISBN13_INDEX_PREFIX + isbn13;
    }

    private String getGoogleBooksIdIndexKey(String googleBooksId) {
        return GOOGLE_BOOKS_ID_INDEX_PREFIX + googleBooksId;
    }

    /**
     * Updates all secondary indexes for a book with automatic cleanup
     * Creates new index entries and removes obsolete ones when identifiers change
     *
     * @param newBook current state of the CachedBook for indexing
     * @param oldBook previous state for cleanup of changed identifiers (optional)
     */
    public void updateAllIndexes(CachedBook newBook, Optional<CachedBook> oldBook) {
        if (newBook == null || newBook.getId() == null) {
            logger.warn("Cannot update indexes for null book or book with null ID.");
            return;
        }
        String bookId = newBook.getId();

        // Clean up old indexes if an old version of the book is provided and identifiers differ
        oldBook.ifPresent(previous -> {
            if (previous.getGoogleBooksId() != null && !previous.getGoogleBooksId().equals(newBook.getGoogleBooksId())) {
                deleteIndexEntry(getGoogleBooksIdIndexKey(previous.getGoogleBooksId()));
            }
            if (previous.getIsbn10() != null && !previous.getIsbn10().equals(newBook.getIsbn10())) {
                deleteIndexEntry(getIsbn10IndexKey(previous.getIsbn10()));
            }
            if (previous.getIsbn13() != null && !previous.getIsbn13().equals(newBook.getIsbn13())) {
                deleteIndexEntry(getIsbn13IndexKey(previous.getIsbn13()));
            }
        });

        // Create/update new indexes
        if (newBook.getGoogleBooksId() != null && !newBook.getGoogleBooksId().isEmpty()) {
            createIndexEntry(getGoogleBooksIdIndexKey(newBook.getGoogleBooksId()), bookId);
        }
        if (newBook.getIsbn10() != null && !newBook.getIsbn10().isEmpty()) {
            createIndexEntry(getIsbn10IndexKey(newBook.getIsbn10()), bookId);
        }
        if (newBook.getIsbn13() != null && !newBook.getIsbn13().isEmpty()) {
            createIndexEntry(getIsbn13IndexKey(newBook.getIsbn13()), bookId);
        }
        logger.debug("Updated indexes for book ID: {}", bookId);
    }

    /**
     * Removes all secondary index entries for a book
     *
     * @param book CachedBook whose index entries should be deleted
     */
    public void deleteAllIndexes(CachedBook book) {
        if (book == null) {
            logger.warn("Cannot delete indexes for a null book.");
            return;
        }
        if (book.getGoogleBooksId() != null && !book.getGoogleBooksId().isEmpty()) {
            deleteIndexEntry(getGoogleBooksIdIndexKey(book.getGoogleBooksId()));
        }
        if (book.getIsbn10() != null && !book.getIsbn10().isEmpty()) {
            deleteIndexEntry(getIsbn10IndexKey(book.getIsbn10()));
        }
        if (book.getIsbn13() != null && !book.getIsbn13().isEmpty()) {
            deleteIndexEntry(getIsbn13IndexKey(book.getIsbn13()));
        }
        logger.debug("Deleted all indexes for book ID: {}", book.getId());
    }

    /**
     * Looks up primary book ID using Google Books identifier
     *
     * @param googleBooksId Google Books API identifier
     * @return Optional containing primary book ID if index entry exists
     */
    public Optional<String> getBookIdByGoogleBooksId(String googleBooksId) {
        if (googleBooksId == null || googleBooksId.isEmpty()) return Optional.empty();
        return getIndexEntry(getGoogleBooksIdIndexKey(googleBooksId));
    }

    /**
     * Looks up primary book ID using ISBN-10 identifier
     *
     * @param isbn10 10-digit ISBN identifier
     * @return Optional containing primary book ID if index entry exists
     */
    public Optional<String> getBookIdByIsbn10(String isbn10) {
        if (isbn10 == null || isbn10.isEmpty()) return Optional.empty();
        return getIndexEntry(getIsbn10IndexKey(isbn10));
    }

    /**
     * Looks up primary book ID using ISBN-13 identifier
     *
     * @param isbn13 13-digit ISBN identifier
     * @return Optional containing primary book ID if index entry exists
     */
    public Optional<String> getBookIdByIsbn13(String isbn13) {
        if (isbn13 == null || isbn13.isEmpty()) return Optional.empty();
        return getIndexEntry(getIsbn13IndexKey(isbn13));
    }

    private void createIndexEntry(String indexKey, String bookId) {
        try {
            jedisPooled.setex(indexKey, INDEX_TTL_SECONDS, bookId);
            logger.debug("Created/Updated index: {} -> {}", indexKey, bookId);
        } catch (Exception e) {
            logger.error("Error creating index entry {} for book ID {}: {}", indexKey, bookId, e.getMessage(), e);
        }
    }

    private void deleteIndexEntry(String indexKey) {
        try {
            jedisPooled.del(indexKey);
            logger.debug("Deleted index entry: {}", indexKey);
        } catch (Exception e) {
            logger.error("Error deleting index entry {}: {}", indexKey, e.getMessage(), e);
        }
    }

    private Optional<String> getIndexEntry(String indexKey) {
        try {
            String bookId = jedisPooled.get(indexKey);
            return Optional.ofNullable(bookId);
        } catch (Exception e) {
            logger.error("Error getting index entry {}: {}", indexKey, e.getMessage(), e);
            return Optional.empty();
        }
    }
}
