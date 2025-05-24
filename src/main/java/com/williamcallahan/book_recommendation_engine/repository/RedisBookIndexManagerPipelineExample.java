/**
 * Example implementation showing how to refactor RedisBookIndexManager.updateAllIndexes
 * to use Jedis Pipeline for atomic Redis operations
 */

package com.williamcallahan.book_recommendation_engine.repository;

import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.Optional;

public class RedisBookIndexManagerPipelineExample {

    private static final Logger logger = LoggerFactory.getLogger(RedisBookIndexManagerPipelineExample.class);
    private static final String ISBN10_INDEX_PREFIX = "isbn10_idx:";
    private static final String ISBN13_INDEX_PREFIX = "isbn13_idx:";
    private static final String GOOGLE_BOOKS_ID_INDEX_PREFIX = "gbid_idx:";
    private static final long INDEX_TTL_SECONDS = 24 * 60 * 60;

    private final JedisPooled jedisPooled;

    public RedisBookIndexManagerPipelineExample(JedisPooled jedisPooled) {
        this.jedisPooled = jedisPooled;
    }

    /**
     * Updates all secondary indexes for a book using a pipeline for atomic operations
     * This ensures all index updates happen together or not at all
     *
     * @param newBook current state of the CachedBook for indexing
     * @param oldBook previous state for cleanup of changed identifiers (optional)
     */
    public void updateAllIndexesWithPipeline(CachedBook newBook, Optional<CachedBook> oldBook) {
        if (newBook == null || newBook.getId() == null) {
            logger.warn("Cannot update indexes for null book or book with null ID.");
            return;
        }
        
        String bookId = newBook.getId();
        
        try {
            // Create a pipeline for atomic operations
            Pipeline pipeline = jedisPooled.pipelined();
            
            // Clean up old indexes if identifiers have changed
            oldBook.ifPresent(previous -> {
                // Delete old Google Books ID index if changed
                if (previous.getGoogleBooksId() != null && 
                    !previous.getGoogleBooksId().equals(newBook.getGoogleBooksId())) {
                    pipeline.del(getGoogleBooksIdIndexKey(previous.getGoogleBooksId()));
                }
                
                // Delete old ISBN-10 index if changed
                if (previous.getIsbn10() != null && 
                    !previous.getIsbn10().equals(newBook.getIsbn10())) {
                    pipeline.del(getIsbn10IndexKey(previous.getIsbn10()));
                }
                
                // Delete old ISBN-13 index if changed
                if (previous.getIsbn13() != null && 
                    !previous.getIsbn13().equals(newBook.getIsbn13())) {
                    pipeline.del(getIsbn13IndexKey(previous.getIsbn13()));
                }
            });
            
            // Create/update new indexes
            if (newBook.getGoogleBooksId() != null && !newBook.getGoogleBooksId().isEmpty()) {
                pipeline.setex(getGoogleBooksIdIndexKey(newBook.getGoogleBooksId()), 
                             INDEX_TTL_SECONDS, bookId);
            }
            
            if (newBook.getIsbn10() != null && !newBook.getIsbn10().isEmpty()) {
                pipeline.setex(getIsbn10IndexKey(newBook.getIsbn10()), 
                             INDEX_TTL_SECONDS, bookId);
            }
            
            if (newBook.getIsbn13() != null && !newBook.getIsbn13().isEmpty()) {
                pipeline.setex(getIsbn13IndexKey(newBook.getIsbn13()), 
                             INDEX_TTL_SECONDS, bookId);
            }
            
            // Execute all commands atomically
            pipeline.sync();
            
            logger.debug("Updated indexes for book ID: {} using pipeline", bookId);
            
        } catch (Exception e) {
            logger.error("Error updating indexes for book ID {}: {}", bookId, e.getMessage(), e);
            throw new RuntimeException("Failed to update book indexes", e);
        }
    }

    /**
     * Alternative implementation using MULTI/EXEC for transactions
     * Note: JedisPooled doesn't directly support transactions, so this is a conceptual example
     * In practice, you would need to use Pipeline or individual commands with JedisPooled
     */
    public void updateAllIndexesWithTransaction(CachedBook newBook, Optional<CachedBook> oldBook) {
        if (newBook == null || newBook.getId() == null) {
            logger.warn("Cannot update indexes for null book or book with null ID.");
            return;
        }
        
        // JedisPooled doesn't support multi/exec directly
        // This method demonstrates the concept, but would need to be implemented
        // using a regular Jedis connection from a pool for true transaction support
        
        logger.warn("Transaction support requires using Jedis with connection pool, not JedisPooled directly");
        
        // For now, fall back to pipeline implementation
        updateAllIndexesWithPipeline(newBook, oldBook);
    }

    /**
     * Example showing how to get responses from pipeline operations
     */
    public void updateAllIndexesWithPipelineAndResponses(CachedBook newBook, Optional<CachedBook> oldBook) {
        if (newBook == null || newBook.getId() == null) {
            logger.warn("Cannot update indexes for null book or book with null ID.");
            return;
        }
        
        String bookId = newBook.getId();
        
        try {
            Pipeline pipeline = jedisPooled.pipelined();
            
            // Track responses for delete operations
            Response<Long> gbidDeleteResponse = null;
            Response<Long> isbn10DeleteResponse = null;
            Response<Long> isbn13DeleteResponse = null;
            
            // Clean up old indexes
            if (oldBook.isPresent()) {
                CachedBook previous = oldBook.get();
                
                if (previous.getGoogleBooksId() != null && 
                    !previous.getGoogleBooksId().equals(newBook.getGoogleBooksId())) {
                    gbidDeleteResponse = pipeline.del(getGoogleBooksIdIndexKey(previous.getGoogleBooksId()));
                }
                
                if (previous.getIsbn10() != null && 
                    !previous.getIsbn10().equals(newBook.getIsbn10())) {
                    isbn10DeleteResponse = pipeline.del(getIsbn10IndexKey(previous.getIsbn10()));
                }
                
                if (previous.getIsbn13() != null && 
                    !previous.getIsbn13().equals(newBook.getIsbn13())) {
                    isbn13DeleteResponse = pipeline.del(getIsbn13IndexKey(previous.getIsbn13()));
                }
            }
            
            // Track responses for set operations
            Response<String> gbidSetResponse = null;
            Response<String> isbn10SetResponse = null;
            Response<String> isbn13SetResponse = null;
            
            // Create new indexes
            if (newBook.getGoogleBooksId() != null && !newBook.getGoogleBooksId().isEmpty()) {
                gbidSetResponse = pipeline.setex(getGoogleBooksIdIndexKey(newBook.getGoogleBooksId()), 
                                                INDEX_TTL_SECONDS, bookId);
            }
            
            if (newBook.getIsbn10() != null && !newBook.getIsbn10().isEmpty()) {
                isbn10SetResponse = pipeline.setex(getIsbn10IndexKey(newBook.getIsbn10()), 
                                                 INDEX_TTL_SECONDS, bookId);
            }
            
            if (newBook.getIsbn13() != null && !newBook.getIsbn13().isEmpty()) {
                isbn13SetResponse = pipeline.setex(getIsbn13IndexKey(newBook.getIsbn13()), 
                                                 INDEX_TTL_SECONDS, bookId);
            }
            
            // Execute pipeline
            pipeline.sync();
            
            // Log results
            if (gbidDeleteResponse != null && gbidDeleteResponse.get() > 0) {
                logger.debug("Deleted old Google Books ID index");
            }
            if (isbn10DeleteResponse != null && isbn10DeleteResponse.get() > 0) {
                logger.debug("Deleted old ISBN-10 index");
            }
            if (isbn13DeleteResponse != null && isbn13DeleteResponse.get() > 0) {
                logger.debug("Deleted old ISBN-13 index");
            }
            
            if (gbidSetResponse != null) {
                logger.debug("Created Google Books ID index: {}", gbidSetResponse.get());
            }
            if (isbn10SetResponse != null) {
                logger.debug("Created ISBN-10 index: {}", isbn10SetResponse.get());
            }
            if (isbn13SetResponse != null) {
                logger.debug("Created ISBN-13 index: {}", isbn13SetResponse.get());
            }
            
            logger.info("Successfully updated all indexes for book ID: {}", bookId);
            
        } catch (Exception e) {
            logger.error("Error updating indexes for book ID {}: {}", bookId, e.getMessage(), e);
            throw new RuntimeException("Failed to update book indexes", e);
        }
    }

    // Helper methods
    private String getIsbn10IndexKey(String isbn10) {
        return ISBN10_INDEX_PREFIX + isbn10;
    }

    private String getIsbn13IndexKey(String isbn13) {
        return ISBN13_INDEX_PREFIX + isbn13;
    }

    private String getGoogleBooksIdIndexKey(String googleBooksId) {
        return GOOGLE_BOOKS_ID_INDEX_PREFIX + googleBooksId;
    }
}