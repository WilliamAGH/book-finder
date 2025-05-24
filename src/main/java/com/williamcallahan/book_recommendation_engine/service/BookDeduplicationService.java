/**
 * Central service for deduplicating books by ISBN-13, ISBN-10, and Google Books ID
 * Provides a unified entry point for finding existing records in Redis
 * Coordinates with RedisBookSearchService to prevent duplicate book entries
 *
 * @author William Callahan
 *
 * Features:
 * - Unified book deduplication across multiple identifiers
 * - Finds existing books by ISBN-13, ISBN-10, or Google Books ID
 * - Returns complete book information including Redis key
 * - Used by migration processes to prevent duplicate imports
 */

package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.repository.RedisBookSearchService;
import com.williamcallahan.book_recommendation_engine.repository.RedisBookSearchService.BookSearchResult;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Central book deduplication service
 */
@Service
public class BookDeduplicationService {
    private final RedisBookSearchService redisBookSearchService;

    public BookDeduplicationService(RedisBookSearchService redisBookSearchService) {
        this.redisBookSearchService = redisBookSearchService;
    }

    /**
     * Finds an existing book by ISBN-13, ISBN-10, or Google Books ID
     * Delegates to RedisBookSearchService for efficient book deduplication
     * 
     * @param isbn13 the ISBN-13 identifier to search for
     * @param isbn10 the ISBN-10 identifier to search for  
     * @param googleBooksId the Google Books ID to search for
     * @return Optional containing BookSearchResult with Redis key and cached book if found
     */
    public Optional<BookSearchResult> findExistingBook(String isbn13, String isbn10, String googleBooksId) {
        return redisBookSearchService.findExistingBook(isbn13, isbn10, googleBooksId);
    }
} 