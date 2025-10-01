package com.williamcallahan.book_recommendation_engine.util.cover;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;

/**
 * Single Source of Truth for resolving book identifiers for cover caching operations.
 * 
 * Consolidates logic from:
 * - ImageCacheUtils.getIdentifierKey()
 * - ValidationUtils.BookValidator.getPreferredIsbn()
 * 
 * Priority order:
 * 1. ISBN-13 (most specific, internationally standardized)
 * 2. ISBN-10 (older but widely used)
 * 3. Google Books Volume ID (platform-specific but unique)
 * 
 * @author William Callahan
 */
public final class CoverIdentifierResolver {
    
    private CoverIdentifierResolver() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Resolves the canonical identifier for a book, prioritizing ISBNs.
     * 
     * @param book The book to extract identifier from
     * @return Most specific identifier, or null if book has no valid identifiers
     */
    public static String resolve(Book book) {
        if (book == null) {
            return null;
        }
        
        // Priority 1: ISBN-13
        if (ValidationUtils.hasText(book.getIsbn13())) {
            return book.getIsbn13().trim();
        }
        
        // Priority 2: ISBN-10
        if (ValidationUtils.hasText(book.getIsbn10())) {
            return book.getIsbn10().trim();
        }
        
        // Priority 3: Google Books Volume ID
        if (ValidationUtils.hasText(book.getId())) {
            return book.getId().trim();
        }
        
        return null;
    }
    
    /**
     * Extracts the preferred ISBN for a book (13 over 10).
     * 
     * @param book The book to extract ISBN from
     * @return ISBN-13 if available, otherwise ISBN-10, or null
     */
    public static String getPreferredIsbn(Book book) {
        if (book == null) {
            return null;
        }
        
        if (ValidationUtils.hasText(book.getIsbn13())) {
            return book.getIsbn13().trim();
        }
        
        if (ValidationUtils.hasText(book.getIsbn10())) {
            return book.getIsbn10().trim();
        }
        
        return null;
    }
    
    /**
     * Checks if a book has any valid identifier.
     * 
     * @param book The book to check
     * @return true if book has ISBN-13, ISBN-10, or Google Volume ID
     */
    public static boolean hasIdentifier(Book book) {
        return resolve(book) != null;
    }
}
