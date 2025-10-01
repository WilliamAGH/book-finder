package com.williamcallahan.book_recommendation_engine.model;

/**
 * Enumeration of external identifier types used across book data providers.
 * 
 * This type system prevents misuse of identifiers (e.g., sending slugs to
 * Google Books volumes API) by explicitly classifying each identifier string.
 * 
 * @author William Callahan
 */
public enum ExternalIdentifierType {
    /**
     * Internal canonical UUID (e.g., "550e8400-e29b-41d4-a716-446655440000")
     */
    CANONICAL_ID,
    
    /**
     * URL-safe slug (e.g., "think-java-allen-b-downey")
     */
    SLUG,
    
    /**
     * Google Books volume ID (e.g., "QVz8AQAAQBAJ", "yOz8BAAAQBAJ")
     * Typically 12 alphanumeric characters with _ or - 
     */
    GOOGLE_BOOKS_ID,
    
    /**
     * ISBN-13 (13 digits, may include hyphens)
     */
    ISBN_13,
    
    /**
     * ISBN-10 (10 digits, may include hyphens)
     */
    ISBN_10,
    
    /**
     * Amazon ASIN (10 alphanumeric characters)
     */
    ASIN,
    
    /**
     * OpenLibrary work ID (e.g., "OL123456W")
     */
    OPENLIBRARY_WORK,
    
    /**
     * OpenLibrary edition ID (e.g., "OL123456M")
     */
    OPENLIBRARY_EDITION,
    
    /**
     * Identifier type cannot be determined
     */
    UNKNOWN
}
