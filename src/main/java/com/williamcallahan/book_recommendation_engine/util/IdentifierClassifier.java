package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.model.ExternalIdentifierType;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Utility to classify identifier strings by their type.
 * 
 * Prevents misuse of identifiers by explicitly determining what each string represents
 * before calling external APIs. For example, this prevents sending slugs to the
 * Google Books volumes/{id} endpoint.
 * 
 * @author William Callahan
 */
public class IdentifierClassifier {
    
    // Google Books volume IDs are typically 12 characters, alphanumeric with _ or -
    // Examples: "QVz8AQAAQBAJ", "yOz8BAAAQBAJ", "dGvMCgAAQBAJ"
    private static final Pattern GOOGLE_BOOKS_ID_PATTERN = Pattern.compile("^[A-Za-z0-9_-]{10,14}$");
    
    // Slugs contain hyphens and are typically longer with multiple words
    // Examples: "think-java-allen-b-downey", "effective-java-joshua-bloch"
    private static final Pattern SLUG_PATTERN = Pattern.compile("^[a-z0-9]+(-[a-z0-9]+){2,}$");
    
    // OpenLibrary work IDs: OL followed by digits and W
    private static final Pattern OL_WORK_PATTERN = Pattern.compile("^OL\\d+W$", Pattern.CASE_INSENSITIVE);
    
    // OpenLibrary edition IDs: OL followed by digits and M
    private static final Pattern OL_EDITION_PATTERN = Pattern.compile("^OL\\d+M$", Pattern.CASE_INSENSITIVE);
    
    // ASIN: 10 alphanumeric characters
    private static final Pattern ASIN_PATTERN = Pattern.compile("^[A-Z0-9]{10}$");
    
    /**
     * Classifies an identifier string by its type.
     * 
     * Priority order:
     * 1. Canonical UUID
     * 2. ISBN-13 / ISBN-10
     * 3. OpenLibrary IDs
     * 4. ASIN
     * 5. Slug (multi-hyphen pattern)
     * 6. Google Books ID (shorter alphanumeric)
     * 7. UNKNOWN
     * 
     * @param identifier The identifier string to classify
     * @return The classified identifier type
     */
    public static ExternalIdentifierType classify(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            return ExternalIdentifierType.UNKNOWN;
        }
        
        String trimmed = identifier.trim();
        
        // Check for canonical UUID
        if (isUuid(trimmed)) {
            return ExternalIdentifierType.CANONICAL_ID;
        }
        
        // Check for ISBN-13
        if (IsbnUtils.isValidIsbn13(trimmed)) {
            return ExternalIdentifierType.ISBN_13;
        }
        
        // Check for ISBN-10
        if (IsbnUtils.isValidIsbn10(trimmed)) {
            return ExternalIdentifierType.ISBN_10;
        }
        
        // Check for OpenLibrary IDs
        if (OL_WORK_PATTERN.matcher(trimmed).matches()) {
            return ExternalIdentifierType.OPENLIBRARY_WORK;
        }
        if (OL_EDITION_PATTERN.matcher(trimmed).matches()) {
            return ExternalIdentifierType.OPENLIBRARY_EDITION;
        }
        
        // Check for ASIN (uppercase alphanumeric, 10 chars)
        if (ASIN_PATTERN.matcher(trimmed).matches()) {
            return ExternalIdentifierType.ASIN;
        }
        
        // Check for slug pattern (multi-hyphen lowercase)
        if (SLUG_PATTERN.matcher(trimmed).matches()) {
            return ExternalIdentifierType.SLUG;
        }
        
        // Check for Google Books volume ID pattern
        // Must NOT match slug pattern (slugs have 3+ hyphens, GB IDs rarely have hyphens)
        if (GOOGLE_BOOKS_ID_PATTERN.matcher(trimmed).matches()) {
            // Additional heuristic: if it has 3+ hyphens, it's probably a slug
            long hyphenCount = trimmed.chars().filter(ch -> ch == '-').count();
            if (hyphenCount < 3) {
                return ExternalIdentifierType.GOOGLE_BOOKS_ID;
            }
        }
        
        return ExternalIdentifierType.UNKNOWN;
    }
    
    /**
     * Checks if a string is a valid UUID.
     */
    private static boolean isUuid(String str) {
        try {
            UUID.fromString(str);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
    
    /**
     * Checks if an identifier is safe to use with Google Books volumes/{id} endpoint.
     * 
     * @param identifier The identifier to check
     * @return true if it's a valid Google Books volume ID or ISBN
     */
    public static boolean isSafeForGoogleBooksVolumesApi(String identifier) {
        ExternalIdentifierType type = classify(identifier);
        return type == ExternalIdentifierType.GOOGLE_BOOKS_ID ||
               type == ExternalIdentifierType.ISBN_13 ||
               type == ExternalIdentifierType.ISBN_10;
    }
    
    /**
     * Checks if an identifier should use Google Books search API instead of volumes API.
     * 
     * @param identifier The identifier to check
     * @return true if it should use search API (query-based)
     */
    public static boolean shouldUseGoogleBooksSearchApi(String identifier) {
        ExternalIdentifierType type = classify(identifier);
        return type == ExternalIdentifierType.SLUG ||
               type == ExternalIdentifierType.UNKNOWN;
    }
}
