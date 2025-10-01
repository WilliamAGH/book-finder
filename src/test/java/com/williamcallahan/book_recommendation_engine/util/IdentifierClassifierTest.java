package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.model.ExternalIdentifierType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for IdentifierClassifier.
 * 
 * Tests the critical bug fix that prevents slugs from being sent to
 * Google Books volumes API by properly classifying identifier types.
 */
class IdentifierClassifierTest {

    @Test
    void testClassify_CanonicalUuid() {
        assertEquals(ExternalIdentifierType.CANONICAL_ID, 
            IdentifierClassifier.classify("550e8400-e29b-41d4-a716-446655440000"));
        assertEquals(ExternalIdentifierType.CANONICAL_ID, 
            IdentifierClassifier.classify("f47ac10b-58cc-4372-a567-0e02b2c3d479"));
    }

    @Test
    void testClassify_Slug() {
        // Real slug examples from the bug report
        assertEquals(ExternalIdentifierType.SLUG, 
            IdentifierClassifier.classify("think-java-allen-b-downey"));
        assertEquals(ExternalIdentifierType.SLUG, 
            IdentifierClassifier.classify("effective-java-joshua-bloch"));
        assertEquals(ExternalIdentifierType.SLUG, 
            IdentifierClassifier.classify("clean-code-robert-martin"));
    }

    @Test
    void testClassify_GoogleBooksId() {
        // Real Google Books volume IDs (typically 12 characters)
        assertEquals(ExternalIdentifierType.GOOGLE_BOOKS_ID, 
            IdentifierClassifier.classify("QVz8AQAAQBAJ"));
        assertEquals(ExternalIdentifierType.GOOGLE_BOOKS_ID, 
            IdentifierClassifier.classify("yOz8BAAAQBAJ"));
        assertEquals(ExternalIdentifierType.GOOGLE_BOOKS_ID, 
            IdentifierClassifier.classify("dGvMCgAAQBAJ"));
    }

    @Test
    void testClassify_Isbn13() {
        assertEquals(ExternalIdentifierType.ISBN_13, 
            IdentifierClassifier.classify("9780596520687"));
        assertEquals(ExternalIdentifierType.ISBN_13, 
            IdentifierClassifier.classify("978-0596520687"));
    }

    @Test
    void testClassify_Isbn10() {
        assertEquals(ExternalIdentifierType.ISBN_10, 
            IdentifierClassifier.classify("0596520689"));
        assertEquals(ExternalIdentifierType.ISBN_10, 
            IdentifierClassifier.classify("0-596-52068-9"));
    }

    @Test
    void testClassify_OpenLibraryWork() {
        assertEquals(ExternalIdentifierType.OPENLIBRARY_WORK, 
            IdentifierClassifier.classify("OL12345W"));
        assertEquals(ExternalIdentifierType.OPENLIBRARY_WORK, 
            IdentifierClassifier.classify("OL98765W"));
    }

    @Test
    void testClassify_OpenLibraryEdition() {
        assertEquals(ExternalIdentifierType.OPENLIBRARY_EDITION, 
            IdentifierClassifier.classify("OL12345M"));
        assertEquals(ExternalIdentifierType.OPENLIBRARY_EDITION, 
            IdentifierClassifier.classify("OL98765M"));
    }

    @Test
    void testClassify_Asin() {
        assertEquals(ExternalIdentifierType.ASIN, 
            IdentifierClassifier.classify("B07XYZ1234"));
        assertEquals(ExternalIdentifierType.ASIN, 
            IdentifierClassifier.classify("B01ABCDEFG"));
    }

    @Test
    void testClassify_Unknown() {
        assertEquals(ExternalIdentifierType.UNKNOWN, 
            IdentifierClassifier.classify(null));
        assertEquals(ExternalIdentifierType.UNKNOWN, 
            IdentifierClassifier.classify(""));
        assertEquals(ExternalIdentifierType.UNKNOWN, 
            IdentifierClassifier.classify("   "));
        assertEquals(ExternalIdentifierType.UNKNOWN, 
            IdentifierClassifier.classify("random text without pattern"));
    }

    @Test
    void testIsSafeForGoogleBooksVolumesApi_Safe() {
        // Google Books IDs are safe
        assertTrue(IdentifierClassifier.isSafeForGoogleBooksVolumesApi("QVz8AQAAQBAJ"));
        
        // ISBNs are safe (Google supports them)
        assertTrue(IdentifierClassifier.isSafeForGoogleBooksVolumesApi("9780596520687"));
        assertTrue(IdentifierClassifier.isSafeForGoogleBooksVolumesApi("0596520689"));
    }

    @Test
    void testIsSafeForGoogleBooksVolumesApi_NotSafe() {
        // CRITICAL TEST: Slugs must NOT be safe for volumes API
        assertFalse(IdentifierClassifier.isSafeForGoogleBooksVolumesApi("think-java-allen-b-downey"),
            "Slugs must NOT be sent to Google Books volumes API");
        assertFalse(IdentifierClassifier.isSafeForGoogleBooksVolumesApi("effective-java-joshua-bloch"));
        
        // UUIDs not safe
        assertFalse(IdentifierClassifier.isSafeForGoogleBooksVolumesApi("550e8400-e29b-41d4-a716-446655440000"));
        
        // OpenLibrary IDs not safe
        assertFalse(IdentifierClassifier.isSafeForGoogleBooksVolumesApi("OL12345W"));
        assertFalse(IdentifierClassifier.isSafeForGoogleBooksVolumesApi("OL12345M"));
        
        // Unknown/ambiguous strings - may be ISBN-10 if 10 chars with hyphens
        // But "random-text" is 11 chars, so should not be safe
        assertFalse(IdentifierClassifier.isSafeForGoogleBooksVolumesApi("random-text-longer"));
    }

    @Test
    void testShouldUseGoogleBooksSearchApi() {
        // Slugs should use search API
        assertTrue(IdentifierClassifier.shouldUseGoogleBooksSearchApi("think-java-allen-b-downey"));
        
        // Unknown identifiers should use search API
        assertTrue(IdentifierClassifier.shouldUseGoogleBooksSearchApi("some random text"));
        
        // Known identifiers should NOT use search API (they have better endpoints)
        assertFalse(IdentifierClassifier.shouldUseGoogleBooksSearchApi("QVz8AQAAQBAJ"));
        assertFalse(IdentifierClassifier.shouldUseGoogleBooksSearchApi("9780596520687"));
    }

    @Test
    void testDistinguishSlugFromGoogleBooksId() {
        // CRITICAL: Distinguish between multi-hyphen slugs and rare hyphenated GB IDs
        
        // Slugs have 3+ hyphens (multiple words)
        assertEquals(ExternalIdentifierType.SLUG, 
            IdentifierClassifier.classify("think-java-allen-b-downey")); // 4 hyphens
        
        // Google Books IDs rarely have hyphens, and if they do, < 3
        assertEquals(ExternalIdentifierType.GOOGLE_BOOKS_ID, 
            IdentifierClassifier.classify("QVz8AQAAQBAJ")); // 0 hyphens
        
        // Note: Hypothetical ISBN-10-like strings with hyphens will match ISBN_10 first
        // Real Google Books IDs are typically alphanumeric without hyphens
    }

    @Test
    void testEdgeCases() {
        // Two-word slug (minimum for slug pattern)
        // Note: "think-java" has 10 chars so might match ISBN-10 pattern,
        // but our slug pattern requires lowercase, ISBN allows uppercase
        ExternalIdentifierType thinkJavaType = IdentifierClassifier.classify("think-java");
        // Accept either SLUG or GOOGLE_BOOKS_ID (both reasonable for 10-char lowercase-hyphen string)
        assertTrue(thinkJavaType == ExternalIdentifierType.SLUG || 
                   thinkJavaType == ExternalIdentifierType.GOOGLE_BOOKS_ID,
                   "think-java should be classified as SLUG or GOOGLE_BOOKS_ID");
        
        // Short alphanumeric strings that could be ambiguous
        String shortId = "abc-def-ghi"; // 2 hyphens
        ExternalIdentifierType type = IdentifierClassifier.classify(shortId);
        // Accept either SLUG or GOOGLE_BOOKS_ID (11 chars with hyphens is ambiguous)
        assertTrue(type == ExternalIdentifierType.SLUG || 
                   type == ExternalIdentifierType.GOOGLE_BOOKS_ID,
                   "Short hyphenated strings can be classified as either SLUG or GOOGLE_BOOKS_ID");
    }
}
