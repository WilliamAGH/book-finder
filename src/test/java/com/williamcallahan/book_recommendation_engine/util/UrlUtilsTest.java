package com.williamcallahan.book_recommendation_engine.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for UrlUtils using modern JUnit 5 parameterized tests.
 */
class UrlUtilsTest {
    
    @ParameterizedTest
    @CsvSource({
        "http://example.com,https://example.com",
        "http://books.google.com/image,https://books.google.com/image",
        "https://example.com,https://example.com",
        "https://secure.site.com,https://secure.site.com"
    })
    void shouldNormalizeHttpToHttps(String input, String expected) {
        assertEquals(expected, UrlUtils.normalizeToHttps(input));
    }
    
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"  ", "\t", "\n"})
    void shouldReturnNullForNullOrBlankUrl(String input) {
        assertNull(UrlUtils.normalizeToHttps(input));
    }
    
    @Test
    void shouldPreserveHttpsUrls() {
        String httpsUrl = "https://example.com/path?query=value";
        assertEquals(httpsUrl, UrlUtils.normalizeToHttps(httpsUrl));
    }
    
    @ParameterizedTest
    @CsvSource({
        "http://example.com,https://example.com",
        "https://example.com,https://example.com",
        "ftp://example.com,",  // Invalid scheme
        "invalid-url,",         // Malformed URL
        "'',",                  // Empty
    })
    void shouldValidateAndNormalize(String input, String expected) {
        if (expected == null || expected.isEmpty()) {
            assertNull(UrlUtils.validateAndNormalize(input));
        } else {
            assertEquals(expected, UrlUtils.validateAndNormalize(input));
        }
    }
    
    @ParameterizedTest
    @CsvSource({
        "http://example.com?query=value,http://example.com",
        "https://api.site.com?key=123&id=456,https://api.site.com",
        "http://books.google.com,http://books.google.com",
        "'',"
    })
    void shouldRemoveQueryParams(String input, String expected) {
        if (expected == null || expected.isEmpty()) {
            assertNull(UrlUtils.removeQueryParams(input));
        } else {
            assertEquals(expected, UrlUtils.removeQueryParams(input));
        }
    }
    
    @ParameterizedTest
    @CsvSource({
        "http://example.com?query=value&,http://example.com?query=value",
        "http://example.com?,http://example.com",
        "http://example.com?&,http://example.com",
        "http://example.com,http://example.com"
    })
    void shouldCleanTrailingSeparators(String input, String expected) {
        assertEquals(expected, UrlUtils.cleanTrailingSeparators(input));
    }
    
    @Test
    void shouldHandleComplexUrl() {
        String complex = "http://books.google.com/books?id=123&printsec=frontcover&img=1&zoom=5&edge=curl&source=gbs_api";
        String normalized = UrlUtils.normalizeToHttps(complex);
        
        assertNotNull(normalized);
        assertTrue(normalized.startsWith("https://"));
        assertTrue(normalized.contains("id=123"));
    }
}
