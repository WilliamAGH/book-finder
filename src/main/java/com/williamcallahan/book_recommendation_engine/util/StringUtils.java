package com.williamcallahan.book_recommendation_engine.util;

import java.util.Locale;

/**
 * Centralized string manipulation utilities.
 * 
 * Consolidates scattered string operations including:
 * - Null-safe coalescing (firstNonBlank pattern)
 * - Case normalization with trimming
 * - Safe string operations
 * 
 * @author William Callahan
 * @since 0.9.0
 */
public final class StringUtils {
    
    private StringUtils() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Returns the first non-blank string from the provided arguments.
     * 
     * <p>Replacement for scattered firstNonBlank() implementations.
     * Provides null-safe coalescing of string values.
     * 
     * @param values Variable number of strings to check
     * @return First non-blank string, or null if all are blank/null
     * 
     * @example
     * <pre>{@code
     * String result = StringUtils.coalesce(null, "", "  ", "value", "other");
     * // Returns: "value"
     * 
     * String result = StringUtils.coalesce(null, "");
     * // Returns: null
     * }</pre>
     */
    public static String coalesce(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }
    
    /**
     * Trims and converts string to lowercase in a null-safe manner.
     * 
     * <p>Consolidates scattered {@code trim().toLowerCase()} and
     * {@code toLowerCase().trim()} patterns into single canonical operation.
     * 
     * @param value String to normalize
     * @return Trimmed lowercase string, or null if input is null
     * 
     * @example
     * <pre>{@code
     * String result = StringUtils.normalizeLowercase("  HELLO World  ");
     * // Returns: "hello world"
     * 
     * String result = StringUtils.normalizeLowercase(null);
     * // Returns: null
     * }</pre>
     */
    public static String normalizeLowercase(String value) {
        if (value == null) {
            return null;
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }
    
    /**
     * Trims and converts string to uppercase in a null-safe manner.
     * 
     * <p>Consolidates scattered {@code trim().toUpperCase()} and
     * {@code toUpperCase().trim()} patterns into single canonical operation.
     * 
     * @param value String to normalize
     * @return Trimmed uppercase string, or null if input is null
     * 
     * @example
     * <pre>{@code
     * String result = StringUtils.normalizeUppercase("  hello world  ");
     * // Returns: "HELLO WORLD"
     * 
     * String result = StringUtils.normalizeUppercase(null);
     * // Returns: null
     * }</pre>
     */
    public static String normalizeUppercase(String value) {
        if (value == null) {
            return null;
        }
        return value.trim().toUpperCase(Locale.ROOT);
    }
    
    /**
     * Safely trims a string, returning null if the input is null.
     * 
     * @param value String to trim
     * @return Trimmed string, or null if input is null
     */
    public static String safeTrim(String value) {
        return value == null ? null : value.trim();
    }
    
    /**
     * Checks if a string is null, empty, or contains only whitespace.
     * 
     * @param value String to check
     * @return true if blank, false otherwise
     */
    public static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
    
    /**
     * Checks if a string has actual text content (not null, empty, or whitespace).
     * 
     * @param value String to check
     * @return true if has text, false otherwise
     */
    public static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
    
    /**
     * Capitalizes the first letter of a string, lowercasing the rest.
     * <p>
     * Handles leading punctuation (e.g., "'til" → "'Til").
     * Returns null if input is null, empty string if input is empty.
     * 
     * @param word String to capitalize
     * @return String with first letter capitalized, rest lowercase
     * 
     * @example
     * <pre>{@code
     * StringUtils.capitalize("hello")    // "Hello"
     * StringUtils.capitalize("WORLD")    // "World"
     * StringUtils.capitalize("'til")     // "'Til"
     * StringUtils.capitalize("a")        // "A"
     * StringUtils.capitalize(null)       // null
     * StringUtils.capitalize("")         // ""
     * }</pre>
     */
    public static String capitalize(String word) {
        if (word == null || word.isEmpty()) {
            return word;
        }
        
        // Handle words with leading punctuation like quotes or apostrophes
        int firstLetterIndex = 0;
        while (firstLetterIndex < word.length() && !Character.isLetterOrDigit(word.charAt(firstLetterIndex))) {
            firstLetterIndex++;
        }
        
        if (firstLetterIndex >= word.length()) {
            // No letters found, return as-is
            return word;
        }
        
        return word.substring(0, firstLetterIndex) 
             + Character.toUpperCase(word.charAt(firstLetterIndex))
             + word.substring(firstLetterIndex + 1).toLowerCase();
    }
}
