package com.williamcallahan.book_recommendation_engine.util;

import lombok.extern.slf4j.Slf4j;

/**
 * Safe number parsing utilities with consistent error handling.
 * 
 * Consolidates scattered parseInt/parseDouble/parseLong patterns throughout
 * the codebase, providing consistent null-safe parsing with default values.
 * 
 * Eliminates duplicate try-catch blocks and provides centralized logging
 * of parse failures.
 * 
 * @author William Callahan
 * @since 0.9.0
 */
@Slf4j
public final class SafeNumberParser {
    
    private SafeNumberParser() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Safely parses a string to an Integer, returning a default value on failure.
     * 
     * @param value String to parse
     * @param defaultValue Value to return if parsing fails
     * @return Parsed integer or default value
     * 
     * @example
     * <pre>{@code
     * int result = SafeNumberParser.parseInt("123", 0);
     * // Returns: 123
     * 
     * int result = SafeNumberParser.parseInt("invalid", 0);
     * // Returns: 0
     * 
     * int result = SafeNumberParser.parseInt(null, -1);
     * // Returns: -1
     * }</pre>
     */
    public static int parseInt(String value, int defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            log.debug("Failed to parse integer '{}', using default {}: {}", 
                value, defaultValue, e.getMessage());
            return defaultValue;
        }
    }
    
    /**
     * Safely parses a string to an Integer, returning null on failure.
     * 
     * @param value String to parse
     * @return Parsed Integer or null
     */
    public static Integer parseIntOrNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            log.debug("Failed to parse integer '{}': {}", value, e.getMessage());
            return null;
        }
    }
    
    /**
     * Safely parses a string to a Long, returning a default value on failure.
     * 
     * @param value String to parse
     * @param defaultValue Value to return if parsing fails
     * @return Parsed long or default value
     */
    public static long parseLong(String value, long defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            log.debug("Failed to parse long '{}', using default {}: {}", 
                value, defaultValue, e.getMessage());
            return defaultValue;
        }
    }
    
    /**
     * Safely parses a string to a Long, returning null on failure.
     * 
     * @param value String to parse
     * @return Parsed Long or null
     */
    public static Long parseLongOrNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            log.debug("Failed to parse long '{}': {}", value, e.getMessage());
            return null;
        }
    }
    
    /**
     * Safely parses a string to a Double, returning a default value on failure.
     * 
     * @param value String to parse
     * @param defaultValue Value to return if parsing fails
     * @return Parsed double or default value
     */
    public static double parseDouble(String value, double defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            log.debug("Failed to parse double '{}', using default {}: {}", 
                value, defaultValue, e.getMessage());
            return defaultValue;
        }
    }
    
    /**
     * Safely parses a string to a Double, returning null on failure.
     * 
     * @param value String to parse
     * @return Parsed Double or null
     */
    public static Double parseDoubleOrNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            log.debug("Failed to parse double '{}': {}", value, e.getMessage());
            return null;
        }
    }
    
    /**
     * Safely parses a string to a Float, returning a default value on failure.
     * 
     * @param value String to parse
     * @param defaultValue Value to return if parsing fails
     * @return Parsed float or default value
     */
    public static float parseFloat(String value, float defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(value.trim());
        } catch (NumberFormatException e) {
            log.debug("Failed to parse float '{}', using default {}: {}", 
                value, defaultValue, e.getMessage());
            return defaultValue;
        }
    }
    
    /**
     * Safely parses a string to a Float, returning null on failure.
     * 
     * @param value String to parse
     * @return Parsed Float or null
     */
    public static Float parseFloatOrNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Float.parseFloat(value.trim());
        } catch (NumberFormatException e) {
            log.debug("Failed to parse float '{}': {}", value, e.getMessage());
            return null;
        }
    }
    
    /**
     * Safely parses a string to a Boolean, with flexible true/false detection.
     * 
     * @param value String to parse (accepts: true, false, 1, 0, yes, no, y, n)
     * @param defaultValue Value to return if parsing fails
     * @return Parsed boolean or default value
     */
    public static boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        String normalized = value.trim().toLowerCase();
        return switch (normalized) {
            case "true", "1", "yes", "y" -> true;
            case "false", "0", "no", "n" -> false;
            default -> {
                log.debug("Failed to parse boolean '{}', using default {}", value, defaultValue);
                yield defaultValue;
            }
        };
    }
}
