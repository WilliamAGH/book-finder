package com.williamcallahan.book_recommendation_engine.util;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Utility class for UUID validation and parsing operations.
 * Centralizes UUID-related logic to eliminate duplication across services.
 */
public final class UuidUtils {

    private static final Pattern UUID_PATTERN = Pattern.compile(
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    );

    private UuidUtils() {
        // Utility class
    }

    /**
     * Checks if a string looks like a UUID without attempting to parse it.
     * More lenient than parsing - just checks format.
     *
     * @param value the string to check
     * @return true if the string matches UUID format, false otherwise
     */
    public static boolean looksLikeUuid(String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }
        return UUID_PATTERN.matcher(value).matches();
    }

    /**
     * Attempts to parse a string as a UUID.
     *
     * @param value the string to parse
     * @return the parsed UUID, or null if parsing fails
     */
    public static UUID parseUuidOrNull(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Checks if a string is a valid UUID by attempting to parse it.
     * Stricter than looksLikeUuid - actually validates the UUID.
     *
     * @param value the string to check
     * @return true if the string can be parsed as a valid UUID
     */
    public static boolean isValidUuid(String value) {
        return parseUuidOrNull(value) != null;
    }

    /**
     * Converts a string to UUID if valid, otherwise returns null.
     * Convenience method combining validation and parsing.
     *
     * @param value the string to convert
     * @return UUID if valid, null otherwise
     */
    public static UUID toUuidIfValid(String value) {
        return parseUuidOrNull(value);
    }

    /**
     * Generates a new random UUID.
     *
     * @return a new UUID
     */
    public static UUID generateUuid() {
        return UUID.randomUUID();
    }

    /**
     * Converts a UUID to its string representation, handling null safely.
     *
     * @param uuid the UUID to convert
     * @return string representation or null if input is null
     */
    public static String toStringOrNull(UUID uuid) {
        return uuid != null ? uuid.toString() : null;
    }
}