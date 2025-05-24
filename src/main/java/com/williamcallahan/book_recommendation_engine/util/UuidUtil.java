/**
 * Utility class for generating and validating UUID values
 *
 * @author William Callahan
 *
 * Features:
 * - Generates time-ordered epoch-based UUIDs (UUIDv7) for consistent ordering
 * - Validates UUID string format for all standard UUID versions
 * - Uses external library for UUIDv7 generation
 * - Provides utility methods for UUID operations
 */

package com.williamcallahan.book_recommendation_engine.util;

import com.github.f4b6a3.uuid.UuidCreator;
import java.util.UUID;
import java.util.regex.Pattern;

public final class UuidUtil {

    private static final Pattern UUID_PATTERN =
        Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    /**
     * Private constructor to prevent instantiation
     */
    private UuidUtil() {
    }

    /**
     * Generates a time-ordered epoch-based UUID (UUIDv7)
     *
     * @return A new UUIDv7
     */
    public static UUID getTimeOrderedEpoch() {
        return UuidCreator.getTimeOrderedEpoch();
    }

    /**
     * Checks if the given string is a valid UUID
     *
     * @param uuidString The string to check
     * @return true if the string is a valid UUID, false otherwise
     */
    public static boolean isUuid(String uuidString) {
        if (uuidString == null) {
            return false;
        }
        return UUID_PATTERN.matcher(uuidString).matches();
    }
}
