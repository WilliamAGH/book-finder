package com.williamcallahan.book_recommendation_engine.util;

import java.util.Locale;
import java.util.function.Consumer;

/**
 * Shared helper for safely parsing user-provided strings into enum values.
 */
public final class EnumParsingUtils {

    private EnumParsingUtils() {
        // Utility class
    }

    public static <E extends Enum<E>> E parseOrDefault(String raw,
                                                       Class<E> enumType,
                                                       E defaultValue) {
        return parseOrDefault(raw, enumType, defaultValue, null);
    }

    public static <E extends Enum<E>> E parseOrDefault(String raw,
                                                       Class<E> enumType,
                                                       E defaultValue,
                                                       Consumer<String> onInvalid) {
        if (raw == null) {
            return defaultValue;
        }

        String candidate = raw.trim();
        if (candidate.isEmpty()) {
            return defaultValue;
        }

        try {
            return Enum.valueOf(enumType, candidate.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            if (onInvalid != null) {
                onInvalid.accept(raw);
            }
            return defaultValue;
        }
    }
}
