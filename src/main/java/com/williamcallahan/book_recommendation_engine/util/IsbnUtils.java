package com.williamcallahan.book_recommendation_engine.util;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Shared helpers for normalising ISBN input before lookups or persistence.
 */
public final class IsbnUtils {

    private static final Pattern NON_ISBN_CHARACTERS = Pattern.compile("[^0-9Xx]");

    private IsbnUtils() {
    }

    /**
     * Normalises an ISBN by removing non-numeric characters (except the X check digit) and
     * uppercasing the result.
     *
     * @param raw user-provided ISBN input
     * @return cleaned ISBN string, or {@code null} if nothing usable remains
     */
    public static String sanitize(String raw) {
        if (raw == null) {
            return null;
        }
        String cleaned = NON_ISBN_CHARACTERS.matcher(raw).replaceAll("");
        if (cleaned.isBlank()) {
            return null;
        }
        return cleaned.toUpperCase(Locale.ROOT);
    }

    /**
     * Validate if a string is a valid ISBN-13.
     */
    public static boolean isValidIsbn13(String isbn) {
        if (isbn == null) {
            return false;
        }
        String cleaned = sanitize(isbn);
        return cleaned != null && cleaned.length() == 13 && cleaned.matches("\\d{13}");
    }

    /**
     * Validate if a string is a valid ISBN-10.
     */
    public static boolean isValidIsbn10(String isbn) {
        if (isbn == null) {
            return false;
        }
        String cleaned = sanitize(isbn);
        return cleaned != null && cleaned.length() == 10 && cleaned.matches("\\d{9}[\\dX]");
    }
}
