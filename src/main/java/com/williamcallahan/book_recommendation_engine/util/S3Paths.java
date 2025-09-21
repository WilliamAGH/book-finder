package com.williamcallahan.book_recommendation_engine.util;

/**
 * Centralised S3 path helpers so cache prefixes stay consistent across services
 * and migrations.
 */
public final class S3Paths {

    /** Canonical prefix for Google Books cache objects. */
    public static final String GOOGLE_BOOK_CACHE_PREFIX = "books/v1/";

    private S3Paths() {
        // Utility class
    }

    /**
     * Ensure a prefix ends with a single trailing slash so callers can safely
     * concatenate object keys afterwards.
     */
    public static String ensureTrailingSlash(String prefix) {
        return ensureTrailingSlash(prefix, GOOGLE_BOOK_CACHE_PREFIX);
    }

    public static String ensureTrailingSlash(String prefix, String defaultValue) {
        String fallback = (defaultValue == null || defaultValue.isBlank()) ? "/" : defaultValue.trim();
        String base = (prefix == null || prefix.isBlank()) ? fallback : prefix.trim();
        return base.endsWith("/") ? base : base + "/";
    }
}
