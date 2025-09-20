package com.williamcallahan.book_recommendation_engine.util;

/**
 * Lightweight helpers for common paging maths so controllers and services can
 * share the same clamping semantics without re-implementing them.
 */
public final class PagingUtils {

    private PagingUtils() {
        // Utility class
    }

    /**
     * Clamp {@code value} to the inclusive {@code [min, max]} range.
     */
    public static int clamp(int value, int min, int max) {
        return Math.clamp(value, min, max);
    }

    /**
     * Snap {@code value} up to at least {@code min}.
     */
    public static int atLeast(int value, int min) {
        return value < min ? min : value;
    }

    /**
     * Ensure a positive (>=1) value with an inclusive upper bound.
     */
    public static int positiveClamp(int value, int max) {
        return clamp(value, 1, max);
    }
}
