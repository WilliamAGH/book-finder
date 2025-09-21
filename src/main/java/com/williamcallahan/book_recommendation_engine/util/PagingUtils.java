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
     * Clamp a page size request, honouring defaults when {@code requested <= 0}.
     */
    public static int safeLimit(int requested, int defaultValue, int min, int max) {
        int base = requested > 0 ? requested : defaultValue;
        return clamp(base, min, max);
    }

    /**
     * Produce a reusable window descriptor, keeping start, limit, and overall desired total in sync.
     *
     * @param requestedStart user-supplied start index (can be negative)
     * @param requestedSize user-supplied page size (0 or negative means "take the default")
     * @param defaultSize fallback page size when {@code requestedSize <= 0}
     * @param minSize inclusive minimum page size
     * @param maxSize inclusive maximum page size
     * @param totalCap overall cap for total results to request from downstream layers; {@code <= 0} disables the cap
     */
    public static Window window(int requestedStart,
                                 int requestedSize,
                                 int defaultSize,
                                 int minSize,
                                 int maxSize,
                                 int totalCap) {
        int safeStart = Math.max(0, requestedStart);
        int safeLimit = safeLimit(requestedSize, defaultSize, minSize, maxSize);
        int cappedTotal = totalCap > 0 ? Math.min(totalCap, safeStart + safeLimit) : safeStart + safeLimit;
        return new Window(safeStart, safeLimit, cappedTotal);
    }

    /**
     * Immutable descriptor for a paging request.
     */
    public record Window(int startIndex, int limit, int totalRequested) {}

}
