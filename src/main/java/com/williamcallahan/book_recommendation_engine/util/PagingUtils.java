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

        long baseWindow = (long) safeStart + safeLimit;
        int multiplier = Math.max(1, ApplicationConstants.Paging.SEARCH_PREFETCH_MULTIPLIER);
        long desiredWindow = (long) safeStart + (long) safeLimit * multiplier;
        long target = Math.max(baseWindow, desiredWindow);

        if (totalCap > 0) {
            target = Math.min(totalCap, target);
        }

        int totalRequested = (int) Math.min(Integer.MAX_VALUE, Math.max(0L, target));
        return new Window(safeStart, safeLimit, totalRequested);
    }

    /**
     * Produce a defensive slice of {@code items} using the provided {@code startIndex} and {@code limit}.
     * Always returns a new list, never throws on bounds issues, and gracefully handles null/empty inputs.
     */
    public static <T> java.util.List<T> slice(java.util.List<T> items, int startIndex, int limit) {
        if (items == null || items.isEmpty() || limit <= 0) {
            return java.util.List.of();
        }
        int safeStart = Math.max(0, startIndex);
        int boundedStart = Math.min(safeStart, items.size());
        int boundedEnd = Math.min(boundedStart + limit, items.size());
        if (boundedStart >= boundedEnd) {
            return java.util.List.of();
        }
        return java.util.List.copyOf(items.subList(boundedStart, boundedEnd));
    }

    /**
     * Immutable descriptor for a paging request.
     */
    public record Window(int startIndex, int limit, int totalRequested) {}

    public static boolean hasMore(int totalFetched, int startIndex, int pageSize) {
        if (pageSize <= 0 || totalFetched <= 0) {
            return false;
        }
        long threshold = (long) Math.max(0, startIndex) + Math.max(0, pageSize);
        return (long) totalFetched > threshold;
    }

    public static int prefetchedCount(int totalFetched, int startIndex, int pageSize) {
        if (pageSize <= 0 || totalFetched <= 0) {
            return 0;
        }
        long threshold = (long) Math.max(0, startIndex) + Math.max(0, pageSize);
        long extra = (long) totalFetched - threshold;
        if (extra <= 0) {
            return 0;
        }
        return (int) Math.min(extra, Integer.MAX_VALUE);
    }

}
