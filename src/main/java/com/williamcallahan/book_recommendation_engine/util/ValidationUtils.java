package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.model.Book;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Utility helpers for common null/blank/empty validation checks (per user DRY request).
 */
public final class ValidationUtils {
    private ValidationUtils() {
    }

    /**
     * @deprecated Use {@code !ValidationUtils.hasText(value)} or {@code !StringUtils.hasText(value)} instead.
     * This method is the inverse of hasText() and creates confusion about which to use.
     * Using negated hasText() is clearer and reduces the API surface.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * if (ValidationUtils.isNullOrBlank(value)) { ... }
     * 
     * // New:
     * if (!ValidationUtils.hasText(value)) { ... }
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static boolean isNullOrBlank(String value) {
        return value == null || value.isBlank();
    }

    public static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * @deprecated Use direct null/isEmpty check: {@code value == null || value.isEmpty()}.
     * This wrapper adds no value over Java's built-in isEmpty() and increases API complexity.
     * Prefer hasText() for most validation needs as it handles both null and blank strings.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * if (ValidationUtils.isNullOrEmpty(value)) { ... }
     * 
     * // New:
     * if (value == null || value.isEmpty()) { ... }
     * // Or better, use hasText for blank checking:
     * if (!ValidationUtils.hasText(value)) { ... }
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }

    /**
     * @deprecated Too specific for a utility method. Use inline ternary instead.
     * This single-line wrapper doesn't justify the additional API method.
     * Will be removed in version 1.0.0.
     * 
     * <p><b>Migration Example:</b></p>
     * <pre>{@code
     * // Old:
     * String result = ValidationUtils.nullIfBlank(value);
     * 
     * // New:
     * String result = ValidationUtils.hasText(value) ? value : null;
     * // Or:
     * String result = StringUtils.hasText(value) ? value : null;
     * }</pre>
     */
    @Deprecated(since = "0.9.0", forRemoval = true)
    public static String nullIfBlank(String value) {
        return hasText(value) ? value : null;
    }

    public static boolean isNullOrEmpty(Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }

    public static boolean isNullOrEmpty(Map<?, ?> map) {
        return map == null || map.isEmpty();
    }

    public static boolean anyNull(Object... values) {
        if (values == null) {
            return true;
        }
        for (Object value : values) {
            if (value == null) {
                return true;
            }
        }
        return false;
    }

    public static boolean allNotNull(Object... values) {
        if (values == null) {
            return false;
        }
        for (Object value : values) {
            if (value == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Remove a single pair of wrapping quotes from the provided value.
     * This preserves embedded apostrophes and internal quotes while
     * guarding against upstream sources that defensively wrap strings
     * in quotes for transport.
     *
     * @param value raw string from an external source or database
     * @return value without outer quotes, or the original value when no wrapping quotes are present
     */
    public static String stripWrappingQuotes(String value) {
        if (value == null) {
            return null;
        }
        int length = value.length();
        if (length < 2) {
            return value;
        }
        int start = 0;
        int end = length - 1;
        if (isQuoteCharacter(value.charAt(start)) && isQuoteCharacter(value.charAt(end))) {
            return value.substring(start + 1, end);
        }
        return value;
    }

    private static boolean isQuoteCharacter(char c) {
        return c == '"' || c == '\u201C' || c == '\u201D';
    }

    /**
     * Book-specific validation utilities.
     */
    public static class BookValidator {

        /**
         * Get a non-null identifier for a book (ISBN13, ISBN10, or ID).
         * 
         * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.CoverIdentifierResolver#resolve(Book)} instead.
         * This method duplicates identifier resolution logic. CoverIdentifierResolver is the canonical
         * implementation that provides consistent identifier prioritization across the application.
         * Will be removed in version 1.0.0.
         * 
         * <p><b>Migration Example:</b></p>
         * <pre>{@code
         * // Old:
         * String identifier = ValidationUtils.BookValidator.getIdentifier(book);
         * if (!identifier.equals("unknown")) {
         *     processIdentifier(identifier);
         * }
         * 
         * // New:
         * String identifier = CoverIdentifierResolver.resolve(book);
         * if (ValidationUtils.hasText(identifier)) {
         *     processIdentifier(identifier);
         * }
         * // Note: New method returns null instead of "unknown" for invalid books
         * }</pre>
         */
        @Deprecated(since = "0.9.0", forRemoval = true)
        public static String getIdentifier(Book book) {
            if (book == null) {
                return "unknown";
            }
            return Optional.ofNullable(book.getIsbn13())
                .or(() -> Optional.ofNullable(book.getIsbn10()))
                .or(() -> Optional.ofNullable(book.getId()))
                .orElse("unknown");
        }

        /**
         * Check if a book has a valid ISBN (either 13 or 10).
         * 
         * @deprecated Use {@link IsbnUtils#isValidIsbn13(String)} or {@link IsbnUtils#isValidIsbn10(String)} directly.
         * This wrapper adds no value and obscures the actual validation logic. Use the specific ISBN validation
         * methods from IsbnUtils for clarity and maintainability.
         * Will be removed in version 1.0.0.
         * 
         * <p><b>Migration Example:</b></p>
         * <pre>{@code
         * // Old:
         * if (ValidationUtils.BookValidator.hasValidIsbn(book)) {
         *     processBook(book);
         * }
         * 
         * // New:
         * if (IsbnUtils.isValidIsbn13(book.getIsbn13()) || 
         *     IsbnUtils.isValidIsbn10(book.getIsbn10())) {
         *     processBook(book);
         * }
         * }</pre>
         */
        @Deprecated(since = "0.9.0", forRemoval = true)
        public static boolean hasValidIsbn(Book book) {
            return book != null &&
                   (IsbnUtils.isValidIsbn13(book.getIsbn13()) ||
                    IsbnUtils.isValidIsbn10(book.getIsbn10()));
        }

        /**
         * Check if a book has basic required fields.
         */
        public static boolean hasRequiredFields(Book book) {
            return book != null &&
                   hasText(book.getTitle()) &&
                   (hasText(book.getIsbn13()) || hasText(book.getIsbn10()) || hasText(book.getId()));
        }

        /**
         * Get the preferred ISBN (13 over 10).
         * 
         * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.CoverIdentifierResolver#resolve(Book)} instead.
         * This method is part of the old identifier resolution logic. The new resolver provides comprehensive
         * identifier prioritization and is the single source of truth for book identification.
         * Will be removed in version 1.0.0.
         * 
         * <p><b>Migration Example:</b></p>
         * <pre>{@code
         * // Old:
         * String isbn = ValidationUtils.BookValidator.getPreferredIsbn(book);
         * 
         * // New:
         * String identifier = CoverIdentifierResolver.resolve(book);
         * // Note: The new method may return ID if no ISBN exists, matching the full resolution logic
         * }</pre>
         */
        @Deprecated(since = "0.9.0", forRemoval = true)
        public static String getPreferredIsbn(Book book) {
            if (book == null) {
                return null;
            }
            return hasText(book.getIsbn13()) ? book.getIsbn13() : book.getIsbn10();
        }

        /**
         * Determine if a book has at least one usable identifier (ISBN-13, ISBN-10, or ID).
         * 
         * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.util.cover.CoverIdentifierResolver#resolve(Book)} instead.
         * Check if the returned identifier is non-null and non-blank. The new resolver is the canonical
         * way to determine book identifiability.
         * Will be removed in version 1.0.0.
         * 
         * <p><b>Migration Example:</b></p>
         * <pre>{@code
         * // Old:
         * if (ValidationUtils.BookValidator.hasIdentifier(book)) {
         *     // process book
         * }
         * 
         * // New:
         * String identifier = CoverIdentifierResolver.resolve(book);
         * if (ValidationUtils.hasText(identifier)) {
         *     // process book
         * }
         * }</pre>
         */
        @Deprecated(since = "0.9.0", forRemoval = true)
        public static boolean hasIdentifier(Book book) {
            if (book == null) {
                return false;
            }
            if (hasText(getPreferredIsbn(book))) {
                return true;
            }
            return hasText(book.getId());
        }

        /**
         * Check if book has cover images.
         */
        public static boolean hasCoverImages(Book book) {
            return book != null &&
                   book.getCoverImages() != null &&
                   hasText(book.getCoverImages().getPreferredUrl());
        }

        /**
         * Determine if a cover URL points to a real cover instead of a known placeholder.
         * 
         * @deprecated Use {@link UrlPatternMatcher#isPlaceholderUrl(String)} for consistent placeholder detection.
         * This method duplicates placeholder detection logic. UrlPatternMatcher provides comprehensive
         * placeholder pattern matching that is used consistently across the application.
         * Will be removed in version 1.0.0.
         * 
         * <p><b>Migration Example:</b></p>
         * <pre>{@code
         * // Old:
         * if (ValidationUtils.BookValidator.hasActualCover(coverUrl, placeholderPath)) {
         *     processCover(coverUrl);
         * }
         * 
         * // New:
         * if (!UrlPatternMatcher.isPlaceholderUrl(coverUrl)) {
         *     processCover(coverUrl);
         * }
         * // Note: Logic is inverted - isPlaceholderUrl returns true for placeholders
         * }</pre>
         */
        @Deprecated(since = "0.9.0", forRemoval = true)
        public static boolean hasActualCover(String coverUrl, String placeholderPath) {
            if (!ValidationUtils.hasText(coverUrl)) {
                return false;
            }
            if (ValidationUtils.hasText(placeholderPath) && coverUrl.equals(placeholderPath)) {
                return false;
            }
            String lower = coverUrl.toLowerCase(Locale.ROOT);
            return !lower.contains("placeholder-book-cover.svg")
                && !lower.contains("image-not-available.png")
                && !lower.contains("mock-placeholder.svg")
                && !lower.endsWith("/images/transparent.gif");
        }

        /**
         * Convenience method to check if a book currently references a real cover image.
         * 
         * @deprecated Use {@link UrlPatternMatcher#isPlaceholderUrl(String)} for each URL field individually.
         * This method checks multiple fields but uses deprecated hasActualCover(String, String) internally.
         * Check each relevant URL field (s3ImagePath, externalImageUrl, etc.) with UrlPatternMatcher instead.
         * Will be removed in version 1.0.0.
         * 
         * <p><b>Migration Example:</b></p>
         * <pre>{@code
         * // Old:
         * if (ValidationUtils.BookValidator.hasActualCover(book, placeholderPath)) {
         *     processBookCover(book);
         * }
         * 
         * // New:
         * boolean hasActualCover = false;
         * if (book.getS3ImagePath() != null && 
         *     !UrlPatternMatcher.isPlaceholderUrl(book.getS3ImagePath())) {
         *     hasActualCover = true;
         * } else if (book.getCoverImages() != null &&
         *            !UrlPatternMatcher.isPlaceholderUrl(book.getCoverImages().getPreferredUrl())) {
         *     hasActualCover = true;
         * }
         * if (hasActualCover) {
         *     processBookCover(book);
         * }
         * }</pre>
         */
        @Deprecated(since = "0.9.0", forRemoval = true)
        public static boolean hasActualCover(Book book, String placeholderPath) {
            if (book == null) {
                return false;
            }
            if (hasActualCover(book.getS3ImagePath(), placeholderPath)) {
                return true;
            }

            String preferredUrl = null;
            if (book.getCoverImages() != null) {
                preferredUrl = book.getCoverImages().getPreferredUrl();
            }
            if (hasActualCover(preferredUrl, placeholderPath)) {
                return true;
            }

            return hasActualCover(book.getExternalImageUrl(), placeholderPath);
        }

        /**
         * Get cover URL safely.
         */
        public static String getCoverUrl(Book book) {
            if (book == null || book.getCoverImages() == null) {
                return null;
            }
            return book.getCoverImages().getPreferredUrl();
        }
    }
}
