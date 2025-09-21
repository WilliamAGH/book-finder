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

    public static boolean isNullOrBlank(String value) {
        return value == null || value.isBlank();
    }

    public static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    public static boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }

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
     * Book-specific validation utilities.
     */
    public static class BookValidator {

        /**
         * Get a non-null identifier for a book (ISBN13, ISBN10, or ID).
         */
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
         */
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
         */
        public static String getPreferredIsbn(Book book) {
            if (book == null) {
                return null;
            }
            return hasText(book.getIsbn13()) ? book.getIsbn13() : book.getIsbn10();
        }

        /**
         * Determine if a book has at least one usable identifier (ISBN-13, ISBN-10, or ID).
         */
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
         */
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
         */
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
