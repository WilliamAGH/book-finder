package com.williamcallahan.book_recommendation_engine.util;

import java.text.Normalizer;
import java.util.Locale;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utility for generating SEO-friendly URL slugs from book titles and authors.
 * Slugs are generated once at creation time and remain stable.
 */
public final class SlugGenerator {
    private static final Pattern NON_LATIN = Pattern.compile("[^\\w\\s-]");
    private static final Pattern WHITESPACE = Pattern.compile("[\\s_]+");
    private static final Pattern EDGE_DASHES = Pattern.compile("^-+|-+$");
    private static final Pattern MULTIPLE_DASHES = Pattern.compile("-{2,}");

    private static final int MAX_SLUG_LENGTH = 100;
    private static final int MAX_TITLE_LENGTH = 60;
    private static final int MAX_AUTHOR_LENGTH = 30;

    private SlugGenerator() {}

    /**
     * Generate a slug from book title and optional authors.
     * Format: title-author1-author2
     * Example: "Harry Potter and the Philosopher's Stone" by "J.K. Rowling"
     *          becomes "harry-potter-and-the-philosophers-stone-j-k-rowling"
     *
     * @param title The book title
     * @param authors List of author names (can be null or empty)
     * @return SEO-friendly slug
     */
    public static String generateBookSlug(String title, List<String> authors) {
        if (title == null || title.trim().isEmpty()) {
            return null;
        }

        StringBuilder slugBuilder = new StringBuilder();

        // Process title
        String titleSlug = slugify(title);
        if (titleSlug.length() > MAX_TITLE_LENGTH) {
            // Truncate at word boundary
            titleSlug = truncateAtWordBoundary(titleSlug, MAX_TITLE_LENGTH);
        }
        slugBuilder.append(titleSlug);

        // Add first author if available
        if (authors != null && !authors.isEmpty()) {
            String firstAuthor = authors.get(0);
            if (firstAuthor != null && !firstAuthor.trim().isEmpty()) {
                String authorSlug = slugify(firstAuthor);
                if (authorSlug.length() > MAX_AUTHOR_LENGTH) {
                    authorSlug = truncateAtWordBoundary(authorSlug, MAX_AUTHOR_LENGTH);
                }
                slugBuilder.append("-").append(authorSlug);
            }
        }

        String finalSlug = slugBuilder.toString();

        // Ensure final slug doesn't exceed max length
        if (finalSlug.length() > MAX_SLUG_LENGTH) {
            finalSlug = truncateAtWordBoundary(finalSlug, MAX_SLUG_LENGTH);
        }

        return finalSlug;
    }

    /**
     * Generate a slug from book title and single author.
     */
    public static String generateBookSlug(String title, String author) {
        return generateBookSlug(title, author != null ? List.of(author) : null);
    }

    /**
     * Generate a slug from title only.
     */
    public static String generateBookSlug(String title) {
        return generateBookSlug(title, (List<String>) null);
    }

    /**
     * Convert any string to a slug format.
     * Handles Unicode, accents, and special characters.
     */
    public static String slugify(String input) {
        if (input == null) {
            return "";
        }

        // Convert to lowercase
        String slug = input.toLowerCase(Locale.ROOT).trim();

        // Normalize Unicode characters (é -> e, etc.)
        slug = Normalizer.normalize(slug, Normalizer.Form.NFD);
        slug = slug.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");

        // Replace common contractions and special cases
        slug = slug.replace("&", "and");
        slug = slug.replace("'", "");
        slug = slug.replace("'", "");
        // Remove left and right typographic double quotes
        slug = slug.replace("\u201C", "");
        slug = slug.replace("\u201D", "");

        // Remove all non-word characters except spaces and hyphens
        slug = NON_LATIN.matcher(slug).replaceAll("");

        // Replace spaces and underscores with hyphens
        slug = WHITESPACE.matcher(slug).replaceAll("-");

        // Remove multiple consecutive hyphens
        slug = MULTIPLE_DASHES.matcher(slug).replaceAll("-");

        // Remove leading and trailing hyphens
        slug = EDGE_DASHES.matcher(slug).replaceAll("");

        return slug;
    }

    /**
     * Truncate a slug at the nearest word boundary.
     */
    private static String truncateAtWordBoundary(String slug, int maxLength) {
        if (slug.length() <= maxLength) {
            return slug;
        }

        // Find the last dash before maxLength
        int lastDash = slug.lastIndexOf('-', maxLength);

        // If no dash found or it's too short, just truncate
        if (lastDash <= 0 || lastDash < maxLength / 2) {
            return slug.substring(0, maxLength);
        }

        return slug.substring(0, lastDash);
    }

    /**
     * Make a slug unique by appending a counter.
     * Used when a slug already exists in the database.
     *
     * @param baseSlug The original slug
     * @param counter The counter to append
     * @return Unique slug with counter (e.g., "harry-potter-2")
     */
    public static String makeSlugUnique(String baseSlug, int counter) {
        if (baseSlug == null || baseSlug.isEmpty()) {
            return String.valueOf(counter);
        }
        return baseSlug + "-" + counter;
    }

    /**
     * Validate that a slug is well-formed.
     */
    public static boolean isValidSlug(String slug) {
        if (slug == null || slug.isEmpty()) {
            return false;
        }

        // Should only contain lowercase letters, numbers, and hyphens
        // Should not start or end with hyphen
        // Should not have consecutive hyphens
        return slug.matches("^[a-z0-9]+(-[a-z0-9]+)*$");
    }
}