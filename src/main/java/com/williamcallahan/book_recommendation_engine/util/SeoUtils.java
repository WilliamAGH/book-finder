package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.model.Book;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Utility class for search engine optimization operations
 *
 * Provides methods for formatting and optimizing content for SEO
 * Handles text truncation, metadata generation, and SEO-friendly formatting
 */
public class SeoUtils {

    /**
     * Truncates a string to a maximum length while preserving whole words
     * - Removes HTML tags for clean text presentation
     * - Adds ellipsis to indicate truncation
     * - Attempts to break at word boundaries when possible
     * - Used for generating SEO meta descriptions
     *
     * @param text The text to truncate
     * @param maxLength The maximum length of the truncated text
     * @return The truncated plain text with HTML removed
     */
    public static String truncateDescription(String text, int maxLength) {
        if (text == null || text.isEmpty()) {
            return "No description available.";
        }
        // Basic HTML removal and normalize whitespace
        String plainText = text.replaceAll("<[^>]*>", "").replaceAll("\\s+", " ").trim();

        if (plainText.length() <= maxLength) {
            return plainText;
        }

        String suffix = "...";
        int truncatedLength = maxLength - suffix.length();

        if (truncatedLength <= 0) {
            return suffix; // Or an empty string, depending on desired behavior
        }
        
        String sub = plainText.substring(0, Math.min(truncatedLength, plainText.length())); // Ensure substring doesn't go out of bounds
        int lastSpace = sub.lastIndexOf(' ');

        if (lastSpace > 0 && lastSpace < sub.length()) { // Ensure lastSpace is within the bounds of 'sub'
            return sub.substring(0, lastSpace) + suffix;
        } else { // No space found or it's at the very end, just cut
            return sub + suffix;
        }
    }

    /**
     * Generates a keyword string from book title, authors, and categories.
     */
    public static String generateKeywords(Book book) {
        if (book == null) {
            return "book, literature, reading";
        }

        List<String> keywords = new ArrayList<>();

        if (book.getTitle() != null && !book.getTitle().isEmpty()) {
            keywords.addAll(Arrays.asList(book.getTitle().toLowerCase(Locale.ROOT).split("\\s+")));
        }
        if (book.getAuthors() != null && !book.getAuthors().isEmpty()) {
            for (String author : book.getAuthors()) {
                if (author != null) {
                    keywords.addAll(Arrays.asList(author.toLowerCase(Locale.ROOT).split("\\s+")));
                }
            }
        }
        if (book.getCategories() != null && !book.getCategories().isEmpty()) {
            for (String category : book.getCategories()) {
                if (category != null) {
                    keywords.addAll(Arrays.asList(category.toLowerCase(Locale.ROOT).split("\\s+")));
                }
            }
        }

        keywords.add("book");
        keywords.add("details");
        keywords.add("review");
        keywords.add("summary");

        return keywords.stream()
            .filter(ValidationUtils::hasText)
            .map(String::trim)
            .map(token -> token.replaceAll("[^a-z0-9]", ""))
            .filter(ValidationUtils::hasText)
            .map(token -> token.toLowerCase(Locale.ROOT))
            .distinct()
            .filter(token -> token.length() > 2)
            .limit(15)
            .collect(Collectors.joining(", "));
    }
}
