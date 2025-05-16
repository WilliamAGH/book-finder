package com.williamcallahan.book_recommendation_engine.util;

/**
 * Utility class for search engine optimization operations
 *
 * @author William Callahan
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
}
