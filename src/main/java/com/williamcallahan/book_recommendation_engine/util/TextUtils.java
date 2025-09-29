package com.williamcallahan.book_recommendation_engine.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for text normalization and formatting operations.
 * Provides consistent text handling across the application.
 */
public class TextUtils {

    private static final Logger logger = LoggerFactory.getLogger(TextUtils.class);

    // Words that should remain lowercase in title case (unless they're the first word)
    private static final Set<String> LOWERCASE_WORDS = new HashSet<>(Arrays.asList(
        "a", "an", "and", "as", "at", "but", "by", "for", "from", "in", 
        "into", "nor", "of", "on", "or", "over", "so", "the", "to", 
        "up", "with", "yet", "vs", "vs.", "v", "v."
    ));

    // Common abbreviations and acronyms that should be uppercase
    private static final Set<String> UPPERCASE_WORDS = new HashSet<>(Arrays.asList(
        "usa", "uk", "us", "tv", "fbi", "cia", "nasa", "nato", "un", "eu",
        "pdf", "html", "css", "sql", "api", "json", "xml", "http", "https",
        "isbn", "id", "ceo", "cfo", "cto", "vp", "phd", "md", "dds", "jr", "sr"
    ));

    // Roman numerals pattern
    private static final Set<String> ROMAN_NUMERALS = new HashSet<>(Arrays.asList(
        "i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x",
        "xi", "xii", "xiii", "xiv", "xv", "xvi", "xvii", "xviii", "xix", "xx"
    ));

    private TextUtils() {
        // Private constructor to prevent instantiation
    }

    /**
     * Normalizes a book title to proper title case.
     * Handles edge cases like:
     * - All uppercase titles (converts to title case)
     * - All lowercase titles (converts to title case)
     * - Mixed case titles (preserves intentional capitalization)
     * - Articles and prepositions (lowercase unless first/last word)
     * - Abbreviations and acronyms (uppercase)
     * - Roman numerals (uppercase)
     * 
     * @param title The raw book title
     * @return Normalized title, or null if input is null
     */
    public static String normalizeBookTitle(String title) {
        if (title == null || title.isBlank()) {
            return title;
        }

        // Trim whitespace
        String trimmed = title.trim();
        
        // Check if the title is all uppercase or all lowercase (needs normalization)
        boolean isAllUppercase = trimmed.equals(trimmed.toUpperCase()) && !trimmed.equals(trimmed.toLowerCase());
        boolean isAllLowercase = trimmed.equals(trimmed.toLowerCase()) && !trimmed.equals(trimmed.toUpperCase());
        
        // If mixed case and not all uppercase/lowercase, preserve original (likely intentional formatting)
        if (!isAllUppercase && !isAllLowercase) {
            logger.debug("Title '{}' appears to have intentional mixed case formatting, preserving as-is", trimmed);
            return trimmed;
        }

        // Convert to title case
        return toTitleCase(trimmed);
    }

    /**
     * Converts a string to title case following standard English title capitalization rules.
     * 
     * @param text The text to convert
     * @return Title-cased text
     */
    private static String toTitleCase(String text) {
        if (text == null || text.isBlank()) {
            return text;
        }

        // Split by spaces and punctuation boundaries while preserving delimiters
        String[] words = text.split("(?<=\\s)|(?=\\s)|(?<=[-:;,.])|(?=[-:;,.])");
        StringBuilder result = new StringBuilder();
        
        boolean isFirstWord = true;
        int wordCount = 0;
        
        for (String word : words) {
            if (word.isBlank() || word.matches("[\\s\\-:;,.]")) {
                // Preserve whitespace and punctuation as-is
                result.append(word);
                continue;
            }

            wordCount++;
            String lowerWord = word.toLowerCase();
            String processedWord;

            // First word is always capitalized
            if (isFirstWord) {
                processedWord = capitalize(word);
                isFirstWord = false;
            }
            // Check if it's a known acronym or abbreviation
            else if (UPPERCASE_WORDS.contains(lowerWord)) {
                processedWord = word.toUpperCase();
            }
            // Check if it's a Roman numeral
            else if (ROMAN_NUMERALS.contains(lowerWord)) {
                processedWord = word.toUpperCase();
            }
            // Check if it should be lowercase (articles, prepositions, conjunctions)
            else if (LOWERCASE_WORDS.contains(lowerWord)) {
                processedWord = lowerWord;
            }
            // Check if word starts with a number (e.g., "3rd", "21st")
            else if (word.matches("^\\d+.*")) {
                processedWord = word.toLowerCase();
            }
            // Default: capitalize first letter
            else {
                processedWord = capitalize(word);
            }

            result.append(processedWord);
        }

        // Ensure last significant word is capitalized (if it was lowercase)
        String finalResult = result.toString().trim();
        
        // Capitalize after colons and dashes (common in subtitles)
        finalResult = capitalizeAfterDelimiter(finalResult, ':');
        finalResult = capitalizeAfterDelimiter(finalResult, '—');
        finalResult = capitalizeAfterDelimiter(finalResult, '–');
        
        logger.debug("Normalized title from '{}' to '{}'", text, finalResult);
        return finalResult;
    }

    /**
     * Capitalizes the first letter of a word.
     * 
     * @param word The word to capitalize
     * @return Word with first letter capitalized
     */
    private static String capitalize(String word) {
        if (word == null || word.isEmpty()) {
            return word;
        }
        
        // Handle words with leading punctuation like quotes
        int firstLetterIndex = 0;
        while (firstLetterIndex < word.length() && !Character.isLetterOrDigit(word.charAt(firstLetterIndex))) {
            firstLetterIndex++;
        }
        
        if (firstLetterIndex >= word.length()) {
            return word;
        }
        
        return word.substring(0, firstLetterIndex) 
             + Character.toUpperCase(word.charAt(firstLetterIndex))
             + word.substring(firstLetterIndex + 1).toLowerCase();
    }

    /**
     * Capitalizes the first letter after a delimiter (e.g., colon, em-dash).
     * Used for subtitles and multi-part titles.
     * 
     * @param text The text to process
     * @param delimiter The delimiter character
     * @return Text with words after delimiter capitalized
     */
    private static String capitalizeAfterDelimiter(String text, char delimiter) {
        if (text == null || text.isEmpty()) {
            return text;
        }

        StringBuilder result = new StringBuilder();
        boolean capitalizeNext = false;
        
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            
            if (c == delimiter) {
                result.append(c);
                capitalizeNext = true;
            } else if (capitalizeNext && !Character.isWhitespace(c)) {
                result.append(Character.toUpperCase(c));
                capitalizeNext = false;
            } else {
                result.append(c);
            }
        }
        
        return result.toString();
    }

    /**
     * Normalizes author names to proper case.
     * Handles special cases like "McDONALD" -> "McDonald" and "VON NEUMANN" -> "von Neumann".
     * 
     * @param name The author name to normalize
     * @return Normalized author name
     */
    public static String normalizeAuthorName(String name) {
        if (name == null || name.isBlank()) {
            return name;
        }

        String trimmed = name.trim();
        
        // If already mixed case, preserve it
        boolean isAllUppercase = trimmed.equals(trimmed.toUpperCase()) && !trimmed.equals(trimmed.toLowerCase());
        if (!isAllUppercase) {
            return trimmed;
        }

        // Split by spaces and capitalize each part
        String[] parts = trimmed.split("\\s+");
        return Arrays.stream(parts)
            .map(TextUtils::capitalizeNamePart)
            .collect(Collectors.joining(" "));
    }

    /**
     * Capitalizes a part of a name (handles prefixes like Mc, Mac, O', etc.).
     * 
     * @param part The name part to capitalize
     * @return Capitalized name part
     */
    private static String capitalizeNamePart(String part) {
        if (part == null || part.isEmpty()) {
            return part;
        }

        String lower = part.toLowerCase();
        
        // Handle common name prefixes
        if (lower.startsWith("mc") && part.length() > 2) {
            return "Mc" + Character.toUpperCase(part.charAt(2)) + part.substring(3).toLowerCase();
        }
        if (lower.startsWith("mac") && part.length() > 3) {
            return "Mac" + Character.toUpperCase(part.charAt(3)) + part.substring(4).toLowerCase();
        }
        if (lower.startsWith("o'") && part.length() > 2) {
            return "O'" + Character.toUpperCase(part.charAt(2)) + part.substring(3).toLowerCase();
        }
        
        // Lowercase prefixes (von, van, de, etc.)
        // Note: 'da' is excluded as it's less commonly lowercase in practice (e.g., "Da Vinci")
        if (lower.equals("von") || lower.equals("van") || lower.equals("de") || 
            lower.equals("del") || lower.equals("della") || lower.equals("di")) {
            return lower;
        }

        // Default capitalization
        return capitalize(part);
    }
}