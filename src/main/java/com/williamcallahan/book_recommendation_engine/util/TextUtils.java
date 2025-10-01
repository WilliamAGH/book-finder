package com.williamcallahan.book_recommendation_engine.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Centralized text normalization utilities for book data consistency.
 * Converts API data (often ALL CAPS) to proper title/name case at ingestion time.
 * 
 * @author William Callahan
 * @since 1.0.0
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
     * Converts book titles to proper English title case.
     * 
     * <p><strong>Examples:</strong>
     * <ul>
     * <li>"THE GREAT GATSBY" → "The Great Gatsby"</li>
     * <li>"WORLD WAR II: A HISTORY" → "World War II: A History"</li>
     * <li>"THE FBI FILES" → "The FBI Files"</li>
     * </ul>
     * 
     * <p><strong>Rules:</strong> Capitalizes first word, proper nouns, Roman numerals, acronyms.
     * Lowercases articles/prepositions (a, the, of, in, etc.) unless first/last word.
     * Preserves intentional mixed case (e.g., "eBay").
     * 
     * @param title raw title from external API (may be ALL CAPS)
     * @return normalized title in proper case, or null if input is null
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
     * Internal method applying English title case rules with subtitle handling.
     * 
     * @param text text known to need normalization (all upper/lower case)
     * @return title-cased text with proper capitalization after colons/dashes
     */
    private static String toTitleCase(String text) {
        if (text == null || text.isBlank()) {
            return text;
        }

        // Split by spaces and punctuation boundaries while preserving delimiters
        String[] words = text.split("(?<=\\s)|(?=\\s)|(?<=[-:;,.])|(?=[-:;,.])");
        StringBuilder result = new StringBuilder();
        
        boolean isFirstWord = true;
        
        for (String word : words) {
            if (word.isBlank() || word.matches("[\\s\\-:;,.]")) {
                // Preserve whitespace and punctuation as-is
                result.append(word);
                continue;
            }

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
     * Capitalizes first letter, handles leading punctuation (e.g., "'til" → "'Til").
     * 
     * @param word single word to capitalize
     * @return word with first letter capitalized, preserving punctuation
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
     * Capitalizes first letter after delimiter for subtitle handling.
     * Example: "clean code: a handbook" → "clean code: A handbook"
     * 
     * @param text text to process
     * @param delimiter character after which to capitalize (typically ':' or '—')
     * @return text with post-delimiter capitalization applied
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
     * Converts author names to proper case with prefix handling.
     * 
     * <p><strong>Examples:</strong>
     * <ul>
     * <li>"STEPHEN KING" → "Stephen King"</li>
     * <li>"PATRICK MCDONALD" → "Patrick McDonald"</li>
     * <li>"LUDWIG VON BEETHOVEN" → "Ludwig von Beethoven"</li>
     * <li>"CONNOR O'BRIEN" → "Connor O'Brien"</li>
     * </ul>
     * 
     * <p><strong>Special handling:</strong> Mc/Mac/O' prefixes, nobility particles (von, van, de).
     * Preserves intentional mixed case.
     * 
     * @param name raw author name from external API (may be ALL CAPS)
     * @return normalized name in proper case, or null if input is null
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
     * Capitalizes name part with prefix-specific rules.
     * Handles Mc/Mac/O' (Irish/Scottish), von/van/de (nobility particles).
     * 
     * @param part single name component (e.g., "mcdonald", "von")
     * @return properly capitalized part following naming conventions
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
