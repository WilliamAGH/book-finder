/**
 * Universal data extractor for book information from various API formats
 * Supports Google Books API, OpenLibrary API, NYT Bestsellers API, and consolidated book format
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.util;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class UniversalBookDataExtractor {
    
    private static final Logger log = LoggerFactory.getLogger(UniversalBookDataExtractor.class);
    
    /**
     * Extract ISBN-13 from any book data format
     */
    @SuppressWarnings("unchecked")
    public static String extractIsbn13(Map<String, Object> bookData) {
        if (bookData == null) return null;
        
        // Check consolidated format first (flattened field)
        if (bookData.containsKey("isbn13")) {
            return (String) bookData.get("isbn13");
        }
        
        // Google Books format
        if (bookData.containsKey("volumeInfo")) {
            String isbn13 = extractFromGoogleBooksIdentifiers(bookData, "ISBN_13");
            if (isbn13 != null) return isbn13;
        }
        
        // OpenLibrary format
        if (bookData.containsKey("isbn_13")) {
            List<String> isbns = (List<String>) bookData.get("isbn_13");
            if (isbns != null && !isbns.isEmpty()) {
                return isbns.get(0);
            }
        }
        
        // NYT format
        if (bookData.containsKey("isbns")) {
            return extractFromNYTIdentifiers(bookData);
        }
        
        return null;
    }
    
    /**
     * Extract ISBN-10 from any book data format
     */
    @SuppressWarnings("unchecked")
    public static String extractIsbn10(Map<String, Object> bookData) {
        if (bookData == null) return null;
        
        // Check consolidated format first (flattened field)
        if (bookData.containsKey("isbn10")) {
            return (String) bookData.get("isbn10");
        }
        
        // Google Books format
        if (bookData.containsKey("volumeInfo")) {
            String isbn10 = extractFromGoogleBooksIdentifiers(bookData, "ISBN_10");
            if (isbn10 != null) return isbn10;
        }
        
        // OpenLibrary format
        if (bookData.containsKey("isbn_10")) {
            List<String> isbns = (List<String>) bookData.get("isbn_10");
            if (isbns != null && !isbns.isEmpty()) {
                return isbns.get(0);
            }
        }
        
        return null;
    }
    
    /**
     * Extract Google Books ID from any book data format
     */
    public static String extractGoogleBooksId(Map<String, Object> bookData) {
        if (bookData == null) return null;
        
        // Check consolidated format first
        if (bookData.containsKey("googleBooksId")) {
            return (String) bookData.get("googleBooksId");
        }
        
        // Google Books format (top-level id)
        if (bookData.containsKey("id") && bookData.containsKey("volumeInfo")) {
            return (String) bookData.get("id");
        }
        
        return null;
    }
    
    /**
     * Extract title from any book data format
     */
    @SuppressWarnings("unchecked")
    public static String extractTitle(Map<String, Object> bookData) {
        if (bookData == null) return "Unknown Title";
        
        // Check consolidated format first (flattened field)
        if (bookData.containsKey("title")) {
            String title = (String) bookData.get("title");
            if (title != null && !title.trim().isEmpty()) {
                return title;
            }
        }
        
        // Google Books format
        if (bookData.containsKey("volumeInfo")) {
            Map<String, Object> volumeInfo = (Map<String, Object>) bookData.get("volumeInfo");
            if (volumeInfo != null && volumeInfo.containsKey("title")) {
                String title = (String) volumeInfo.get("title");
                if (title != null && !title.trim().isEmpty()) {
                    return title;
                }
            }
        }
        
        // OpenLibrary format
        if (bookData.containsKey("title")) {
            String title = (String) bookData.get("title");
            if (title != null && !title.trim().isEmpty()) {
                return title;
            }
        }
        
        // NYT format
        if (bookData.containsKey("title")) {
            String title = (String) bookData.get("title");
            if (title != null && !title.trim().isEmpty()) {
                return title;
            }
        }
        
        return "Unknown Title";
    }
    
    /**
     * Extract identifier from Google Books industryIdentifiers
     */
    @SuppressWarnings("unchecked")
    private static String extractFromGoogleBooksIdentifiers(Map<String, Object> bookData, String type) {
        try {
            Map<String, Object> volumeInfo = (Map<String, Object>) bookData.get("volumeInfo");
            if (volumeInfo != null && volumeInfo.containsKey("industryIdentifiers")) {
                List<Map<String, String>> identifiers = (List<Map<String, String>>) volumeInfo.get("industryIdentifiers");
                if (identifiers != null) {
                    for (Map<String, String> identifierObj : identifiers) {
                        if (type.equals(identifierObj.get("type"))) {
                            return identifierObj.get("identifier");
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error extracting {} from Google Books identifiers: {}", type, e.getMessage());
        }
        return null;
    }
    
    /**
     * Extract ISBN-13 from NYT isbns array
     */
    @SuppressWarnings("unchecked")
    private static String extractFromNYTIdentifiers(Map<String, Object> bookData) {
        try {
            List<Map<String, String>> isbns = (List<Map<String, String>>) bookData.get("isbns");
            if (isbns != null) {
                for (Map<String, String> isbnEntry : isbns) {
                    String isbn13 = isbnEntry.get("isbn13");
                    if (isbn13 != null && !isbn13.isEmpty()) {
                        return isbn13;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error extracting ISBN-13 from NYT identifiers: {}", e.getMessage());
        }
        return null;
    }
    
    /**
     * Detect if data is in Google Books API format
     */
    public static boolean isGoogleBooksFormat(JsonNode data) {
        return data.has("kind") && 
               data.get("kind").asText().startsWith("books#") &&
               data.has("volumeInfo");
    }
    
    /**
     * Detect if data is in OpenLibrary API format
     */
    public static boolean isOpenLibraryFormat(JsonNode data) {
        return data.has("key") && 
               data.get("key").asText().startsWith("/books/") ||
               data.has("works") || 
               data.has("isbn_10") || 
               data.has("isbn_13");
    }
    
    /**
     * Detect if data is in NYT Bestsellers API format
     */
    public static boolean isNYTFormat(JsonNode data) {
        return data.has("isbns") && 
               data.has("rank") ||
               data.has("list_name") ||
               data.has("weeks_on_list");
    }
    
    /**
     * Detect if data is in consolidated book format
     */
    public static boolean isConsolidatedBookFormat(JsonNode data) {
        return data.has("_metadata") ||
               (data.has("id") && data.has("title") && !data.has("volumeInfo") && !data.has("key"));
    }
}