/**
 * Utility class for parsing Google Books API JSON responses into Book objects
 *
 * @author William Callahan
 * 
 * Features:
 * - Converts Google Books API JSON data to Book objects
 * - Handles book metadata extraction and mapping
 * - Provides image URL processing for best quality covers
 * - Supports extraction of qualifiers from search queries
 * - Contains robust error handling for malformed JSON or missing fields
 */
package com.williamcallahan.book_recommendation_engine.util;

import com.fasterxml.jackson.databind.JsonNode;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.Normalizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Locale;


public class BookJsonParser {

    private static final Logger logger = LoggerFactory.getLogger(BookJsonParser.class);
    private static final Pattern EDITION_WORD_PATTERN = Pattern.compile("(\\d+)(?:st|nd|rd|th)?\\s*(?:edition|ed\\b)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TRAILING_NUMBER_PATTERN = Pattern.compile("(\\d+)(?:\\.\\d+)*$");
    private static final Pattern QUALIFIER_NORMALIZE_PATTERN = Pattern.compile("[^a-z0-9]+");

    /**
     * Converts Google Books API JSON to Book object
     *
     * @param item JsonNode with volume data
     * @return Populated Book object or null if input is null
     */
    public static Book convertJsonToBook(JsonNode item) {
        if (item == null) {
            logger.warn("Input JsonNode is null. Cannot convert to Book.");
            return null;
        }

        Book book = new Book();
        book.setRawJsonResponse(item.toString()); // Store raw JSON

        extractBookBaseInfo(item, book);
        setAdditionalFields(item, book);
        setLinks(item, book);
        extractQualifiersFromItem(item, book); // Extract qualifiers if present
        normalizeQualifierKeys(book);
        applyEditionMetadata(item, book);

        return book;
    }

    /**
     * Extracts book core data from JSON
     * 
     * @param item Volume JSON node
     * @param book Book to populate
     */
    private static void extractBookBaseInfo(JsonNode item, Book book) {
        if (!item.has("volumeInfo")) {
            logger.debug("JsonNode item for book ID {} lacks 'volumeInfo'. Base info might be incomplete.", item.has("id") ? item.get("id").asText() : "UNKNOWN");
            // Still set ID if available at the top level
            if (item.has("id")) {
                book.setId(item.get("id").asText());
            }
            return;
        }

        JsonNode volumeInfo = item.get("volumeInfo");

        book.setId(item.has("id") ? item.get("id").asText() : null);
        book.setTitle(volumeInfo.has("title") ? volumeInfo.get("title").asText() : null);
        book.setAuthors(getAuthorsFromVolumeInfo(volumeInfo));

        String rawPublisher = volumeInfo.has("publisher") ? volumeInfo.get("publisher").asText() : null;
        if (rawPublisher != null) {
            rawPublisher = rawPublisher.replaceAll("^\"|\"$", ""); // Sanitize
        }
        book.setPublisher(rawPublisher);

        book.setPublishedDate(parsePublishedDate(volumeInfo));
        book.setDescription(volumeInfo.has("description") ? volumeInfo.get("description").asText() : null);
        book.setExternalImageUrl(getGoogleCoverImageFromVolumeInfo(volumeInfo));
        book.setLanguage(volumeInfo.has("language") ? volumeInfo.get("language").asText() : null);

        if (volumeInfo.has("industryIdentifiers")) {
            List<Book.EditionInfo> otherEditions = new ArrayList<>();
            for (JsonNode identifierNode : volumeInfo.get("industryIdentifiers")) {
                extractEditionInfoFromItem(identifierNode, otherEditions);

                String type = identifierNode.has("type") ? identifierNode.get("type").asText() : null;
                String idValue = identifierNode.has("identifier") ? identifierNode.get("identifier").asText() : null;

                if (idValue != null && !idValue.isEmpty()) {
                    if ("ISBN_10".equals(type) && book.getIsbn10() == null) {
                        book.setIsbn10(idValue);
                    } else if ("ISBN_13".equals(type) && book.getIsbn13() == null) {
                        book.setIsbn13(idValue);
                    }
                }
            }
            book.setOtherEditions(otherEditions);
        }
    }

    private static void normalizeQualifierKeys(Book book) {
        Map<String, Object> qualifiers = book.getQualifiers();
        if (qualifiers == null || qualifiers.isEmpty()) {
            return;
        }

        Map<String, Object> normalized = new HashMap<>();
        qualifiers.forEach((key, value) -> {
            if (key == null) return;
            String trimmed = key.trim().toLowerCase();
            if (trimmed.isEmpty()) return;
            String canonical = QUALIFIER_NORMALIZE_PATTERN.matcher(trimmed).replaceAll("_");
            canonical = canonical.replaceAll("_{2,}", "_");
            canonical = canonical.replaceAll("^_+|_+$", "");
            if (!canonical.isEmpty()) {
                normalized.put(canonical, value);
            }
        });

        if (!normalized.isEmpty()) {
            book.setQualifiers(normalized);
        }
    }

    private static void applyEditionMetadata(JsonNode item, Book book) {
        Integer editionNumber = deriveEditionNumber(item, book);
        if (editionNumber != null) {
            book.setEditionNumber(editionNumber);
        }

        String groupKey = buildEditionGroupKey(book);
        if (groupKey != null && !groupKey.isBlank()) {
            book.setEditionGroupKey(groupKey);
        }
    }

    private static Integer deriveEditionNumber(JsonNode item, Book book) {
        List<Integer> candidates = new ArrayList<>();

        JsonNode volumeInfo = item != null ? item.get("volumeInfo") : null;
        if (volumeInfo != null) {
            addEditionCandidate(candidates, volumeInfo.get("edition"));
            addEditionCandidate(candidates, volumeInfo.get("editionInformation"));
            addEditionCandidate(candidates, volumeInfo.get("editionInfo"));
            addEditionCandidate(candidates, volumeInfo.get("contentVersion"));
            if (volumeInfo.has("subtitle")) {
                addEditionCandidate(candidates, volumeInfo.get("subtitle"));
            }
            if (volumeInfo.has("title")) {
                addEditionCandidate(candidates, volumeInfo.get("title"));
            }
        }

        if (item != null) {
            addEditionCandidate(candidates, item.get("edition"));
            addEditionCandidate(candidates, item.get("editionNumber"));
            addEditionCandidate(candidates, item.get("edition_number"));
        }

        Map<String, Object> qualifiers = book.getQualifiers();
        if (qualifiers != null) {
            addEditionCandidate(candidates, qualifiers.get("edition_number"));
            addEditionCandidate(candidates, qualifiers.get("edition-number"));
            addEditionCandidate(candidates, qualifiers.get("edition"));
        }

        addEditionCandidate(candidates, book.getTitle());

        return candidates.isEmpty() ? null : candidates.get(0);
    }

    private static void addEditionCandidate(List<Integer> candidates, Object value) {
        Integer parsed = parseEditionCandidate(value);
        if (parsed != null) {
            candidates.add(parsed);
        }
    }

    private static Integer parseEditionCandidate(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            int candidate = number.intValue();
            return candidate > 0 ? candidate : null;
        }
        if (value instanceof JsonNode node) {
            if (node.isNumber()) {
                int candidate = node.intValue();
                return candidate > 0 ? candidate : null;
            }
            if (node.isTextual()) {
                return parseEditionCandidate(node.textValue());
            }
            return null;
        }
        if (value instanceof String text) {
            String trimmed = text.trim();
            if (trimmed.isEmpty()) {
                return null;
            }
            try {
                int direct = Integer.parseInt(trimmed);
                if (direct > 0) {
                    return direct;
                }
            } catch (NumberFormatException ignored) {
                Matcher editionMatcher = EDITION_WORD_PATTERN.matcher(trimmed);
                if (editionMatcher.find()) {
                    int candidate = Integer.parseInt(editionMatcher.group(1));
                    return candidate > 0 ? candidate : null;
                }
                Matcher trailingMatcher = TRAILING_NUMBER_PATTERN.matcher(trimmed);
                if (trailingMatcher.find()) {
                    int candidate = Integer.parseInt(trailingMatcher.group(1));
                    return candidate > 0 ? candidate : null;
                }
            }
        }
        return null;
    }

    private static String buildEditionGroupKey(Book book) {
        String titleComponent = normalizeKeyComponent(book.getTitle());
        if (titleComponent.isEmpty()) {
            return null;
        }
        List<String> authors = book.getAuthors();
        String authorComponent = (authors != null && !authors.isEmpty()) ? normalizeKeyComponent(authors.get(0)) : "";
        return authorComponent.isEmpty() ? titleComponent : titleComponent + "__" + authorComponent;
    }

    private static String normalizeKeyComponent(String value) {
        if (value == null) {
            return "";
        }
            String normalized = Normalizer.normalize(value, Normalizer.Form.NFKD)
            .replaceAll("\\p{M}+", "")
            .toLowerCase(Locale.ROOT)
            .replaceAll("[^a-z0-9\\s]+", " ")
            .trim()
            .replaceAll("\\s+", " ");
        return normalized;
    }

    /**
     * Extracts authors from volume info
     * 
     * @param volumeInfo Volume info JSON node
     * @return List of author names (never null)
     */
    private static List<String> getAuthorsFromVolumeInfo(JsonNode volumeInfo) {
        List<String> authors = new ArrayList<>();
        if (volumeInfo == null) {
            logger.warn("volumeInfo is null when extracting authors");
            return authors;
        }
        
        if (volumeInfo.has("authors")) {
            JsonNode authorsNode = volumeInfo.get("authors");
            if (authorsNode.isArray()) {
                authorsNode.forEach(authorNode -> {
                    if (authorNode != null && !authorNode.isNull()) {
                        String authorText = authorNode.asText("").trim();
                        if (!authorText.isEmpty()) {
                            authors.add(authorText);
                        }
                    }
                });
            } else if (authorsNode.isTextual()) {
                // Handle case where authors might be a single string instead of an array
                String authorText = authorsNode.asText("").trim();
                if (!authorText.isEmpty()) {
                    authors.add(authorText);
                }
            } else {
                logger.warn("Authors field is present but is neither an array nor a string: {}", authorsNode.getNodeType());
            }
        } else {
            logger.debug("No 'authors' field found in volumeInfo");
        }
        
        return authors;
    }

    /**
     * Extracts best cover image URL from JSON
     * 
     * @param volumeInfo Volume info JSON node
     * @return Best quality cover URL or null
     */
    private static String getGoogleCoverImageFromVolumeInfo(JsonNode volumeInfo) {
        if (volumeInfo.has("imageLinks")) {
            JsonNode imageLinks = volumeInfo.get("imageLinks");
            String coverUrl = null;

            if (logger.isDebugEnabled()) {
                String bookTitleForLog = volumeInfo.has("title") ? volumeInfo.get("title").asText() : "Unknown Title";
                logger.debug("Raw Google Books imageLinks for '{}': {}", bookTitleForLog, imageLinks.toString());
            }

            if (imageLinks.has("extraLarge")) coverUrl = imageLinks.get("extraLarge").asText();
            else if (imageLinks.has("large")) coverUrl = imageLinks.get("large").asText();
            else if (imageLinks.has("medium")) coverUrl = imageLinks.get("medium").asText();
            else if (imageLinks.has("thumbnail")) coverUrl = imageLinks.get("thumbnail").asText();
            else if (imageLinks.has("smallThumbnail")) coverUrl = imageLinks.get("smallThumbnail").asText();

            if (coverUrl != null) {
                return enhanceGoogleCoverUrl(coverUrl, "high"); // Default to high quality
            }
        }
        return null;
    }

    /**
     * Enhances Google Books cover URL
     * 
     * @param url Original cover URL
     * @param quality Target quality (high, medium, low)
     * @return Enhanced URL with proper protocol and settings
     */
    private static String enhanceGoogleCoverUrl(String url, String quality) {
        if (url == null) return null;
        String enhancedUrl = url;

        if (enhancedUrl.startsWith("http://")) {
            enhancedUrl = "https://" + enhancedUrl.substring(7);
        }

        // Remove fife parameter
        if (enhancedUrl.contains("&fife=")) {
            enhancedUrl = enhancedUrl.replaceAll("&fife=w\\d+", "");
        } else if (enhancedUrl.contains("?fife=")) {
            enhancedUrl = enhancedUrl.replaceAll("\\?fife=w\\d+", "?");
            if (enhancedUrl.endsWith("?")) {
                enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
            }
        }
        if (enhancedUrl.endsWith("&")) {
            enhancedUrl = enhancedUrl.substring(0, enhancedUrl.length() - 1);
        }
        
        // Adjust zoom based on quality
        String zoomParam = "zoom=1"; // Default for low/medium
        if ("high".equalsIgnoreCase(quality)) {
             // For "high" quality, if zoom exists and is > 2, set to 2. Otherwise, don't add/change zoom.
            if (enhancedUrl.contains("zoom=")) {
                try {
                    String zoomValueStr = enhancedUrl.substring(enhancedUrl.indexOf("zoom=") + 5);
                    if (zoomValueStr.contains("&")) {
                        zoomValueStr = zoomValueStr.substring(0, zoomValueStr.indexOf("&"));
                    }
                    int currentZoom = Integer.parseInt(zoomValueStr);
                    if (currentZoom > 2) {
                        enhancedUrl = enhancedUrl.replaceAll("zoom=\\d+", "zoom=2");
                    }
                    // If currentZoom <=2, keep it as is
                    zoomParam = null; // Indicate no change or addition needed
                } catch (NumberFormatException e) {
                    logger.warn("Could not parse zoom value in URL: {}. Applying default zoom=1 for high quality.", enhancedUrl);
                    enhancedUrl = enhancedUrl.replaceAll("zoom=\\d+", "zoom=1"); // Fallback to zoom=1
                    zoomParam = null; 
                }
            } else {
                 zoomParam = null; // No zoom present, don't add one for high quality
            }
        } else { // medium or low
            if (enhancedUrl.contains("zoom=")) {
                enhancedUrl = enhancedUrl.replaceAll("zoom=\\d+", zoomParam);
            } else {
                // Append zoom if not present
                enhancedUrl += (enhancedUrl.contains("?") ? "&" : "?") + zoomParam;
            }
        }
        // If zoomParam is still set (i.e., for medium/low and it wasn't already there or for high quality fallback)
        if (zoomParam != null && !enhancedUrl.contains("zoom=")) {
             enhancedUrl += (enhancedUrl.contains("?") ? "&" : "?") + zoomParam;
        }


        return enhancedUrl;
    }

    /**
     * Sets commercial fields (price, currency)
     * 
     * @param item Volume JSON node
     * @param book Book to populate
     */
    private static void setAdditionalFields(JsonNode item, Book book) {
        if (item.has("saleInfo")) {
            JsonNode saleInfo = item.get("saleInfo");
            if (saleInfo.has("listPrice")) {
                JsonNode listPrice = saleInfo.get("listPrice");
                if (listPrice.has("amount")) book.setListPrice(listPrice.get("amount").asDouble());
                if (listPrice.has("currencyCode")) book.setCurrencyCode(listPrice.get("currencyCode").asText());
            }
        }
    }

    /**
     * Sets access links and availability flags
     * 
     * @param item Volume JSON node
     * @param book Book to populate
     */
    private static void setLinks(JsonNode item, Book book) {
        if (item.has("accessInfo")) {
            JsonNode accessInfo = item.get("accessInfo");
            if (accessInfo.has("webReaderLink")) book.setWebReaderLink(accessInfo.get("webReaderLink").asText());
            if (accessInfo.has("pdf") && accessInfo.get("pdf").has("isAvailable")) {
                book.setPdfAvailable(accessInfo.get("pdf").get("isAvailable").asBoolean());
            }
            if (accessInfo.has("epub") && accessInfo.get("epub").has("isAvailable")) {
                book.setEpubAvailable(accessInfo.get("epub").get("isAvailable").asBoolean());
            }
        }
    }

    /**
     * Parses published date with format fallbacks
     * 
     * @param volumeInfo Volume info JSON node
     * @return Date object or null if parsing fails
     */
    private static Date parsePublishedDate(JsonNode volumeInfo) {
        if (volumeInfo.has("publishedDate")) {
            String dateString = volumeInfo.get("publishedDate").asText();
            // Try yyyy-MM-dd first
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            try {
                return format.parse(dateString);
            } catch (ParseException e) {
                // Try yyyy-MM
                format = new SimpleDateFormat("yyyy-MM");
                try {
                    return format.parse(dateString);
                } catch (ParseException ex) {
                    // Try yyyy
                    format = new SimpleDateFormat("yyyy");
                    try {
                        return format.parse(dateString);
                    } catch (ParseException exc) {
                        logger.warn("Failed to parse published date: {} with formats yyyy-MM-dd, yyyy-MM, yyyy", dateString);
                        return null;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Extracts edition identifier info
     * 
     * @param identifier Identifier JSON node
     * @param otherEditions List to add edition info
     */
    private static void extractEditionInfoFromItem(JsonNode identifier, List<Book.EditionInfo> otherEditions) {
        if (identifier.has("type") && identifier.has("identifier")) {
            String type = identifier.get("type").asText();
            String ident = identifier.get("identifier").asText();
            Book.EditionInfo editionInfo = new Book.EditionInfo();
            editionInfo.setType(type);
            editionInfo.setIdentifier(ident);
            otherEditions.add(editionInfo);
        }
    }
    
    /**
     * Extracts qualifiers from JSON
     * 
     * @param item Volume JSON with qualifiers
     * @param book Book to populate with qualifiers
     */
    private static void extractQualifiersFromItem(JsonNode item, Book book) {
        if (item != null && item.has("qualifiers")) {
            try {
                JsonNode qualifiersNode = item.get("qualifiers");
                if (qualifiersNode != null && qualifiersNode.isObject()) {
                    qualifiersNode.fields().forEachRemaining(entry -> {
                        String key = entry.getKey();
                        JsonNode valueNode = entry.getValue();
                        
                        if (valueNode.isBoolean()) book.addQualifier(key, valueNode.booleanValue());
                        else if (valueNode.isTextual()) book.addQualifier(key, valueNode.textValue());
                        else if (valueNode.isNumber()) book.addQualifier(key, valueNode.numberValue());
                        else if (valueNode.isArray()) {
                            List<Object> values = new ArrayList<>();
                            valueNode.elements().forEachRemaining(element -> {
                                if (element.isTextual()) values.add(element.textValue());
                                else if (element.isBoolean()) values.add(element.booleanValue());
                                else if (element.isNumber()) values.add(element.numberValue());
                                else values.add(element.toString());
                            });
                            book.addQualifier(key, values);
                        } else {
                            book.addQualifier(key, valueNode.toString());
                        }
                    });
                    logger.debug("Extracted {} qualifiers from JSON for book {}", book.getQualifiers().size(), book.getId());
                }
            } catch (Exception e) {
                logger.warn("Error extracting qualifiers from JSON for book {}: {}", book.getId(), e.getMessage());
            }
        }
    }

    /**
     * Extracts qualifiers from search query
     *
     * @param query Search query string
     * @return Map of extracted qualifiers
     */
    public static Map<String, Object> extractQualifiersFromSearchQuery(String query) {
        Map<String, Object> qualifiers = new HashMap<>();
        if (query == null || query.trim().isEmpty()) {
            return qualifiers;
        }

        String normalizedQuery = SearchQueryUtils.canonicalize(query);
        if (normalizedQuery == null || normalizedQuery.isEmpty()) {
            return qualifiers;
        }

        if (normalizedQuery.contains("new york times bestseller") || 
            normalizedQuery.contains("nyt bestseller") ||
            normalizedQuery.contains("ny times bestseller")) {
            qualifiers.put("nytBestseller", true);
        }
        
        if (normalizedQuery.contains("award winner") || 
            normalizedQuery.contains("prize winner") ||
            normalizedQuery.contains("pulitzer") ||
            normalizedQuery.contains("nobel")) {
            qualifiers.put("awardWinner", true);
            if (normalizedQuery.contains("pulitzer")) qualifiers.put("pulitzerPrize", true);
            if (normalizedQuery.contains("nobel")) qualifiers.put("nobelPrize", true);
        }
        
        if (normalizedQuery.contains("best books") || 
            normalizedQuery.contains("top books") ||
            normalizedQuery.contains("must read")) {
            qualifiers.put("recommendedList", true);
        }
        
        // Add the raw query terms as a qualifier
        qualifiers.put("queryTerms", new ArrayList<>(Arrays.asList(normalizedQuery.split("\\s+"))));
        // Add the original full query as a qualifier
        qualifiers.put("searchQuery", query.trim());


        return qualifiers;
    }

    /**
     * Validates if the given string is a plausible ISBN-10 or ISBN-13
     * This is a basic structural check, not a checksum validation
     *
     * @param isbn The string to validate
     * @return true if it structurally resembles an ISBN, false otherwise
     */
    public static boolean isValidIsbn(String isbn) {
        if (isbn == null) {
            return false;
        }
        String sanitizedIsbn = isbn.replace("-", "").replace(" ", "").toUpperCase(Locale.ROOT);
        if (sanitizedIsbn.length() == 10) {
            // Basic check for ISBN-10 (9 digits + 1 check digit which can be X)
            return sanitizedIsbn.matches("^[0-9]{9}[0-9X]$");
        } else if (sanitizedIsbn.length() == 13) {
            // Basic check for ISBN-13 (13 digits)
            return sanitizedIsbn.matches("^[0-9]{13}$");
        }
        return false;
    }
}
