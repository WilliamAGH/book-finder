/**
 * Service for merging book data from multiple sources
 *
 * @author William Callahan
 *
 * Features:
 * - Combines book data from Google Books API and New York Times API
 * - Resolves conflicts and merges overlapping fields intelligently
 * - Preserves source-specific identifiers and metadata
 * - Handles special fields like ranks, weeks on list, and buy links
 * - Creates enriched book records with data from all available sources
 * - Ensures consistent field naming and data structure
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Service
public class BookDataAggregatorService {

    private static final Logger logger = LoggerFactory.getLogger(BookDataAggregatorService.class);
    private final ObjectMapper objectMapper;

    /**
     * Constructs the BookDataAggregatorService with required dependencies
     *
     * @param objectMapper Jackson object mapper for JSON processing
     */
    public BookDataAggregatorService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Aggregates book data from multiple JsonNode sources.
     *
     * @param primaryId The primary identifier for the book (e.g., Google Books ID or ISBN).
     * @param sourceIdentifierField The field name in each JsonNode that holds its native primary ID (e.g., "id" for Google, "key" for OpenLibrary works).
     * @param dataSources Varargs of JsonNode, each representing book data from a different source.
     *                    It's recommended to pass sources in order of preference (e.g., Google, OpenLibrary, Longitood).
     * @return An ObjectNode containing the merged book data.
     *
     * @deprecated Use {@link com.williamcallahan.book_recommendation_engine.mapper.GoogleBooksMapper} and
     * {@link com.williamcallahan.book_recommendation_engine.service.BookUpsertService} to normalise provider
     * payloads directly into Postgres before projecting DTOs.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    public ObjectNode aggregateBookDataSources(String primaryId, String sourceIdentifierField, JsonNode... dataSources) {
        ObjectNode aggregatedJson = objectMapper.createObjectNode();
        List<JsonNode> validSources = Arrays.stream(dataSources)
                                            .filter(node -> node != null && node.isObject())
                                            .collect(Collectors.toList());

        if (validSources.isEmpty()) {
            logger.warn("No valid data sources provided for aggregation with primaryId: {}", primaryId);
            aggregatedJson.put("id", primaryId); // At least set the primary ID
            return aggregatedJson;
        }

        // Set the primary ID first
        aggregatedJson.put("id", primaryId);

        // Field-specific aggregation logic
        // Title: Prefer first non-empty title
        aggregatedJson.put("title", 
            getStringValueFromSources(validSources, "title")
            .orElse(primaryId)); // Fallback to primaryId if no title found

        // Authors: Union of unique authors
        Set<String> allAuthors = new HashSet<>();
        for (JsonNode source : validSources) {
            if (source.has("authors") && source.get("authors").isArray()) {
                source.get("authors").forEach(authorNode -> {
                    if (authorNode.isTextual()) allAuthors.add(authorNode.asText());
                });
            } else if (source.has("volumeInfo") && source.path("volumeInfo").has("authors") && source.path("volumeInfo").get("authors").isArray()){ // Google Books structure
                 source.path("volumeInfo").get("authors").forEach(authorNode -> {
                    if (authorNode.isTextual()) allAuthors.add(authorNode.asText());
                });
            }
        }
        if (!allAuthors.isEmpty()) {
            ArrayNode authorsArray = objectMapper.createArrayNode();
            allAuthors.forEach(authorsArray::add);
            aggregatedJson.set("authors", authorsArray);
        }

        // Description: Prefer longest, non-empty description
        String longestDescription = "";
        for (JsonNode source : validSources) {
            String currentDesc = Optional.ofNullable(source.get("description"))
                                        .map(JsonNode::asText)
                                        .orElse(Optional.ofNullable(source.path("volumeInfo").get("description")) // Google Books
                                                        .map(JsonNode::asText)
                                                        .orElse(""));
            if (currentDesc.length() > longestDescription.length()) {
                longestDescription = currentDesc;
            }
        }
        if (!longestDescription.isEmpty()) {
            aggregatedJson.put("description", longestDescription);
        }
        
        // Publisher: Prefer first non-empty publisher
        getStringValueFromSources(validSources, "publisher")
            .or(() -> getStringValueFromSources(validSources, "volumeInfo", "publisher")) // Google Books
            .ifPresent(publisher -> aggregatedJson.put("publisher", publisher));

        // Published Date: Prefer first non-empty, try to parse if needed
        // This needs more robust date parsing and comparison if formats differ widely
        getStringValueFromSources(validSources, "publishedDate")
            .or(() -> getStringValueFromSources(validSources, "volumeInfo", "publishedDate")) // Google Books
            .ifPresent(date -> aggregatedJson.put("publishedDate", date));


        // ISBNs: Collect all unique ISBN-10 and ISBN-13
        Set<String> isbn10s = new HashSet<>();
        Set<String> isbn13s = new HashSet<>();
        for (JsonNode source : validSources) {
            JsonNode identifiersNode = source.get("industryIdentifiers"); // Google Books style
            if (identifiersNode == null && source.has("isbn_10")) { // OpenLibrary style (direct array)
                source.get("isbn_10").forEach(isbnNode -> isbn10s.add(isbnNode.asText()));
            }
            if (identifiersNode == null && source.has("isbn_13")) { // OpenLibrary style (direct array)
                source.get("isbn_13").forEach(isbnNode -> isbn13s.add(isbnNode.asText()));
            }

            if (identifiersNode != null && identifiersNode.isArray()) {
                identifiersNode.forEach(idNode -> {
                    String type = idNode.path("type").asText("");
                    String identifier = idNode.path("identifier").asText("");
                    if (!identifier.isEmpty()) {
                        if ("ISBN_10".equals(type)) isbn10s.add(identifier);
                        if ("ISBN_13".equals(type)) isbn13s.add(identifier);
                    }
                });
            }
        }
        if (!isbn10s.isEmpty()) aggregatedJson.put("isbn10", isbn10s.iterator().next()); // Typically one primary
        if (!isbn13s.isEmpty()) aggregatedJson.put("isbn13", isbn13s.iterator().next()); // Typically one primary
        
        ArrayNode allIsbnsArray = objectMapper.createArrayNode();
        isbn10s.forEach(isbn -> allIsbnsArray.add(objectMapper.createObjectNode().put("type", "ISBN_10").put("identifier", isbn)));
        isbn13s.forEach(isbn -> allIsbnsArray.add(objectMapper.createObjectNode().put("type", "ISBN_13").put("identifier", isbn)));
        if(allIsbnsArray.size() > 0) {
            aggregatedJson.set("industryIdentifiers", allIsbnsArray);
        }


        // Page Count: Prefer first valid (integer > 0)
        getIntValueFromSources(validSources, "pageCount")
            .or(() -> getIntValueFromSources(validSources, "volumeInfo", "pageCount")) // Google Books
            .or(() -> getIntValueFromSources(validSources, "number_of_pages")) // OpenLibrary
            .ifPresent(pc -> aggregatedJson.put("pageCount", pc));
            
        // Categories/Subjects: Union of unique values
        Set<String> allCategories = new HashSet<>();
        for (JsonNode source : validSources) {
            StreamSupport.stream(Optional.ofNullable(source.get("categories")).orElse(objectMapper.createArrayNode()).spliterator(), false) // Google
                .map(JsonNode::asText)
                .filter(s -> s != null && !s.isEmpty())
                .forEach(allCategories::add);
            StreamSupport.stream(Optional.ofNullable(source.get("subjects")).orElse(objectMapper.createArrayNode()).spliterator(), false) // OpenLibrary
                .map(JsonNode::asText)
                .filter(s -> s != null && !s.isEmpty())
                .forEach(allCategories::add);
        }
        if (!allCategories.isEmpty()) {
            ArrayNode categoriesArray = objectMapper.createArrayNode();
            allCategories.forEach(categoriesArray::add);
            aggregatedJson.set("categories", categoriesArray);
        }

        // Add source-specific IDs (e.g., OLID)
        getStringValueFromSources(validSources, "key") // OpenLibrary work/edition ID often in "key"
            .filter(key -> key.startsWith("/works/") || key.startsWith("/books/"))
            .map(key -> key.substring(key.lastIndexOf('/') + 1))
            .ifPresent(olid -> aggregatedJson.put("olid", olid));

        // Store the raw JSON from the first (preferred) source if available
        if (!validSources.isEmpty() && validSources.get(0) != null) {
            aggregatedJson.put("rawJsonResponse", validSources.get(0).toString());
             // Also add a field indicating the primary source of this rawJsonResponse
            aggregatedJson.put("rawJsonSource", determineSourceType(validSources.get(0), sourceIdentifierField));
        }
        
        // Add a list of all source systems that contributed to this aggregated record
        ArrayNode contributingSources = objectMapper.createArrayNode();
        validSources.forEach(source -> contributingSources.add(determineSourceType(source, sourceIdentifierField)));
        aggregatedJson.set("contributingSources", contributingSources);


        logger.info("Aggregated data for primaryId {}. Contributing sources: {}", primaryId, contributingSources.toString());
        return aggregatedJson;
    }

    private Optional<String> getStringValueFromSources(List<JsonNode> sources, String... fieldPath) {
        for (JsonNode source : sources) {
            JsonNode valueNode = getNestedValue(source, fieldPath);
            if (valueNode != null && valueNode.isTextual() && !valueNode.asText().isEmpty()) {
                return Optional.of(valueNode.asText());
            }
        }
        return Optional.empty();
    }
    
    private Optional<Integer> getIntValueFromSources(List<JsonNode> sources, String... fieldPath) {
        for (JsonNode source : sources) {
            JsonNode valueNode = getNestedValue(source, fieldPath);
            if (valueNode != null && valueNode.isInt() && valueNode.asInt() > 0) {
                return Optional.of(valueNode.asInt());
            }
             if (valueNode != null && valueNode.isTextual()) { // Try parsing if it's text
                try {
                    int val = Integer.parseInt(valueNode.asText());
                    if (val > 0) return Optional.of(val);
                } catch (NumberFormatException e) {
                    // Ignore
                }
            }
        }
        return Optional.empty();
    }

    private JsonNode getNestedValue(JsonNode source, String... fieldPath) {
        JsonNode currentNode = source;
        for (String field : fieldPath) {
            if (currentNode == null || !currentNode.has(field)) return null;
            currentNode = currentNode.get(field);
        }
        return currentNode;
    }
    
    /**
     * Check if source is Google Books format.
     * Used to delegate parsing to GoogleBooksMapper (SSOT).
     * 
     * @deprecated Use GoogleBooksMapper.map() and check for null instead, or create GoogleBooksMapper.isGoogleBooksJson().
     *             This method embeds knowledge of Google Books JSON structure (volumeInfo, kind fields).
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private boolean isGoogleBooksSource(JsonNode sourceNode) {
        return sourceNode.has("volumeInfo") || 
               (sourceNode.has("kind") && sourceNode.get("kind").asText("").contains("books#volume"));
    }
    
    private String determineSourceType(JsonNode sourceNode, String idField) {
        if (isGoogleBooksSource(sourceNode)) return "GoogleBooks";
        if (sourceNode.has("key") && sourceNode.get("key").asText("").contains("/works/OL")) return "OpenLibraryWorks";
        if (sourceNode.has("key") && sourceNode.get("key").asText("").contains("/books/OL")) return "OpenLibraryEditions";
        if (sourceNode.has("isbn_13") || sourceNode.has("isbn_10")) return "OpenLibrary"; // General OpenLibrary
        if (sourceNode.has("rank") && sourceNode.has("nyt_buy_links")) return "NewYorkTimes"; // Heuristic for NYT
        // Add more heuristics for Longitood or other sources if specific fields are known
        if (idField != null && sourceNode.has(idField)) return "UnknownSourceWith_" + idField;
        return "UnknownSource";
    }
}
