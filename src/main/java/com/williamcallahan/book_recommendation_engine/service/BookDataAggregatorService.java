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
import org.springframework.scheduling.annotation.Async;
import java.util.concurrent.CompletableFuture;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
     * Async wrapper for prepareEnrichedBookJson to avoid blocking caller threads.
     */
    @Async
    public CompletableFuture<ObjectNode> prepareEnrichedBookJsonAsync(JsonNode googleBooksJsonNode, JsonNode nytBookJsonNode, String googleBookId) {
        return CompletableFuture.supplyAsync(() -> prepareEnrichedBookJson(googleBooksJsonNode, nytBookJsonNode, googleBookId));
    }

    /**
     * Async wrapper for aggregateBookDataSources to avoid blocking caller threads.
     */
    @Async
    public CompletableFuture<ObjectNode> aggregateBookDataSourcesAsync(String primaryId, String sourceIdentifierField, JsonNode... dataSources) {
        return CompletableFuture.supplyAsync(() -> aggregateBookDataSources(primaryId, sourceIdentifierField, dataSources));
    }

    /**
     * Merges data from Google Books API and New York Times API for a single book
     *
     * @deprecated Use {@link #prepareEnrichedBookJsonAsync(JsonNode, JsonNode, String)} instead.
     * @param googleBooksJsonNode The JsonNode representing the full book data from Google Books API
     * @param nytBookJsonNode The JsonNode representing the book data from the NYT bestseller list
     * @param googleBookId The Google Books ID of the book
     * @return An ObjectNode containing the merged book data
     *
     * @implNote Uses Google Books data as the base, with NYT data supplementing or overriding
     * Preserves source-specific identifiers and handles special NYT fields differently
     * Books with the same ID may have different data from each source
     */
    @Deprecated
    public ObjectNode prepareEnrichedBookJson(JsonNode googleBooksJsonNode, JsonNode nytBookJsonNode, String googleBookId) {
        logger.warn("Deprecated synchronous prepareEnrichedBookJson called; use prepareEnrichedBookJsonAsync instead.");
        ObjectNode mergedBookJson;

        // Start with Google Books data as the base
        if (googleBooksJsonNode != null && googleBooksJsonNode.isObject()) {
            mergedBookJson = (ObjectNode) googleBooksJsonNode.deepCopy();
        } else {
            logger.warn("Google Books data is null or not an object for ID: {}. Starting with an empty JSON object.", googleBookId);
            mergedBookJson = objectMapper.createObjectNode();
        }

        // Ensure google_book_id is present (it might be missing if googleBooksJsonNode was null)
        if (!mergedBookJson.has("id") || mergedBookJson.get("id").asText().isEmpty()) {
             mergedBookJson.put("id", googleBookId); // Google Books API usually has 'id' field
        }
         // We'll use a field named "google_book_id" for consistency with what scheduler expects for S3 key.
        mergedBookJson.put("google_book_id", googleBookId);


        if (nytBookJsonNode != null && nytBookJsonNode.isObject()) {
            logger.debug("Merging NYT data for Google Book ID: {}", googleBookId);
            // Iterate over NYT fields and merge them
            Iterator<Map.Entry<String, JsonNode>> nytFields = nytBookJsonNode.fields();
            while (nytFields.hasNext()) {
                Map.Entry<String, JsonNode> entry = nytFields.next();
                String fieldName = entry.getKey();
                JsonNode nytValue = entry.getValue();

                // Simple merge: NYT data overrides if field exists, or adds if new.
                // More sophisticated logic can be added here (e.g., prefer non-null, combine lists, etc.)
                // For now, let's be careful not to overwrite essential Google Books ID if NYT has a conflicting 'id' field (unlikely for 'id').
                if (fieldName.equals("id") && mergedBookJson.has("id") && !mergedBookJson.get("id").asText("").equals(nytValue.asText(""))) {
                    logger.warn("NYT data has an 'id' field ({}) different from Google's primary ID ({}). Prioritizing Google's 'id'. NYT 'id' ignored for this key.", 
                                nytValue.asText(""), mergedBookJson.get("id").asText(""));
                    continue; // Skip overwriting the primary 'id' field from Google.
                }
                
                // Specific fields from NYT to prioritize or handle specially:
                // Example: 'amazon_product_url', 'rank', 'weeks_on_list', 'buy_links'
                // For 'buy_links', NYT provides an array. Google might have 'saleInfo.buyLink'.
                // We might want to store NYT buy_links under a specific key like 'nyt_buy_links'.

                // For now, a general merge, NYT data takes precedence for shared field names (except 'id').
                if (!"buy_links".equals(fieldName)) {
                    mergedBookJson.set(fieldName, nytValue);
                }
            }
            // Add NYT specific fields if they are not directly named in a conflicting way
            if (nytBookJsonNode.has("rank")) {
                 mergedBookJson.put("nyt_rank", nytBookJsonNode.get("rank").asInt());
            }
            if (nytBookJsonNode.has("weeks_on_list")) {
                mergedBookJson.put("nyt_weeks_on_list", nytBookJsonNode.get("weeks_on_list").asInt());
            }
            if (nytBookJsonNode.has("buy_links")) {
                mergedBookJson.set("nyt_buy_links", nytBookJsonNode.get("buy_links"));
            }
             if (nytBookJsonNode.has("amazon_product_url") && !nytBookJsonNode.get("amazon_product_url").asText("").isEmpty()) {
                mergedBookJson.put("amazon_product_url", nytBookJsonNode.get("amazon_product_url").asText());
            }


        } else {
            logger.debug("NYT data is null or not an object for Google Book ID: {}. No NYT-specific data to merge.", googleBookId);
        }
        
        // Ensure the primary 'id' field is the Google Book ID if it was overwritten or missing
        if (!mergedBookJson.path("id").asText("").equals(googleBookId)) {
            mergedBookJson.put("id", googleBookId);
        }


        return mergedBookJson;
    }

    /**
     * Aggregates book data from multiple JsonNode sources.
     *
     * @deprecated Use {@link #aggregateBookDataSourcesAsync(String, String, JsonNode...)} instead.
     *
     * @param primaryId The primary identifier for the book (e.g., Google Books ID or ISBN).
     * @param sourceIdentifierField The field name in each JsonNode that holds its native primary ID (e.g., "id" for Google, "key" for OpenLibrary works).
     * @param dataSources Varargs of JsonNode, each representing book data from a different source.
     *                    It's recommended to pass sources in order of preference (e.g., Google, OpenLibrary, Longitood).
     * @return An ObjectNode containing the merged book data.
     */
    @Deprecated
    public ObjectNode aggregateBookDataSources(String primaryId, String sourceIdentifierField, JsonNode... dataSources) {
        logger.warn("Deprecated synchronous aggregateBookDataSources called; use aggregateBookDataSourcesAsync instead.");
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
    
    private String determineSourceType(JsonNode sourceNode, String idField) {
        if (sourceNode.has("volumeInfo")) return "GoogleBooks"; // Characteristic of Google Books API
        if (sourceNode.has("key") && sourceNode.get("key").asText("").contains("/works/OL")) return "OpenLibraryWorks";
        if (sourceNode.has("key") && sourceNode.get("key").asText("").contains("/books/OL")) return "OpenLibraryEditions";
        if (sourceNode.has("isbn_13") || sourceNode.has("isbn_10")) return "OpenLibrary"; // General OpenLibrary
        if (sourceNode.has("rank") && sourceNode.has("nyt_buy_links")) return "NewYorkTimes"; // Heuristic for NYT
        // Add more heuristics for Longitood or other sources if specific fields are known
        if (idField != null && sourceNode.has(idField)) return "UnknownSourceWith_" + idField;
        return "UnknownSource";
    }
}
