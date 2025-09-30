package com.williamcallahan.book_recommendation_engine.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.util.DateParsingUtils;
import com.williamcallahan.book_recommendation_engine.util.ImageUrlEnhancer;
import com.williamcallahan.book_recommendation_engine.util.IsbnUtils;
import com.williamcallahan.book_recommendation_engine.util.SlugGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps Google Books Volume JSON to normalized BookAggregate.
 * <p>
 * Reference: <a href="https://developers.google.com/books/docs/v1/reference/volumes">Google Books Volume Resource</a>
 * <p>
 * Handles:
 * - Missing/null volumeInfo or fields
 * - Multiple ISBN identifiers
 * - Date parsing in various formats (year, year-month, full date)
 * - Image links of different sizes
 * - Sale info and accessibility metadata
 */
@Component
@Slf4j
public class GoogleBooksMapper implements ExternalBookMapper {
    
    private static final String SOURCE_NAME = "GOOGLE_BOOKS";
    
    @Override
    public BookAggregate map(JsonNode json) {
        if (json == null || !json.has("volumeInfo")) {
            log.warn("Invalid Google Books JSON: missing volumeInfo");
            return null;
        }
        
        JsonNode volumeInfo = json.get("volumeInfo");
        JsonNode saleInfo = json.has("saleInfo") ? json.get("saleInfo") : null;
        JsonNode accessInfo = json.has("accessInfo") ? json.get("accessInfo") : null;
        
        // Extract primary ID
        String externalId = json.has("id") ? json.get("id").asText() : null;
        if (externalId == null) {
            log.warn("Google Books volume missing 'id' field");
            return null;
        }
        
        // Extract title (required)
        String title = getTextValue(volumeInfo, "title");
        if (title == null || title.isBlank()) {
            log.warn("Google Books volume {} missing title", externalId);
            return null;
        }
        
        // Extract ISBNs
        String isbn13 = extractIsbn(volumeInfo, "ISBN_13");
        String isbn10 = extractIsbn(volumeInfo, "ISBN_10");
        
        // Extract authors
        List<String> authors = extractAuthors(volumeInfo);
        
        // Extract categories
        List<String> categories = extractCategories(volumeInfo);
        
        // Build external identifiers
        BookAggregate.ExternalIdentifiers identifiers = buildExternalIdentifiers(
            externalId, 
            volumeInfo, 
            saleInfo, 
            accessInfo, 
            isbn10, 
            isbn13
        );
        
        // Parse published date
        LocalDate publishedDate = parsePublishedDate(volumeInfo);
        
        // Generate slug base
        String slugBase = SlugGenerator.generateBookSlug(title, authors);
        
        // Extract dimensions
        BookAggregate.Dimensions dimensions = extractDimensions(volumeInfo);
        
        return BookAggregate.builder()
            .title(title)
            .subtitle(getTextValue(volumeInfo, "subtitle"))
            .description(getTextValue(volumeInfo, "description"))
            .isbn13(isbn13)
            .isbn10(isbn10)
            .publishedDate(publishedDate)
            .language(getTextValue(volumeInfo, "language"))
            .publisher(getTextValue(volumeInfo, "publisher"))
            .pageCount(getIntValue(volumeInfo, "pageCount"))
            .authors(authors)
            .categories(categories)
            .identifiers(identifiers)
            .slugBase(slugBase)
            .dimensions(dimensions)
            .editionNumber(null)  // TODO: Derive from title/subtitle/contentVersion
            .editionGroupKey(null)  // TODO: Build from normalized title + first author
            .build();
    }
    
    @Override
    public String getSourceName() {
        return SOURCE_NAME;
    }
    
    /**
     * Extract ISBN of specific type from industryIdentifiers array.
     */
    private String extractIsbn(JsonNode volumeInfo, String type) {
        if (!volumeInfo.has("industryIdentifiers")) {
            return null;
        }
        
        JsonNode identifiers = volumeInfo.get("industryIdentifiers");
        if (!identifiers.isArray()) {
            return null;
        }
        
        for (JsonNode id : identifiers) {
            if (id.has("type") && type.equals(id.get("type").asText())) {
                String identifier = id.get("identifier").asText();
                // Sanitize and validate ISBN
                String sanitized = IsbnUtils.sanitize(identifier);
                if (sanitized != null) {
                    return sanitized;
                }
            }
        }
        
        return null;
    }
    
    /**
     * Extract authors from volumeInfo.authors array.
     */
    private List<String> extractAuthors(JsonNode volumeInfo) {
        List<String> authors = new ArrayList<>();
        
        if (!volumeInfo.has("authors") || !volumeInfo.get("authors").isArray()) {
            return authors;
        }
        
        JsonNode authorsNode = volumeInfo.get("authors");
        for (JsonNode authorNode : authorsNode) {
            String author = authorNode.asText();
            if (author != null && !author.isBlank()) {
                authors.add(author.trim());
            }
        }
        
        return authors;
    }
    
    /**
     * Extract categories from volumeInfo.categories array.
     */
    private List<String> extractCategories(JsonNode volumeInfo) {
        List<String> categories = new ArrayList<>();
        
        if (!volumeInfo.has("categories") || !volumeInfo.get("categories").isArray()) {
            return categories;
        }
        
        JsonNode categoriesNode = volumeInfo.get("categories");
        for (JsonNode categoryNode : categoriesNode) {
            String category = categoryNode.asText();
            if (category != null && !category.isBlank()) {
                categories.add(category.trim());
            }
        }
        
        return categories;
    }
    
    /**
     * Parse published date from various Google Books formats.
     * Handles: "2023", "2023-05", "2023-05-15"
     */
    private LocalDate parsePublishedDate(JsonNode volumeInfo) {
        if (!volumeInfo.has("publishedDate")) {
            return null;
        }
        
        String dateStr = volumeInfo.get("publishedDate").asText();
        if (dateStr == null || dateStr.isBlank()) {
            return null;
        }
        
        try {
            // Try ISO date format first
            LocalDate date = DateParsingUtils.parseIsoLocalDate(dateStr);
            if (date != null) {
                return date;
            }
            
            // Handle year-only format
            if (dateStr.matches("\\d{4}")) {
                return LocalDate.of(Integer.parseInt(dateStr), 1, 1);
            }
            
            // Handle year-month format (e.g., "2023-05")
            if (dateStr.matches("\\d{4}-\\d{2}")) {
                String[] parts = dateStr.split("-");
                return LocalDate.of(
                    Integer.parseInt(parts[0]), 
                    Integer.parseInt(parts[1]), 
                    1
                );
            }
            
            log.debug("Unable to parse publishedDate: {}", dateStr);
            return null;
        } catch (Exception e) {
            log.debug("Error parsing publishedDate '{}': {}", dateStr, e.getMessage());
            return null;
        }
    }
    
    /**
     * Build external identifiers from all available Google Books metadata.
     */
    private BookAggregate.ExternalIdentifiers buildExternalIdentifiers(
        String externalId,
        JsonNode volumeInfo,
        JsonNode saleInfo,
        JsonNode accessInfo,
        String providerIsbn10,
        String providerIsbn13
    ) {
        Map<String, String> imageLinks = extractImageLinks(volumeInfo);
        
        return BookAggregate.ExternalIdentifiers.builder()
            // Primary identifiers
            .source(SOURCE_NAME)
            .externalId(externalId)
            .providerIsbn10(providerIsbn10)
            .providerIsbn13(providerIsbn13)
            
            // Links
            .infoLink(getTextValue(volumeInfo, "infoLink"))
            .previewLink(getTextValue(volumeInfo, "previewLink"))
            .canonicalVolumeLink(getTextValue(volumeInfo, "canonicalVolumeLink"))
            .webReaderLink(accessInfo != null ? getTextValue(accessInfo, "webReaderLink") : null)
            
            // Ratings
            .averageRating(getDoubleValue(volumeInfo, "averageRating"))
            .ratingsCount(getIntValue(volumeInfo, "ratingsCount"))
            
            // Availability (from accessInfo)
            .embeddable(accessInfo != null ? getBooleanValue(accessInfo, "embeddable") : null)
            .publicDomain(accessInfo != null ? getBooleanValue(accessInfo, "publicDomain") : null)
            .viewability(accessInfo != null ? getTextValue(accessInfo, "viewability") : null)
            .textReadable(extractReadingMode(volumeInfo, "text"))
            .imageReadable(extractReadingMode(volumeInfo, "image"))
            .pdfAvailable(extractFormatAvailability(accessInfo, "pdf"))
            .epubAvailable(extractFormatAvailability(accessInfo, "epub"))
            .textToSpeechPermission(accessInfo != null ? getTextValue(accessInfo, "textToSpeechPermission") : null)
            
            // Content metadata
            .printType(getTextValue(volumeInfo, "printType"))
            .maturityRating(getTextValue(volumeInfo, "maturityRating"))
            .contentVersion(getTextValue(volumeInfo, "contentVersion"))
            
            // Sale info
            .isEbook(saleInfo != null ? getBooleanValue(saleInfo, "isEbook") : null)
            .saleability(saleInfo != null ? getTextValue(saleInfo, "saleability") : null)
            .countryCode(saleInfo != null ? getTextValue(saleInfo, "country") : null)
            .listPrice(extractPrice(saleInfo, "listPrice"))
            .retailPrice(extractPrice(saleInfo, "retailPrice"))
            .currencyCode(extractCurrency(saleInfo, "listPrice"))
            
            // Work identifiers
            .googleCanonicalId(extractGoogleCanonicalId(volumeInfo))
            
            // Image URLs
            .imageLinks(imageLinks)
            .build();
    }
    
    /**
     * Extract Google canonical ID from canonicalVolumeLink.
     */
    private String extractGoogleCanonicalId(JsonNode volumeInfo) {
        String link = getTextValue(volumeInfo, "canonicalVolumeLink");
        if (link == null) {
            return null;
        }
        
        // Extract ID from URL like "https://books.google.com/books?id=ABC123"
        int idIndex = link.indexOf("id=");
        if (idIndex != -1) {
            String idPart = link.substring(idIndex + 3);
            int ampIndex = idPart.indexOf('&');
            return ampIndex != -1 ? idPart.substring(0, ampIndex) : idPart;
        }
        
        return null;
    }
    
    /**
     * Extract dimensions from volumeInfo.dimensions per official Google Books API schema.
     * Per https://developers.google.com/books/docs/v1/reference/volumes:
     * dimensions.height: "Height or length of this volume (in cm)."
     * dimensions.width: "Width of this volume (in cm)."
     * dimensions.thickness: "Thickness of this volume (in cm)."
     */
    private BookAggregate.Dimensions extractDimensions(JsonNode volumeInfo) {
        if (!volumeInfo.has("dimensions")) {
            return null;
        }
        
        JsonNode dimensionsNode = volumeInfo.get("dimensions");
        if (!dimensionsNode.isObject()) {
            return null;
        }
        
        String height = getTextValue(dimensionsNode, "height");
        String width = getTextValue(dimensionsNode, "width");
        String thickness = getTextValue(dimensionsNode, "thickness");
        
        // Only create Dimensions if at least one field is present
        if (height == null && width == null && thickness == null) {
            return null;
        }
        
        return BookAggregate.Dimensions.builder()
            .height(height)
            .width(width)
            .thickness(thickness)
            .build();
    }
    
    /**
     * Extract image links map from volumeInfo.imageLinks.
     * Enhances URLs for better quality as per BookJsonParser legacy logic.
     * Per official API: thumbnail (~128px), small (~300px), medium (~575px), large (~800px), extraLarge (~1280px)
     */
    private Map<String, String> extractImageLinks(JsonNode volumeInfo) {
        Map<String, String> imageLinks = new HashMap<>();
        
        if (!volumeInfo.has("imageLinks")) {
            return imageLinks;
        }
        
        JsonNode imageLinksNode = volumeInfo.get("imageLinks");
        if (!imageLinksNode.isObject()) {
            return imageLinks;
        }
        
        // Extract all image sizes per official Google Books API schema
        String[] sizeKeys = {"smallThumbnail", "thumbnail", "small", "medium", "large", "extraLarge"};
        for (String key : sizeKeys) {
            if (imageLinksNode.has(key)) {
                String url = imageLinksNode.get(key).asText();
                if (url != null && !url.isBlank()) {
                    // Enhance URL for better quality (HTTP->HTTPS, optimize zoom parameter)
                    String enhancedUrl = enhanceGoogleImageUrl(url, key);
                    imageLinks.put(key, enhancedUrl);
                }
            }
        }
        
        return imageLinks;
    }
    
    /**
     * Enhance Google Books cover URL for better quality.
     * Delegates to ImageUrlEnhancer utility (DRY principle).
     * 
     * @param url Original URL from Google Books API
     * @param sizeKey Image size key (thumbnail, small, medium, large, extraLarge)
     * @return Enhanced URL with HTTPS and optimized zoom parameter
     */
    private String enhanceGoogleImageUrl(String url, String sizeKey) {
        // Use shared utility instead of duplicate implementation
        return ImageUrlEnhancer.enhanceGoogleImageUrl(url, sizeKey);
    }
    
    /**
     * Extract reading mode availability.
     */
    private Boolean extractReadingMode(JsonNode volumeInfo, String mode) {
        if (!volumeInfo.has("readingModes")) {
            return null;
        }
        
        JsonNode readingModes = volumeInfo.get("readingModes");
        return readingModes.has(mode) ? readingModes.get(mode).asBoolean() : null;
    }
    
    /**
     * Extract format availability (PDF/EPUB).
     */
    private Boolean extractFormatAvailability(JsonNode accessInfo, String format) {
        if (accessInfo == null || !accessInfo.has(format)) {
            return null;
        }
        
        JsonNode formatNode = accessInfo.get(format);
        return formatNode.has("isAvailable") ? formatNode.get("isAvailable").asBoolean() : null;
    }
    
    /**
     * Extract price amount from saleInfo.
     */
    private Double extractPrice(JsonNode saleInfo, String priceField) {
        if (saleInfo == null || !saleInfo.has(priceField)) {
            return null;
        }
        
        JsonNode priceNode = saleInfo.get(priceField);
        return priceNode.has("amount") ? priceNode.get("amount").asDouble() : null;
    }
    
    /**
     * Extract currency code from saleInfo.
     */
    private String extractCurrency(JsonNode saleInfo, String priceField) {
        if (saleInfo == null || !saleInfo.has(priceField)) {
            return null;
        }
        
        JsonNode priceNode = saleInfo.get(priceField);
        return priceNode.has("currencyCode") ? priceNode.get("currencyCode").asText() : null;
    }
    
    // Helper methods
    
    /**
     * Extract and sanitize text value from JSON node.
     * Removes leading/trailing quotes that may be present in the JSON data.
     * 
     * @param node Parent JSON node
     * @param field Field name to extract
     * @return Sanitized text value or null if field doesn't exist
     */
    private String getTextValue(JsonNode node, String field) {
        if (node == null || !node.has(field)) {
            return null;
        }
        JsonNode fieldNode = node.get(field);
        if (!fieldNode.isTextual()) {
            return null;
        }
        
        String value = fieldNode.asText();
        if (value == null || value.isEmpty()) {
            return value;
        }
        
        // Remove leading and trailing quotes (handles "value" -> value)
        // This protects against malformed JSON where text fields contain literal quotes
        return value.replaceAll("^\"|\"$", "");
    }
    
    private Integer getIntValue(JsonNode node, String field) {
        if (node == null || !node.has(field)) {
            return null;
        }
        JsonNode fieldNode = node.get(field);
        return fieldNode.isInt() ? fieldNode.asInt() : null;
    }
    
    private Double getDoubleValue(JsonNode node, String field) {
        if (node == null || !node.has(field)) {
            return null;
        }
        JsonNode fieldNode = node.get(field);
        return fieldNode.isNumber() ? fieldNode.asDouble() : null;
    }
    
    private Boolean getBooleanValue(JsonNode node, String field) {
        if (node == null || !node.has(field)) {
            return null;
        }
        JsonNode fieldNode = node.get(field);
        return fieldNode.isBoolean() ? fieldNode.asBoolean() : null;
    }
}
