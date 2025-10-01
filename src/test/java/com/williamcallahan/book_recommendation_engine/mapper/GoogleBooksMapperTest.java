package com.williamcallahan.book_recommendation_engine.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for GoogleBooksMapper using real Google Books API responses.
 * Verifies that the mapper correctly parses all fields per the official API schema.
 */
class GoogleBooksMapperTest {
    
    private GoogleBooksMapper mapper;
    private ObjectMapper objectMapper;
    
    @BeforeEach
    void setUp() {
        mapper = new GoogleBooksMapper();
        objectMapper = new ObjectMapper();
    }
    
    @Test
    void shouldMapRealGoogleBooksVolume() throws IOException {
        // Load real Google Books API response
        JsonNode json = loadFixture("google-books-sample.json");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate, "Aggregate should not be null");
        assertEquals("The Google Story (2018 Updated Edition)", aggregate.getTitle());
        assertEquals("Inside the Hottest Business, Media, and Technology Success of Our Time", aggregate.getSubtitle());
        assertNotNull(aggregate.getDescription());
        assertTrue(aggregate.getDescription().contains("Google"));
    }
    
    @Test
    void shouldExtractISBNs() throws IOException {
        JsonNode json = loadFixture("google-books-sample.json");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertEquals("9780440335702", aggregate.getIsbn13());
        assertEquals("0440335701", aggregate.getIsbn10());
    }
    
    @Test
    void shouldExtractAuthors() throws IOException {
        JsonNode json = loadFixture("google-books-sample.json");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertNotNull(aggregate.getAuthors());
        assertEquals(2, aggregate.getAuthors().size());
        assertTrue(aggregate.getAuthors().contains("David A. Vise"));
        assertTrue(aggregate.getAuthors().contains("Mark Malseed"));
    }
    
    @Test
    void shouldExtractCategories() throws IOException {
        JsonNode json = loadFixture("google-books-sample.json");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertNotNull(aggregate.getCategories());
        assertTrue(aggregate.getCategories().size() >= 3);
        assertTrue(aggregate.getCategories().stream()
            .anyMatch(cat -> cat.contains("Business")));
    }
    
    @Test
    void shouldExtractExternalIdentifiers() throws IOException {
        JsonNode json = loadFixture("google-books-sample.json");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertNotNull(aggregate.getIdentifiers());
        
        BookAggregate.ExternalIdentifiers ids = aggregate.getIdentifiers();
        assertEquals("GOOGLE_BOOKS", ids.getSource());
        assertEquals("zyTCAlFPjgYC", ids.getExternalId());
        assertNotNull(ids.getInfoLink());
        assertNotNull(ids.getPreviewLink());
        assertEquals(4.0, ids.getAverageRating());
        assertEquals(4, ids.getRatingsCount());
    }
    
    @Test
    void shouldExtractImageLinks() throws IOException {
        JsonNode json = loadFixture("google-books-sample.json");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertNotNull(aggregate.getIdentifiers());
        assertNotNull(aggregate.getIdentifiers().getImageLinks());
        
        var imageLinks = aggregate.getIdentifiers().getImageLinks();
        assertTrue(imageLinks.size() >= 5, "Should have multiple image sizes");
        assertTrue(imageLinks.containsKey("smallThumbnail"));
        assertTrue(imageLinks.containsKey("thumbnail"));
        assertTrue(imageLinks.containsKey("small"));
        assertTrue(imageLinks.containsKey("medium"));
        assertTrue(imageLinks.containsKey("large"));
        
        // Verify URLs are enhanced (HTTPS)
        imageLinks.values().forEach(url -> 
            assertTrue(url.startsWith("https://"), "All image URLs should use HTTPS: " + url)
        );
    }
    
    @Test
    void shouldExtractDimensions() throws IOException {
        JsonNode json = loadFixture("google-books-sample.json");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertNotNull(aggregate.getDimensions());
        assertEquals("24.00 cm", aggregate.getDimensions().getHeight());
        // Width and thickness may be null in this particular volume
    }
    
    @Test
    void shouldExtractPublishedDate() throws IOException {
        JsonNode json = loadFixture("google-books-sample.json");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertNotNull(aggregate.getPublishedDate());
        assertEquals(2005, aggregate.getPublishedDate().getYear());
        assertEquals(11, aggregate.getPublishedDate().getMonthValue());
        assertEquals(15, aggregate.getPublishedDate().getDayOfMonth());
    }
    
    @Test
    void shouldExtractPageCount() throws IOException {
        JsonNode json = loadFixture("google-books-sample.json");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertEquals(384, aggregate.getPageCount());
    }
    
    @Test
    void shouldExtractPublisher() throws IOException {
        JsonNode json = loadFixture("google-books-sample.json");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertEquals("Random House Publishing Group", aggregate.getPublisher());
    }
    
    @Test
    void shouldExtractLanguage() throws IOException {
        JsonNode json = loadFixture("google-books-sample.json");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertEquals("en", aggregate.getLanguage());
    }
    
    @Test
    void shouldGenerateSlugBase() throws IOException {
        JsonNode json = loadFixture("google-books-sample.json");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertNotNull(aggregate.getSlugBase());
        // The slug base should contain the title and at least one author
        // Google Books mapper generates slug from title-author pattern
        assertTrue(aggregate.getSlugBase().contains("google-story"), 
            "Slug should contain 'google-story', but was: " + aggregate.getSlugBase());
        assertTrue(aggregate.getSlugBase().contains("david") || aggregate.getSlugBase().contains("vise"), 
            "Slug should contain author name, but was: " + aggregate.getSlugBase());
    }
    
    @Test
    void shouldReturnNullForMissingVolumeInfo() {
        JsonNode json = objectMapper.createObjectNode();
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNull(aggregate, "Should return null for invalid JSON");
    }
    
    @Test
    void shouldReturnNullForNullInput() {
        BookAggregate aggregate = mapper.map(null);
        
        assertNull(aggregate, "Should return null for null input");
    }
    
    @Test
    void shouldReturnNullForMissingTitle() throws IOException {
        JsonNode json = objectMapper.createObjectNode();
        ((com.fasterxml.jackson.databind.node.ObjectNode) json).set("volumeInfo", objectMapper.createObjectNode());
        ((com.fasterxml.jackson.databind.node.ObjectNode) json).put("id", "test123");
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNull(aggregate, "Should return null when title is missing");
    }
    
    @Test
    void shouldGetSourceName() {
        assertEquals("GOOGLE_BOOKS", mapper.getSourceName());
    }
    
    @Test
    void shouldSanitizePublisherWithQuotes() {
        // Test that publisher field with surrounding quotes is cleaned
        com.fasterxml.jackson.databind.node.ObjectNode json = objectMapper.createObjectNode();
        json.put("id", "test123");
        
        com.fasterxml.jackson.databind.node.ObjectNode volumeInfo = objectMapper.createObjectNode();
        volumeInfo.put("title", "Test Book");
        volumeInfo.put("publisher", "\"O'Reilly Media, Inc.\"");  // Publisher with quotes
        
        json.set("volumeInfo", volumeInfo);
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertEquals("O'Reilly Media, Inc.", aggregate.getPublisher(), 
            "Publisher should have surrounding quotes removed");
    }
    
    @Test
    void shouldSanitizeTitleWithQuotes() {
        // Test that title field with surrounding quotes is cleaned
        com.fasterxml.jackson.databind.node.ObjectNode json = objectMapper.createObjectNode();
        json.put("id", "test123");
        
        com.fasterxml.jackson.databind.node.ObjectNode volumeInfo = objectMapper.createObjectNode();
        volumeInfo.put("title", "\"Think Java\"");  // Title with quotes
        
        json.set("volumeInfo", volumeInfo);
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertEquals("Think Java", aggregate.getTitle(), 
            "Title should have surrounding quotes removed");
    }
    
    @Test
    void shouldHandleNormalPublisherWithoutQuotes() {
        // Ensure normal publishers without quotes remain unchanged
        com.fasterxml.jackson.databind.node.ObjectNode json = objectMapper.createObjectNode();
        json.put("id", "test123");
        
        com.fasterxml.jackson.databind.node.ObjectNode volumeInfo = objectMapper.createObjectNode();
        volumeInfo.put("title", "Test Book");
        volumeInfo.put("publisher", "Random House Publishing Group");
        
        json.set("volumeInfo", volumeInfo);
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertEquals("Random House Publishing Group", aggregate.getPublisher(), 
            "Publisher without quotes should remain unchanged");
    }
    
    @Test
    void shouldHandlePublisherWithInternalQuotes() {
        // Test publisher with quotes in the middle (should only remove leading/trailing)
        com.fasterxml.jackson.databind.node.ObjectNode json = objectMapper.createObjectNode();
        json.put("id", "test123");
        
        com.fasterxml.jackson.databind.node.ObjectNode volumeInfo = objectMapper.createObjectNode();
        volumeInfo.put("title", "Test Book");
        volumeInfo.put("publisher", "\"Simon \"The Great\" & Schuster\"");  
        
        json.set("volumeInfo", volumeInfo);
        
        BookAggregate aggregate = mapper.map(json);
        
        assertNotNull(aggregate);
        assertEquals("Simon \"The Great\" & Schuster", aggregate.getPublisher(), 
            "Only leading and trailing quotes should be removed, internal quotes preserved");
    }
    
    private JsonNode loadFixture(String filename) throws IOException {
        String path = "src/test/resources/fixtures/" + filename;
        String content = Files.readString(Paths.get(path));
        return objectMapper.readTree(content);
    }
}
