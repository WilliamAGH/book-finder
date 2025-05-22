/**
 * Mock service for Google Books API to prevent excessive API calls in development and testing environments
 * This service is active when the Spring profiles 'dev' or 'test' are active
 * It intercepts calls that would normally go to the {@link GoogleBooksService} and provides
 * predefined or dynamically saved mock responses
 *
 * Key Features:
 * - Provides canned/mock responses for common book queries (by ID) and search queries
 * - Implements file-based lookup and persistence for mock responses in a configurable directory
 *   (typically {@code src/test/resources/mock-responses})
 * - Loads mock responses from the filesystem during initialization
 * - Can save new responses (retrieved from the actual API via {@link BookApiProxy} or {@link GoogleBooksService}
 *   during a cache miss in dev mode) back to the filesystem to expand the mock dataset
 * - Logs all real API calls (when mock is missed and real API is hit) to help identify frequent queries
 *   that could be added to the static mock dataset
 * - Supports intelligent fallback: if a mock is not found, the call typically proceeds to the real
 *   {@link GoogleBooksService} (orchestrated by {@link BookApiProxy})
 *
 * This service is primarily used by {@link BookApiProxy} when the appropriate profiles are active
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Mono;

/**
 * Service for mocking Google Books API calls to reduce API usage in tests and development
 * - Stores mock responses for popular books to avoid API calls
 * - Logs real API usage to identify candidates for additional mocking
 * - Implements intelligent fallback between local cache and S3 before API calls
 */
@Service
@Profile({"dev", "test"})
public class GoogleBooksMockService {
    private static final Logger logger = LoggerFactory.getLogger(GoogleBooksMockService.class);

    /**
     * Helper method to normalize search queries for caching and internal lookups.
     * @param query The raw search query.
     * @return The normalized query (lowercase, trimmed), or null if the input is null.
     */
    public static String normalizeQuery(String query) {
        return query == null ? null : query.toLowerCase().trim();
    }
    
    private final Map<String, JsonNode> mockBookResponses = new ConcurrentHashMap<>();
    private final Map<String, List<Book>> mockSearchResults = new ConcurrentHashMap<>();
    
    @Value("${app.mock.response.directory:src/test/resources/mock-responses}")
    private String mockResponseDirectory;
    
    @Value("${google.books.api.mock-enabled:true}")
    private boolean mockEnabled;
    
    private final ObjectMapper objectMapper;
    private final ResourceLoader resourceLoader;
    
    public GoogleBooksMockService(ObjectMapper objectMapper, ResourceLoader resourceLoader) {
        this.objectMapper = objectMapper;
        this.resourceLoader = resourceLoader;
    }
    
    /**
     * Initializes the mock service by loading predefined mock responses if mock functionality is enabled
     * This method is called after the bean has been constructed
     */
    @PostConstruct
    public void init() {
        if (!mockEnabled) {
            logger.info("Google Books mock service is disabled");
            return;
        }
        
        loadMockResponses();
        logger.info("GoogleBooksMockService initialized with {} mock book responses and {} mock search results", 
                mockBookResponses.size(), mockSearchResults.size());
    }
    
    /**
     * Loads mock responses from files for common book requests
     */
    private void loadMockResponses() {
        try {
            // Load from classpath resources (for tests)
            loadMockResponsesFromClasspath();
            
            // Load from filesystem (for dev)
            loadMockResponsesFromFilesystem();
            
            // Load search responses
            loadMockSearchResponses();
            
        } catch (Exception e) {
            logger.warn("Error loading mock responses: {}", e.getMessage());
        }
    }
    
    /**
     * Loads mock responses from classpath resources for tests
     */
    private void loadMockResponsesFromClasspath() {
        try {
            // Try loading from classpath mock-responses/books directory
            Resource mockDirResource = resourceLoader.getResource("classpath:mock-responses/books");
            if (mockDirResource.exists()) {
                // This is more complicated as we need to list files in a classpath directory
                // We'd need Spring's PathMatchingResourcePatternResolver to list all files
                logger.info("Found classpath mock responses directory");
            }
        } catch (Exception e) {
            logger.debug("Could not load mock responses from classpath: {}", e.getMessage());
        }
    }
    
    /**
     * Loads mock responses from filesystem for development
     */
    private void loadMockResponsesFromFilesystem() {
        Path booksDir = Paths.get(mockResponseDirectory, "books");
        if (Files.exists(booksDir) && Files.isDirectory(booksDir)) {
            try (var stream = Files.list(booksDir)) {
                stream.filter(path -> path.toString().endsWith(".json"))
                    .forEach(path -> {
                        try {
                            String filename = path.getFileName().toString();
                            String bookId = filename.substring(0, filename.lastIndexOf('.'));
                            JsonNode bookNode = objectMapper.readTree(path.toFile());
                            mockBookResponses.put(bookId, bookNode);
                            logger.debug("Loaded mock response for book ID: {}", bookId);
                        } catch (IOException e) {
                            logger.warn("Could not load mock response from {}: {}", path, e.getMessage());
                        }
                    });
            } catch (IOException e) {
                logger.warn("Could not list files in mock directory: {}", e.getMessage());
            }
        } else {
            // Create directory structure if it doesn't exist
            try {
                Files.createDirectories(booksDir);
                logger.info("Created mock responses directory: {}", booksDir);
            } catch (IOException e) {
                logger.warn("Could not create mock responses directory: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Loads mock search responses for common searches
     */
    private void loadMockSearchResponses() {
        Path searchDir = Paths.get(mockResponseDirectory, "searches");
        if (Files.exists(searchDir) && Files.isDirectory(searchDir)) {
            try (var stream = Files.list(searchDir)) {
                stream.filter(path -> path.toString().endsWith(".json"))
                    .forEach(path -> {
                        try {
                            String filename = path.getFileName().toString();
                            String searchQuery = filename.substring(0, filename.lastIndexOf('.'));
                            JsonNode searchNode = objectMapper.readTree(path.toFile());
                            
                            // Convert to Book objects
                            List<Book> books = new ArrayList<>();
                            if (searchNode.has("items") && searchNode.get("items").isArray()) {
                                searchNode.get("items").forEach(item -> {
                                    try {
                                        Book book = objectMapper.treeToValue(item, Book.class);
                                        books.add(book);
                                    } catch (Exception e) {
                                        logger.warn("Could not convert search result to Book: {}", e.getMessage());
                                    }
                                });
                            }
                            
                            mockSearchResults.put(searchQuery, books);
                            logger.debug("Loaded mock search results for query: {}", searchQuery);
                        } catch (IOException e) {
                            logger.warn("Could not load mock search from {}: {}", path, e.getMessage());
                        }
                    });
            } catch (IOException e) {
                logger.warn("Could not list files in mock searches directory: {}", e.getMessage());
            }
        } else {
            // Create directory if it doesn't exist
            try {
                Files.createDirectories(searchDir);
                logger.info("Created mock searches directory: {}", searchDir);
            } catch (IOException e) {
                logger.warn("Could not create mock searches directory: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Saves a book's JSON response to the mock repository (in-memory and filesystem) for future use
     * This is typically called when a new book is fetched from the live API in a dev environment,
     * allowing the mock dataset to grow
     * 
     * @param bookId The book ID
     * @param bookNode The {@link JsonNode} representing the book's API response to save
     */
    public void saveBookResponse(String bookId, JsonNode bookNode) {
        if (!mockEnabled) return;
        
        mockBookResponses.put(bookId, bookNode);

        // Also save to filesystem for persistence
        String safeBookId = bookId.replaceAll("[^A-Za-z0-9-_]", "_");
        Path bookFile = Paths.get(mockResponseDirectory, "books", safeBookId + ".json");
        try {
            // Ensure directory exists
            Files.createDirectories(bookFile.getParent());
            
            // Write JSON with pretty printing
            objectMapper.writerWithDefaultPrettyPrinter()
                .writeValue(bookFile.toFile(), bookNode);
            
            logger.info("Saved mock response for book ID: {} to {}", bookId, bookFile);
        } catch (IOException e) {
            logger.warn("Could not save mock response for book ID {}: {}", bookId, e.getMessage());
        }
    }
    
    /**
     * Saves search results to the mock repository (in-memory and filesystem) for future use
     * 
     * @param searchQuery The search query string
     * @param books List of {@link Book} objects matching the search to be saved as a mock response
     */
    public void saveSearchResults(String searchQuery, List<Book> books) {
        if (!mockEnabled || searchQuery == null || books == null) return;
        
        // Normalize the query for consistent caching
        String normalizedQuery = normalizeQuery(searchQuery); // Use helper
        
        mockSearchResults.put(normalizedQuery, books);
        
        // Create a JSON representation of the search results
        try {
            ObjectNode searchNode = objectMapper.createObjectNode();
            searchNode.put("kind", "books#volumes");
            searchNode.put("totalItems", books.size());
            
            // Convert Book objects to raw JSON
            searchNode.set("items", objectMapper.valueToTree(books));
            
            // Save to filesystem
            Path searchFile = Paths.get(mockResponseDirectory, "searches", 
                    normalizedQuery.replaceAll("[^a-zA-Z0-9-_]", "_") + ".json");
            
            // Ensure directory exists
            Files.createDirectories(searchFile.getParent());
            
            // Write JSON with pretty printing
            objectMapper.writerWithDefaultPrettyPrinter()
                .writeValue(searchFile.toFile(), searchNode);
            
            logger.info("Saved mock search results for query: {} to {}", normalizedQuery, searchFile);
        } catch (IOException e) {
            logger.warn("Could not save mock search results for query {}: {}", normalizedQuery, e.getMessage());
        }
    }
    
    /**
     * Gets a book by ID from the mock repository (primarily in-memory cache populated from files)
     * Uses Spring's caching abstraction ({@code @Cacheable}) for an additional in-memory layer
     * 
     * @param bookId The book ID to look up
     * @return {@link Book} object if found in mock data, null otherwise
     */
    @Cacheable(value = "mockBooks", key = "#bookId", condition = "#root.target.mockEnabled")
    public Book getBookById(String bookId) {
        if (!mockEnabled) return null;
        
        JsonNode bookNode = mockBookResponses.get(bookId);
        if (bookNode != null) {
            try {
                Book book = objectMapper.treeToValue(bookNode, Book.class);
                logger.debug("Retrieved mock book: {}", bookId);
                return book;
            } catch (Exception e) {
                logger.warn("Could not convert mock book to Book object: {}", e.getMessage());
            }
        }
        
        // Not in mock data
        return null;
    }
    
    /**
     * Gets a book by ID from the mock repository as a CompletionStage
     * 
     * @param bookId The book ID to look up
     * @return CompletionStage containing the Book if found, completed with null otherwise
     */
    public CompletionStage<Book> getBookByIdAsync(String bookId) {
        Book book = getBookById(bookId);
        return CompletableFuture.completedFuture(book);
    }
    
    /**
     * Gets search results from the mock repository for a given query
     * Uses Spring's caching abstraction ({@code @Cacheable}) for an additional in-memory layer
     * 
     * @param searchQuery The search query
     * @return List of {@link Book} objects if found in mock data, an empty list otherwise
     */
    @Cacheable(
        value = "mockSearches", 
        key = "T(com.williamcallahan.book_recommendation_engine.service.GoogleBooksMockService).normalizeQuery(#searchQuery)", 
        condition = "#root.target.mockEnabled"
    )
    public List<Book> searchBooks(String searchQuery) {
        if (!mockEnabled || searchQuery == null) return Collections.emptyList();
        
        // Normalize the query for consistent caching
        String normalizedQuery = normalizeQuery(searchQuery); // Use helper
        
        List<Book> results = mockSearchResults.get(normalizedQuery);
        if (results != null && !results.isEmpty()) {
            logger.debug("Retrieved mock search results for: {}", normalizedQuery);
            return new ArrayList<>(results); // Return a copy to prevent modification
        }
        
        // Not in mock data
        return Collections.emptyList();
    }
    
    /**
     * Gets search results from the mock repository as a Mono
     * 
     * @param searchQuery The search query
     * @return Mono containing a list of Book objects if found, empty list otherwise
     */
    public Mono<List<Book>> searchBooksReactive(String searchQuery) {
        List<Book> books = searchBooks(searchQuery);
        return Mono.just(books);
    }
    
    /**
     * Returns whether the mock service has data for the given book ID
     * 
     * @param bookId The book ID to check
     * @return true if mock data exists, false otherwise
     */
    public boolean hasMockDataForBook(String bookId) {
        return mockEnabled && mockBookResponses.containsKey(bookId);
    }
    
    /**
     * Returns whether the mock service has data for the given search query
     * 
     * @param searchQuery The search query to check
     * @return true if mock data exists, false otherwise
     */
    public boolean hasMockDataForSearch(String searchQuery) {
        if (!mockEnabled || searchQuery == null) return false;
        
        String normalizedQuery = normalizeQuery(searchQuery); // Use helper
        return mockSearchResults.containsKey(normalizedQuery);
    }
}
