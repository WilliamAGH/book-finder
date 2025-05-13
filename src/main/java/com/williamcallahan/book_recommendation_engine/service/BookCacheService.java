package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class BookCacheService {
    private static final Logger logger = LoggerFactory.getLogger(BookCacheService.class);
    
    private final GoogleBooksService googleBooksService;
    private final CachedBookRepository cachedBookRepository;
    private final ObjectMapper objectMapper;
    private final WebClient embeddingClient;
    
    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled;
    
    @Value("${app.embedding.service.url:#{null}}")
    private String embeddingServiceUrl;
    
    @Autowired
    public BookCacheService(
            GoogleBooksService googleBooksService,
            ObjectMapper objectMapper,
            WebClient.Builder webClientBuilder,
            @Autowired(required = false) CachedBookRepository cachedBookRepository) {
        this.googleBooksService = googleBooksService;
        this.cachedBookRepository = cachedBookRepository;
        this.objectMapper = objectMapper;
        this.embeddingClient = webClientBuilder.baseUrl(
                embeddingServiceUrl != null ? embeddingServiceUrl : "http://localhost:8080/api/embedding"
        ).build();
        
        // Disable cache if repository is not available
        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false;
            logger.info("Database cache is not available. Running in API-only mode.");
        }
    }
    
    /**
     * Get a book by ID with cache-first approach
     * @param id The book ID
     * @return The book if found
     */
    @Cacheable(value = "books", key = "#id", unless = "#result == null")
    public Book getBookById(String id) {
        // Try to get from cache first
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> cachedBook = cachedBookRepository.findByGoogleBooksId(id);
                if (cachedBook.isPresent()) {
                    logger.info("Cache hit for book ID: {}", id);
                    return cachedBook.get().toBook();
                }
            } catch (Exception e) {
                logger.warn("Error accessing database cache for book ID {}: {}", id, e.getMessage());
            }
        }
        
        logger.info("Cache miss for book ID: {}, fetching from Google Books API", id);
        Book book = googleBooksService.getBookById(id).blockOptional().orElse(null);
        
        if (book != null && cacheEnabled && cachedBookRepository != null) {
            CompletableFuture.runAsync(() -> cacheBook(book));
        }
        
        return book;
    }
    
    /**
     * Get a book by ISBN with cache-first approach
     * @param isbn The book ISBN (can be ISBN-10 or ISBN-13)
     * @return List of books matching the ISBN
     */
    public List<Book> getBooksByIsbn(String isbn) {
        // Try to get from cache first
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> cachedBook;
                if (isbn.length() == 10) {
                    cachedBook = cachedBookRepository.findByIsbn10(isbn);
                } else if (isbn.length() == 13) {
                    cachedBook = cachedBookRepository.findByIsbn13(isbn);
                } else {
                    cachedBook = Optional.empty();
                }
                
                if (cachedBook.isPresent()) {
                    logger.info("Cache hit for book ISBN: {}", isbn);
                    return Collections.singletonList(cachedBook.get().toBook());
                }
            } catch (Exception e) {
                logger.warn("Error accessing database cache for ISBN {}: {}", isbn, e.getMessage());
                // Continue to fallback instead of letting the exception bubble up
            }
        }
        
        // Fall back to Google Books API
        logger.info("Cache miss for book ISBN: {}, fetching from Google Books API", isbn);
        List<Book> books = googleBooksService.searchBooksByISBN(isbn).blockOptional().orElse(Collections.emptyList());
        
        // If found, save to cache asynchronously
        if (!books.isEmpty() && cacheEnabled && cachedBookRepository != null) {
            CompletableFuture.runAsync(() -> {
                for (Book book : books) {
                    cacheBook(book);
                }
            });
        }
        
        return books;
    }
    
    /**
     * Search books with cache augmentation
     * @param query The search query
     * @param startIndex The index to start from
     * @param maxResults The maximum number of results to return
     * @return List of books matching the query
     */
    public List<Book> searchBooks(String query, int startIndex, int maxResults) {
        logger.info("BookCacheService searching books with query: {}, requested startIndex: {}, maxResults: {}", query, startIndex, maxResults);
        List<Book> fetchedBooks = googleBooksService.searchBooksAsyncReactive(query).blockOptional().orElse(Collections.emptyList());
        
        final List<Book> booksToCacheAndReturn;
        if (fetchedBooks.size() > maxResults && maxResults > 0) {
            booksToCacheAndReturn = fetchedBooks.subList(0, maxResults);
        } else {
            booksToCacheAndReturn = fetchedBooks;
        }

        // Cache all results asynchronously
        if (!booksToCacheAndReturn.isEmpty() && cacheEnabled && cachedBookRepository != null) {
            CompletableFuture.runAsync(() -> {
                for (Book book : booksToCacheAndReturn) {
                    cacheBook(book);
                }
            });
        }
        
        return booksToCacheAndReturn;
    }
    
    /**
     * Get similar books based on semantic similarity
     * @param bookId The source book ID
     * @param count The number of similar books to return
     * @return List of similar books
     */
    public List<Book> getSimilarBooks(String bookId, int count) {
        // First try using vector similarity if the book is in the cache and database is available
        if (cacheEnabled && cachedBookRepository != null) {
            try {
                Optional<CachedBook> sourceCachedBookOpt = cachedBookRepository.findByGoogleBooksId(bookId);
                if (sourceCachedBookOpt.isPresent()) {
                    // Use vector similarity for recommendations
                    List<CachedBook> similarCachedBooks = cachedBookRepository.findSimilarBooksById(sourceCachedBookOpt.get().getId(), count);
                    if (!similarCachedBooks.isEmpty()) {
                        logger.info("Found {} similar books using vector similarity for book ID: {}", 
                                similarCachedBooks.size(), bookId);
                        return similarCachedBooks.stream()
                                .map(CachedBook::toBook)
                                .collect(Collectors.toList());
                    }
                }
            } catch (Exception e) {
                logger.warn("Error retrieving similar books from database: {}", e.getMessage());
            }
        }
        
        logger.info("No vector similarity data for book ID: {}, using GoogleBooksService category/author matching", bookId);
        // Need the Book object to call the new getSimilarBooks method
        Book sourceBook = googleBooksService.getBookById(bookId).blockOptional().orElse(null);
        if (sourceBook == null) {
            logger.warn("Source book for similar search not found (ID: {}), returning empty list.", bookId);
            return Collections.emptyList();
        }
        // Now call the corrected getSimilarBooks method
        return googleBooksService.getSimilarBooks(sourceBook).blockOptional().orElse(Collections.emptyList());
    }
    
    /**
     * Cache a book for future use
     * @param book The book to cache
     */
    private void cacheBook(Book book) {
        // If database caching is disabled or repository is not available, do nothing
        if (!cacheEnabled || cachedBookRepository == null) {
            return;
        }
        
        try {
            if (book == null || book.getId() == null) {
                return;
            }
            
            // Check if already cached
            if (cachedBookRepository.findByGoogleBooksId(book.getId()).isPresent()) {
                logger.debug("Book already cached: {}", book.getId());
                return;
            }
            
            // Convert to JSON for raw data storage
            JsonNode rawData = objectMapper.valueToTree(book);
            
            // Generate embeddings for the book (title + description + categories)
            float[] embedding = generateEmbedding(book);
            
            // Create cached book entity
            CachedBook cachedBook = CachedBook.fromBook(book, rawData, embedding);
            
            // Save to database
            cachedBookRepository.save(cachedBook);
            logger.info("Successfully cached book: {}", book.getId());
            
        } catch (Exception e) {
            // Log but don't propagate - this ensures API operations continue to work
            logger.error("Error caching book {}: {}", book.getId(), e.getMessage(), e);
        }
    }
    
    /**
     * Generate embeddings for a book
     * @param book The book to generate embeddings for
     * @return A float array representing the embedding
     */
    private float[] generateEmbedding(Book book) {
        try {
            // Create a text representation of the book for embedding generation
            StringBuilder textBuilder = new StringBuilder();
            textBuilder.append(book.getTitle()).append(" ");
            
            if (book.getAuthors() != null && !book.getAuthors().isEmpty()) {
                textBuilder.append(String.join(" ", book.getAuthors())).append(" ");
            }
            
            if (book.getDescription() != null) {
                textBuilder.append(book.getDescription()).append(" ");
            }
            
            if (book.getCategories() != null && !book.getCategories().isEmpty()) {
                textBuilder.append(String.join(" ", book.getCategories()));
            }
            
            String text = textBuilder.toString();
            
            // Call embedding service if available
            if (embeddingServiceUrl != null) {
                try {
                    return embeddingClient.post()
                            .bodyValue(Collections.singletonMap("text", text))
                            .retrieve()
                            .bodyToMono(float[].class)
                            .block();
                } catch (Exception e) {
                    logger.warn("Error generating embedding from service: {}", e.getMessage());
                }
            }
            
            // Fallback to a simple embedding based on hash code
            // This is just a placeholder - in production, use a proper embedding service
            float[] placeholder = new float[384];
            int hash = text.hashCode();
            for (int i = 0; i < placeholder.length; i++) {
                placeholder[i] = (float) Math.sin(hash * (i + 1) / 100.0);
            }
            return placeholder;
            
        } catch (Exception e) {
            logger.error("Error generating embeddings: {}", e.getMessage(), e);
            // Return zero vector as fallback
            return new float[384];
        }
    }
    
    /**
     * Clean expired cache entries (run daily at midnight)
     */
    @CacheEvict(value = "books", allEntries = true)
    @Scheduled(cron = "0 0 0 * * ?") 
    public void cleanExpiredCacheEntries() {
        logger.info("Cleaning expired in-memory cache entries");
    }
}
