/**
 * Advanced search and similarity matching service for cached book data
 * Provides complex search capabilities using RediSearch with intelligent fallback mechanisms
 * Handles vector similarity matching and attribute-based filtering for book recommendations
 *
 * @author William Callahan
 *
 * Features:
 * - Vector-based similarity search using cosine similarity calculations
 * - RediSearch integration with query optimization and error handling
 * - Intelligent fallback to scan-based search when RediSearch unavailable
 * - Advanced filtering for publication dates, cover quality, and bestseller status
 */

package com.williamcallahan.book_recommendation_engine.repository;

import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.types.RedisVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;
import redis.clients.jedis.search.Document;
import com.williamcallahan.book_recommendation_engine.repository.RedisBookIndexManager;
import com.williamcallahan.book_recommendation_engine.util.RedisHelper;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Collections;
import java.util.ArrayList;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.scheduling.annotation.Async;

// for vector search and byte manipulation
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.Locale; // for toLowerCase

@SuppressWarnings("unused")
@Service
public class RedisBookSearchService {

    private static final Logger logger = LoggerFactory.getLogger(RedisBookSearchService.class);
    private static final String CACHED_BOOK_PREFIX = "book:";
    
    // Quality cover URL patterns for high-resolution images
    private static final String[] QUALITY_COVER_PATTERNS = {
        "*google*zoom\\=2*", "*s3*", "*digitalocean*", "*cloudfront*", "*openlibrary*-L.jpg*"
    };
    
    // Patterns to exclude low-quality or placeholder images
    private static final String[] EXCLUDE_PATTERNS = {
        "*placeholder*", "*image-not-available*"
    };

    private final JedisPooled jedisPooled;
    private final RedisBookAccessor redisBookAccessor;
    private final ObjectMapper objectMapper;
    private final RedisBookIndexManager indexManager;

    /**
     * Constructs search service with required dependencies
     *
     * @param jedisPooled Redis connection pool for search operations
     * @param redisBookAccessor service for book data retrieval
     * @param objectMapper Jackson ObjectMapper for JSON processing
     * @param indexManager RedisBookIndexManager for secondary index lookup
     */
    public RedisBookSearchService(JedisPooled jedisPooled,
                                  RedisBookAccessor redisBookAccessor,
                                  ObjectMapper objectMapper,
                                  RedisBookIndexManager indexManager) {
        this.jedisPooled = jedisPooled;
        this.redisBookAccessor = redisBookAccessor;
        this.objectMapper = objectMapper;
        this.indexManager = indexManager;
        logger.info("RedisBookSearchService initialized");
    }

    /**
     * Finds books similar to target book using vector embeddings or attribute matching
     * Uses streaming approach to avoid loading all books into memory
     *
     * @param bookId identifier of target book for similarity comparison
     * @param limit maximum number of similar books to return
     * @return list of similar books ranked by similarity score
     */
    // Helper method to convert float array to byte array for RediSearch
    // RediSearch expects vector data as a blob of float32s.
    private byte[] floatArrayToByteArray(float[] floats) {
        if (floats == null) return new byte[0];
        ByteBuffer byteBuffer = ByteBuffer.allocate(floats.length * Float.BYTES);
        // Ensure Little Endian byte order, common for many systems and C-based libraries like RediSearch
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        for (float f : floats) {
            byteBuffer.putFloat(f);
        }
        return byteBuffer.array();
    }

    // Helper to escape values for TAG queries.
    // TAG fields are indexed on specific separators (default: ,).
    // If tag values themselves contain these separators or RediSearch special characters, they need escaping.
    private String escapeTagQuery(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        // Basic escaping for common characters in tags.
        // Adjust based on actual tag content and RediSearch version/behavior.
        // Common characters to escape: , . < > { } [ ] " ' : ; ! @ # $ % ^ & * ( ) - + = ~
        // For simplicity, only a few common ones are handled here. More robust escaping might be needed.
        return value.replace("-", "\\-")
                    .replace(" ", "\\ ") // If tags can have spaces and are not quoted
                    .replace(":", "\\:")
                    .replace("{", "\\{")
                    .replace("}", "\\}")
                    .replace("[", "\\[")
                    .replace("]", "\\]")
                    .replace("|", "\\|")
                    .replace("@", "\\@");
    }

    public List<CachedBook> findSimilarBooksById(String bookId, int limit) {
        Optional<CachedBook> targetBookOpt = redisBookAccessor.findJsonByIdWithRedisJsonFallback(bookId)
                .flatMap(redisBookAccessor::deserializeBook);

        if (!targetBookOpt.isPresent()) {
            logger.warn("Target book with ID {} not found for similarity search.", bookId);
            return Collections.emptyList();
        }
        CachedBook target = targetBookOpt.get();
        RedisVector targetEmbedding = target.getEmbedding();

        List<CachedBook> similarBooks = new ArrayList<>();

        // Attempt vector search if embedding is valid
        if (targetEmbedding != null && targetEmbedding.getValues() != null && targetEmbedding.getDimension() > 0) {
            logger.debug("Attempting vector similarity search for book ID: {}", bookId);
            try {
                byte[] vectorBlob = floatArrayToByteArray(targetEmbedding.getValues());
                // KNN search query: find K nearest neighbors to the vector $BLOB in the @embedding field
                // Exclude the source document itself by checking ID.
                // The $ indicates we want the full document JSON. 'dist' is the distance.
                String knnQueryString = String.format(Locale.ROOT, "*=>[KNN %d @embedding $VEC AS dist IF @id != \"%s\"]", limit + 1, escapeTagQuery(bookId));

                Query query = new Query(knnQueryString)
                        .addParam("VEC", vectorBlob)
                        .returnFields("dist", "$") // $ is the root JSON object
                        .setSortBy("dist", true) // true for ASC (closer is better)
                        .dialect(2); // Dialect 2+ for KNN

                SearchResult searchResult = safeFtSearch(RedisBookIndexManager.PRIMARY_INDEX_NAME, query);

                for (Document doc : searchResult.getDocuments()) {
                    String json = doc.getString("$");
                    // String docId = doc.getId().substring(CACHED_BOOK_PREFIX.length()); // Assuming key is "book:ID"
                    // if (bookId.equals(docId)) continue; // Double check exclusion if IF clause not fully reliable or for older Redis versions

                    if (json != null) {
                        redisBookAccessor.deserializeBook(json).ifPresent(similarBooks::add);
                    }
                    if (similarBooks.size() >= limit) break;
                }
                logger.info("Found {} similar books using vector search for book ID: {}", similarBooks.size(), bookId);
                if (!similarBooks.isEmpty()) {
                    return similarBooks; // Already sorted by distance by RediSearch
                }
            } catch (Exception e) {
                logger.error("Error during vector similarity search for book ID {}: {}. Falling back to attribute search.", bookId, e.getMessage(), e);
                // Clear any partial results from failed vector search
                similarBooks.clear();
            }
        }

        // Fallback to attribute-based search if vector search fails or no embeddings
        // Or if vector search returned no results and we still want to try attribute based.
        if (similarBooks.isEmpty()) {
            logger.debug("Attempting attribute-based similarity search for book ID: {}", bookId);
            StringBuilder attributeQuery = new StringBuilder();
            List<String> conditions = new ArrayList<>();

            if (target.getAuthors() != null && !target.getAuthors().isEmpty()) {
                String authorsCondition = target.getAuthors().stream()
                        .map(this::escapeTagQuery)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.joining("|"));
                if (!authorsCondition.isEmpty()) {
                    conditions.add("@author:{" + authorsCondition + "}");
                }
            }

            if (target.getCategories() != null && !target.getCategories().isEmpty()) {
                String categoriesCondition = target.getCategories().stream()
                        .map(this::escapeTagQuery)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.joining("|"));
                if (!categoriesCondition.isEmpty()) {
                    conditions.add("@category:{" + categoriesCondition + "}");
                }
            }

            if (!conditions.isEmpty()) {
                attributeQuery.append("(").append(String.join(" | ", conditions)).append(")"); // Books matching any of target's authors OR categories
                attributeQuery.append(" (-@id:{" + escapeTagQuery(bookId) + "})"); // Exclude the target book itself

                Query query = new Query(attributeQuery.toString())
                        .limit(0, limit)
                        .returnFields("$");

                try {
                    SearchResult searchResult = safeFtSearch(RedisBookIndexManager.PRIMARY_INDEX_NAME, query);
                    for (Document doc : searchResult.getDocuments()) {
                        String json = doc.getString("$");
                        if (json != null) {
                            redisBookAccessor.deserializeBook(json).ifPresent(similarBooks::add);
                        }
                    }
                    logger.info("Found {} similar books using attribute search for book ID: {}", similarBooks.size(), bookId);
                } catch (Exception e) {
                    logger.error("Error during attribute-based similarity search for book ID {}: {}", bookId, e.getMessage(), e);
                }
            } else {
                logger.warn("No attributes (authors/categories) available for attribute-based search for book ID: {}", bookId);
            }
        }
        return similarBooks;
    }

    /**
     * Asynchronous version of findSimilarBooksById
     *
     * - executed asynchronously by Spring's taskExecutor
     * - wraps synchronous result in completed CompletableFuture
     * - logs and propagates exceptions correctly
     */
    @Async
    public CompletableFuture<List<CachedBook>> findSimilarBooksByIdAsync(String bookId, int limit) {
        try {
            List<CachedBook> result = findSimilarBooksById(bookId, limit);
            return CompletableFuture.completedFuture(result);
        } catch (Exception ex) {
            logger.error("Async findSimilarBooksById failed for bookId {}", bookId, ex);
            CompletableFuture<List<CachedBook>> failed = new CompletableFuture<>();
            failed.completeExceptionally(ex);
            return failed;
        }
    }

    /**
     * Determines similarity between books based on shared attributes
     * Checks for common categories and authors when vector embeddings unavailable
     *
     * @param target reference book for comparison
     * @param candidate book being evaluated for similarity
     * @return true if books share categories or authors
     */
    private boolean hasSimilarAttributes(CachedBook target, CachedBook candidate) {
        if (target.getCategories() != null && candidate.getCategories() != null) {
            boolean hasSharedCategory = target.getCategories().stream()
                    .anyMatch(category -> candidate.getCategories().contains(category));
            if (hasSharedCategory) return true;
        }
        if (target.getAuthors() != null && candidate.getAuthors() != null) {
            boolean hasSharedAuthor = target.getAuthors().stream()
                    .anyMatch(author -> candidate.getAuthors().contains(author));
            if (hasSharedAuthor) return true;
        }
        return false;
    }

    /**
     * Searches for books with matching title while excluding specific book ID
     * Uses RediSearch for efficient title matching with fallback to streaming scan
     *
     * @param title book title to search for (case-insensitive)
     * @param idToExclude book ID to exclude from results
     * @return list of books with matching titles
     */
    public List<CachedBook> findByTitleIgnoreCaseAndIdNot(String title, String idToExclude) {
        if (title == null || idToExclude == null) {
            return Collections.emptyList();
        }
        
        // Try RediSearch first for efficient title matching
        try {
            String escapedTitle = escapeRedisSearchString(title);
            Query query = new Query("@title:\"" + escapedTitle + "\"")
                    .returnFields("$")
                    .limit(0, 100);
            
            SearchResult searchResult = safeFtSearch(RedisBookIndexManager.PRIMARY_INDEX_NAME, query);
            List<CachedBook> results = new ArrayList<>();
            
            for (Document doc : searchResult.getDocuments()) {
                try {
                    String json = doc.getString("$");
                    if (json != null) {
                        Optional<CachedBook> bookOpt = redisBookAccessor.deserializeBook(json);
                        if (bookOpt.isPresent() && !idToExclude.equals(bookOpt.get().getId())) {
                            results.add(bookOpt.get());
                        }
                    }
                } catch (Exception e) {
                    logger.debug("Error processing search result for title '{}': {}", title, e.getMessage());
                }
            }
            
            if (!results.isEmpty()) {
                logger.debug("Found {} books using RediSearch for title: '{}'", results.size(), title);
                return results;
            }
            // If RediSearch returns no results, or if an exception occurred (handled by safeFtSearch returning empty),
            // return empty list. Do not fall back to streamAllBooks.
            logger.debug("RediSearch found no results for title '{}' or an error occurred, returning empty list.", title);
            return Collections.emptyList();
        } catch (Exception e) {
            // This catch block is for unexpected errors not caught by safeFtSearch, though unlikely.
            logger.error("Unexpected error in findByTitleIgnoreCaseAndIdNot for title '{}': {}", title, e.getMessage(), e);
            return Collections.emptyList();
        }
    }
    
    /**
     * Escapes special characters for RediSearch query
     * @param input raw search string
     * @return escaped string safe for RediSearch
     */
    private String escapeRedisSearchString(String input) {
        return input.replace("\\", "\\\\")
                   .replace("\"", "\\\"")
                   .replace("{", "\\{")
                   .replace("}", "\\}")
                   .replace("(", "\\(")
                   .replace(")", "\\)")
                   .replace("[", "\\[")
                   .replace("]", "\\]")
                   .replace("@", "\\@")
                   .replace("!", "\\!");
    }

    /**
     * Asynchronous version of findByTitleIgnoreCaseAndIdNot
     *
     * - executed asynchronously by Spring's taskExecutor
     * - wraps synchronous result in completed CompletableFuture
     * - logs and propagates exceptions correctly
     */
    @Async
    public CompletableFuture<List<CachedBook>> findByTitleIgnoreCaseAndIdNotAsync(String title, String idToExclude) {
        try {
            List<CachedBook> result = findByTitleIgnoreCaseAndIdNot(title, idToExclude);
            return CompletableFuture.completedFuture(result);
        } catch (Exception ex) {
            logger.error("Async findByTitleIgnoreCaseAndIdNot failed for title {}", title, ex);
            CompletableFuture<List<CachedBook>> failed = new CompletableFuture<>();
            failed.completeExceptionally(ex);
            return failed;
        }
    }

    /**
     * Retrieves book by URL-friendly slug identifier using RediSearch
     * Escapes special characters and performs exact slug matching
     *
     * @param slug URL-safe book identifier
     * @return Optional containing book if found by slug
     */
    public Optional<CachedBook> findBySlug(String slug) {
        if (slug == null || slug.trim().isEmpty()) {
            return Optional.empty();
        }
        String escapedSlug = slug.replace("-", "\\-")
                                 .replace(":", "\\:")
                                 .replace("{", "\\{")
                                 .replace("}", "\\}")
                                 .replace("(", "\\(")
                                 .replace(")", "\\)")
                                 .replace("[", "\\[")
                                 .replace("]", "\\]")
                                 .replace("@", "\\@")
                                 .replace("!", "\\!");

        Query query = new Query("@slug:{" + escapedSlug + "}")
                .limit(0, 1);

        try {
            logger.debug("Executing RediSearch query on {} for slug query: @slug:{{}}", RedisBookIndexManager.PRIMARY_INDEX_NAME, escapedSlug);
            SearchResult searchResult = safeFtSearch(RedisBookIndexManager.PRIMARY_INDEX_NAME, query);
            if (searchResult.getTotalResults() > 0) {
                Document doc = searchResult.getDocuments().get(0);
                String json = doc.getString("$");
                if (json != null) {
                    return redisBookAccessor.deserializeBook(json);
                }
            }
        } catch (Exception e) {
            logger.error("Error finding book by slug '{}' using RediSearch: {}", slug, e.getMessage(), e);
        }
        return Optional.empty();
    }
    
    /**
     * Finds recent books with high-quality cover images for display purposes
     * Filters for recent publications with good cover resolution and excludes bestsellers
     *
     * @param count maximum number of books to return
     * @param excludeIds set of book IDs to exclude from results
     * @param fromYear Starting year for recent books (inclusive)
     * @return randomized list of recent books with quality covers
     */
    public List<CachedBook> findRandomRecentBooksWithGoodCovers(int count, Set<String> excludeIds, int fromYear) {
        List<CachedBook> searchResults = tryRedisSearchQueryForRecentGoodCovers(count, excludeIds, fromYear);
        if (!searchResults.isEmpty()) {
            logger.info("Found {} books using RediSearch for recent good covers from year {}.", searchResults.size(), fromYear);
            return searchResults;
        }
        
        logger.info("RediSearch query returned no results for recent good covers. Not falling back to scan method.");
        return Collections.emptyList(); // Do not fall back to findRandomRecentBooksFallback
    }
    
    /**
     * Attempts RediSearch query for recent books with quality covers
     * Uses multiple search indexes with sophisticated filtering criteria
     *
     * @param count desired number of books
     * @param excludeIds book IDs to exclude
     * @param fromYear Starting year for recent books (inclusive)
     * @return list of books matching search criteria
     */
    private List<CachedBook> tryRedisSearchQueryForRecentGoodCovers(int count, Set<String> excludeIds, int fromYear) {
        try {
            // Use fromYear and the year after for the range
            int endYear = fromYear + 1;
            // Use the numeric publishedYear field for range query
            String yearQuery = String.format(Locale.ROOT, "(@publishedYear:[%d %d])", fromYear, endYear);
            
            // Build include patterns from configuration
            String includePatterns = Arrays.stream(QUALITY_COVER_PATTERNS)
                .map(p -> "@coverImageUrl:(" + p + ")")
                .collect(Collectors.joining(" | "));
            
            // Build exclude patterns from configuration
            String excludePatterns = Arrays.stream(EXCLUDE_PATTERNS)
                .map(p -> " -@coverImageUrl:(" + p + ")")
                .collect(Collectors.joining());
            
            String queryString = yearQuery + " (" + includePatterns + ")" + excludePatterns;
            
            Query searchQuery = new Query(queryString)
                .limit(0, Math.max(count * 3, 100));
            
            List<CachedBook> results = new ArrayList<>();
            try {
                SearchResult searchResult = safeFtSearch(RedisBookIndexManager.PRIMARY_INDEX_NAME, searchQuery);
                results.addAll(processSearchResultsForRecentGoodCovers(searchResult, count, excludeIds, "book:"));
                if (results.size() >= count) {
                    return results.stream().limit(count).collect(Collectors.toList());
                }
            } catch (Exception e) {
                logger.debug("{} search failed for recent good covers: {}", RedisBookIndexManager.PRIMARY_INDEX_NAME, e.getMessage());
            }
            
            // The schema 'optimized_redisearch_schema.redis' only defines 'idx:books'.
            // If 'idx:cached_books_vector' was a separate, older index for vectors,
            // and now 'idx:books' includes vectors, this second search might be redundant or incorrect.
            // For now, assuming 'idx:books' is the sole, consolidated index as per the optimized schema.
            // If 'idx:cached_books_vector' is still relevant and distinct, this logic might need to be preserved.
            // Based on the provided optimized schema, 'idx:books' contains the vector field.
            // Thus, searching a different index for the same query might not be intended unless it has different data.
            // Removing the second search against 'idx:cached_books_vector' for now to align with the single optimized index.
            // if (results.size() < count) {
            //     try {
            //         SearchResult searchResultVector = safeFtSearch("idx:cached_books_vector", searchQuery); // Potentially change this if it's also PRIMARY_INDEX_NAME or remove
            //         results.addAll(processSearchResultsForRecentGoodCovers(searchResultVector, count - results.size(), excludeIds, "book:"));
            //         if (results.size() >= count) {
            //             return results.stream().limit(count).collect(Collectors.toList());
            //         }
            //     } catch (Exception e) {
            //         logger.debug("idx:cached_books_vector search failed for recent good covers: {}", e.getMessage());
            //     }
            // }
            return results.stream().limit(count).collect(Collectors.toList());
            
        } catch (Exception e) {
            logger.warn("RediSearch query failed for recent good covers: {}", e.getMessage());
        }
        return Collections.emptyList();
    }
    
    /**
     * Processes RediSearch results and filters for recent books with quality covers
     * Validates cover quality, publication dates, and bestseller status
     *
     * @param searchResult raw search results from RediSearch
     * @param count desired number of books
     * @param excludeIds book IDs to exclude
     * @param keyPrefix Redis key prefix for book identification
     * @return filtered and processed list of candidate books
     */
    private List<CachedBook> processSearchResultsForRecentGoodCovers(SearchResult searchResult, int count, Set<String> excludeIds, String keyPrefix) {
        List<CachedBook> candidateBooks = new ArrayList<>();
        if (searchResult == null || searchResult.getDocuments() == null) return candidateBooks;

        for (Document doc : searchResult.getDocuments()) {
            try {
                String docKey = doc.getId();
                String bookId;
                
                if (docKey.startsWith(keyPrefix)) {
                    bookId = docKey.substring(keyPrefix.length());
                } else if (docKey.startsWith(CACHED_BOOK_PREFIX)) {
                    bookId = docKey.substring(CACHED_BOOK_PREFIX.length());
                } else {
                    bookId = docKey;
                }
                
                if (excludeIds != null && excludeIds.contains(bookId)) {
                    continue;
                }
                
                Optional<CachedBook> bookOpt = redisBookAccessor.findJsonByIdWithRedisJsonFallback(bookId)
                                                 .flatMap(redisBookAccessor::deserializeBook);
                if (bookOpt.isPresent()) {
                    CachedBook book = bookOpt.get();
                    if (isRecentPublication(book) && hasHighQualityCover(book) && !isBestseller(book)) {
                        candidateBooks.add(book);
                    }
                }
            } catch (Exception e) {
                logger.debug("Error processing search result document {} for recent good covers: {}", doc.getId(), e.getMessage());
            }
        }
        
        Collections.shuffle(candidateBooks);
        return candidateBooks;
    }
    
    /**
     * Fallback method for finding recent books when RediSearch unavailable
     * Uses streaming approach with sampling to avoid loading entire dataset
     *
     * @param count desired number of books
     * @param excludeIds book IDs to exclude
     * @return filtered list of recent books with quality covers
     */
    // findRandomRecentBooksFallback is no longer called, so it can be removed.
    // private List<CachedBook> findRandomRecentBooksFallback(int count, Set<String> excludeIds) { ... }
    
    /**
     * Asynchronous version of findRandomRecentBooksWithGoodCovers
     *
     * - executed asynchronously by Spring's taskExecutor
     * - wraps synchronous result in completed CompletableFuture
     * - logs and propagates exceptions correctly
     */
    @Async
    public CompletableFuture<List<CachedBook>> findRandomRecentBooksWithGoodCoversAsync(int count, Set<String> excludeIds, int fromYear) {
        try {
            List<CachedBook> result = findRandomRecentBooksWithGoodCovers(count, excludeIds, fromYear);
            return CompletableFuture.completedFuture(result);
        } catch (Exception ex) {
            logger.error("Async findRandomRecentBooksWithGoodCovers failed for count {} fromYear {}", count, fromYear, ex);
            CompletableFuture<List<CachedBook>> failed = new CompletableFuture<>();
            failed.completeExceptionally(ex);
            return failed;
        }
    }
    
    /**
     * Asynchronous version of findBySlug
     *
     * - executed asynchronously by Spring's taskExecutor
     * - wraps synchronous result in completed CompletableFuture
     * - logs and propagates exceptions correctly
     */
    @Async
    public CompletableFuture<Optional<CachedBook>> findBySlugAsync(String slug) {
        try {
            Optional<CachedBook> result = findBySlug(slug);
            return CompletableFuture.completedFuture(result);
        } catch (Exception ex) {
            logger.error("Async findBySlug failed for slug {}", slug, ex);
            CompletableFuture<Optional<CachedBook>> failed = new CompletableFuture<>();
            failed.completeExceptionally(ex);
            return failed;
        }
    }

    /**
     * Determines if book has recent publication date (current year or next year)
     *
     * @param book cached book to evaluate
     * @return true if published in current year or next year
     */
    private boolean isRecentPublication(CachedBook book) {
        if (book.getPublishedDate() == null) return false;
        try {
            int year = book.getPublishedDate().getYear();
            int currentYear = LocalDate.now().getYear();
            // Consider books from current year and next year as recent
            return year == currentYear || year == currentYear + 1;
        } catch (Exception e) {
            logger.debug("Error parsing publication date for book {}: {}", book.getId(), e.getMessage());
            return false;
        }
    }
    
    /**
     * Evaluates cover image quality based on URL patterns and resolution indicators
     * Checks for high-resolution sources and excludes placeholder images
     *
     * @param book cached book to evaluate
     * @return true if cover meets quality standards
     */
    private boolean hasHighQualityCover(CachedBook book) {
        String coverUrl = book.getCoverImageUrl();
        if (coverUrl == null || coverUrl.isEmpty()) return false;
        if (coverUrl.contains("placeholder") || coverUrl.contains("image-not-available")) return false;
        if (coverUrl.contains("books.google.com") && coverUrl.contains("zoom=")) {
            try {
                int zoomIndex = coverUrl.indexOf("zoom=") + 5;
                if (zoomIndex < coverUrl.length()) {
                    char zoomChar = coverUrl.charAt(zoomIndex);
                    return Character.getNumericValue(zoomChar) >= 2;
                }
            } catch (Exception e) {
                return true;
            }
        }
        if (coverUrl.contains("s3.amazonaws.com") || coverUrl.contains("digitaloceanspaces.com") || 
            coverUrl.contains("cloudfront.net") || coverUrl.startsWith("/book-covers/")) return true;
        if (coverUrl.contains("openlibrary.org") && coverUrl.contains("-L.jpg")) return true;
        return false;
    }
    
    /**
     * Determines if book is marked as bestseller based on qualifiers
     * Checks various qualifier patterns for bestseller indicators
     *
     * @param book cached book to evaluate
     * @return true if book has bestseller qualifications
     */
    private boolean isBestseller(CachedBook book) {
        if (book.getQualifiers() == null) return false;
        Object bestsellerQualifier = book.getQualifiers().get("new york times bestseller");
        if (bestsellerQualifier != null) {
            if (bestsellerQualifier instanceof Boolean) return (Boolean) bestsellerQualifier;
            if (bestsellerQualifier instanceof String) {
                String qStr = ((String) bestsellerQualifier).toLowerCase();
                return qStr.contains("bestseller") || qStr.equals("true");
            }
            return true;
        }
        for (Map.Entry<String, Object> entry : book.getQualifiers().entrySet()) {
            String key = entry.getKey().toLowerCase();
            if (key.contains("bestseller") || key.contains("best seller") || key.contains("nyt")) {
                Object value = entry.getValue();
                if (value instanceof Boolean) return (Boolean) value;
                if (value != null) {
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * Finds a book by ISBN-13 using RediSearch with fallback to streaming scan
     * Handles different JSON nesting structures including the "value" wrapper issue
     *
     * @param isbn13 The ISBN-13 to search for
     * @return Optional containing the book if found
     */
    public Optional<CachedBook> findByIsbn13(String isbn13) {
        if (isbn13 == null || isbn13.trim().isEmpty()) {
            return Optional.empty();
        }
        
        // Try RediSearch first
        try {
            String escapedIsbn = escapeRedisSearchString(isbn13);
            Query query = new Query("@isbn13:{" + escapedIsbn + "}")
                    .returnFields("$")
                    .limit(0, 1);
            
            SearchResult searchResult = safeFtSearch(RedisBookIndexManager.PRIMARY_INDEX_NAME, query);
            if (searchResult.getTotalResults() > 0) {
                Document doc = searchResult.getDocuments().get(0);
                String json = doc.getString("$");
                if (json != null) {
                    return cleanAndDeserializeBook(json, doc.getId());
                }
            }
        } catch (Exception e) {
            logger.debug("RediSearch failed for ISBN-13 '{}', falling back to scan: {}", isbn13, e.getMessage());
        }
        
        // Fallback to streaming scan removed. If RediSearch fails (handled by safeFtSearch)
        // or finds nothing, return Optional.empty().
        logger.debug("RediSearch found no result for ISBN-13 '{}' or an error occurred.", isbn13);
        return Optional.empty();
    }
    
    /**
     * Finds a book by ISBN-10 using RediSearch with fallback to streaming scan
     * Handles different JSON nesting structures including the "value" wrapper issue
     *
     * @param isbn10 The ISBN-10 to search for
     * @return Optional containing the book if found
     */
    public Optional<CachedBook> findByIsbn10(String isbn10) {
        if (isbn10 == null || isbn10.trim().isEmpty()) {
            return Optional.empty();
        }
        
        // Try RediSearch first
        try {
            String escapedIsbn = escapeRedisSearchString(isbn10);
            Query query = new Query("@isbn10:{" + escapedIsbn + "}")
                    .returnFields("$")
                    .limit(0, 1);
            
            SearchResult searchResult = safeFtSearch(RedisBookIndexManager.PRIMARY_INDEX_NAME, query);
            if (searchResult.getTotalResults() > 0) {
                Document doc = searchResult.getDocuments().get(0);
                String json = doc.getString("$");
                if (json != null) {
                    return cleanAndDeserializeBook(json, doc.getId());
                }
            }
        } catch (Exception e) {
            logger.debug("RediSearch failed for ISBN-10 '{}', falling back to scan: {}", isbn10, e.getMessage());
        }
        
        // Fallback to streaming scan removed.
        logger.debug("RediSearch found no result for ISBN-10 '{}' or an error occurred.", isbn10);
        return Optional.empty();
    }
    
    /**
     * Finds a book by Google Books ID using RediSearch with fallback to streaming scan
     * Handles different JSON nesting structures including the "value" wrapper issue
     *
     * @param googleBooksId The Google Books ID to search for
     * @return Optional containing the book if found
     */
    public Optional<CachedBook> findByGoogleBooksId(String googleBooksId) {
        if (googleBooksId == null || googleBooksId.trim().isEmpty()) {
            return Optional.empty();
        }
        
        // Try RediSearch first
        try {
            String escapedId = escapeRedisSearchString(googleBooksId);
            Query query = new Query("@googleBooksId:{" + escapedId + "}")
                    .returnFields("$")
                    .limit(0, 1);
            
            SearchResult searchResult = safeFtSearch(RedisBookIndexManager.PRIMARY_INDEX_NAME, query);
            if (searchResult.getTotalResults() > 0) {
                Document doc = searchResult.getDocuments().get(0);
                String json = doc.getString("$");
                if (json != null) {
                    return cleanAndDeserializeBook(json, doc.getId());
                }
            }
        } catch (Exception e) {
            logger.debug("RediSearch failed for Google Books ID '{}', falling back to scan: {}", googleBooksId, e.getMessage());
        }
        
        // Fallback to streaming scan removed.
        logger.debug("RediSearch found no result for Google Books ID '{}' or an error occurred.", googleBooksId);
        return Optional.empty();
    }
    
    /**
     * Fallback method to find book by identifier when RediSearch is unavailable
     * Uses streaming approach to scan through all books
     *
     * @param identifierType Type of identifier (ISBN_13 or ISBN_10)
     * @param identifierValue The identifier value to search for
     * @return Optional containing the book if found
     */
    // findByIdentifierFallback is no longer called, so it can be removed.
    // private Optional<CachedBook> findByIdentifierFallback(String identifierType, String identifierValue) { ... }
    
    /**
     * Fallback method to find book by Google Books ID when RediSearch is unavailable
     *
     * @param googleBooksId The Google Books ID to search for
     * @return Optional containing the book if found
     */
    // findByGoogleBooksIdFallback is no longer called, so it can be removed.
    // private Optional<CachedBook> findByGoogleBooksIdFallback(String googleBooksId) { ... }
    
    /**
     * Cleans and deserializes book JSON data, handling various nesting structures
     * Removes incorrect "value" wrapper if present and ensures proper format
     *
     * @param json The JSON string to clean and deserialize
     * @param key The Redis key (for logging purposes)
     * @return Optional containing the deserialized book
     */
    private Optional<CachedBook> cleanAndDeserializeBook(String json, String key) {
        try {
            JsonNode rootNode = objectMapper.readTree(json);
            
            // Handle array wrapper from JSONPath
            if (rootNode.isArray() && rootNode.size() > 0) {
                JsonNode firstElement = rootNode.get(0);
                
                // Check if it has the problematic "value" wrapper
                if (firstElement.isObject() && firstElement.has("value")) {
                    logger.debug("Found and removing incorrect 'value' wrapper in key {}", key);
                    JsonNode valueNode = firstElement.get("value");
                    String cleanedJson = objectMapper.writeValueAsString(valueNode);
                    return redisBookAccessor.deserializeBook(cleanedJson);
                } else {
                    // Normal case - just array wrapped
                    String cleanedJson = objectMapper.writeValueAsString(firstElement);
                    return redisBookAccessor.deserializeBook(cleanedJson);
                }
            } else if (rootNode.isObject()) {
                // Check if root object has "value" wrapper
                if (rootNode.has("value")) {
                    logger.debug("Found and removing incorrect 'value' wrapper at root in key {}", key);
                    JsonNode valueNode = rootNode.get("value");
                    String cleanedJson = objectMapper.writeValueAsString(valueNode);
                    return redisBookAccessor.deserializeBook(cleanedJson);
                } else {
                    // Normal object
                    return redisBookAccessor.deserializeBook(json);
                }
            }
            
            // If we get here, try direct deserialization
            return redisBookAccessor.deserializeBook(json);
            
        } catch (Exception e) {
            logger.error("Error cleaning and deserializing book data from key {}: {}", key, e.getMessage());
            return Optional.empty();
        }
    }
    
    /**
     * Helper class to hold book information with cleaned data
     */
    public static class BookSearchResult {
        private final String redisKey;
        private final String uuid;
        private final CachedBook book;
        
        public BookSearchResult(String redisKey, String uuid, CachedBook book) {
            this.redisKey = redisKey;
            this.uuid = uuid;
            this.book = book;
        }
        
        public String getRedisKey() { return redisKey; }
        public String getUuid() { return uuid; }
        public CachedBook getBook() { return book; }
    }
    
    /**
     * Enhanced search method that returns full book information including Redis key
     * Used by migration process to find existing records
     *
     * @param isbn13 The ISBN-13 to search for
     * @param isbn10 The ISBN-10 to search for
     * @param googleBooksId The Google Books ID to search for
     * @return Optional containing the book search result with key and cleaned data
     */
    public Optional<BookSearchResult> findExistingBook(String isbn13, String isbn10, String googleBooksId) {
        long startTime = System.currentTimeMillis();
        logger.debug("REDIS_DEBUG: Starting findExistingBook search for ISBN-13: {}, ISBN-10: {}, Google ID: {}", isbn13, isbn10, googleBooksId);
        
        // Try ISBN-13 first (most reliable)
        if (isbn13 != null && !isbn13.isEmpty()) {
            Optional<CachedBook> bookOpt = findByIsbn13(isbn13);
            if (bookOpt.isPresent()) {
                CachedBook book = bookOpt.get();
                String redisKey = "book:" + book.getId();
                logger.debug("Found book by ISBN-13: {} with key: {}", isbn13, redisKey);
                return Optional.of(new BookSearchResult(redisKey, book.getId(), book));
            }
        }
        
        // Then try ISBN-10
        if (isbn10 != null && !isbn10.isEmpty()) {
            Optional<CachedBook> bookOpt = findByIsbn10(isbn10);
            if (bookOpt.isPresent()) {
                CachedBook book = bookOpt.get();
                String redisKey = "book:" + book.getId();
                logger.debug("Found book by ISBN-10: {} with key: {}", isbn10, redisKey);
                return Optional.of(new BookSearchResult(redisKey, book.getId(), book));
            }
        }
        
        // Finally try Google Books ID
        if (googleBooksId != null && !googleBooksId.isEmpty()) {
            Optional<CachedBook> bookOpt = findByGoogleBooksId(googleBooksId);
            if (bookOpt.isPresent()) {
                CachedBook book = bookOpt.get();
                String redisKey = "book:" + book.getId();
                logger.debug("Found book by Google Books ID: {} with key: {}", googleBooksId, redisKey);
                return Optional.of(new BookSearchResult(redisKey, book.getId(), book));
            }
        }
        
        long endTime = System.currentTimeMillis();
        logger.debug("REDIS_DEBUG: findExistingBook completed in {}ms (no match found)", (endTime - startTime));
        return Optional.empty();
    }

    /**
     * Wraps ftSearch with JedisConnectionException handling to prevent log spam and fallback to empty result.
     */
    private SearchResult safeFtSearch(String index, Query query) {
        // Prepare a fallback empty result
        SearchResult fallback = new SearchResult.SearchResultBuilder(false, false, false)
                .build(Collections.singletonList(0L));
        String operationName = "ftSearch(" + index + "," + query.toString() + ")";
        return RedisHelper.executeWithTiming(
            logger,
            () -> jedisPooled.ftSearch(index, query),
            operationName,
            fallback
        );
    }
}
