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
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;
import redis.clients.jedis.search.Document;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Collections;
import java.util.ArrayList;
import java.util.AbstractMap;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class RedisBookSearchService {

    private static final Logger logger = LoggerFactory.getLogger(RedisBookSearchService.class);
    private static final String CACHED_BOOK_PREFIX = "book:";

    private final JedisPooled jedisPooled;
    private final RedisBookAccessor redisBookAccessor;

    /**
     * Constructs search service with required dependencies
     *
     * @param jedisPooled Redis connection pool for search operations
     * @param redisBookAccessor service for book data retrieval
     */
    public RedisBookSearchService(JedisPooled jedisPooled,
                                  RedisBookAccessor redisBookAccessor) {
        this.jedisPooled = jedisPooled;
        this.redisBookAccessor = redisBookAccessor;
        logger.info("RedisBookSearchService initialized");
    }

    /**
     * Finds books similar to target book using vector embeddings or attribute matching
     * Uses cosine similarity for vector-based matching when embeddings available
     *
     * @param bookId identifier of target book for similarity comparison
     * @param limit maximum number of similar books to return
     * @return list of similar books ranked by similarity score
     */
    public List<CachedBook> findSimilarBooksById(String bookId, int limit) {
        Optional<CachedBook> targetBookOpt = redisBookAccessor.findJsonByIdWithRedisJsonFallback(bookId)
                                                .flatMap(redisBookAccessor::deserializeBook);

        if (!targetBookOpt.isPresent()) {
            logger.warn("Target book with ID {} not found for similarity search.", bookId);
            return Collections.emptyList();
        }
        CachedBook target = targetBookOpt.get();
        RedisVector targetVec = target.getEmbedding();

        List<CachedBook> allBooks = redisBookAccessor.scanAndDeserializeAllBooks();

        if (targetVec != null && targetVec.getDimension() > 0) {
            return allBooks.stream()
                    .filter(book -> !bookId.equals(book.getId()))
                    .filter(book -> {
                        RedisVector vec = book.getEmbedding();
                        return vec != null && vec.getDimension() == targetVec.getDimension();
                    })
                    .map(book -> new AbstractMap.SimpleEntry<>(book, targetVec.cosineSimilarity(book.getEmbedding())))
                    .sorted((e1, e2) -> Double.compare(e2.getValue(), e1.getValue()))
                    .limit(limit)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
        }
        return allBooks.stream()
                .filter(book -> !bookId.equals(book.getId()))
                .filter(book -> hasSimilarAttributes(target, book))
                .limit(limit)
                .collect(Collectors.toList());
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
     * Performs case-insensitive title matching across all cached books
     *
     * @param title book title to search for (case-insensitive)
     * @param idToExclude book ID to exclude from results
     * @return list of books with matching titles
     */
    public List<CachedBook> findByTitleIgnoreCaseAndIdNot(String title, String idToExclude) {
        if (title == null || idToExclude == null) {
            return Collections.emptyList();
        }
        return redisBookAccessor.scanAndDeserializeAllBooks().stream()
                .filter(book -> !idToExclude.equals(book.getId()))
                .filter(book -> book.getTitle() != null &&
                              book.getTitle().toLowerCase().equals(title.toLowerCase()))
                .collect(Collectors.toList());
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
            logger.debug("Executing RediSearch query on idx:books for slug query: @slug:{{}}", escapedSlug);
            SearchResult searchResult = jedisPooled.ftSearch("idx:books", query);
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
     * @return randomized list of recent books with quality covers
     */
    public List<CachedBook> findRandomRecentBooksWithGoodCovers(int count, Set<String> excludeIds) {
        List<CachedBook> searchResults = tryRedisSearchQueryForRecentGoodCovers(count, excludeIds);
        if (!searchResults.isEmpty()) {
            logger.info("Found {} books using RediSearch for recent good covers.", searchResults.size());
            return searchResults;
        }
        
        logger.info("RediSearch query returned no results for recent good covers, using fallback scan method.");
        return findRandomRecentBooksFallback(count, excludeIds);
    }
    
    /**
     * Attempts RediSearch query for recent books with quality covers
     * Uses multiple search indexes with sophisticated filtering criteria
     *
     * @param count desired number of books
     * @param excludeIds book IDs to exclude
     * @return list of books matching search criteria
     */
    private List<CachedBook> tryRedisSearchQueryForRecentGoodCovers(int count, Set<String> excludeIds) {
        try {
            String queryString = "(@publishedDate:{2024} | @publishedDate:{2025}) @coverImageUrl:(*google*zoom\\=2* | *s3* | *digitalocean* | *cloudfront* | *openlibrary*-L.jpg*)";
            queryString += " -@coverImageUrl:(*placeholder* | *image-not-available*)";
            
            Query searchQuery = new Query(queryString)
                .limit(0, Math.max(count * 3, 100));
            
            List<CachedBook> results = new ArrayList<>();
            try {
                SearchResult searchResult = jedisPooled.ftSearch("idx:books", searchQuery);
                results.addAll(processSearchResultsForRecentGoodCovers(searchResult, count, excludeIds, "book:"));
                if (results.size() >= count) {
                    return results.stream().limit(count).collect(Collectors.toList());
                }
            } catch (Exception e) {
                logger.debug("idx:books search failed for recent good covers: {}", e.getMessage());
            }
            
            if (results.size() < count) {
                try {
                    SearchResult searchResultVector = jedisPooled.ftSearch("idx:cached_books_vector", searchQuery);
                    results.addAll(processSearchResultsForRecentGoodCovers(searchResultVector, count - results.size(), excludeIds, "book:"));
                    if (results.size() >= count) {
                        return results.stream().limit(count).collect(Collectors.toList());
                    }
                } catch (Exception e) {
                    logger.debug("idx:cached_books_vector search failed for recent good covers: {}", e.getMessage());
                }
            }
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
     * Scans all cached books and applies filtering criteria manually
     *
     * @param count desired number of books
     * @param excludeIds book IDs to exclude
     * @return filtered list of recent books with quality covers
     */
    private List<CachedBook> findRandomRecentBooksFallback(int count, Set<String> excludeIds) {
        logger.info("Using fallback method for finding random recent books with good covers.");
        List<CachedBook> allBooks = redisBookAccessor.scanAndDeserializeAllBooks();
        List<CachedBook> candidateBooks = new ArrayList<>();

        for (CachedBook book : allBooks) {
            if (excludeIds != null && excludeIds.contains(book.getId())) {
                continue;
            }
            if (isRecentPublication(book) && hasHighQualityCover(book) && !isBestseller(book)) {
                candidateBooks.add(book);
            }
        }
        
        Collections.shuffle(candidateBooks);
        return candidateBooks.stream().limit(count).collect(Collectors.toList());
    }

    /**
     * Determines if book has recent publication date (2024-2025)
     *
     * @param book cached book to evaluate
     * @return true if published in 2024 or 2025
     */
    private boolean isRecentPublication(CachedBook book) {
        if (book.getPublishedDate() == null) return false;
        try {
            int year = book.getPublishedDate().getYear();
            return year == 2024 || year == 2025;
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
}
