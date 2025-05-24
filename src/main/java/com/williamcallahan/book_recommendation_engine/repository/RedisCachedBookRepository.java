/**
 * Redis implementation of CachedBookRepository for JSON-based persistence
 *
 * @author William Callahan
 *
 * Features:
 * - Provides full book caching with Redis JSON storage
 * - Conditionally activates when Redis is available and configured
 * - Implements search functionality using Redis capabilities
 * - Supports full CRUD operations for cached book data
 * - Uses existing RedisCacheService infrastructure
 * - Includes specialized search methods for titles, authors, and categories
 */

package com.williamcallahan.book_recommendation_engine.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.service.RedisCacheService;
import com.williamcallahan.book_recommendation_engine.types.RedisVector;
import com.williamcallahan.book_recommendation_engine.util.UuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

// RediSearch client imports
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.json.Path2;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;
import redis.clients.jedis.search.Document;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;

/**
 * Original Redis implementation of CachedBookRepository.
 * This class is being refactored and will be replaced by RedisCachedBookRepositoryImpl
 * and its associated specialized services.
 * @deprecated Use RedisCachedBookRepositoryImpl instead.
 */
@Deprecated
public class RedisCachedBookRepository implements CachedBookRepository {

    private static final Logger logger = LoggerFactory.getLogger(RedisCachedBookRepository.class);
    private static final String CACHED_BOOK_PREFIX = "book:";
    private static final String ISBN10_INDEX_PREFIX = "isbn10_idx:";
    private static final String ISBN13_INDEX_PREFIX = "isbn13_idx:";
    private static final String GOOGLE_BOOKS_ID_INDEX_PREFIX = "gbid_idx:";
    private final int cacheExpirationSeconds;

    private final RedisCacheService redisCacheService;
    private final ObjectMapper objectMapper;
    private final JedisPooled jedisPooled; // For all Redis operations

    public RedisCachedBookRepository(RedisCacheService redisCacheService,
                                   ObjectMapper objectMapper,
                                   JedisPooled jedisPooled) {
        this(redisCacheService, objectMapper, jedisPooled, 86400); // Default to 1 day
    }
    
    public RedisCachedBookRepository(RedisCacheService redisCacheService,
                                   ObjectMapper objectMapper,
                                   JedisPooled jedisPooled,
                                   int cacheExpirationSeconds) {
        this.redisCacheService = redisCacheService;
        this.objectMapper = objectMapper;
        this.jedisPooled = jedisPooled;
        this.cacheExpirationSeconds = cacheExpirationSeconds;
        logger.info("Redis CachedBookRepository initialized - using Redis for book storage and RediSearch. Cache TTL: {} seconds", cacheExpirationSeconds);
    }

    private String getCachedBookKey(String bookId) {
        return CACHED_BOOK_PREFIX + bookId;
    }

    private String getIsbn10IndexKey(String isbn10) {
        return ISBN10_INDEX_PREFIX + isbn10;
    }

    private String getIsbn13IndexKey(String isbn13) {
        return ISBN13_INDEX_PREFIX + isbn13;
    }

    private String getGoogleBooksIdIndexKey(String googleBooksId) {
        return GOOGLE_BOOKS_ID_INDEX_PREFIX + googleBooksId;
    }

    @Override
    public Optional<CachedBook> findByGoogleBooksId(String googleBooksId) {
        if (!redisCacheService.isRedisAvailableAsync().join() || googleBooksId == null) {
            return Optional.empty();
        }

        try {
            // First check the index to get the actual book ID
            Optional<String> bookIdOpt = redisCacheService.getCachedStringAsync(getGoogleBooksIdIndexKey(googleBooksId)).join();
            if (bookIdOpt.isPresent()) {
                return findById(bookIdOpt.get());
            }

            // Fallback: scan all cached books (less efficient but ensures we don't miss anything)
            return scanAllCachedBooks().stream()
                    .filter(book -> googleBooksId.equals(book.getGoogleBooksId()))
                    .findFirst();
        } catch (Exception e) {
            logger.error("Error finding book by Google Books ID {}: {}", googleBooksId, e.getMessage(), e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<CachedBook> findByIsbn10(String isbn10) {
        if (!redisCacheService.isRedisAvailableAsync().join() || isbn10 == null) {
            return Optional.empty();
        }

        try {
            // First check the index
            Optional<String> bookIdOpt = redisCacheService.getCachedStringAsync(getIsbn10IndexKey(isbn10)).join();
            if (bookIdOpt.isPresent()) {
                return findById(bookIdOpt.get());
            }

            // Fallback: scan all cached books
            return scanAllCachedBooks().stream()
                    .filter(book -> isbn10.equals(book.getIsbn10()))
                    .findFirst();
        } catch (Exception e) {
            logger.error("Error finding book by ISBN-10 {}: {}", isbn10, e.getMessage(), e);
            return Optional.empty();
        }
    }

    @Override
    public Optional<CachedBook> findByIsbn13(String isbn13) {
        if (!redisCacheService.isRedisAvailableAsync().join() || isbn13 == null) {
            return Optional.empty();
        }

        try {
            // First check the index
            Optional<String> bookIdOpt = redisCacheService.getCachedStringAsync(getIsbn13IndexKey(isbn13)).join();
            if (bookIdOpt.isPresent()) {
                return findById(bookIdOpt.get());
            }

            // Fallback: scan all cached books
            return scanAllCachedBooks().stream()
                    .filter(book -> isbn13.equals(book.getIsbn13()))
                    .findFirst();
        } catch (Exception e) {
            logger.error("Error finding book by ISBN-13 {}: {}", isbn13, e.getMessage(), e);
            return Optional.empty();
        }
    }

    @Override
    public List<CachedBook> findSimilarBooksById(String bookId, int limit) {
        if (!redisCacheService.isRedisAvailableAsync().join() || bookId == null) {
            return Collections.emptyList();
        }

        try {
            // Attempt vector similarity using embedding if available; fallback to attribute matching
            Optional<CachedBook> targetBook = findById(bookId);
            if (!targetBook.isPresent()) {
                return Collections.emptyList();
            }
            CachedBook target = targetBook.get();
            RedisVector targetVec = target.getEmbedding();
            if (targetVec != null && targetVec.getDimension() > 0) {
                // Compute cosine similarity for all candidates
                return scanAllCachedBooks().stream()
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
            // Fallback: simple similarity based on shared categories/authors
            return scanAllCachedBooks().stream()
                    .filter(book -> !bookId.equals(book.getId()))
                    .filter(book -> hasSimilarAttributes(target, book))
                    .limit(limit)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("Error finding similar books for ID {}: {}", bookId, e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    private boolean hasSimilarAttributes(CachedBook target, CachedBook candidate) {
        // Simple similarity based on shared categories or authors
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
     * Migrate book data format without deleting any existing data
     * This method only updates/repairs data structure issues, never deletes
     */
    public int migrateBookDataFormat() {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            return 0;
        }
        
        int migrated = 0;
        String bookPattern = "book:*";
        
        logger.info("Starting safe book data format migration...");
        
        try {
            String cursor = "0";
            ScanParams scanParams = new ScanParams().match(bookPattern).count(100);
            
            do {
                ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                
                for (String key : scanResult.getResult()) {
                    try {
                        // Check if this is a book key (not search or other data)
                        String keyType = jedisPooled.type(key);
                        if ("string".equals(keyType)) {
                            // Try to parse as book JSON to confirm it's book data
                            String content = jedisPooled.get(key);
                            if (content != null && (content.contains("\"googleBooksId\"") || content.contains("\"title\""))) {
                                // Validate and potentially repair the JSON structure
                                try {
                                    CachedBook book = objectMapper.readValue(content, CachedBook.class);
                                    if (book.getId() != null && !book.getId().trim().isEmpty()) {
                                        // Data is valid - just log it exists
                                        logger.debug("Found valid book data at key: {}", key);
                                        migrated++; // Count as "handled" rather than migrated
                                    }
                                } catch (JsonProcessingException e) {
                                    logger.warn("Found malformed JSON at key {} - consider manual review: {}", key, e.getMessage());
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Error processing key {}: {}", key, e.getMessage());
                    }
                }
            } while (!"0".equals(cursor));
            
            if (migrated > 0) {
                logger.info("Processed {} book data entries - no migration needed, data is valid", migrated);
            } else {
                logger.info("No book data found requiring migration");
            }
            
        } catch (Exception e) {
            logger.error("Error during format migration: {}", e.getMessage(), e);
        }
        
        return migrated;
    }

    @Override
    public <S extends CachedBook> CompletableFuture<S> saveAsync(S entity) {
        return CompletableFuture.supplyAsync(() -> save(entity));
    }

    @Override
    public <S extends CachedBook> S save(S entity) {
        if (!redisCacheService.isRedisAvailableAsync().join() || entity == null) { // Allow entity.getId() to be null initially
            return entity;
        }

        // Ensure the entity has a valid UUIDv7 ID
        if (entity.getId() == null || entity.getId().trim().isEmpty() || !UuidUtil.isUuid(entity.getId())) {
            String originalId = entity.getId();
            entity.setId(UuidUtil.getTimeOrderedEpoch().toString());
            logger.info("Generated new UUIDv7 {} for CachedBook. Original ID was: '{}'", entity.getId(), originalId);
        }

        String bookKey = getCachedBookKey(entity.getId()); // Now entity.getId() is guaranteed to be a UUIDv7
        String lockKey = bookKey + ":lock";
        
        // Acquire distributed lock to prevent race conditions
        boolean acquired = false;
        try {
            // Try to acquire lock with exponential backoff
            int maxRetries = 10;
            long baseDelay = 50; // Start with 50ms
            for (int i = 0; i < maxRetries; i++) {
                String result = jedisPooled.set(lockKey, "locked", SetParams.setParams().nx().ex(10));
                acquired = "OK".equals(result);
                if (acquired) break;
                long delay = Math.min(baseDelay * (1L << i), 1000); // Cap at 1 second
                Thread.sleep(delay);
            }
            
            if (!acquired) {
                logger.warn("Could not acquire lock for book key: {}, proceeding without lock", bookKey);
            }
            
            // Proceed with save operation
            return performSave(entity, bookKey);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while acquiring lock for book {}: {}", entity.getId(), e.getMessage());
            return entity;
        } finally {
            // Release lock
            if (acquired) {
                jedisPooled.del(lockKey);
            }
        }
    }
    
    private <S extends CachedBook> S performSave(S entity, String bookKey) {
        try {
            // Store the main entity as a JSON string for consistency
            String bookJson = objectMapper.writeValueAsString(entity);
            
            // Use standard Redis string storage instead of RedisJSON to avoid format conflicts
            jedisPooled.setex(bookKey, cacheExpirationSeconds, bookJson);

            // Clean up stale indexes when updating a book
            try {
                Optional<CachedBook> previousOpt = findById(entity.getId());
                if (previousOpt.isPresent()) {
                    CachedBook previous = previousOpt.get();
                    if (previous.getGoogleBooksId() != null && !previous.getGoogleBooksId().equals(entity.getGoogleBooksId())) {
                        jedisPooled.del(getGoogleBooksIdIndexKey(previous.getGoogleBooksId()));
                    }
                    if (previous.getIsbn10() != null && !previous.getIsbn10().equals(entity.getIsbn10())) {
                        jedisPooled.del(getIsbn10IndexKey(previous.getIsbn10()));
                    }
                    if (previous.getIsbn13() != null && !previous.getIsbn13().equals(entity.getIsbn13())) {
                        jedisPooled.del(getIsbn13IndexKey(previous.getIsbn13()));
                    }
                }
            } catch (Exception e) {
                logger.debug("Could not retrieve previous version for cleanup: {}", e.getMessage());
            }

            // Update indexes
            if (entity.getGoogleBooksId() != null) {
                redisCacheService.cacheStringAsync(getGoogleBooksIdIndexKey(entity.getGoogleBooksId()), entity.getId()).join();
            }
            if (entity.getIsbn10() != null) {
                redisCacheService.cacheStringAsync(getIsbn10IndexKey(entity.getIsbn10()), entity.getId()).join();
            }
            if (entity.getIsbn13() != null) {
                redisCacheService.cacheStringAsync(getIsbn13IndexKey(entity.getIsbn13()), entity.getId()).join();
            }

            logger.debug("Saved CachedBook with ID: {}", entity.getId());
            return entity;
        } catch (JsonProcessingException e) {
            logger.error("Error serializing CachedBook {}: {}", entity.getId(), e.getMessage(), e);
            return entity;
        } catch (Exception e) {
            logger.error("Error saving CachedBook {}: {}", entity.getId(), e.getMessage(), e);
            return entity;
        }
    }

    @Override
    public <S extends CachedBook> Iterable<S> saveAll(Iterable<S> entities) {
        List<S> result = new ArrayList<>();
        for (S entity : entities) {
            result.add(save(entity));
        }
        return result;
    }

    @Override
    public CompletableFuture<Optional<CachedBook>> findByIdAsync(String id) {
        return CompletableFuture.supplyAsync(() -> findById(id));
    }

    @Override
    public Optional<CachedBook> findById(String id) {
        if (!redisCacheService.isRedisAvailableAsync().join() || id == null) {
            return Optional.empty();
        }
        String bookKey = getCachedBookKey(id);
        
        try {
            // Check if key exists first
            if (!jedisPooled.exists(bookKey)) {
                return Optional.empty();
            }
            
            // Prevent reading lock keys as book data
            if (bookKey.endsWith(":lock")) {
                logger.debug("Skipping lock key: {}", bookKey);
                return Optional.empty();
            }
            
            // Read as string first (our preferred format)
            String bookJson = jedisPooled.get(bookKey);
            if (bookJson != null) {
                
                // Validate it's actually JSON and not a lock value
                if (bookJson.equals("locked") || bookJson.length() < 10) {
                    logger.debug("Skipping non-book data for key: {}", bookKey);
                    return Optional.empty();
                }
                
                try {
                    CachedBook book = objectMapper.readValue(bookJson, CachedBook.class);
                    logger.debug("Successfully read book {} as string", id);
                    return Optional.of(book);
                } catch (JsonProcessingException e) {
                    logger.error("Key {} contains invalid JSON data, deleting corrupted entry. Error: {}", 
                               bookKey, e.getMessage());
                    jedisPooled.del(bookKey);
                    return Optional.empty();
                }
            }
            
            // Fallback: try RedisJSON if string read failed
            return readAsRedisJsonFallback(bookKey, id);
            
        } catch (redis.clients.jedis.exceptions.JedisDataException jde) {
            if (jde.getMessage() != null && jde.getMessage().startsWith("WRONGTYPE")) {
                logger.warn("WRONGTYPE error for key {}, cleaning up", bookKey);
                try {
                    jedisPooled.del(bookKey);
                    logger.info("Deleted key with wrong type: {}", bookKey);
                } catch (Exception deleteEx) {
                    logger.error("Failed to delete wrong type key {}: {}", bookKey, deleteEx.getMessage());
                }
            } else {
                logger.error("Jedis data exception for key {}: {}", bookKey, jde.getMessage());
            }
            return Optional.empty();
        } catch (Exception e) {
            logger.error("Error finding CachedBook by ID {}: {}", id, e.getMessage(), e);
            return Optional.empty();
        }
    }
    
    private Optional<CachedBook> readAsRedisJsonFallback(String bookKey, String id) {
        try {
            // Try RedisJSON path syntax first
            Object jsonResult = jedisPooled.jsonGet(bookKey, Path2.of("$"));
            if (jsonResult == null) {
                // Fall back to root path
                jsonResult = jedisPooled.jsonGet(bookKey);
            }
            
            if (jsonResult != null) {
                String bookJsonString;
                
                // Handle different response types from RedisJSON
                if (jsonResult instanceof String) {
                    bookJsonString = (String) jsonResult;
                } else if (jsonResult instanceof java.util.List) {
                    // RedisJSON path queries return arrays
                    java.util.List<?> resultList = (java.util.List<?>) jsonResult;
                    if (!resultList.isEmpty() && resultList.get(0) instanceof String) {
                        bookJsonString = (String) resultList.get(0);
                    } else {
                        logger.warn("RedisJSON returned non-string list for key {}: {}", bookKey, resultList);
                        return Optional.empty();
                    }
                } else {
                    // Convert any other type to JSON string
                    bookJsonString = objectMapper.writeValueAsString(jsonResult);
                }
                
                if (bookJsonString != null && !bookJsonString.isEmpty()) {
                    try {
                        CachedBook book = objectMapper.readValue(bookJsonString, CachedBook.class);
                        logger.debug("Successfully read book {} using RedisJSON fallback", id);
                        return Optional.of(book);
                    } catch (JsonProcessingException e) {
                        logger.error("Key {} contains invalid RedisJSON data, deleting corrupted entry. Error: {}", 
                                   bookKey, e.getMessage());
                        jedisPooled.del(bookKey);
                    }
                }
            }
            logger.debug("RedisJSON returned null or empty data for key {}", bookKey);
        } catch (redis.clients.jedis.exceptions.JedisDataException jde) {
            if (jde.getMessage() != null && jde.getMessage().startsWith("WRONGTYPE")) {
                logger.debug("WRONGTYPE error for key {} during RedisJSON read", bookKey);
            } else {
                logger.error("JedisDataException reading RedisJSON for key {}: {}", bookKey, jde.getMessage());
            }
        } catch (Exception e) {
            logger.debug("Unexpected error reading RedisJSON for key {}: {}", bookKey, e.getMessage());
        }
        return Optional.empty();
    }
    
    

    @Override
    public CompletableFuture<Boolean> existsByIdAsync(String id) {
        return CompletableFuture.supplyAsync(() -> existsById(id));
    }

    @Override
    public boolean existsById(String id) {
        return findById(id).isPresent();
    }

    @Override
    public CompletableFuture<Iterable<CachedBook>> findAllAsync() {
        return CompletableFuture.supplyAsync(() -> findAll());
    }

    @Override
    public Iterable<CachedBook> findAll() {
        return scanAllCachedBooks();
    }

    @Override
    public Iterable<CachedBook> findAllById(Iterable<String> ids) {
        List<CachedBook> result = new ArrayList<>();
        for (String id : ids) {
            findById(id).ifPresent(result::add);
        }
        return result;
    }

    @Override
    public CompletableFuture<Long> countAsync() {
        return CompletableFuture.supplyAsync(() -> count());
    }

    @Override
    public long count() {
        return scanAllCachedBooks().size();
    }

    @Override
    public CompletableFuture<Void> deleteByIdAsync(String id) {
        return CompletableFuture.runAsync(() -> deleteById(id));
    }

    @Override
    public void deleteById(String id) {
        if (!redisCacheService.isRedisAvailableAsync().join() || id == null) {
            return;
        }

        try {
            // Remove the main record and associated indexes
            Optional<CachedBook> bookOpt = findById(id);
            if (bookOpt.isPresent()) {
                CachedBook book = bookOpt.get();
                
                jedisPooled.del(getCachedBookKey(id));
                
                // Clean up indexes
                if (book.getGoogleBooksId() != null) {
                    jedisPooled.del(getGoogleBooksIdIndexKey(book.getGoogleBooksId()));
                }
                if (book.getIsbn10() != null) {
                    jedisPooled.del(getIsbn10IndexKey(book.getIsbn10()));
                }
                if (book.getIsbn13() != null) {
                    jedisPooled.del(getIsbn13IndexKey(book.getIsbn13()));
                }
                
                logger.debug("Deleted CachedBook with ID: {}", id);
            }
        } catch (Exception e) {
            logger.error("Error deleting CachedBook {}: {}", id, e.getMessage(), e);
        }
    }

    @Override
    public void delete(CachedBook entity) {
        if (entity != null && entity.getId() != null) {
            deleteById(entity.getId());
        }
    }

    @Override
    public void deleteAllById(Iterable<? extends String> ids) {
        for (String id : ids) {
            deleteById(id);
        }
    }

    @Override
    public void deleteAll(Iterable<? extends CachedBook> entities) {
        for (CachedBook entity : entities) {
            delete(entity);
        }
    }

    @Override
    public void deleteAll() {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            return;
        }

        // WARNING: This method deletes ALL book data from Redis!
        logger.warn("WARNING: deleteAll() will delete ALL cached books from Redis!");
        logger.warn("This operation is irreversible and should only be used for testing or complete cache reset");

        try {
            // This is expensive - scan and delete all cached books
            List<CachedBook> allBooks = scanAllCachedBooks();
            for (CachedBook book : allBooks) {
                deleteById(book.getId());
            }
            logger.info("Deleted all {} cached books from Redis", allBooks.size());
        } catch (Exception e) {
            logger.error("Error deleting all cached books: {}", e.getMessage(), e);
        }
    }

    @Override
    public Set<String> findAllDistinctGoogleBooksIds() {
        return scanAllCachedBooks().stream()
                .map(CachedBook::getGoogleBooksId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @Override
    public List<CachedBook> findByTitleIgnoreCaseAndIdNot(String title, String idToExclude) {
        if (title == null || idToExclude == null) {
            return Collections.emptyList();
        }
        // This can be optimized with RediSearch if title is indexed appropriately
        return scanAllCachedBooks().stream()
                .filter(book -> !idToExclude.equals(book.getId()))
                .filter(book -> book.getTitle() != null && 
                              book.getTitle().toLowerCase().equals(title.toLowerCase()))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<CachedBook> findBySlug(String slug) {
        if (!redisCacheService.isRedisAvailableAsync().join() || slug == null || slug.trim().isEmpty()) {
            return Optional.empty();
        }
        // Using RediSearch idx:books which has $.slug as TAG SORTABLE
        // Query for exact match on slug: @slug:{escaped_slug_value}
        // Ensure slug value is properly escaped for RediSearch query
        // For TAG fields, exact match is usually done by escaping spaces and other separators.
        // Since slugs are already hyphenated and lowercased, complex escaping might not be needed
        // if the TAG SEPARATOR in RediSearch is the default (',').
        // However, if slugs can contain characters that are special to RediSearch query syntax (like '-'),
        // they should be escaped.
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
                .limit(0, 1); // We only need one result, no scores needed by default

        try {
            logger.debug("Executing RediSearch query on idx:books for slug query: @slug:{{{}}}", escapedSlug);
            SearchResult searchResult = jedisPooled.ftSearch("idx:books", query);
            if (searchResult.getTotalResults() > 0) {
                Document doc = searchResult.getDocuments().get(0);
                String json = doc.getString("$"); // Get the root JSON object
                if (json != null) {
                    try {
                        CachedBook book = objectMapper.readValue(json, CachedBook.class);
                        return Optional.of(book);
                    } catch (JsonProcessingException e) {
                        logger.error("Invalid JSON data found in RediSearch result for slug '{}', Error: {}, Data: {}", 
                                   slug, e.getMessage(), 
                                   json.length() > 100 ? json.substring(0, 100) + "..." : json);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error finding book by slug '{}' using RediSearch: {}", slug, e.getMessage(), e);
        }
        return Optional.empty();
    }

    private List<CachedBook> scanAllCachedBooks() {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            logger.debug("Redis not available, skipping scanAllCachedBooks.");
            return Collections.emptyList();
        }

        List<CachedBook> books = new ArrayList<>();
        Set<String> corruptedKeys = new HashSet<>();
        String scanPattern = CACHED_BOOK_PREFIX + "*";
        logger.debug("Initiating SCAN with pattern: {}", scanPattern);
        try {
            Set<String> keys = new HashSet<>();
            
            // Use JedisPooled scan method
            String cursor = "0";
            ScanParams scanParams = new ScanParams().match(scanPattern).count(1000);
            
            try {
                do {
                    ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);
                    cursor = scanResult.getCursor();
                    keys.addAll(scanResult.getResult());
                } while (!"0".equals(cursor));
                
                logger.debug("SCAN found {} raw keys matching pattern {}.", keys.size(), scanPattern);
            } catch (Exception e) {
                logger.error("Error during Redis SCAN operation with pattern {}: {}", scanPattern, e.getMessage(), e);
                // Depending on desired behavior, you might rethrow or return empty/partial list
            }

            if (!keys.isEmpty()) {
                logger.debug("Processing {} keys found by SCAN.", keys.size());
                for (String key : keys) {
                    // Ensure the key actually belongs to this repository's prefix, as SCAN can return other keys if not careful with patterns
                    if (key.startsWith(CACHED_BOOK_PREFIX)) {
                         String bookId = key.substring(CACHED_BOOK_PREFIX.length());
                         try {
                             Optional<CachedBook> bookOpt = findById(bookId);
                             if (bookOpt.isPresent()) {
                                 books.add(bookOpt.get());
                             }
                         } catch (redis.clients.jedis.exceptions.JedisDataException jde) {
                             if (jde.getMessage() != null && jde.getMessage().startsWith("WRONGTYPE")) {
                                 logger.warn("WRONGTYPE error for book ID {} (key: {}): {}", bookId, key, jde.getMessage());
                             } else {
                                 logger.error("Jedis data exception for book ID {} (key: {}): {}", bookId, key, jde.getMessage());
                             }
                             corruptedKeys.add(key);
                         } catch (Exception e) {
                             logger.error("Error processing book ID {} during scan: {}", bookId, e.getMessage());
                             corruptedKeys.add(key);
                         }
                    } else {
                        logger.warn("SCAN returned a key '{}' that does not match the expected prefix '{}'. Skipping.", key, CACHED_BOOK_PREFIX);
                    }
                }
            }
            
            // Clean up corrupted keys if any were found
            if (!corruptedKeys.isEmpty()) {
                logger.warn("Found {} corrupted keys during scan. Attempting cleanup.", corruptedKeys.size());
                for (String corruptedKey : corruptedKeys) {
                    try {
                        jedisPooled.del(corruptedKey);
                        logger.info("Deleted corrupted key: {}", corruptedKey);
                    } catch (Exception e) {
                        logger.error("Failed to delete corrupted key {}: {}", corruptedKey, e.getMessage());
                    }
                }
            }
            
             logger.debug("Finished processing keys from SCAN. Found {} CachedBook objects.", books.size());
        } catch (Exception e) {
            logger.error("Error scanning all cached books with pattern {}: {}", scanPattern, e.getMessage(), e);
        }
        return books;
    }
    
    /**
     * Diagnostic method to check for corrupted data in Redis
     * @return Map with statistics about data integrity
     */
    public Map<String, Integer> diagnoseCacheIntegrity() {
        Map<String, Integer> stats = new HashMap<>();
        stats.put("total_keys", 0);
        stats.put("valid_json", 0);
        stats.put("valid_redisJson", 0);
        stats.put("corrupted", 0);
        stats.put("cleaned_up", 0);
        
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            logger.warn("Redis not available for cache integrity diagnosis");
            return stats;
        }
        
        String scanPattern = CACHED_BOOK_PREFIX + "*";
        try {
            Set<String> keys = new HashSet<>();
            
            // Use JedisPooled scan method
            String cursor = "0";
            ScanParams scanParams = new ScanParams().match(scanPattern).count(1000);
            
            do {
                ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                keys.addAll(scanResult.getResult());
            } while (!"0".equals(cursor));
            
            stats.put("total_keys", keys.size());
            logger.info("Diagnosing {} cached book keys", keys.size());
            
            for (String key : keys) {
                if (key.startsWith(CACHED_BOOK_PREFIX)) {
                    try {
                        // Check data type
                        String actualTypeString = jedisPooled.type(key);
                        
                        boolean isValid = false;
                        if ("ReJSON-RL".equalsIgnoreCase(actualTypeString) || "json".equalsIgnoreCase(actualTypeString)) {
                            // Try to read as RedisJSON
                            try {
                                Object jsonResult = jedisPooled.jsonGet(key);
                                if (jsonResult != null && isValidJson(jsonResult.toString())) {
                                    stats.put("valid_redisJson", stats.get("valid_redisJson") + 1);
                                    isValid = true;
                                }
                            } catch (Exception e) {
                                logger.debug("RedisJSON read failed for key {}: {}", key, e.getMessage());
                            }
                        } else if ("string".equalsIgnoreCase(actualTypeString)) {
                            // Try to read as string
                            try {
                                String value = jedisPooled.get(key);
                                if (value != null && isValidJson(value)) {
                                    stats.put("valid_json", stats.get("valid_json") + 1);
                                    isValid = true;
                                }
                            } catch (Exception e) {
                                logger.debug("String read failed for key {}: {}", key, e.getMessage());
                            }
                        }
                        
                        if (!isValid) {
                            stats.put("corrupted", stats.get("corrupted") + 1);
                            logger.warn("Found corrupted key: {} (type: {})", key, actualTypeString);
                        }
                        
                    } catch (Exception e) {
                        stats.put("corrupted", stats.get("corrupted") + 1);
                        logger.error("Error diagnosing key {}: {}", key, e.getMessage());
                    }
                }
            }
            
        } catch (Exception e) {
            logger.error("Error during cache integrity diagnosis: {}", e.getMessage(), e);
        }
        
        return stats;
    }
    
    
    /**
     * Repair corrupted cache entries without deleting any data
     * @param dryRun If true, only logs what would be repaired without actually repairing
     * @return Number of keys repaired
     */
    public int repairCorruptedCache(boolean dryRun) {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            logger.warn("Redis not available for cache repair");
            return 0;
        }
        
        int repaired = 0;
        String scanPattern = CACHED_BOOK_PREFIX + "*";
        
        logger.info("Starting cache repair scan. Dry run: {}", dryRun);
        long startTime = System.currentTimeMillis();
        
        try {
            Set<String> keys = new HashSet<>();
            
            // Use JedisPooled scan method to avoid connection thrashing
            String cursor = "0";
            ScanParams scanParams = new ScanParams().match(scanPattern).count(1000);
            
            do {
                ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                keys.addAll(scanResult.getResult());
            } while (!"0".equals(cursor));
            
            logger.info("Scanned {} cache keys in {}ms", keys.size(), System.currentTimeMillis() - startTime);
            
            Set<String> keysToRepair = new HashSet<>();
            int processed = 0;
            int validKeys = 0;
            
            logger.info("Analyzing {} keys for repair needs...", keys.size());
            
            for (String key : keys) {
                if (key.startsWith(CACHED_BOOK_PREFIX)) {
                    processed++;
                    boolean needsRepair = false;
                    
                    try {
                        // Check if the key data can be read and parsed
                        String actualTypeString = jedisPooled.type(key).toLowerCase();
                        
                        if ("string".equalsIgnoreCase(actualTypeString)) {
                            try {
                                String value = jedisPooled.get(key);
                                if (value != null && isValidJson(value)) {
                                    // Try to parse as CachedBook to validate structure
                                    CachedBook book = objectMapper.readValue(value, CachedBook.class);
                                    if (book.getId() == null || book.getId().trim().isEmpty()) {
                                        needsRepair = true; // Missing required ID field
                                    }
                                } else {
                                    needsRepair = true; // Invalid JSON
                                }
                            } catch (Exception e) {
                                needsRepair = true; // Parse error
                            }
                        } else if ("none".equalsIgnoreCase(actualTypeString)) {
                            // Key doesn't exist - skip silently
                            continue;
                        } else {
                            // Other types (JSON, etc.) - assume valid if they exist
                            validKeys++;
                            continue;
                        }
                        
                        if (needsRepair) {
                            keysToRepair.add(key);
                        } else {
                            validKeys++;
                        }
                        
                        // Progress logging every 100 keys
                        if (processed % 100 == 0) {
                            logger.info("Progress: {}/{} keys analyzed", processed, keys.size());
                        }
                        
                    } catch (Exception e) {
                        logger.debug("Error analyzing key {}: {}", key, e.getMessage());
                        // Do NOT mark for repair if we can't analyze it safely
                    }
                }
            }
            
            int keysNeedingRepair = keysToRepair.size();
            logger.info("Analysis complete: {} valid, {} need repair out of {} total keys", 
                       validKeys, keysNeedingRepair, processed);
            
            // Attempt to repair keys with issues (but NEVER delete them)
            if (!keysToRepair.isEmpty()) {
                if (!dryRun) {
                    logger.info("Attempting to repair {} keys with issues...", keysToRepair.size());
                    repaired = repairKeys(keysToRepair);
                    logger.info("Successfully repaired {} keys", repaired);
                } else {
                    logger.info("DRY RUN: Would attempt to repair {} keys", keysToRepair.size());
                    repaired = keysNeedingRepair; // For dry run, return count of keys that would be repaired
                }
            } else {
                logger.info("No keys need repair - cache is healthy");
            }
            
        } catch (Exception e) {
            logger.error("Error during cache repair: {}", e.getMessage(), e);
        }
        
        long totalTime = System.currentTimeMillis() - startTime;
        logger.info("Cache repair completed in {}ms. {} keys {}", 
                   totalTime, repaired, dryRun ? "identified for repair (dry run)" : "repaired");
        return repaired;
    }

    private boolean isValidJson(String jsonString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            return false;
        }
        try {
            objectMapper.readTree(jsonString);
            return true;
        } catch (JsonProcessingException e) {
            return false;
        }
    }

    /**
     * Safely repair keys with data issues - NEVER deletes data
     * @param keysToRepair Set of keys that need repair
     * @return Number of keys successfully repaired
     */
    private int repairKeys(Set<String> keysToRepair) {
        if (keysToRepair.isEmpty()) return 0;
        
        int repairedCount = 0;
        
        for (String key : keysToRepair) {
            try {
                String content = jedisPooled.get(key);
                if (content != null) {
                    // Try to parse and repair the JSON structure
                    try {
                        CachedBook book = objectMapper.readValue(content, CachedBook.class);
                        
                        // Check if key fields are missing and can be repaired
                        boolean needsUpdate = false;
                        
                        if (book.getId() == null || book.getId().trim().isEmpty()) {
                            // Try to extract ID from the key name
                            String potentialId = key.replace(CACHED_BOOK_PREFIX, "");
                            if (!potentialId.isEmpty()) {
                                book.setId(potentialId);
                                needsUpdate = true;
                            }
                        }
                        
                        if (needsUpdate) {
                            // Save the repaired book back to Redis
                            String repairedJson = objectMapper.writeValueAsString(book);
                            jedisPooled.set(key, repairedJson);
                            repairedCount++;
                            logger.debug("Repaired book data at key: {}", key);
                        }
                        
                    } catch (JsonProcessingException e) {
                        logger.warn("Could not repair malformed JSON at key {}: {}", key, e.getMessage());
                        // Do NOT delete - just log and skip
                    }
                } else {
                    logger.warn("Key {} exists but has null content - skipping repair", key);
                }
            } catch (Exception e) {
                logger.error("Error repairing key {}: {}", key, e.getMessage());
            }
        }
        
        return repairedCount;
    }

    
    /**
     * @return List of cached books matching the criteria
     */
    public List<CachedBook> findRandomRecentBooksWithGoodCovers(int count, Set<String> excludeIds) {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            logger.warn("Redis not available for findRandomRecentBooksWithGoodCovers");
            return Collections.emptyList();
        }

        try {
            // Try RediSearch first, but note that our index may not match the schema prefix
            List<CachedBook> searchResults = tryRedisSearchQuery(count, excludeIds);
            if (!searchResults.isEmpty()) {
                logger.info("Found {} books using RediSearch", searchResults.size());
                return searchResults;
            }
            
            // RediSearch failed or returned empty results, use fallback scan method
            logger.info("RediSearch query returned no results, using fallback scan method");
            return findRandomRecentBooksFallback(count, excludeIds);
            
        } catch (Exception e) {
            logger.error("Error in findRandomRecentBooksWithGoodCovers: {}", e.getMessage(), e);
            // Fallback to scanning approach if RediSearch fails
            return findRandomRecentBooksFallback(count, excludeIds);
        }
    }
    
    /**
     * Try to use RediSearch to find recent books with good covers
     * 
     * @param count Number of books to find
     * @param excludeIds Set of book IDs to exclude
     * @return List of books found via RediSearch, empty if search fails
     */
    private List<CachedBook> tryRedisSearchQuery(int count, Set<String> excludeIds) {
        try {
            // Note: Our cached books use "book:" prefix to match schema
            // Try both idx:books and idx:cached_books_vector indexes
            
            // Get current and next year dynamically
            int currentYear = LocalDate.now().getYear();
            int nextYear = currentYear + 1;
            String query = String.format("(@publishedDate:{%d} | @publishedDate:{%d}) @coverImageUrl:(*google*zoom\\=2* | *s3* | *digitalocean* | *cloudfront* | *openlibrary*-L.jpg*)", 
                                        currentYear, nextYear);
            
            // Exclude placeholder covers
            query += " -@coverImageUrl:(*placeholder* | *image-not-available*)";
            
            Query searchQuery = new Query(query)
                .limit(0, Math.max(count * 3, 100)); // Get more than needed for filtering
            
            // Try the main books index first
            try {
                SearchResult searchResult = jedisPooled.ftSearch("idx:books", searchQuery);
                List<CachedBook> results = processSearchResults(searchResult, count, excludeIds, "book:");
                if (!results.isEmpty()) {
                    return results;
                }
            } catch (Exception e) {
                logger.debug("idx:books search failed: {}", e.getMessage());
            }
            
            // Try the vector index as fallback
            try {
                SearchResult searchResult = jedisPooled.ftSearch("idx:cached_books_vector", searchQuery);
                List<CachedBook> results = processSearchResults(searchResult, count, excludeIds, "book:");
                if (!results.isEmpty()) {
                    return results;
                }
            } catch (Exception e) {
                logger.debug("idx:cached_books_vector search failed: {}", e.getMessage());
            }
            
        } catch (Exception e) {
            logger.warn("RediSearch query failed: {}", e.getMessage());
        }
        
        return Collections.emptyList();
    }
    
    /**
     * Process search results from RediSearch
     * 
     * @param searchResult RediSearch result
     * @param count Number of books needed
     * @param excludeIds Set of book IDs to exclude
     * @param keyPrefix Expected key prefix in search results
     * @return List of processed books
     */
    private List<CachedBook> processSearchResults(SearchResult searchResult, int count, Set<String> excludeIds, String keyPrefix) {
        List<CachedBook> candidateBooks = new ArrayList<>();
        
        for (Document doc : searchResult.getDocuments()) {
            try {
                // Extract book ID from document key
                String docKey = doc.getId();
                String bookId;
                
                if (docKey.startsWith(keyPrefix)) {
                    bookId = docKey.substring(keyPrefix.length());
                } else if (docKey.startsWith(CACHED_BOOK_PREFIX)) {
                    bookId = docKey.substring(CACHED_BOOK_PREFIX.length());
                } else {
                    bookId = docKey; // Use as-is if no recognized prefix
                }
                
                // Skip excluded books
                if (excludeIds != null && excludeIds.contains(bookId)) {
                    continue;
                }
                
                // Try to find the book in our cache
                Optional<CachedBook> bookOpt = findById(bookId);
                if (bookOpt.isPresent()) {
                    CachedBook book = bookOpt.get();
                    
                    // Additional validation for our specific requirements
                    if (isRecentPublication(book) && hasHighQualityCover(book) && !isBestseller(book)) {
                        candidateBooks.add(book);
                    }
                }
            } catch (Exception e) {
                logger.debug("Error processing search result document {}: {}", doc.getId(), e.getMessage());
            }
        }
        
        // Shuffle for randomness and limit to requested count
        Collections.shuffle(candidateBooks);
        List<CachedBook> result = candidateBooks.stream()
            .limit(count)
            .collect(Collectors.toList());
        
        logger.debug("RediSearch found {} candidates, returning {} books", candidateBooks.size(), result.size());
        return result;
    }
    
    /**
     * Fallback method to find recent books by scanning the cache
     * Used when RediSearch is unavailable or configured incorrectly
     * 
     * @param count Number of books to find
     * @param excludeIds Set of book IDs to exclude
     * @return List of books found via cache scanning
     */
    private List<CachedBook> findRandomRecentBooksFallback(int count, Set<String> excludeIds) {
        logger.info("Using fallback method for finding random recent books");
        
        try {
            List<CachedBook> candidateBooks = new ArrayList<>();
            
            // Scan through cached books with pagination to avoid memory issues
            int maxScan = 2000; // Increased limit to find more candidates
            int scannedCount = 0;
            int validCandidates = 0;
            
            String cursor = "0";
            ScanParams scanParams = new ScanParams().match(CACHED_BOOK_PREFIX + "*").count(200);
            
            do {
                ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                
                for (String key : scanResult.getResult()) {
                    if (scannedCount >= maxScan || candidateBooks.size() >= count * 3) {
                        break;
                    }
                    
                    String bookId = key.replace(CACHED_BOOK_PREFIX, "");
                    scannedCount++;
                    
                    // Skip excluded books (recently viewed and bestsellers)
                    if (excludeIds != null && excludeIds.contains(bookId)) {
                        continue;
                    }
                    
                    try {
                        Optional<CachedBook> bookOpt = findById(bookId);
                        if (bookOpt.isPresent()) {
                            CachedBook book = bookOpt.get();
                            
                            // Apply all filters: recent publication, good covers, not bestsellers
                            if (isRecentPublication(book) && hasHighQualityCover(book) && !isBestseller(book)) {
                                candidateBooks.add(book);
                                validCandidates++;
                            }
                        }
                    } catch (Exception e) {
                        logger.debug("Error processing book {}: {}", bookId, e.getMessage());
                    }
                    
                    // Log progress periodically
                    if (scannedCount % 500 == 0) {
                        logger.debug("Fallback scan progress: {}/{} scanned, {} valid candidates found", 
                                   scannedCount, maxScan, validCandidates);
                    }
                }
            } while (!"0".equals(cursor) && scannedCount < maxScan && candidateBooks.size() < count * 3);
            
            // Shuffle and return requested count
            Collections.shuffle(candidateBooks);
            List<CachedBook> result = candidateBooks.stream()
                .limit(count)
                .collect(Collectors.toList());
            
            logger.info("Fallback scan completed: {}/{} books found from {} scanned ({} valid candidates)", 
                       result.size(), count, scannedCount, validCandidates);
            return result;
            
        } catch (Exception e) {
            logger.error("Error in fallback method: {}", e.getMessage(), e);
            return Collections.emptyList();
        }
    }
    
    private boolean isRecentPublication(CachedBook book) {
        if (book.getPublishedDate() == null) {
            return false;
        }
        
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
    
    private boolean hasHighQualityCover(CachedBook book) {
        String coverUrl = book.getCoverImageUrl();
        if (coverUrl == null || coverUrl.isEmpty()) {
            return false;
        }
        
        // Exclude placeholder images
        if (coverUrl.contains("placeholder") || coverUrl.contains("image-not-available")) {
            return false;
        }
        
        // High quality Google Books images (zoom=2 or higher)
        if (coverUrl.contains("books.google.com") && coverUrl.contains("zoom=")) {
            try {
                int zoomIndex = coverUrl.indexOf("zoom=") + 5;
                if (zoomIndex < coverUrl.length()) {
                    char zoomChar = coverUrl.charAt(zoomIndex);
                    int zoom = Character.getNumericValue(zoomChar);
                    return zoom >= 2;
                }
            } catch (Exception e) {
                // Default to true for Google Books if can't parse
                return true;
            }
        }
        
        // S3/CDN images are generally good quality
        if (coverUrl.contains("s3.amazonaws.com") || coverUrl.contains("digitaloceanspaces.com") || 
            coverUrl.contains("cloudfront.net") || coverUrl.startsWith("/book-covers/")) {
            return true;
        }
        
        // OpenLibrary large images
        if (coverUrl.contains("openlibrary.org") && coverUrl.contains("-L.jpg")) {
            return true;
        }
        
        // Default to false for unknown sources to be conservative
        return false;
    }
    
    /**
     * Check if a book is marked as a bestseller
     * 
     * @param book The book to check
     * @return true if the book is a bestseller, false otherwise
     */
    private boolean isBestseller(CachedBook book) {
        if (book.getQualifiers() == null) {
            return false;
        }
        
        // Check for NY Times bestseller qualification
        Object bestsellerQualifier = book.getQualifiers().get("new york times bestseller");
        if (bestsellerQualifier != null) {
            // If it's a boolean, check its value
            if (bestsellerQualifier instanceof Boolean) {
                return (Boolean) bestsellerQualifier;
            }
            // If it's a string, check if it indicates bestseller status
            if (bestsellerQualifier instanceof String) {
                String qualifierStr = ((String) bestsellerQualifier).toLowerCase();
                return qualifierStr.contains("bestseller") || qualifierStr.equals("true");
            }
            // If the qualifier exists and is not false/null, assume it's a bestseller
            return true;
        }
        
        // Check other common bestseller indicators in qualifiers
        for (Map.Entry<String, Object> entry : book.getQualifiers().entrySet()) {
            String key = entry.getKey().toLowerCase();
            if (key.contains("bestseller") || key.contains("best seller") || key.contains("nyt")) {
                Object value = entry.getValue();
                if (value instanceof Boolean) {
                    return (Boolean) value;
                } else if (value != null) {
                    return true; // Non-null value for bestseller-related key
                }
            }
        }
        
        return false;
    }

    @Override
    public List<CachedBook> findRandomRecentBooksWithGoodCovers(int count, Set<String> excludeIds, int fromYear) {
        // Delegate to existing two-arg implementation
        return findRandomRecentBooksWithGoodCovers(count, excludeIds);
    }

    @Override
    public CompletableFuture<List<CachedBook>> findRandomRecentBooksWithGoodCoversAsync(int count, Set<String> excludeIds, int fromYear) {
        // Wrap synchronous call in a future
        return CompletableFuture.completedFuture(findRandomRecentBooksWithGoodCovers(count, excludeIds, fromYear));
    }
}
