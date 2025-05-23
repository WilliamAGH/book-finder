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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Collectors;

@Repository
@Primary
@ConditionalOnExpression("'${spring.data.redis.url:}' != '' or '${spring.data.redis.host:}' != ''")
public class RedisCachedBookRepository implements CachedBookRepository {

    private static final Logger logger = LoggerFactory.getLogger(RedisCachedBookRepository.class);
    private static final String CACHED_BOOK_PREFIX = "cached_book:";
    private static final String ISBN10_INDEX_PREFIX = "isbn10_idx:";
    private static final String ISBN13_INDEX_PREFIX = "isbn13_idx:";
    private static final String GOOGLE_BOOKS_ID_INDEX_PREFIX = "gbid_idx:";

    private final RedisCacheService redisCacheService;
    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;

    public RedisCachedBookRepository(RedisCacheService redisCacheService, 
                                   StringRedisTemplate stringRedisTemplate,
                                   ObjectMapper objectMapper) {
        this.redisCacheService = redisCacheService;
        this.stringRedisTemplate = stringRedisTemplate;
        this.objectMapper = objectMapper;
        logger.info("Redis CachedBookRepository initialized - using Redis for book storage");
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
        if (!redisCacheService.isRedisAvailable() || googleBooksId == null) {
            return Optional.empty();
        }

        try {
            // First check the index to get the actual book ID
            Optional<String> bookIdOpt = redisCacheService.getCachedString(getGoogleBooksIdIndexKey(googleBooksId));
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
        if (!redisCacheService.isRedisAvailable() || isbn10 == null) {
            return Optional.empty();
        }

        try {
            // First check the index
            Optional<String> bookIdOpt = redisCacheService.getCachedString(getIsbn10IndexKey(isbn10));
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
        if (!redisCacheService.isRedisAvailable() || isbn13 == null) {
            return Optional.empty();
        }

        try {
            // First check the index
            Optional<String> bookIdOpt = redisCacheService.getCachedString(getIsbn13IndexKey(isbn13));
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
        if (!redisCacheService.isRedisAvailable() || bookId == null) {
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

    @Override
    public <S extends CachedBook> S save(S entity) {
        if (!redisCacheService.isRedisAvailable() || entity == null || entity.getId() == null) {
            return entity;
        }

        try {
            String bookJson = objectMapper.writeValueAsString(entity);
            redisCacheService.cacheString(getCachedBookKey(entity.getId()), bookJson);

            // Update indexes
            if (entity.getGoogleBooksId() != null) {
                redisCacheService.cacheString(getGoogleBooksIdIndexKey(entity.getGoogleBooksId()), entity.getId());
            }
            if (entity.getIsbn10() != null) {
                redisCacheService.cacheString(getIsbn10IndexKey(entity.getIsbn10()), entity.getId());
            }
            if (entity.getIsbn13() != null) {
                redisCacheService.cacheString(getIsbn13IndexKey(entity.getIsbn13()), entity.getId());
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
    public Optional<CachedBook> findById(String id) {
        if (!redisCacheService.isRedisAvailable() || id == null) {
            return Optional.empty();
        }

        try {
            Optional<String> bookJsonOpt = redisCacheService.getCachedString(getCachedBookKey(id));
            if (bookJsonOpt.isPresent()) {
                CachedBook book = objectMapper.readValue(bookJsonOpt.get(), CachedBook.class);
                return Optional.of(book);
            }
            return Optional.empty();
        } catch (JsonProcessingException e) {
            logger.error("Error deserializing CachedBook {}: {}", id, e.getMessage(), e);
            return Optional.empty();
        } catch (Exception e) {
            logger.error("Error finding CachedBook by ID {}: {}", id, e.getMessage(), e);
            return Optional.empty();
        }
    }

    @Override
    public boolean existsById(String id) {
        return findById(id).isPresent();
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
    public long count() {
        return scanAllCachedBooks().size();
    }

    @Override
    public void deleteById(String id) {
        if (!redisCacheService.isRedisAvailable() || id == null) {
            return;
        }

        try {
            // Remove the main record and associated indexes
            Optional<CachedBook> bookOpt = findById(id);
            if (bookOpt.isPresent()) {
                CachedBook book = bookOpt.get();
                
                stringRedisTemplate.delete(getCachedBookKey(id));
                
                // Clean up indexes
                if (book.getGoogleBooksId() != null) {
                    stringRedisTemplate.delete(getGoogleBooksIdIndexKey(book.getGoogleBooksId()));
                }
                if (book.getIsbn10() != null) {
                    stringRedisTemplate.delete(getIsbn10IndexKey(book.getIsbn10()));
                }
                if (book.getIsbn13() != null) {
                    stringRedisTemplate.delete(getIsbn13IndexKey(book.getIsbn13()));
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
        if (!redisCacheService.isRedisAvailable()) {
            return;
        }

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

        return scanAllCachedBooks().stream()
                .filter(book -> !idToExclude.equals(book.getId()))
                .filter(book -> book.getTitle() != null && 
                              book.getTitle().toLowerCase().equals(title.toLowerCase()))
                .collect(Collectors.toList());
    }

    private List<CachedBook> scanAllCachedBooks() {
        if (!redisCacheService.isRedisAvailable()) {
            return Collections.emptyList();
        }

        List<CachedBook> books = new ArrayList<>();
        try {
            Set<String> keys = stringRedisTemplate.keys(CACHED_BOOK_PREFIX + "*");
            if (keys != null) {
                for (String key : keys) {
                    String bookId = key.substring(CACHED_BOOK_PREFIX.length());
                    findById(bookId).ifPresent(books::add);
                }
            }
        } catch (Exception e) {
            logger.error("Error scanning all cached books: {}", e.getMessage(), e);
        }
        return books;
    }
}
