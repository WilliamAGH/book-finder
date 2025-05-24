/**
 * Primary implementation of CachedBookRepository using Redis storage
 * Orchestrates specialized Redis services for comprehensive book data management
 * Provides transaction-safe operations with distributed locking and index maintenance
 *
 * @author William Callahan
 *
 * Features:
 * - Multi-service architecture with dedicated components for access, indexing, search, and maintenance
 * - Distributed locking for concurrent write safety using Redis-based locks
 * - Automatic UUID generation with time-ordered UUIDv7 for new entities
 * - Comprehensive secondary indexing for ISBN and Google Books ID lookups
 */

package com.williamcallahan.book_recommendation_engine.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.config.RedisEnvironmentCondition;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.service.RedisCacheService;
import com.williamcallahan.book_recommendation_engine.util.UuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.SetParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Repository("redisCachedBookRepositoryImpl")
@Primary
@Profile("!test")
@Conditional(RedisEnvironmentCondition.class)
public class RedisCachedBookRepositoryImpl implements CachedBookRepository {

    private static final Logger logger = LoggerFactory.getLogger(RedisCachedBookRepositoryImpl.class);
    private static final long DEFAULT_BOOK_TTL_SECONDS = 24 * 60 * 60;

    private final RedisBookAccessor bookAccessor;
    private final RedisBookIndexManager indexManager;
    private final RedisBookSearchService searchService;
    private final RedisBookMaintenanceService maintenanceService;
    private final RedisCacheService redisCacheService;
    private final JedisPooled jedisPooled;

    public RedisCachedBookRepositoryImpl(RedisBookAccessor bookAccessor,
                                         RedisBookIndexManager indexManager,
                                         RedisBookSearchService searchService,
                                         RedisBookMaintenanceService maintenanceService,
                                         RedisCacheService redisCacheService,
                                         JedisPooled jedisPooled,
                                         ObjectMapper objectMapper) {
        this.bookAccessor = bookAccessor;
        this.indexManager = indexManager;
        this.searchService = searchService;
        this.maintenanceService = maintenanceService;
        this.redisCacheService = redisCacheService;
        this.jedisPooled = jedisPooled;
        logger.info("RedisCachedBookRepositoryImpl initialized, orchestrating specialized Redis services");
    }

    /**
     * Constructs Redis key with book prefix for locking operations
     *
     * @param bookId unique book identifier
     * @return formatted Redis key with prefix
     */
    private String getCachedBookKeyWithPrefix(String bookId) {
        return "book:" + bookId;
    }
    

    @Override
    public <S extends CachedBook> CompletableFuture<S> saveAsync(S entity) {
        return redisCacheService.isRedisAvailableAsync()
            .thenCompose(isAvailable -> {
                if (!isAvailable || entity == null) {
                    return CompletableFuture.completedFuture(entity);
                }

                boolean isNewEntity = entity.getId() == null || entity.getId().trim().isEmpty() || !UuidUtil.isUuid(entity.getId());
                String originalId = entity.getId();

                if (isNewEntity) {
                    entity.setId(UuidUtil.getTimeOrderedEpoch().toString());
                    logger.info("Generated new UUIDv7 {} for CachedBook. Original ID was: '{}'", entity.getId(), originalId);
                }
                
                String bookId = entity.getId();
                String bookKey = getCachedBookKeyWithPrefix(bookId);
                String lockKey = bookKey + ":lock";

                return acquireLockAsync(lockKey)
                    .thenCompose(lockAcquired -> {
                        CompletableFuture<Optional<CachedBook>> oldBookFuture;
                        if (!isNewEntity) {
                            oldBookFuture = CompletableFuture.supplyAsync(() -> 
                                bookAccessor.findJsonByIdWithRedisJsonFallback(bookId)
                                           .flatMap(bookAccessor::deserializeBook)
                            );
                        } else {
                            oldBookFuture = CompletableFuture.completedFuture(Optional.empty());
                        }

                        return oldBookFuture.thenCompose(oldBookOpt -> {
                            return CompletableFuture.supplyAsync(() -> {
                                try {
                                    String bookJson = bookAccessor.serializeBook(entity);
                                    if (bookJson == null) {
                                        logger.error("Failed to serialize book with ID: {}. Save operation aborted", bookId);
                                        return entity;
                                    }
                                    bookAccessor.saveJson(bookId, bookJson, DEFAULT_BOOK_TTL_SECONDS);
                                    indexManager.updateAllIndexes(entity, oldBookOpt);

                                    logger.debug("Saved CachedBook with ID: {}", bookId);
                                    return entity;
                                } catch (Exception e) {
                                    logger.error("Error saving CachedBook {}: {}", bookId, e.getMessage(), e);
                                    return entity;
                                }
                            }).whenComplete((result, throwable) -> {
                                if (lockAcquired) {
                                    releaseLockAsync(lockKey);
                                }
                            });
                        });
                    });
            });
    }

    private CompletableFuture<Boolean> acquireLockAsync(String lockKey) {
        return CompletableFuture.supplyAsync(() -> {
            for (int i = 0; i < 50; i++) {
                try {
                    String result = jedisPooled.set(lockKey, "locked", SetParams.setParams().nx().ex(10));
                    if ("OK".equals(result)) {
                        return true;
                    }
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Interrupted while acquiring lock for key: {}", lockKey);
                    return false;
                } catch (Exception e) {
                    logger.error("Error acquiring lock for key {}: {}", lockKey, e.getMessage());
                    return false;
                }
            }
            logger.warn("Could not acquire lock for key: {}, proceeding without lock (risk of race condition)", lockKey);
            return false;
        });
    }

    private CompletableFuture<Void> releaseLockAsync(String lockKey) {
        return CompletableFuture.runAsync(() -> {
            try {
                jedisPooled.del(lockKey);
            } catch (Exception e) {
                logger.error("Error releasing lock for key {}: {}", lockKey, e.getMessage());
            }
        });
    }

    @Override
    @Deprecated
    public <S extends CachedBook> S save(S entity) {
        logger.warn("Using deprecated synchronous save() method. Consider migrating to saveAsync() for better performance.");
        return saveAsync(entity).join();
    }

    @Override
    public <S extends CachedBook> Iterable<S> saveAll(Iterable<S> entities) {
        List<S> result = new ArrayList<>();
        if (entities != null) {
            for (S entity : entities) {
                result.add(save(entity));
            }
        }
        return result;
    }

    @Override
    public CompletableFuture<Optional<CachedBook>> findByIdAsync(String id) {
        return redisCacheService.isRedisAvailableAsync()
            .thenCompose(isAvailable -> {
                if (!isAvailable || id == null) {
                    return CompletableFuture.completedFuture(Optional.empty());
                }
                return CompletableFuture.supplyAsync(() -> 
                    bookAccessor.findJsonByIdWithRedisJsonFallback(id)
                               .flatMap(bookAccessor::deserializeBook)
                );
            });
    }

    @Override
    @Deprecated
    public Optional<CachedBook> findById(String id) {
        logger.warn("Using deprecated synchronous findById() method. Consider migrating to findByIdAsync() for better performance.");
        return findByIdAsync(id).join();
    }

    @Override
    public CompletableFuture<Boolean> existsByIdAsync(String id) {
        return redisCacheService.isRedisAvailableAsync()
            .thenCompose(isAvailable -> {
                if (!isAvailable || id == null) {
                    return CompletableFuture.completedFuture(false);
                }
                return CompletableFuture.supplyAsync(() -> bookAccessor.exists(id));
            });
    }

    @Override
    @Deprecated
    public boolean existsById(String id) {
        logger.warn("Using deprecated synchronous existsById() method. Consider migrating to existsByIdAsync() for better performance.");
        return existsByIdAsync(id).join();
    }

    @Override
    public Iterable<CachedBook> findAll() {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            return Collections.emptyList();
        }
        return bookAccessor.scanAndDeserializeAllBooks();
    }

    @Override
    public Iterable<CachedBook> findAllById(Iterable<String> ids) {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            return Collections.emptyList();
        }
        List<CachedBook> result = new ArrayList<>();
        if (ids != null) {
            for (String id : ids) {
                findById(id).ifPresent(result::add);
            }
        }
        return result;
    }

    @Override
    public long count() {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            return 0L;
        }
        return bookAccessor.countAllBooks();
    }

    @Override
    public void deleteById(String id) {
        if (!redisCacheService.isRedisAvailableAsync().join() || id == null) {
            return;
        }
        Optional<CachedBook> bookOpt = findById(id);
        if (bookOpt.isPresent()) {
            bookAccessor.deleteJsonById(id);
            indexManager.deleteAllIndexes(bookOpt.get());
            logger.debug("Deleted CachedBook and its indexes for ID: {}", id);
        } else {
            logger.debug("Book with ID {} not found for deletion.", id);
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
        if (ids != null) {
            for (String id : ids) {
                deleteById(id);
            }
        }
    }

    @Override
    public void deleteAll(Iterable<? extends CachedBook> entities) {
        if (entities != null) {
            for (CachedBook entity : entities) {
                delete(entity);
            }
        }
    }

    @Override
    public void deleteAll() {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            return;
        }
        logger.warn("deleteAll() invoked: This will delete ALL cached books and their indexes from Redis");
        List<CachedBook> allBooks = bookAccessor.scanAndDeserializeAllBooks();
        for (CachedBook book : allBooks) {
            bookAccessor.deleteJsonById(book.getId());
            indexManager.deleteAllIndexes(book);
        }
        logger.info("Deleted all {} cached books and their indexes from Redis", allBooks.size());
    }

    @Override
    public Optional<CachedBook> findByGoogleBooksId(String googleBooksId) {
        if (!redisCacheService.isRedisAvailableAsync().join() || googleBooksId == null) {
            return Optional.empty();
        }
        return indexManager.getBookIdByGoogleBooksId(googleBooksId)
                           .flatMap(this::findById);
    }

    @Override
    public Optional<CachedBook> findByIsbn10(String isbn10) {
        if (!redisCacheService.isRedisAvailableAsync().join() || isbn10 == null) {
            return Optional.empty();
        }
        return indexManager.getBookIdByIsbn10(isbn10)
                           .flatMap(this::findById);
    }

    @Override
    public Optional<CachedBook> findByIsbn13(String isbn13) {
        if (!redisCacheService.isRedisAvailableAsync().join() || isbn13 == null) {
            return Optional.empty();
        }
        return indexManager.getBookIdByIsbn13(isbn13)
                           .flatMap(this::findById);
    }
    
    @Override
    public List<CachedBook> findSimilarBooksById(String bookId, int limit) {
        if (!redisCacheService.isRedisAvailableAsync().join() || bookId == null) {
            return Collections.emptyList();
        }
        return searchService.findSimilarBooksById(bookId, limit);
    }

    @Override
    public List<CachedBook> findByTitleIgnoreCaseAndIdNot(String title, String idToExclude) {
        if (!redisCacheService.isRedisAvailableAsync().join() || title == null || idToExclude == null) {
            return Collections.emptyList();
        }
        return searchService.findByTitleIgnoreCaseAndIdNot(title, idToExclude);
    }
    
    @Override
    public Optional<CachedBook> findBySlug(String slug) {
        if (!redisCacheService.isRedisAvailableAsync().join() || slug == null || slug.trim().isEmpty()) {
            return Optional.empty();
        }
        return searchService.findBySlug(slug);
    }

    @Override
    public List<CachedBook> findRandomRecentBooksWithGoodCovers(int count, Set<String> excludeIds) {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            return Collections.emptyList();
        }
        return searchService.findRandomRecentBooksWithGoodCovers(count, excludeIds);
    }
    
    @Override
    public Set<String> findAllDistinctGoogleBooksIds() {
        if (!redisCacheService.isRedisAvailableAsync().join()) {
            return Collections.emptySet();
        }
        return bookAccessor.scanAndDeserializeAllBooks().stream()
                .map(CachedBook::getGoogleBooksId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }


    /**
     * Performs diagnostic analysis of cache integrity
     * Delegates to maintenance service for comprehensive data health assessment
     *
     * @return statistics map with integrity analysis results
     */
    public Map<String, Integer> diagnoseCacheIntegrity() {
        return maintenanceService.diagnoseCacheIntegrity();
    }

    /**
     * Repairs corrupted cache entries using non-destructive methods
     * Delegates to maintenance service for safe data repair operations
     *
     * @param dryRun when true, performs analysis without making changes
     * @return count of entries repaired or identified for repair
     */
    public int repairCorruptedCache(boolean dryRun) {
        return maintenanceService.repairCorruptedCache(dryRun);
    }
    
    /**
     * Migrates book data to current format without data loss
     * Delegates to maintenance service for schema evolution support
     *
     * @return count of entries processed during migration
     */
    public int migrateBookDataFormat() {
        return maintenanceService.migrateBookDataFormat();
    }
}
