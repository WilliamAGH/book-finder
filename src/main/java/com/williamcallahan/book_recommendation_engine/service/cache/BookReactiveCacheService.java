/**
 * Reactive service for handling book caching operations with non-blocking I/O
 * This service provides reactive programming support for cache operations using Project Reactor
 * It handles:
 * - Non-blocking book retrieval and storage operations
 * - Reactive search functionality with back-pressure support
 * - Asynchronous cache update and invalidation operations
 * - Integration with reactive data sources and external APIs
 * - Streaming of book data for high-throughput scenarios
 * - Reactive error handling and circuit breaker patterns
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.DuplicateBookService;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksCachingStrategy;
import com.williamcallahan.book_recommendation_engine.service.RedisCacheService;
import com.williamcallahan.book_recommendation_engine.types.PgVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import com.github.benmanes.caffeine.cache.Cache;
import java.util.stream.Collectors;

@Service
public class BookReactiveCacheService {

    private static final Logger logger = LoggerFactory.getLogger(BookReactiveCacheService.class);
    private static final String BOOK_SEARCH_RESULTS_CACHE_NAME = "bookSearchResults";

    private final GoogleBooksCachingStrategy googleBooksCachingStrategy;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final RedisCacheService redisCacheService;
    private final CacheManager cacheManager;
    private final ObjectMapper objectMapper;
    private final DuplicateBookService duplicateBookService;
    private final CachedBookRepository cachedBookRepository;
    private final Cache<String, Book> bookDetailCache;
    private final WebClient embeddingClient;
    private final boolean embeddingServiceEnabled;
    
    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled;

    @Value("${app.embedding.service.url:#{null}}")
    private String embeddingServiceUrl;

    public BookReactiveCacheService(
            GoogleBooksCachingStrategy googleBooksCachingStrategy,
            BookDataOrchestrator bookDataOrchestrator,
            RedisCacheService redisCacheService,
            CacheManager cacheManager,
            ObjectMapper objectMapper,
            DuplicateBookService duplicateBookService,
            @Autowired(required = false) CachedBookRepository cachedBookRepository,
            Cache<String, Book> bookDetailCache,
            WebClient.Builder webClientBuilder,
            @Value("${app.feature.embedding-service.enabled:false}") boolean embeddingServiceEnabled
    ) {
        this.googleBooksCachingStrategy = googleBooksCachingStrategy;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.redisCacheService = redisCacheService;
        this.cacheManager = cacheManager;
        this.objectMapper = objectMapper;
        this.duplicateBookService = duplicateBookService;
        this.cachedBookRepository = cachedBookRepository;
        this.bookDetailCache = bookDetailCache;
        this.embeddingServiceEnabled = embeddingServiceEnabled;

        this.embeddingClient = webClientBuilder.baseUrl(
            this.embeddingServiceUrl != null ? this.embeddingServiceUrl : "http://localhost:8080/api/embedding"
        ).build();
        
        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false;
            logger.info("BookReactiveCacheService: Database cache (CachedBookRepository) is not available.");
        }
    }

    public Mono<Book> getBookByIdReactive(String id) {
        Book cachedBookInMemory = bookDetailCache.getIfPresent(id);
        if (cachedBookInMemory != null) {
            logger.info("In-memory cache hit for book ID: {}", id);
            return Mono.just(cachedBookInMemory);
        }

        logger.info("In-memory cache miss for book ID: {}, delegating to GoogleBooksCachingStrategy.", id);
        return googleBooksCachingStrategy.getReactive(id)
            .doOnSuccess(book -> {
                if (book != null) {
                    bookDetailCache.put(id, book);
                    logger.info("BookReactiveCacheService.getBookByIdReactive: Successfully retrieved book ID {} via strategy.", id);
                } else {
                    logger.info("BookReactiveCacheService.getBookByIdReactive: Book ID {} not found via strategy.", id);
                }
            })
            .onErrorResume(e -> {
                logger.error("Error in BookReactiveCacheService.getBookByIdReactive for ID {} from strategy: {}", id, e.getMessage(), e);
                return Mono.empty();
            });
    }

    public Mono<Book> getBookByIdReactiveFromCacheOnly(String id) {
        if (id == null || id.isEmpty()) {
            return Mono.empty();
        }
        return googleBooksCachingStrategy.getReactiveFromCacheOnly(id)
            .doOnSuccess(book -> {
                if (book != null) {
                    logger.debug("BookReactiveCacheService.getBookByIdReactiveFromCacheOnly: Cache hit for book ID {}.", id);
                } else {
                    logger.debug("BookReactiveCacheService.getBookByIdReactiveFromCacheOnly: Cache miss for book ID {}.", id);
                }
            })
            .onErrorResume(e -> {
                logger.error("Error in BookReactiveCacheService.getBookByIdReactiveFromCacheOnly for ID {}: {}", id, e.getMessage(), e);
                return Mono.empty();
            });
    }

    public Mono<List<Book>> getBooksByIsbnReactive(String isbn) {
        if (cacheEnabled && cachedBookRepository != null) {
            Mono<Optional<CachedBook>> cachedBookMono;
            if (isbn.length() == 10) {
                cachedBookMono = Mono.fromCallable(() -> cachedBookRepository.findByIsbn10(isbn));
            } else if (isbn.length() == 13) {
                cachedBookMono = Mono.fromCallable(() -> cachedBookRepository.findByIsbn13(isbn));
            } else {
                logger.warn("Invalid ISBN format: {}", isbn);
                return Mono.just(Collections.emptyList());
            }

            return cachedBookMono.subscribeOn(Schedulers.boundedElastic())
                .flatMap(cachedBookOpt -> {
                    if (cachedBookOpt.isPresent()) {
                        logger.info("DB Cache hit for book ISBN: {}", isbn);
                        return Mono.just(Collections.singletonList(cachedBookOpt.get().toBook()));
                    }
                    logger.info("DB Cache miss for book ISBN: {}, fetching via searchBooksReactive.", isbn);
                    return searchBooksReactive("isbn:" + isbn, 0, 5, null, null, "relevance");
                })
                .onErrorResume(e -> {
                    logger.warn("Error accessing database cache for ISBN {}: {}. Falling back to searchBooksReactive.", isbn, e.getMessage());
                    return searchBooksReactive("isbn:" + isbn, 0, 5, null, null, "relevance");
                });
        }
        logger.info("DB Cache disabled, fetching ISBN: {} via searchBooksReactive.", isbn);
        return searchBooksReactive("isbn:" + isbn, 0, 5, null, null, "relevance");
    }

    public Mono<List<Book>> searchBooksReactive(String query, int startIndex, int maxResults, Integer publishedYear, String langCode, String orderBy) {
        String cacheKey = generateSearchCacheKey(query, startIndex, maxResults, publishedYear, langCode, orderBy);
        org.springframework.cache.Cache searchCache = this.cacheManager.getCache(BOOK_SEARCH_RESULTS_CACHE_NAME);

        if (searchCache != null) {
            @SuppressWarnings("unchecked")
            List<String> cachedBookIds = (List<String>) searchCache.get(cacheKey, List.class);
            if (cachedBookIds != null) {
                logger.info("Search cache HIT for key: {}", cacheKey);
                if (cachedBookIds.isEmpty()) {
                    return Mono.just(Collections.emptyList());
                }
                return Flux.fromIterable(cachedBookIds)
                           .flatMap(this::getBookByIdReactive)
                           .filter(Objects::nonNull)
                           .collectList();
            }
        }

        logger.info("Search cache MISS for key: {}. Delegating to BookDataOrchestrator.", cacheKey);
        return fetchProcessAndCacheSearch(query, startIndex, maxResults, publishedYear, langCode, orderBy, cacheKey, searchCache);
    }

    private Mono<List<Book>> fetchProcessAndCacheSearch(String query, int startIndex, int maxResults, Integer publishedYear, String langCode, String orderBy, String cacheKey, org.springframework.cache.Cache searchCache) {
        final String effectiveOrderBy = (orderBy != null && !orderBy.trim().isEmpty()) ? orderBy : "relevance";
        int orchestratorDesiredResults = (publishedYear != null) ? 200 : Math.max(40, startIndex + maxResults * 2);
        orchestratorDesiredResults = Math.min(orchestratorDesiredResults, 200);

        return this.bookDataOrchestrator.searchBooksTiered(query, langCode, orchestratorDesiredResults, effectiveOrderBy)
            .flatMap(fetchedBooksGlobalList -> {
                List<Book> booksToProcess = (fetchedBooksGlobalList == null) ? Collections.emptyList() : fetchedBooksGlobalList;
                logger.debug("Fetched {} books from BookDataOrchestrator for query '{}' before local filtering/pagination.", booksToProcess.size(), query);

                List<Book> yearFilteredBooks = filterBooksByYear(booksToProcess, publishedYear);
                List<Book> finalPaginatedBooks = paginateBooks(yearFilteredBooks, startIndex, maxResults);
                
                cacheSearchResults(searchCache, cacheKey, finalPaginatedBooks);
                
                Mono<Void> cacheIndividualBooksMono = Flux.fromIterable(booksToProcess)
                        .filter(book -> book != null && book.getId() != null)
                        .flatMap(this::cacheBookReactive)
                        .then(); 
                
                return cacheIndividualBooksMono.thenReturn(finalPaginatedBooks);
            })
            .onErrorResume(e -> {
                logger.error("Error during fetchProcessAndCacheSearch for query '{}' (key {}): {}. Returning empty list.", query, cacheKey, e.getMessage(), e);
                if (searchCache != null && this.cacheEnabled) {
                    searchCache.put(cacheKey, Collections.emptyList()); 
                }
                return Mono.just(Collections.emptyList());
            });
    }

    private List<Book> filterBooksByYear(List<Book> books, Integer publishedYear) {
        if (publishedYear == null) {
            return books;
        }
        return books.stream()
            .filter(book -> {
                if (book != null && book.getPublishedDate() != null) {
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(book.getPublishedDate());
                    return cal.get(Calendar.YEAR) == publishedYear;
                }
                return false;
            })
            .collect(Collectors.toList());
    }

    private List<Book> paginateBooks(List<Book> books, int startIndex, int maxResults) {
        if (books.isEmpty()) {
            return Collections.emptyList();
        }
        int from = Math.min(startIndex, books.size());
        int to = Math.min(startIndex + maxResults, books.size());
        return (from >= to) ? Collections.emptyList() : books.subList(from, to);
    }

    private void cacheSearchResults(org.springframework.cache.Cache searchCache, String cacheKey, List<Book> booksToCacheIdsFor) {
        if (searchCache != null && this.cacheEnabled) {
            List<String> bookIdsToCache = booksToCacheIdsFor.stream()
                                                          .map(Book::getId)
                                                          .filter(Objects::nonNull)
                                                          .collect(Collectors.toList());
            searchCache.put(cacheKey, bookIdsToCache);
            logger.info("Cached search key '{}' with {} book IDs.", cacheKey, bookIdsToCache.size());
        }
    }

    private String generateSearchCacheKey(String query, int startIndex, int maxResults, Integer publishedYear, String langCode, String orderBy) {
        String year = (publishedYear == null) ? "null" : publishedYear.toString();
        String lc = (langCode == null || langCode.trim().isEmpty()) ? "null" : langCode.trim().toLowerCase();
        String ob = (orderBy == null || orderBy.trim().isEmpty()) ? "default" : orderBy.trim().toLowerCase();
        
        return String.format("q:%s_s:%d_m:%d_y:%s_l:%s_o:%s", 
            query, startIndex, maxResults, year, lc, ob);
    }

    public Mono<Void> cacheBookReactive(Book book) { 
        if (!cacheEnabled || cachedBookRepository == null || book == null || book.getId() == null) {
            return Mono.empty();
        }

        Optional<CachedBook> primaryBookOpt = duplicateBookService.findPrimaryCanonicalBook(book);

        if (primaryBookOpt.isPresent()) {
            final CachedBook primaryCachedBook = primaryBookOpt.get();
            logger.info("Found existing primary book (ID: {}) for new book (Title: {}). Will not create new cache entry. Attempting rich merge.",
                        primaryCachedBook.getId(), book.getTitle());
            
            return performRichMergeAndUpdate(primaryCachedBook, book)
                .flatMap(wasUpdated -> {
                    if (Boolean.TRUE.equals(wasUpdated)) { 
                        return Mono.fromCallable(() -> cachedBookRepository.save(primaryCachedBook))
                            .subscribeOn(Schedulers.boundedElastic())
                            .doOnSuccess(savedBook -> 
                                logger.info("Updated primary cached book ID: {} with data from new book via rich merge.", savedBook.getId())
                            )
                            .doOnError(e -> logger.error("Error updating primary cached book ID {} after rich merge: {}", primaryCachedBook.getId(), e.getMessage()))
                            .flatMap(savedBook -> 
                                redisCacheService.cacheBookReactive(savedBook.getGoogleBooksId(), savedBook.toBook())
                            )
                            .then();
                    } else {
                        logger.debug("No data from new book was merged into primary book ID: {} via rich merge.", primaryCachedBook.getId());
                        return Mono.empty();
                    }
                });
        } else { 
            logger.debug("No existing primary book found for new book (Title: {}). Caching as new entry.", book.getTitle());
            return Mono.defer(() -> 
                Mono.fromCallable(() -> cachedBookRepository.findByGoogleBooksId(book.getId()))
                    .subscribeOn(Schedulers.boundedElastic())
                    .flatMap(existingOpt -> {
                        if (existingOpt.isPresent()) {
                            final CachedBook existingCachedBook = existingOpt.get();
                            logger.info("Book with Google ID {} already in cache (found by direct ID lookup). Attempting rich merge.", book.getId());
                            return performRichMergeAndUpdate(existingCachedBook, book)
                                .flatMap(wasUpdated -> {
                                    if (Boolean.TRUE.equals(wasUpdated)) {
                                        return Mono.fromCallable(() -> cachedBookRepository.save(existingCachedBook))
                                            .subscribeOn(Schedulers.boundedElastic())
                                            .doOnSuccess(savedBook ->
                                                logger.info("Updated existing cached book (ID: {}) via direct ID lookup and rich merge.", savedBook.getId())
                                            )
                                            .flatMap(savedBook -> 
                                                redisCacheService.cacheBookReactive(savedBook.getGoogleBooksId(), savedBook.toBook())
                                            )
                                            .then();
                                    }
                                    return Mono.empty();
                                });
                        }
                        return generateEmbeddingReactive(book)
                            .flatMap(embedding -> {
                                try {
                                    String rawJsonString = book.getRawJsonResponse();
                                    if (rawJsonString == null || rawJsonString.isEmpty()) {
                                        logger.warn("Book ID {} for new cache entry has no rawJsonResponse. Serializing Book object itself.", book.getId());
                                        rawJsonString = objectMapper.writeValueAsString(book);
                                    }
                                    JsonNode rawJsonNode = objectMapper.readTree(rawJsonString);
                                    CachedBook cachedBookToSave = CachedBook.fromBook(book, rawJsonNode, new PgVector(embedding));
                                    return Mono.fromCallable(() -> cachedBookRepository.save(cachedBookToSave))
                                        .subscribeOn(Schedulers.boundedElastic())
                                        .doOnSuccess(savedBook -> 
                                            logger.info("Successfully cached new book ID: {} Title: {}", savedBook.getId(), savedBook.getTitle())
                                        )
                                        .doOnError(e -> logger.error("Error saving new book ID {} to cache: {}", book.getId(), e.getMessage()))
                                        .flatMap(savedBook -> 
                                            redisCacheService.cacheBookReactive(savedBook.getGoogleBooksId(), savedBook.toBook())
                                        )
                                        .then();
                                } catch (Exception e) {
                                    logger.error("Error processing/creating CachedBook for new entry ID {}: {}", book.getId(), e.getMessage());
                                    return Mono.error(e);
                                }
                            });
                    })
                    .onErrorResume(e -> { 
                        logger.error("Error during caching process for new book ID {}: {}", book.getId(), e.getMessage());
                        return Mono.empty(); 
                    })
                    .then()
            );
        }
    }
    
    private Mono<float[]> generateEmbeddingReactive(Book book) {
        try {
            StringBuilder textBuilder = new StringBuilder();
            textBuilder.append(book.getTitle() != null ? book.getTitle() : "").append(" ");
            if (book.getAuthors() != null && !book.getAuthors().isEmpty()) {
                textBuilder.append(String.join(" ", book.getAuthors())).append(" ");
            }
            if (book.getDescription() != null) {
                textBuilder.append(book.getDescription()).append(" ");
            }
            if (book.getCategories() != null && !book.getCategories().isEmpty()) {
                textBuilder.append(String.join(" ", book.getCategories()));
            }
            String text = textBuilder.toString().trim();

            if (text.isEmpty()) {
                logger.warn("Cannot generate embedding for book ID {} as constructed text is empty.", book.getId());
                return Mono.just(new float[384]);
            }

            if (this.embeddingServiceEnabled && this.embeddingServiceUrl != null) {
                return embeddingClient.post()
                    .bodyValue(Collections.singletonMap("text", text))
                    .retrieve()
                    .bodyToMono(float[].class)
                    .onErrorResume(e -> {
                        logger.warn("Error generating embedding from service for book ID {}: {}. Falling back to placeholder.", book.getId(), e.getMessage());
                        return Mono.just(createPlaceholderEmbedding(text));
                    });
            }
            logger.debug("Embedding service is disabled or URL not configured. Using placeholder embedding for book ID {}.", book.getId());
            return Mono.just(createPlaceholderEmbedding(text));
        } catch (Exception e) {
            logger.error("Unexpected error in generateEmbeddingReactive for book ID {}: {}", book.getId(), e.getMessage(), e);
            return Mono.just(new float[384]);
        }
    }

    private float[] createPlaceholderEmbedding(String text) {
        float[] placeholder = new float[384];
        if (text == null || text.isEmpty()) return placeholder;
        int hash = text.hashCode();
        for (int i = 0; i < placeholder.length; i++) {
            placeholder[i] = (float) Math.sin(hash * (i + 1) / 100.0);
        }
        return placeholder;
    }

    private Mono<Boolean> performRichMergeAndUpdate(CachedBook primaryCachedBook, Book newBookData) {
        return Mono.fromCallable(() -> {
            boolean modified = false;
            if (newBookData.getTitle() != null && !newBookData.getTitle().equals(primaryCachedBook.getTitle())) {
                primaryCachedBook.setTitle(newBookData.getTitle());
                modified = true;
            }
            if (newBookData.getAuthors() != null && !Objects.equals(newBookData.getAuthors(), primaryCachedBook.getAuthors())) {
                primaryCachedBook.setAuthors(new ArrayList<>(newBookData.getAuthors()));
                modified = true;
            }
            if (newBookData.getDescription() != null) {
                if (primaryCachedBook.getDescription() == null || newBookData.getDescription().length() > primaryCachedBook.getDescription().length()) {
                    primaryCachedBook.setDescription(newBookData.getDescription());
                    modified = true;
                }
            }
            if (newBookData.getCoverImageUrl() != null && !newBookData.getCoverImageUrl().equals(primaryCachedBook.getCoverImageUrl())) {
                primaryCachedBook.setCoverImageUrl(newBookData.getCoverImageUrl());
                modified = true;
            }
            if (primaryCachedBook.getIsbn10() == null && newBookData.getIsbn10() != null) {
                primaryCachedBook.setIsbn10(newBookData.getIsbn10());
                modified = true;
            }
            if (primaryCachedBook.getIsbn13() == null && newBookData.getIsbn13() != null) {
                primaryCachedBook.setIsbn13(newBookData.getIsbn13());
                modified = true;
            }
            if (newBookData.getPublishedDate() != null) {
                LocalDateTime newLdt = newBookData.getPublishedDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
                if (primaryCachedBook.getPublishedDate() == null || newLdt.isAfter(primaryCachedBook.getPublishedDate())) {
                     primaryCachedBook.setPublishedDate(newLdt);
                     modified = true;
                }
            }
            if (newBookData.getCategories() != null && !Objects.equals(newBookData.getCategories(), primaryCachedBook.getCategories())) {
                primaryCachedBook.setCategories(new ArrayList<>(newBookData.getCategories()));
                modified = true;
            }
            if (newBookData.getAverageRating() != null && (primaryCachedBook.getAverageRating() == null || (newBookData.getRatingsCount() != null && primaryCachedBook.getRatingsCount() != null && newBookData.getRatingsCount() > primaryCachedBook.getRatingsCount()))) {
                primaryCachedBook.setAverageRating(BigDecimal.valueOf(newBookData.getAverageRating()));
                primaryCachedBook.setRatingsCount(newBookData.getRatingsCount());
                modified = true;
            } else if (newBookData.getAverageRating() != null && primaryCachedBook.getAverageRating() == null) {
                 primaryCachedBook.setAverageRating(BigDecimal.valueOf(newBookData.getAverageRating()));
                 primaryCachedBook.setRatingsCount(newBookData.getRatingsCount());
                 modified = true;
            }
            if (newBookData.getPageCount() != null && !newBookData.getPageCount().equals(primaryCachedBook.getPageCount())) {
                primaryCachedBook.setPageCount(newBookData.getPageCount());
                modified = true;
            }
            if (newBookData.getLanguage() != null && !newBookData.getLanguage().equals(primaryCachedBook.getLanguage())) {
                primaryCachedBook.setLanguage(newBookData.getLanguage());
                modified = true;
            }
            if (newBookData.getPublisher() != null && !newBookData.getPublisher().equals(primaryCachedBook.getPublisher())) {
                primaryCachedBook.setPublisher(newBookData.getPublisher());
                modified = true;
            }
            if (newBookData.getInfoLink() != null && !newBookData.getInfoLink().equals(primaryCachedBook.getInfoLink())) {
                primaryCachedBook.setInfoLink(newBookData.getInfoLink());
                modified = true;
            }
            if (newBookData.getPreviewLink() != null && !newBookData.getPreviewLink().equals(primaryCachedBook.getPreviewLink())) {
                primaryCachedBook.setPreviewLink(newBookData.getPreviewLink());
                modified = true;
            }
             if (newBookData.getPurchaseLink() != null && !newBookData.getPurchaseLink().equals(primaryCachedBook.getPurchaseLink())) {
                primaryCachedBook.setPurchaseLink(newBookData.getPurchaseLink());
                modified = true;
            }
            if (newBookData.getRawJsonResponse() != null) {
                try {
                    JsonNode newRawJsonNode = objectMapper.readTree(newBookData.getRawJsonResponse());
                    if (primaryCachedBook.getRawData() == null || !primaryCachedBook.getRawData().equals(newRawJsonNode)) {
                        primaryCachedBook.setRawData(newRawJsonNode);
                        modified = true;
                    }
                } catch (Exception e) {
                    logger.warn("Error processing raw JSON during rich merge for book ID {}: {}", primaryCachedBook.getId(), e.getMessage());
                }
            }
            if (newBookData.getCachedRecommendationIds() != null && !Objects.equals(newBookData.getCachedRecommendationIds(), primaryCachedBook.getCachedRecommendationIds())) {
                primaryCachedBook.setCachedRecommendationIds(new ArrayList<>(newBookData.getCachedRecommendationIds()));
                modified = true;
            }
            if (newBookData.getQualifiers() != null && !Objects.equals(newBookData.getQualifiers(), primaryCachedBook.getQualifiers())) {
                primaryCachedBook.setQualifiers(new HashMap<>(newBookData.getQualifiers()));
                modified = true;
            }
            if (newBookData.getOtherEditions() != null && !Objects.equals(newBookData.getOtherEditions(), primaryCachedBook.getOtherEditions())) {
                primaryCachedBook.setOtherEditions(new ArrayList<>(newBookData.getOtherEditions()));
                modified = true;
            }
            if (modified) {
                primaryCachedBook.setLastAccessed(LocalDateTime.now());
            }
            return modified;
        }).subscribeOn(Schedulers.parallel());
    }
}
