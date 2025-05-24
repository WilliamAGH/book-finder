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
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksCachingStrategy;
import com.williamcallahan.book_recommendation_engine.service.RedisCacheService;
import com.williamcallahan.book_recommendation_engine.service.BookSlugService;
import com.williamcallahan.book_recommendation_engine.service.EmbeddingService;
import com.williamcallahan.book_recommendation_engine.util.UuidUtil;
// import com.williamcallahan.book_recommendation_engine.types.RedisVector; // Unused import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
// import org.springframework.web.reactive.function.client.WebClientResponseException; // Unused import
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class BookReactiveCacheService {

    private static final Logger logger = LoggerFactory.getLogger(BookReactiveCacheService.class);
    private static final String BOOK_SEARCH_RESULTS_CACHE_NAME = "bookSearchResults";

    private GoogleBooksCachingStrategy googleBooksCachingStrategy;
    private BookDataOrchestrator bookDataOrchestrator;
    private RedisCacheService redisCacheService;
    private CacheManager cacheManager;
    private ObjectMapper objectMapper;
    private CachedBookRepository cachedBookRepository;
    private BookSlugService bookSlugService;
    private EmbeddingService embeddingService;
    // private final WebClient embeddingClient; // Unused field
    // private final boolean embeddingServiceEnabled; // Unused field
    
    @Value("${app.cache.enabled:true}")
    private boolean cacheEnabled;

    @Value("${app.embedding.service.url:#{null}}")
    private String embeddingServiceUrl;

    // Default constructor for Mockito inline mocking
    public BookReactiveCacheService(
            GoogleBooksCachingStrategy googleBooksCachingStrategy,
            BookDataOrchestrator bookDataOrchestrator,
            RedisCacheService redisCacheService,
            CacheManager cacheManager,
            ObjectMapper objectMapper,
            @Autowired(required = false) CachedBookRepository cachedBookRepository,
            BookSlugService bookSlugService,
            EmbeddingService embeddingService,
            WebClient.Builder webClientBuilder,
            @Value("${app.feature.embedding-service.enabled:false}") boolean embeddingServiceEnabled // Parameter kept for constructor signature, but field is removed
    ) {
        this.googleBooksCachingStrategy = googleBooksCachingStrategy;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.redisCacheService = redisCacheService;
        this.cacheManager = cacheManager;
        this.objectMapper = objectMapper;
        this.cachedBookRepository = cachedBookRepository;
        this.bookSlugService = bookSlugService;
        this.embeddingService = embeddingService;
        // this.embeddingServiceEnabled = embeddingServiceEnabled; // Field removed

        // this.embeddingClient = webClientBuilder.baseUrl( // Field removed
        //     this.embeddingServiceUrl != null ? this.embeddingServiceUrl : "http://localhost:8080/api/embedding"
        // ).build();
        
        if (this.cachedBookRepository == null) {
            this.cacheEnabled = false;
            logger.info("BookReactiveCacheService: Database cache (CachedBookRepository) is not available.");
        }
    }

    public Mono<Book> getBookByIdReactive(String id) {
        // Note: Spring Cache (Caffeine) is handled by @Cacheable on BookCacheFacadeService.getBookByIdReactive()
        // This method delegates to GoogleBooksCachingStrategy which follows the correct cache hierarchy
        logger.info("BookReactiveCacheService.getBookByIdReactive called for ID: {}, delegating to GoogleBooksCachingStrategy.", id);
        return googleBooksCachingStrategy.getReactive(id)
            .doOnSuccess(book -> {
                if (book != null) {
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

        // First check Spring Cache (Caffeine)
        if (searchCache != null) {
            @SuppressWarnings("unchecked")
            List<String> cachedBookIds = (List<String>) searchCache.get(cacheKey, List.class);
            if (cachedBookIds != null) {
                logger.info("Spring search cache HIT for key: {}", cacheKey);
                if (cachedBookIds.isEmpty()) {
                    return Mono.just(Collections.emptyList());
                }
                return Flux.fromIterable(cachedBookIds)
                           .flatMap(this::getBookByIdReactive)
                           .filter(Objects::nonNull)
                           .collectList();
            }
        }

        // Then check Redis cache
        return redisCacheService.getCachedSearchResultsReactive(cacheKey)
            .flatMap(redisBookIds -> {
                logger.info("Redis search cache HIT for key: {}", cacheKey);
                // Also populate Spring cache for faster future access
                if (searchCache != null) {
                    searchCache.put(cacheKey, redisBookIds);
                }
                if (redisBookIds.isEmpty()) {
                    return Mono.just(Collections.<Book>emptyList());
                }
                return Flux.fromIterable(redisBookIds)
                           .flatMap(bookId -> getBookByIdReactive(bookId))
                           .filter(Objects::nonNull)
                           .collectList();
            })
            .switchIfEmpty(
                fetchProcessAndCacheSearch(query, startIndex, maxResults, publishedYear, langCode, orderBy, cacheKey, searchCache)
                    .doFirst(() -> logger.info("All search caches MISS for key: {}. Delegating to BookDataOrchestrator.", cacheKey))
            );
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
                    return book.getPublishedDate().getYear() == publishedYear;
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
        if (this.cacheEnabled) {
            List<String> bookIdsToCache = booksToCacheIdsFor.stream()
                                                          .map(Book::getId)
                                                          .filter(Objects::nonNull)
                                                          .collect(Collectors.toList());
            
            // Cache in Spring Cache (Caffeine)
            if (searchCache != null) {
                searchCache.put(cacheKey, bookIdsToCache);
                logger.info("Cached search key '{}' with {} book IDs in Spring cache.", cacheKey, bookIdsToCache.size());
            }
            
            // Also cache in Redis for persistence across restarts
            redisCacheService.cacheSearchResultsAsync(cacheKey, bookIdsToCache)
                .thenRun(() -> logger.info("Cached search key '{}' with {} book IDs in Redis.", cacheKey, bookIdsToCache.size()))
                .exceptionally(e -> {
                    logger.error("Failed to cache search results in Redis: {}", e.getMessage());
                    return null;
                });
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
        if (!cacheEnabled || cachedBookRepository == null || book == null) {
            return Mono.empty();
        }
         // book.getId() here is likely the GoogleBooksID or ISBN from the source Book object
        String externalId = book.getId(); 
        if (externalId == null || externalId.isEmpty()){
             logger.warn("Book object has null or empty ID. Cannot process for caching.");
             return Mono.empty();
        }

        return Mono.fromCallable(() -> findOrCreateUuidForBook(book))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(bookUuid -> {
                if (bookUuid == null) {
                    logger.error("Could not determine or create UUID for book with externalID: {}. Skipping cache.", externalId);
                    return Mono.empty();
                }
                
                // Try to fetch existing CachedBook by UUID
                return Mono.fromFuture(cachedBookRepository.findByIdAsync(bookUuid))
                    .flatMap(existingCachedBookOpt -> {
                        CachedBook cachedBookToProcess;
                        boolean isNewEntry = !existingCachedBookOpt.isPresent();

                        if (isNewEntry) {
                            logger.debug("No existing CachedBook found for UUID {}. Creating new entry for externalID: {}", bookUuid, externalId);
                            // Convert Book to CachedBook, ensuring UUID is set
                            JsonNode rawJsonNode = null;
                            try {
                                String rawJsonString = book.getRawJsonResponse();
                                if (rawJsonString == null || rawJsonString.isEmpty()) {
                                    rawJsonString = objectMapper.writeValueAsString(book);
                                }
                                rawJsonNode = objectMapper.readTree(rawJsonString);
                            } catch (Exception e) {
                                logger.error("Error processing raw JSON for new CachedBook (UUID {}): {}", bookUuid, e.getMessage());
                                return Mono.error(e);
                            }
                            // Assuming CachedBook.fromBook sets basic fields. We must ensure ID is UUID.
                            cachedBookToProcess = CachedBook.fromBook(book, rawJsonNode, null); // Embedding will be set later
                            cachedBookToProcess.setId(bookUuid); // Explicitly set UUID
                            // Ensure other identifiers are also set from the source 'book' object
                            cachedBookToProcess.setIsbn13(book.getIsbn13());
                            cachedBookToProcess.setIsbn10(book.getIsbn10());
                            // Assuming book.getId() is the GoogleBooksID if no specific source field exists on Book model
                            // For now, we'll rely on CachedBook.fromBook to correctly map book.getId() to googleBooksId if appropriate
                            // or ensure it's set if book.getId() is indeed the Google ID.
                            // If Book model had a clear getGoogleBooksId(), that would be better.
                            // Let's assume CachedBook.fromBook handles setting googleBooksId from book.getId() if it's the primary ID from Google.
                            // If not, we might need to explicitly set it here:
                            if (book.getId() != null && (book.getIsbn13() == null && book.getIsbn10() == null)) { // Heuristic: if only ID is present, it's likely GoogleID
                                cachedBookToProcess.setGoogleBooksId(book.getId());
                            } else {
                                // If ISBNs are present, googleBooksId might be null or a different value if Book had a separate field
                                // For now, we assume CachedBook.fromBook does its best.
                            }


                        } else {
                            cachedBookToProcess = existingCachedBookOpt.get();
                            logger.info("Existing CachedBook found for UUID {}. Attempting rich merge with new data for externalID: {}", bookUuid, externalId);
                            // Perform rich merge. performRichMergeAndUpdate modifies primaryCachedBook.
                            return performRichMergeAndUpdate(cachedBookToProcess, book)
                                .map(modified -> cachedBookToProcess); // Return the (potentially modified) cachedBookToProcess
                        }
                        return Mono.just(cachedBookToProcess);
                    })
                    .flatMap(bookToSave -> {
                        Mono<Void> slugMono = Mono.fromFuture(bookSlugService.ensureBookHasSlugAsync(bookToSave)).then();

                        Mono<Void> operationMono = Mono.defer(() -> {
                            if (embeddingService.shouldGenerateEmbedding(bookToSave)) {
                                return embeddingService.generateEmbeddingForBook(bookToSave)
                                    .onErrorResume(error -> {
                                        logger.debug("Embedding generation failed for book UUID {}, continuing without embedding: {}",
                                                   bookToSave.getId(), error.getMessage());
                                        return Mono.empty(); // Continue without embedding
                                    })
                                    .flatMap(embedding -> {
                                        if (embedding != null) {
                                            bookToSave.setEmbedding(embedding);
                                        }
                                        return Mono.fromFuture(cachedBookRepository.saveAsync(bookToSave))
                                            .doOnSuccess(savedBook -> logger.info("Successfully saved/updated CachedBook UUID: {}, Title: {}", savedBook.getId(), savedBook.getTitle()))
                                            .doOnError(e -> logger.error("Error saving CachedBook UUID {}: {}", bookToSave.getId(), e.getMessage()))
                                            .then(redisCacheService.cacheBookReactive(bookToSave.getId(), bookToSave.toBook()))
                                            .then();
                                    })
                                    .switchIfEmpty( // Removed Mono.defer()
                                        Mono.fromFuture(cachedBookRepository.saveAsync(bookToSave))
                                            .doOnSuccess(savedBook -> logger.info("Successfully saved/updated CachedBook UUID: {} (no embedding), Title: {}", savedBook.getId(), savedBook.getTitle()))
                                            .doOnError(e -> logger.error("Error saving CachedBook UUID {}: {}", bookToSave.getId(), e.getMessage()))
                                            .then(redisCacheService.cacheBookReactive(bookToSave.getId(), bookToSave.toBook()))
                                            .then()
                                    );
                            } else {
                                // Embedding already exists and is correct dimension
                                logger.debug("Embedding already exists for book UUID: {}", bookToSave.getId());
                                return Mono.fromFuture(cachedBookRepository.saveAsync(bookToSave))
                                    .doOnSuccess(savedBook -> logger.info("Successfully saved/updated CachedBook UUID: {}, Title: {}", savedBook.getId(), savedBook.getTitle()))
                                    .doOnError(e -> logger.error("Error saving CachedBook UUID {}: {}", bookToSave.getId(), e.getMessage()))
                                    .then(redisCacheService.cacheBookReactive(bookToSave.getId(), bookToSave.toBook()))
                                    .then();
                            }
                        });
                        return slugMono.then(operationMono);
                    });
            })
            .onErrorResume(e -> {
                logger.error("Error in cacheBookReactive for externalID {}: {}", externalId, e.getMessage(), e);
                return Mono.empty();
            })
            .then();
    }

    private String findOrCreateUuidForBook(Book book) {
        // This logic needs to be robust. Assumes book object has ISBNs and GoogleID populated if available.
        String bookIsbn13 = book.getIsbn13();
        String bookIsbn10 = book.getIsbn10();
        // Assuming book.getId() is the Google Books ID if other identifiers are not primary
        // Or, if Book model had a specific getGoogleBooksId(), we'd use that
        // For now, let's assume book.getId() is the Google Books ID if it's the only one
        String googleId = book.getId(); // This is often the Google Books ID from source

        if (bookIsbn13 != null && !bookIsbn13.isEmpty()) {
            Optional<CachedBook> existing = cachedBookRepository.findByIsbn13(bookIsbn13);
            if (existing.isPresent() && existing.get().getId() != null && UuidUtil.isUuid(existing.get().getId())) return existing.get().getId();
        }
        if (bookIsbn10 != null && !bookIsbn10.isEmpty()) {
            Optional<CachedBook> existing = cachedBookRepository.findByIsbn10(bookIsbn10);
            if (existing.isPresent() && existing.get().getId() != null && UuidUtil.isUuid(existing.get().getId())) return existing.get().getId();
        }
        if (googleId != null && !googleId.isEmpty()) {
            // Check if this googleId is not an ISBN itself, to avoid re-checking
            if (!googleId.equals(bookIsbn13) && !googleId.equals(bookIsbn10)) {
                 Optional<CachedBook> existing = cachedBookRepository.findByGoogleBooksId(googleId);
                 if (existing.isPresent() && existing.get().getId() != null && UuidUtil.isUuid(existing.get().getId())) return existing.get().getId();
            }
        }
        // If no existing UUID found, generate a new one, but only if we have some identifier to link it to
        if (bookIsbn13 != null || bookIsbn10 != null || (googleId != null && !googleId.isEmpty())) {
            return UuidUtil.getTimeOrderedEpoch().toString();
        }
        logger.warn("Cannot find or create UUID for book: {} as it lacks ISBNs and a distinct Google ID.", book.getTitle());
        return null;
    }
    
    // private Mono<float[]> generateEmbeddingReactive(Book book) { // Unused method
    //     try {
    //         StringBuilder textBuilder = new StringBuilder();
    //         textBuilder.append(book.getTitle() != null ? book.getTitle() : "").append(" ");
    //         if (book.getAuthors() != null && !book.getAuthors().isEmpty()) {
    //             textBuilder.append(String.join(" ", book.getAuthors())).append(" ");
    //         }
    //         if (book.getDescription() != null) {
    //             textBuilder.append(book.getDescription()).append(" ");
    //         }
    //         if (book.getCategories() != null && !book.getCategories().isEmpty()) {
    //             textBuilder.append(String.join(" ", book.getCategories()));
    //         }
    //         String text = textBuilder.toString().trim();

    //         if (text.isEmpty()) {
    //             logger.warn("Cannot generate embedding for book ID {} as constructed text is empty.", book.getId());
    //             return Mono.just(new float[384]);
    //         }

    //         if (this.embeddingServiceEnabled && this.embeddingServiceUrl != null) {
    //             return embeddingClient.post()
    //                 .bodyValue(Collections.singletonMap("text", text))
    //                 .retrieve()
    //                 .onStatus(httpStatus -> httpStatus.is4xxClientError(), response -> {
    //                     logger.warn("Client error when generating embedding for book ID {}: HTTP {}", 
    //                         book.getId(), response.statusCode());
    //                     // Optionally, you could return response.bodyToMono(String.class).map(ErrorDetails::new) etc.
    //                     return Mono.error(new WebClientResponseException(
    //                         "Client error from embedding service", 
    //                         response.statusCode().value(), 
    //                         response.statusCode().toString(), 
    //                         response.headers().asHttpHeaders(), 
    //                         null, 
    //                         null
    //                     ));
    //                 })
    //                 .onStatus(httpStatus -> httpStatus.is5xxServerError(), response -> {
    //                     logger.error("Server error when generating embedding for book ID {}: HTTP {}", 
    //                         book.getId(), response.statusCode());
    //                     return Mono.error(new RuntimeException("Embedding service unavailable: Server error " + response.statusCode()));
    //                 })
    //                 .bodyToMono(float[].class)
    //                 .onErrorResume(e -> {
    //                     if (e instanceof WebClientResponseException) {
    //                         WebClientResponseException wcre = (WebClientResponseException) e;
    //                         logger.error("WebClientResponseException generating embedding for book ID {}: HTTP {} - {}. Falling back to placeholder.", 
    //                             book.getId(), wcre.getStatusCode(), wcre.getMessage());
    //                     } else {
    //                         logger.warn("Non-WebClient error generating embedding from service for book ID {}: {}. Falling back to placeholder.", 
    //                             book.getId(), e.getMessage());
    //                     }
    //                     return Mono.just(createPlaceholderEmbedding(text));
    //                 });
    //         }
    //         logger.debug("Embedding service is disabled or URL not configured. Using placeholder embedding for book ID {}.", book.getId());
    //         return Mono.just(createPlaceholderEmbedding(text));
    //     } catch (Exception e) {
    //         logger.error("Unexpected error in generateEmbeddingReactive for book ID {}: {}", book.getId(), e.getMessage(), e);
    //         return Mono.just(new float[384]);
    //     }
    // }

    // private float[] createPlaceholderEmbedding(String text) { // Unused method (only used by generateEmbeddingReactive)
    //     float[] placeholder = new float[384];
    //     if (text == null || text.isEmpty()) return placeholder;
    //     int hash = text.hashCode();
    //     for (int i = 0; i < placeholder.length; i++) {
    //         placeholder[i] = (float) Math.sin(hash * (i + 1) / 100.0);
    //     }
    //     return placeholder;
    // }

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
                LocalDateTime newLdt = newBookData.getPublishedDate().atStartOfDay();
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
            return (Boolean) modified;
        });
    }
}
