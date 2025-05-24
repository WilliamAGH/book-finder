/**
 * Service for processing individual books from New York Times bestseller lists
 * This service handles the extraction, enrichment, and persistence of book data
 * from NYT bestseller entries, integrating with multiple data sources
 *
 * @author William Callahan
 *
 * Features:
 * - Processes individual NYT book entries with fallback data sources
 * - Checks Redis for existing books to avoid duplicate processing
 * - Fetches additional metadata from Google Books and OpenLibrary
 * - Aggregates data from multiple sources into a unified format
 * - Persists enriched book data to both S3 and Redis
 * - Handles books without ISBNs or Google Books IDs
 * - Generates consistent S3 keys based on available identifiers
 */

package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Service
public class NytIndividualBookProcessorService {

    private static final Logger logger = LoggerFactory.getLogger(NytIndividualBookProcessorService.class);

    private final S3StorageService s3StorageService;
    private final ObjectMapper objectMapper;
    private final BookDataAggregatorService bookDataAggregatorService;
    private final OpenLibraryBookDataService openLibraryBookDataService;
    private final CachedBookRepository cachedBookRepository;
    private final RedisCacheService redisCacheService;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final Executor mvcTaskExecutor;


    public NytIndividualBookProcessorService(
            S3StorageService s3StorageService,
            ObjectMapper objectMapper,
            BookDataAggregatorService bookDataAggregatorService,
            OpenLibraryBookDataService openLibraryBookDataService,
            CachedBookRepository cachedBookRepository,
            RedisCacheService redisCacheService,
            BookDataOrchestrator bookDataOrchestrator,
            @Qualifier("mvcTaskExecutor") AsyncTaskExecutor mvcTaskExecutor) {
        this.s3StorageService = s3StorageService;
        this.objectMapper = objectMapper;
        this.bookDataAggregatorService = bookDataAggregatorService;
        this.openLibraryBookDataService = openLibraryBookDataService;
        this.cachedBookRepository = cachedBookRepository;
        this.redisCacheService = redisCacheService;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.mvcTaskExecutor = mvcTaskExecutor;
    }

    /**
     * Asynchronously processes an individual book from the NYT bestseller list.
     * @param nytBookApiNode The JsonNode representing the book from the NYT API
     * @param preFetchedGoogleBook The pre-fetched full Book object from Google (if available)
     * @param googleBookIdFromIsbn The Google Book ID found via ISBN lookup (if available)
     * @return CompletableFuture resolving to the primary identifier of the processed book, or empty if failed
     */
    @Async("mvcTaskExecutor") // Ensure this runs on a managed thread pool
    public CompletableFuture<Optional<String>> processBookAsync(JsonNode nytBookApiNode, Book preFetchedGoogleBook, String googleBookIdFromIsbn) {
        String titleForLogging = nytBookApiNode.path("title").asText("N/A");
        logger.trace("Entering processBookAsync for NYT title: '{}', preFetchedGoogleBook present: {}, googleBookIdFromIsbn: {}",
            titleForLogging, preFetchedGoogleBook != null, googleBookIdFromIsbn);

        String effectiveIsbn = getEffectiveIsbnFromNytNode(nytBookApiNode);
        String initialGoogleBookId = googleBookIdFromIsbn;

        if (initialGoogleBookId == null && preFetchedGoogleBook != null) {
            initialGoogleBookId = preFetchedGoogleBook.getId();
            logger.debug("Using Google ID '{}' from preFetchedGoogleBook for NYT title: '{}'", initialGoogleBookId, titleForLogging);
        }
        
        final String finalInitialGoogleBookId = initialGoogleBookId;

        String nytIsbn13 = nytBookApiNode.path("primary_isbn13").asText(null);
        String nytIsbn10 = nytBookApiNode.path("primary_isbn10").asText(null);
        logger.debug("NYT Book Identifiers - Title: '{}', Effective ISBN: {}, Google ID (initial): {}, NYT ISBN13: {}, NYT ISBN10: {}",
            titleForLogging, effectiveIsbn, finalInitialGoogleBookId, nytIsbn13, nytIsbn10);

        return findBookByAnyIdentifierAsync(finalInitialGoogleBookId, nytIsbn13, nytIsbn10)
            .thenComposeAsync(existingRedisBookOpt -> {
                if (existingRedisBookOpt.isPresent()) {
                    CachedBook existingRedisBook = existingRedisBookOpt.get();
                    logger.info("Book already exists in Redis cache and is considered processed - Title: '{}', Google ID: {}, ISBN-13: {}, ISBN-10: {}. S3 Key: {}",
                            titleForLogging, existingRedisBook.getGoogleBooksId(), existingRedisBook.getIsbn13(), existingRedisBook.getIsbn10(), existingRedisBook.getS3Key());
                    String primaryExistingId = existingRedisBook.getGoogleBooksId() != null ? existingRedisBook.getGoogleBooksId() : existingRedisBook.getIsbn13();
                    logger.trace("Exiting processBookAsync for NYT title: '{}', returning existing ID: {}", titleForLogging, primaryExistingId);
                    return CompletableFuture.completedFuture(Optional.ofNullable(primaryExistingId));
                }

                logger.debug("Book with Title: '{}' (Effective ISBN: {}, Google ID: {}) not found in Redis. Proceeding with full processing.", titleForLogging, effectiveIsbn, finalInitialGoogleBookId);

                CompletableFuture<JsonNode> googleBookJsonNodeFuture;
                if (finalInitialGoogleBookId != null && !finalInitialGoogleBookId.isEmpty()) {
                    if (preFetchedGoogleBook != null) {
                        try {
                            JsonNode node = (preFetchedGoogleBook.getRawJsonResponse() != null) ?
                                objectMapper.readTree(preFetchedGoogleBook.getRawJsonResponse()) :
                                objectMapper.valueToTree(preFetchedGoogleBook);
                            googleBookJsonNodeFuture = CompletableFuture.completedFuture(node);
                        } catch (IOException e) {
                            logger.error("Error parsing raw JSON for pre-fetched Google Book ID {}: {}", finalInitialGoogleBookId, e.getMessage());
                            googleBookJsonNodeFuture = CompletableFuture.completedFuture(objectMapper.valueToTree(preFetchedGoogleBook));
                        }
                    } else {
                        logger.warn("Full Google Books data for ID {} was not pre-fetched. Attempting fetch via BookDataOrchestrator.", finalInitialGoogleBookId);
                        googleBookJsonNodeFuture = bookDataOrchestrator.getBookByIdTiered(finalInitialGoogleBookId)
                            .toFuture()
                            .thenApply(fetchedNow -> {
                                if (fetchedNow != null) {
                                    try {
                                        return (fetchedNow.getRawJsonResponse() != null) ?
                                            objectMapper.readTree(fetchedNow.getRawJsonResponse()) :
                                            objectMapper.valueToTree(fetchedNow);
                                    } catch (IOException e) {
                                        logger.error("Error parsing raw JSON for directly fetched Google Book ID {}: {}", finalInitialGoogleBookId, e.getMessage());
                                        return objectMapper.valueToTree(fetchedNow);
                                    }
                                } else {
                                    logger.warn("Full Google Books data not found for ID {} even after direct fetch attempt.", finalInitialGoogleBookId);
                                    return null;
                                }
                            });
                    }
                } else {
                    googleBookJsonNodeFuture = CompletableFuture.completedFuture(null);
                }

                return googleBookJsonNodeFuture.thenComposeAsync(googleBookJsonNode -> {
                    CompletableFuture<ObjectNode> finalAggregatedBookDataFuture;
                    String primaryIdForPersistence = finalInitialGoogleBookId;

                    if (googleBookJsonNode != null) {
                        finalAggregatedBookDataFuture = bookDataAggregatorService.prepareEnrichedBookJsonAsync(googleBookJsonNode, nytBookApiNode, finalInitialGoogleBookId);
                        logger.debug("Aggregated data using Google Book ID: {} for NYT title: '{}'", finalInitialGoogleBookId, titleForLogging);
                    } else if (effectiveIsbn != null) {
                        logger.info("No Google Book ID for NYT title: '{}', ISBN {}. Attempting fallback to OpenLibrary.", titleForLogging, effectiveIsbn);
                        primaryIdForPersistence = effectiveIsbn;
                        final String currentPrimaryId = primaryIdForPersistence;

                        List<JsonNode> fallbackSources = new ArrayList<>();
                        fallbackSources.add(nytBookApiNode);

                        finalAggregatedBookDataFuture = openLibraryBookDataService.fetchBookByIsbn(effectiveIsbn)
                            .toFuture()
                            .thenApply(olBook -> {
                                if (olBook != null) {
                                    logger.info("Fetched data from OpenLibrary for ISBN {} (NYT title: '{}')", effectiveIsbn, titleForLogging);
                                    fallbackSources.add(objectMapper.valueToTree(olBook));
                                } else {
                                    logger.info("No data found from OpenLibrary for ISBN {} (NYT title: '{}')", effectiveIsbn, titleForLogging);
                                }
                                return bookDataAggregatorService.aggregateBookDataSourcesAsync(currentPrimaryId, "id", fallbackSources.toArray(new JsonNode[0])).join();
                            }).exceptionally(e -> {
                                logger.warn("Error fetching from OpenLibrary for ISBN {} (NYT title: '{}'): {}", effectiveIsbn, titleForLogging, e.getMessage());
                                return bookDataAggregatorService.aggregateBookDataSourcesAsync(currentPrimaryId, "id", fallbackSources.toArray(new JsonNode[0])).join();
                            })
                            .thenApply(aggregated -> {
                                enrichWithNytData(aggregated, nytBookApiNode);
                                logger.debug("Aggregated data using OpenLibrary fallback for ISBN: {} (NYT title: '{}')", effectiveIsbn, titleForLogging);
                                return aggregated;
                            });
                    } else {
                        logger.warn("Book from NYT has no Google ID or effective ISBN. Creating record with NYT data only. Title: {}", titleForLogging);
                        primaryIdForPersistence = "NYT_" + titleForLogging.replaceAll("[^a-zA-Z0-9.-]", "_") + "_" + System.currentTimeMillis();
                        ObjectNode nytOnlyData = objectMapper.createObjectNode();
                        nytOnlyData.put("id", primaryIdForPersistence);
                        nytOnlyData.set("nyt_data_only", nytBookApiNode);
                        enrichWithNytData(nytOnlyData, nytBookApiNode);
                        finalAggregatedBookDataFuture = CompletableFuture.completedFuture(nytOnlyData);
                        logger.debug("Created NYT-only data structure for title: '{}', generated ID: {}", titleForLogging, primaryIdForPersistence);
                    }
                    
                    final String finalPrimaryIdForPersistence = primaryIdForPersistence;

                    return finalAggregatedBookDataFuture.thenComposeAsync(finalAggregatedBookData -> {
                        if (finalAggregatedBookData == null) {
                            logger.error("Failed to aggregate data for NYT book: {}", titleForLogging);
                            return CompletableFuture.completedFuture(Optional.<String>empty());
                        }

                        String finalGoogleId = finalAggregatedBookData.path("id").asText(null);
                        String resolvedPrimaryId = finalPrimaryIdForPersistence;

                        if (resolvedPrimaryId == null) {
                            resolvedPrimaryId = effectiveIsbn != null ? effectiveIsbn : "UNKNOWN_ID_" + System.currentTimeMillis();
                            logger.warn("primaryIdForPersistence was unexpectedly null for NYT title: '{}'. Defaulting to: {}", titleForLogging, resolvedPrimaryId);
                        }
                        if (resolvedPrimaryId.startsWith("NYT_") && finalGoogleId != null && !finalGoogleId.startsWith("NYT_")) {
                             resolvedPrimaryId = finalGoogleId;
                        }
                        
                        String s3KeyIsbn13 = getIsbnFromAggregatedData(finalAggregatedBookData, "ISBN_13");
                        String s3KeyIsbn10 = getIsbnFromAggregatedData(finalAggregatedBookData, "ISBN_10");
                        String s3KeyGoogleId = (finalGoogleId != null && !finalGoogleId.startsWith("NYT_")) ? finalGoogleId : null;
                        String s3Key = generateConsistentS3Key(s3KeyGoogleId, s3KeyIsbn13, s3KeyIsbn10);
                        
                        logger.debug("Generated S3 key: {} for NYT title: '{}' using GoogleID: {}, ISBN13: {}, ISBN10: {}",
                            s3Key, titleForLogging, s3KeyGoogleId, s3KeyIsbn13, s3KeyIsbn10);

                        try {
                            String jsonToUpload = objectMapper.writeValueAsString(finalAggregatedBookData);
                            final String finalResolvedPrimaryId = resolvedPrimaryId;
                            return s3StorageService.uploadGenericJsonAsync(s3Key, jsonToUpload, false)
                                .thenComposeAsync(v -> persistBookToRedisAsync(finalResolvedPrimaryId, finalAggregatedBookData, s3Key))
                                .thenApply(v -> {
                                    logger.info("Saved individual book JSON to S3: {} for NYT title: '{}'", s3Key, titleForLogging);
                                    logger.trace("Exiting processBookAsync for NYT title: '{}', successfully processed and saved. Returning ID: {}", titleForLogging, finalResolvedPrimaryId);
                                    return Optional.of(finalResolvedPrimaryId);
                                }).exceptionally(e -> {
                                    logger.error("Failed to save individual book (NYT title: '{}', Primary ID: {}) to S3/Redis. S3 Key: {}. Error: {}",
                                        titleForLogging, finalResolvedPrimaryId, s3Key, e.getMessage(), e);
                                    return Optional.empty();
                                });
                        } catch (IOException e) {
                            logger.error("Error serializing final aggregated data for S3 upload: {}", e.getMessage());
                            return CompletableFuture.completedFuture(Optional.<String>empty());
                        }
                    }, mvcTaskExecutor);
                }, mvcTaskExecutor);
            }, mvcTaskExecutor);
    }

    private String getEffectiveIsbnFromNytNode(JsonNode nytBookApiNode) {
        String isbn13 = nytBookApiNode.path("primary_isbn13").asText(null);
        return isbn13 != null ? isbn13 : nytBookApiNode.path("primary_isbn10").asText(null);
    }
    
    private String getIsbnFromAggregatedData(JsonNode aggregatedData, String typeToExtract) {
        JsonNode volumeInfo = aggregatedData.path("volumeInfo"); // Common path for Google Books data
        if (volumeInfo.isMissingNode()) { // Check if it's directly in aggregatedData (e.g. from OpenLibrary)
            volumeInfo = aggregatedData;
        }

        if (volumeInfo.has("industryIdentifiers")) {
            for (JsonNode identifier : volumeInfo.path("industryIdentifiers")) {
                String type = identifier.path("type").asText();
                if (typeToExtract.equals(type)) {
                    return identifier.path("identifier").asText(null);
                }
            }
        }
        // Fallback for OpenLibrary structure if not found in industryIdentifiers
        if (typeToExtract.equals("ISBN_13") && volumeInfo.has("isbn_13") && volumeInfo.get("isbn_13").isArray()) {
            return volumeInfo.get("isbn_13").get(0).asText(null);
        }
        if (typeToExtract.equals("ISBN_10") && volumeInfo.has("isbn_10") && volumeInfo.get("isbn_10").isArray()) {
            return volumeInfo.get("isbn_10").get(0).asText(null);
        }
        return null;
    }

    public String generateConsistentS3Key(String googleBooksId, String isbn13, String isbn10) {
        if (googleBooksId != null && !googleBooksId.isEmpty() && !googleBooksId.startsWith("NYT_")) {
            return String.format("books/v1/%s.json", googleBooksId);
        } else if (isbn13 != null && !isbn13.isEmpty()) {
            return String.format("books/v1/isbn13/%s.json", isbn13);
        } else if (isbn10 != null && !isbn10.isEmpty()) {
            return String.format("books/v1/isbn10/%s.json", isbn10);
        }
        String fallbackId = (googleBooksId != null && !googleBooksId.isEmpty()) ? googleBooksId : "unknown_id_" + System.currentTimeMillis();
        logger.warn("Generating S3 key with fallback ID: {}", fallbackId);
        return String.format("books/v1/unknown/%s.json", fallbackId.replaceAll("[^a-zA-Z0-9.-]", "_"));
    }

    public CompletableFuture<Optional<CachedBook>> findBookByAnyIdentifierAsync(String googleBooksId, String isbn13, String isbn10) {
        logger.debug("Attempting to find book in cache by GoogleID: {}, ISBN13: {}, ISBN10: {}",
            (googleBooksId != null && !googleBooksId.startsWith("NYT_") ? googleBooksId : "N/A_or_NYT_prefixed"), isbn13, isbn10);

        CompletableFuture<Optional<CachedBook>> future = CompletableFuture.completedFuture(Optional.empty());

        if (googleBooksId != null && !googleBooksId.startsWith("NYT_")) {
            final String finalGoogleId = googleBooksId;
            future = future.thenComposeAsync(result -> {
                if (result.isPresent()) return CompletableFuture.completedFuture(result);
                return CompletableFuture.supplyAsync(() -> cachedBookRepository.findByGoogleBooksId(finalGoogleId), mvcTaskExecutor)
                    .thenApply(bookOpt -> {
                        if (bookOpt.isPresent()) logger.debug("Found book by GoogleID: {}", finalGoogleId);
                        return bookOpt;
                    });
            }, mvcTaskExecutor);
        }
        if (isbn13 != null) {
            final String finalIsbn13 = isbn13;
            future = future.thenComposeAsync(result -> {
                if (result.isPresent()) return CompletableFuture.completedFuture(result);
                return CompletableFuture.supplyAsync(() -> cachedBookRepository.findByIsbn13(finalIsbn13), mvcTaskExecutor)
                    .thenApply(bookOpt -> {
                        if (bookOpt.isPresent()) logger.debug("Found book by ISBN13: {}", finalIsbn13);
                        return bookOpt;
                    });
            }, mvcTaskExecutor);
        }
        if (isbn10 != null) {
            final String finalIsbn10 = isbn10;
            future = future.thenComposeAsync(result -> {
                if (result.isPresent()) return CompletableFuture.completedFuture(result);
                return CompletableFuture.supplyAsync(() -> cachedBookRepository.findByIsbn10(finalIsbn10), mvcTaskExecutor)
                    .thenApply(bookOpt -> {
                        if (bookOpt.isPresent()) logger.debug("Found book by ISBN10: {}", finalIsbn10);
                        return bookOpt;
                    });
            }, mvcTaskExecutor);
        }
        return future.thenApply(finalResult -> {
            if (!finalResult.isPresent()) {
                 logger.debug("Book not found in cache by any provided identifier (GoogleID: {}, ISBN13: {}, ISBN10: {})",
                    googleBooksId, isbn13, isbn10);
            }
            return finalResult;
        });
    }

    public CompletableFuture<Void> persistBookToRedisAsync(String primaryId, JsonNode bookData, String s3Key) {
        String titleForLoggingPersist = bookData.path("volumeInfo").path("title").asText(bookData.path("nyt_data_only").path("title").asText("N/A"));
        logger.debug("Attempting to persist book to Redis. Primary ID: {}, S3 Key: {}, Title: {}", primaryId, s3Key, titleForLoggingPersist);

        return redisCacheService.isRedisAvailableAsync().thenComposeAsync(available -> {
            if (!available) {
                logger.debug("Redis not available, skipping book persistence for Primary ID: {}, Title: {}", primaryId, titleForLoggingPersist);
                return CompletableFuture.completedFuture(null);
            }

            String googleBooksIdFromData = bookData.path("id").asText(null);
            String effectiveGoogleIdForCache = googleBooksIdFromData;

            if (effectiveGoogleIdForCache != null && effectiveGoogleIdForCache.startsWith("NYT_") && !primaryId.startsWith("NYT_")) {
                effectiveGoogleIdForCache = primaryId;
            } else if (effectiveGoogleIdForCache == null) {
                effectiveGoogleIdForCache = primaryId;
            }

            String isbn13 = getIsbnFromAggregatedData(bookData, "ISBN_13");
            String isbn10 = getIsbnFromAggregatedData(bookData, "ISBN_10");
            
            final String finalEffectiveGoogleId = effectiveGoogleIdForCache;

            return findBookByAnyIdentifierAsync(
                (finalEffectiveGoogleId != null && !finalEffectiveGoogleId.startsWith("NYT_")) ? finalEffectiveGoogleId : null,
                isbn13,
                isbn10
            ).thenComposeAsync(existingBookOpt -> {
                CachedBook cachedBook;
                String finalCachedBookId = primaryId;

                if (existingBookOpt.isPresent()) {
                    cachedBook = existingBookOpt.get();
                    finalCachedBookId = cachedBook.getId(); // Use existing UUID
                    logger.info("Updating existing book in Redis - CachedBook ID: {}, Title: {}", finalCachedBookId, titleForLoggingPersist);
                } else {
                    cachedBook = new CachedBook();
                    cachedBook.setId(finalCachedBookId); // This might be an ISBN or GoogleID if new
                    logger.info("Creating new book in Redis - CachedBook ID: {}, Title: {}", finalCachedBookId, titleForLoggingPersist);
                }
                
                // Populate CachedBook
                JsonNode volumeInfo = bookData.path("volumeInfo");
                 if (!volumeInfo.isMissingNode()) {
                    cachedBook.setTitle(volumeInfo.path("title").asText(bookData.path("nyt_data_only").path("title").asText(null)));
                    if (volumeInfo.has("authors")) {
                        List<String> authors = new ArrayList<>();
                        volumeInfo.path("authors").forEach(author -> authors.add(author.asText()));
                        cachedBook.setAuthors(authors);
                    } else if (bookData.path("nyt_data_only").has("author")) {
                         cachedBook.setAuthors(List.of(bookData.path("nyt_data_only").path("author").asText()));
                    }
                    // ... (rest of the population logic from original persistBookToRedis) ...
                    if (volumeInfo.has("categories")) {
                        List<String> categories = new ArrayList<>();
                        volumeInfo.path("categories").forEach(category -> categories.add(category.asText()));
                        cachedBook.setCategories(categories);
                    }
                    cachedBook.setPublisher(volumeInfo.path("publisher").asText(bookData.path("nyt_data_only").path("publisher").asText(null)));
                    String publishedDateStr = volumeInfo.path("publishedDate").asText(null);
                    if (publishedDateStr != null) {
                        try {
                            if (publishedDateStr.matches("\\d{4}")) {
                                cachedBook.setPublishedDate(java.time.LocalDateTime.of(Integer.parseInt(publishedDateStr), 1, 1, 0, 0));
                            } else if (publishedDateStr.matches("\\d{4}-\\d{2}")) {
                                String[] parts = publishedDateStr.split("-");
                                cachedBook.setPublishedDate(java.time.LocalDateTime.of(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), 1, 0, 0));
                            } else if (publishedDateStr.matches("\\d{4}-\\d{2}-\\d{2}")) {
                                cachedBook.setPublishedDate(java.time.LocalDateTime.parse(publishedDateStr + "T00:00:00"));
                            }
                        } catch (Exception e) {
                            logger.debug("Could not parse published date: {} for book ID: {}", publishedDateStr, finalCachedBookId);
                        }
                    }
                    cachedBook.setDescription(volumeInfo.path("description").asText(bookData.path("nyt_data_only").path("description").asText(null)));
                    cachedBook.setLanguage(volumeInfo.path("language").asText(null));
                    cachedBook.setPageCount(volumeInfo.path("pageCount").asInt(0));

                } else if (bookData.has("nyt_data_only")) {
                    JsonNode nytData = bookData.path("nyt_data_only");
                    cachedBook.setTitle(nytData.path("title").asText(null));
                    cachedBook.setAuthors(List.of(nytData.path("author").asText("Unknown Author")));
                    cachedBook.setDescription(nytData.path("description").asText(null));
                    cachedBook.setPublisher(nytData.path("publisher").asText(null));
                }

                if (finalEffectiveGoogleId != null && !finalEffectiveGoogleId.startsWith("NYT_")) {
                    cachedBook.setGoogleBooksId(finalEffectiveGoogleId);
                }
                cachedBook.setIsbn13(isbn13);
                cachedBook.setIsbn10(isbn10);
                cachedBook.setS3Key(s3Key);
                cachedBook.setLastUpdated(java.time.Instant.now());

                final CachedBook bookToSave = cachedBook;
                return CompletableFuture.runAsync(() -> cachedBookRepository.save(bookToSave), mvcTaskExecutor);
            }, mvcTaskExecutor);
        }, mvcTaskExecutor).exceptionally(e -> {
            logger.error("Error persisting book to Redis - Primary ID: {}, Title: {}: {}", primaryId, titleForLoggingPersist, e.getMessage(), e);
            return null;
        });
    }
    
    public void enrichWithNytData(ObjectNode bookData, JsonNode nytData) {
        if (!bookData.has("volumeInfo")) {
            bookData.putObject("volumeInfo");
        }
        ObjectNode volumeInfo = (ObjectNode) bookData.get("volumeInfo");
        
        if (!volumeInfo.has("title") || volumeInfo.path("title").asText("").isEmpty()) {
            volumeInfo.put("title", nytData.path("title").asText());
        }
        if (!volumeInfo.has("authors") || volumeInfo.path("authors").isEmpty()) {
            ArrayNode authors = volumeInfo.putArray("authors");
            String author = nytData.path("author").asText();
            if (author != null && !author.isEmpty()) authors.add(author);
        }
        if (!volumeInfo.has("publisher") || volumeInfo.path("publisher").asText("").isEmpty()) {
            volumeInfo.put("publisher", nytData.path("publisher").asText());
        }
        if (!volumeInfo.has("description") || volumeInfo.path("description").asText("").isEmpty()) {
            volumeInfo.put("description", nytData.path("description").asText());
        }
        if (!volumeInfo.has("imageLinks")) {
            String bookImage = nytData.path("book_image").asText();
            if (bookImage != null && !bookImage.isEmpty()) {
                ObjectNode imageLinks = volumeInfo.putObject("imageLinks");
                imageLinks.put("thumbnail", bookImage);
                imageLinks.put("smallThumbnail", bookImage);
            }
        }
        if (!bookData.has("saleInfo")) {
            String amazonUrl = nytData.path("amazon_product_url").asText();
            if (amazonUrl != null && !amazonUrl.isEmpty()) {
                ObjectNode saleInfo = bookData.putObject("saleInfo");
                saleInfo.put("buyLink", amazonUrl);
            }
        }
        ObjectNode nytMetadata = bookData.putObject("nytMetadata");
        nytMetadata.put("rank", nytData.path("rank").asInt());
        nytMetadata.put("weeksOnList", nytData.path("weeks_on_list").asInt());
        nytMetadata.put("bookReviewLink", nytData.path("book_review_link").asText());
        nytMetadata.put("firstChapterLink", nytData.path("first_chapter_link").asText());
        nytMetadata.put("sundayReviewLink", nytData.path("sunday_review_link").asText());
        nytMetadata.put("articleChapterLink", nytData.path("article_chapter_link").asText());
    }

    /**
     * @deprecated Use {@link #processBookAsync(com.fasterxml.jackson.databind.JsonNode, com.williamcallahan.book_recommendation_engine.model.Book, String)} for non-blocking processing.
     */
    @Deprecated
    public Optional<String> processBook(JsonNode nytBookApiNode, Book preFetchedGoogleBook, String googleBookIdFromIsbn) {
        // This method is now just a blocking wrapper for the async version.
        // Consider removing it or ensuring callers migrate.
        try {
            return processBookAsync(nytBookApiNode, preFetchedGoogleBook, googleBookIdFromIsbn)
                    .get(60, java.util.concurrent.TimeUnit.SECONDS); // Add a timeout
        } catch (Exception e) {
            logger.error("Error in synchronous processBook wrapper: {}", e.getMessage(), e);
            Thread.currentThread().interrupt(); // Preserve interrupt status
            return Optional.empty();
        }
    }
}
