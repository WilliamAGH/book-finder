/**
 * Scheduler for periodically fetching and processing New York Times bestseller lists
 *
 * @author William Callahan
 *
 * Features:
 * - Automatically retrieves NYT bestseller lists on a configurable schedule
 * - Fetches complementary book metadata from Google Books API
 * - Stores merged book data in S3 for efficient retrieval
 * - Updates existing book data with new bestseller rankings
 * - Handles rate limiting for external API calls
 * - Maintains separate S3 paths for lists and individual books
 * - Supports configurable job parameters through application properties
 */
package com.williamcallahan.book_recommendation_engine.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.config.S3EnvironmentCondition;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.S3StorageService;
import com.williamcallahan.book_recommendation_engine.types.S3FetchResult;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.service.RedisCacheService;
import com.williamcallahan.book_recommendation_engine.service.ApiCircuitBreakerService;
import com.williamcallahan.book_recommendation_engine.service.NewYorkTimesService;
import com.williamcallahan.book_recommendation_engine.service.NytIndividualBookProcessorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Conditional(S3EnvironmentCondition.class)
@SuppressWarnings("deprecation")
public class NewYorkTimesBestsellerScheduler {

    private static final Logger logger = LoggerFactory.getLogger(NewYorkTimesBestsellerScheduler.class);
    private static final int ISBN_PROCESSING_BATCH_SIZE = 5; // Used for estimating calls
    
    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private final AtomicBoolean shutdownInProgress = new AtomicBoolean(false);

    private final NewYorkTimesService newYorkTimesService;
    private final GoogleBooksService googleBooksService;
    private final S3StorageService s3StorageService;
    private final ObjectMapper objectMapper;
    private final CachedBookRepository cachedBookRepository;
    private final RedisCacheService redisCacheService;
    private final ApiCircuitBreakerService apiCircuitBreakerService;
    private final NytIndividualBookProcessorService nytIndividualBookProcessorService;

    @Value("${app.nyt.scheduler.cron:0 0 4 * * SUN}") // Default: Sunday at 4 AM
    private String cronExpression;

    @Value("${app.nyt.scheduler.enabled:true}")
    private boolean schedulerEnabled;

    @Value("${s3.bucket-name}") // Use existing global S3 bucket name property
    private String s3BucketName;

    // Rate limiting for Google Books API calls within this scheduler
    @Value("${app.nyt.scheduler.google.books.api.max-calls-per-job:200}")
    private int googleBooksApiMaxCallsPerJob;

    // private int googleBooksApiCallsThisRun = 0; // Removed for thread-safety, will be a local variable

    /**
     * Constructs the NewYorkTimesBestsellerScheduler with required dependencies
     *
     * @param newYorkTimesService Service for accessing NYT bestseller lists
     * @param googleBooksService Service for fetching Google Books metadata
     * @param s3StorageService Service for S3 storage operations
     * @param objectMapper JSON serialization/deserialization utility
     * @param cachedBookRepository Repository for persisting books to Redis
     * @param redisCacheService Service for Redis cache operations
     * @param apiCircuitBreakerService Service for monitoring API rate limits
     * @param nytIndividualBookProcessorService Service for processing individual NYT books
     */
    public NewYorkTimesBestsellerScheduler(NewYorkTimesService newYorkTimesService,
                                           GoogleBooksService googleBooksService,
                                           S3StorageService s3StorageService,
                                           ObjectMapper objectMapper,
                                           CachedBookRepository cachedBookRepository,
                                           RedisCacheService redisCacheService,
                                           ApiCircuitBreakerService apiCircuitBreakerService,
                                           NytIndividualBookProcessorService nytIndividualBookProcessorService) {
    this.newYorkTimesService = newYorkTimesService;
        this.googleBooksService = googleBooksService;
        this.s3StorageService = s3StorageService;
        this.objectMapper = objectMapper;
        this.cachedBookRepository = cachedBookRepository;
        this.redisCacheService = redisCacheService;
        this.apiCircuitBreakerService = apiCircuitBreakerService;
        this.nytIndividualBookProcessorService = nytIndividualBookProcessorService;
    }

    /**
     * Scheduled job that processes New York Times bestseller lists
     * - Fetches latest bestseller overview from NYT API
     * - Updates S3 storage with current bestseller data
     * - Enriches book data with Google Books API information
     * - Handles rate limiting for external API calls
     * 
     * @implNote Runs according to configured cron schedule
     * Default is every Sunday at 4 AM
     * Respects configured rate limits for API calls
     */
    @Scheduled(cron = "${app.nyt.scheduler.cron:0 0 4 * * SUN}")
    @PreDestroy
    public void onShutdown() {
        logger.info("NewYorkTimesBestsellerScheduler is shutting down. Signaling ongoing processing to stop.");
        shutdownInProgress.set(true);
        if (isProcessing.get()) {
            logger.warn("Processing was active during shutdown signal. The current iteration will attempt to complete, but new iterations or significant operations will be halted.");
        }
    }

    /**
     * Scheduled job that processes New York Times bestseller lists
     * - Fetches latest bestseller overview from NYT API
     * - Updates S3 storage with current bestseller data
     * - Enriches book data with Google Books API information
     * - Handles rate limiting for external API calls
     * 
     * @implNote Runs according to configured cron schedule
     * Default is every Sunday at 4 AM
     * Respects configured rate limits for API calls
     */
    @Scheduled(cron = "${app.nyt.scheduler.cron:0 0 4 * * SUN}")
    public void processNewYorkTimesBestsellers() {
        if (!schedulerEnabled) {
            logger.info("New York Times Bestseller Scheduler is disabled.");
            return;
        }
        
        if (!isProcessing.compareAndSet(false, true)) {
            logger.warn("New York Times Bestseller processing is already in progress. Skipping this run.");
            return;
        }

        logger.info("Starting New York Times Bestseller processing job.");

        if (shutdownInProgress.get()) {
            logger.info("Shutdown in progress. Aborting New York Times Bestseller processing job before start.");
            isProcessing.set(false);
            return;
        }
        int googleBooksApiCallsThisRun = 0; // Local variable for thread-safety

        try {
            // Rate limiting for Google Books API calls is handled internally by GoogleBooksService 
            // (via Resilience4j on searchBooks) and its batch processing methods (e.g., delayElement in fetchGoogleBookIdsForMultipleIsbns).
            // The googleBooksApiMaxCallsPerJob property serves as a high-level guard for this scheduler job.

            JsonNode nytOverview = newYorkTimesService.fetchBestsellerListOverview().block();
            if (nytOverview == null || !nytOverview.has("results") || !nytOverview.get("results").has("lists")) {
                logger.error("Failed to fetch valid NYT bestseller overview or overview is empty.");
                return;
            }

            ArrayNode lists = (ArrayNode) nytOverview.get("results").get("lists");
            String bestsellersDate = nytOverview.get("results").path("bestsellers_date").asText();
            String publishedDate = nytOverview.get("results").path("published_date").asText();

            // Step 1: Collect all NYT book data and ISBNs that need Google Book IDs
            List<NytBookWithContext> allNytBooksToProcess = new ArrayList<>();
            Map<String, JsonNode> listJsonCache = new HashMap<>(); // Cache for S3 list JSONs

            for (JsonNode listNode : lists) {
                if (shutdownInProgress.get()) {
                    logger.info("Shutdown signaled during list processing. Aborting NYT bestseller processing.");
                    break; // Exit the loop over lists
                }
                String listNameEncoded = listNode.path("list_name_encoded").asText();
                // String displayName = listNode.path("display_name").asText(); // Unused variable
                String s3ListKey = String.format("lists/nyt/%s/%s.json", listNameEncoded, publishedDate);
                
                ObjectNode listJsonForS3 = loadOrCreateS3ListJson(s3ListKey, listNode, bestsellersDate, publishedDate, listJsonCache);
                ArrayNode currentNytApiBooks = (ArrayNode) listNode.path("books");

                for (JsonNode nytBookApiNode : currentNytApiBooks) {
                    allNytBooksToProcess.add(new NytBookWithContext(nytBookApiNode, listJsonForS3, s3ListKey));
                }
            }

            // Step 2: Batch fetch Google Book IDs for all unique ISBNs
            List<String> isbnsToFetchGoogleId = allNytBooksToProcess.stream()
                .map(NytBookWithContext::getEffectiveIsbn)
                .filter(java.util.Objects::nonNull)
                .distinct()
                .filter(isbn -> { // Only fetch if not already present in its corresponding S3 list book entry
                    NytBookWithContext context = allNytBooksToProcess.stream().filter(b -> isbn.equals(b.getEffectiveIsbn())).findFirst().orElse(null);
                    if (context != null) {
                        ObjectNode bookDataInS3List = findBookInS3ListJson(context.listJsonForS3(), isbn);
                        return bookDataInS3List == null || !bookDataInS3List.hasNonNull("google_book_id");
                    }
                    return true;
                })
                .collect(Collectors.toList());
            
            // First check Redis for existing Google Book IDs
            Map<String, String> isbnToGoogleIdFromRedis = new HashMap<>();
            boolean isRedisActuallyAvailable = false;
            try {
                // Use the async version and wait for its result.
                // This makes the blocking point explicit if Redis is slow/down.
                isRedisActuallyAvailable = redisCacheService.isRedisAvailableAsync().get(5, TimeUnit.SECONDS); 
            } catch (InterruptedException e) {
                logger.warn("Redis availability check was interrupted for NYT scheduler", e);
                Thread.currentThread().interrupt(); // Preserve interrupt status
            } catch (TimeoutException e) {
                logger.warn("Redis availability check timed out for NYT scheduler", e);
            } catch (ExecutionException e) { // Catches ExecutionException from future
                logger.warn("Failed to check Redis availability for NYT scheduler due to an exception: {}", e.getMessage(), e);
            }

            if (isRedisActuallyAvailable) {
                logger.info("Checking Redis for {} ISBNs before API calls", isbnsToFetchGoogleId.size());
                int foundInRedisCount = 0;
                for (String isbn : isbnsToFetchGoogleId) {
                    if (shutdownInProgress.get()) {
                        logger.info("Shutdown signaled during Redis check for ISBNs. Aborting further checks.");
                        break;
                    }
                    // Check both ISBN-10 and ISBN-13 lookups
                    Optional<CachedBook> cachedBook = isbn.length() == 10 
                        ? cachedBookRepository.findByIsbn10(isbn)
                        : cachedBookRepository.findByIsbn13(isbn);
                    
                    if (cachedBook.isPresent() && cachedBook.get().getGoogleBooksId() != null) {
                        isbnToGoogleIdFromRedis.put(isbn, cachedBook.get().getGoogleBooksId());
                        logger.debug("Found Google Book ID {} for ISBN {} in Redis", cachedBook.get().getGoogleBooksId(), isbn);
                        foundInRedisCount++;
                    }
                }
                logger.info("Found {} Google Book IDs in Redis out of {} needed.", foundInRedisCount, isbnsToFetchGoogleId.size());
            } else {
                logger.info("Redis is not available or check failed; skipping Redis lookup for Google Book IDs for NYT scheduler.");
            }
            
            // Remove ISBNs that we already found in Redis
            isbnsToFetchGoogleId.removeAll(isbnToGoogleIdFromRedis.keySet());

            Map<String, String> isbnToGoogleIdMap = new HashMap<>(isbnToGoogleIdFromRedis);
            
            // Check if the API circuit breaker is open (rate limit exceeded)
            if (!apiCircuitBreakerService.isApiCallAllowed().join()) {
                logger.warn("API Circuit breaker is OPEN - skipping Google Books API calls. {} ISBNs will use fallback sources.", isbnsToFetchGoogleId.size());
                isbnsToFetchGoogleId.clear(); // Don't try to fetch from API
            }
            
            if (!isbnsToFetchGoogleId.isEmpty() && googleBooksApiCallsThisRun < googleBooksApiMaxCallsPerJob) {
                logger.info("Attempting to batch fetch Google Book IDs for {} unique ISBNs.", isbnsToFetchGoogleId.size());
                try {
                    // Estimate calls for this batch operation for the job's total call count.
                    // Actual API calls and rate limiting are handled within GoogleBooksService.
                    int estimatedCallsForThisBatch = Math.min(isbnsToFetchGoogleId.size() / ISBN_PROCESSING_BATCH_SIZE + 1, googleBooksApiMaxCallsPerJob - googleBooksApiCallsThisRun); // Rough estimate
                    
                    // GoogleBooksService.fetchGoogleBookIdsForMultipleIsbns internally uses Resilience4j via its calls to searchBooks 
                    // and manages delays between its own batches.
                    Map<String, String> newlyFetchedIds = googleBooksService.fetchGoogleBookIdsForMultipleIsbns(isbnsToFetchGoogleId, null)
                                                       .blockOptional(java.time.Duration.ofMinutes(5)) // Generous timeout for batch
                                                       .orElse(Collections.emptyMap());
                    isbnToGoogleIdMap.putAll(newlyFetchedIds);
                    googleBooksApiCallsThisRun += estimatedCallsForThisBatch; // Update job's call count
                    logger.info("Batch fetched {} Google Book IDs. Estimated API calls for this step: {}", newlyFetchedIds.size(), estimatedCallsForThisBatch);

                } catch (Exception e) {
                    logger.error("Error during batch fetching Google Book IDs: {}", e.getMessage(), e);
                }
            }

            // Step 3: Update S3 list JSONs with fetched Google Book IDs and collect all Google IDs for full data fetch
            Set<String> googleBookIdsForFullFetch = new HashSet<>();
            Map<String, NytBookWithContext> googleIdToNytContextMap = new HashMap<>(); // To pass NYT data for merging

            for (NytBookWithContext bookContext : allNytBooksToProcess) {
                String effectiveIsbn = bookContext.getEffectiveIsbn();
                ObjectNode bookDataInS3List = findOrCreateBookInS3ListJson(bookContext.listJsonForS3(), bookContext.nytBookApiNode(), effectiveIsbn);
                // Mark that the S3 list JSON has new NYT book entries and needs persisting
                bookContext.setListModifiedFlag();
                boolean listEntryModified = false;

                // Merge NYT API data into the S3 list entry
                bookContext.nytBookApiNode().fields().forEachRemaining(entry -> {
                    if (!entry.getKey().equals("google_book_id") || !bookDataInS3List.hasNonNull("google_book_id")) {
                        bookDataInS3List.set(entry.getKey(), entry.getValue());
                        // listEntryModified = true; // This modification is expected
                    }
                });


                if (effectiveIsbn != null && isbnToGoogleIdMap.containsKey(effectiveIsbn)) {
                    String googleId = isbnToGoogleIdMap.get(effectiveIsbn);
                    if (!bookDataInS3List.hasNonNull("google_book_id") || !bookDataInS3List.get("google_book_id").asText().equals(googleId)) {
                        bookDataInS3List.put("google_book_id", googleId);
                        listEntryModified = true; // Mark the S3 list JSON as modified
                        bookContext.setListModifiedFlag();
                    }
                }
                
                String finalGoogleId = bookDataInS3List.path("google_book_id").asText(null);
                if (finalGoogleId != null && !finalGoogleId.isEmpty()) {
                    googleBookIdsForFullFetch.add(finalGoogleId);
                    googleIdToNytContextMap.put(finalGoogleId, bookContext); // Store context for merging later
                }
                 if (listEntryModified) { // If the specific book entry in the list was modified (e.g. new google_id)
                    bookContext.setListModifiedFlag();
                }
            }
            
            // Step 4: Batch fetch full book data for all unique Google Book IDs
            Map<String, Book> fullGoogleBooksDataMap = new HashMap<>();
            
            // First check Redis for existing book data
            Set<String> googleBookIdsToFetchFromApi = new HashSet<>(googleBookIdsForFullFetch);
            boolean isRedisActuallyAvailableForBookData = false;
            try {
                isRedisActuallyAvailableForBookData = redisCacheService.isRedisAvailableAsync().get(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.warn("Redis availability check (for book data) was interrupted for NYT scheduler", e);
                Thread.currentThread().interrupt();
            } catch (TimeoutException e) {
                logger.warn("Redis availability check (for book data) timed out for NYT scheduler", e);
            } catch (ExecutionException e) {
                logger.warn("Failed to check Redis availability (for book data) for NYT scheduler: {}", e.getMessage(), e);
            }

            if (isRedisActuallyAvailableForBookData) {
                logger.info("Redis is available. Checking for {} Google Book IDs in cache before API calls.", googleBookIdsForFullFetch.size());
                for (String googleBookId : googleBookIdsForFullFetch) {
                     if (shutdownInProgress.get()) {
                        logger.info("Shutdown signaled during Redis check for full book data. Aborting further checks.");
                        break;
                    }
                    Optional<CachedBook> cachedBook = cachedBookRepository.findByGoogleBooksId(googleBookId);
                    if (cachedBook.isPresent()) {
                        fullGoogleBooksDataMap.put(googleBookId, cachedBook.get().toBook());
                        googleBookIdsToFetchFromApi.remove(googleBookId);
                        logger.debug("Found full book data for Google Book ID {} in Redis", googleBookId);
                    }
                }
                logger.info("Found {} full book data entries in Redis cache. Will attempt to fetch {} remaining from API.", 
                    fullGoogleBooksDataMap.size(), googleBookIdsToFetchFromApi.size());
            } else {
                logger.info("Redis is not available or check failed; will attempt to fetch all {} Google Book IDs from API.", googleBookIdsForFullFetch.size());
            }
            
            // Check circuit breaker before fetching full book data
            if (!apiCircuitBreakerService.isApiCallAllowed().join()) {
                logger.warn("API Circuit breaker is OPEN - skipping full book data fetch from Google Books API for {} IDs", googleBookIdsToFetchFromApi.size());
                googleBookIdsToFetchFromApi.clear();
            }
            
            if (!googleBookIdsToFetchFromApi.isEmpty() && googleBooksApiCallsThisRun < googleBooksApiMaxCallsPerJob) {
                logger.info("Attempting to batch fetch full data for {} unique Google Book IDs.", googleBookIdsToFetchFromApi.size());
                try {
                    // The fetchMultipleBooksByIdsTiered uses BookDataOrchestrator which has its own resilience
                    // The number of actual API calls depends on S3 cache hits within the orchestrator
                    List<Book> fetchedBooks = googleBooksService.fetchMultipleBooksByIdsTiered(new ArrayList<>(googleBookIdsToFetchFromApi))
                                                              .collectList()
                                                              .blockOptional(java.time.Duration.ofMinutes(10)) // Generous timeout
                                                              .orElse(Collections.emptyList());
                    fetchedBooks.forEach(book -> fullGoogleBooksDataMap.put(book.getId(), book));
                    
                    // Estimate calls for this operation for the job's total call count.
                    // Actual API calls and rate limiting are handled by BookDataOrchestrator used by fetchMultipleBooksByIdsTiered.
                    int estimatedCallsForFullData = Math.min(googleBookIdsToFetchFromApi.size(), googleBooksApiMaxCallsPerJob - googleBooksApiCallsThisRun); // Each ID could be a cache miss
                    googleBooksApiCallsThisRun += estimatedCallsForFullData;
                    logger.info("Batch fetched full data for {} Google Books. Estimated API calls for this step: {}", fetchedBooks.size(), estimatedCallsForFullData);

                } catch (Exception e) {
                    logger.error("Error during batch fetching full Google Books data: {}", e.getMessage(), e);
                }
            }

            // Step 5: Process each NYT book: merge with full Google data, save individual S3 book JSON
            for (NytBookWithContext bookContext : allNytBooksToProcess) {
                ObjectNode bookDataInS3List = findBookInS3ListJson(bookContext.listJsonForS3(), bookContext.getEffectiveIsbn());
                if (bookDataInS3List == null) continue; // Should not happen if findOrCreateBookInS3ListJson was used
                
                // String effectiveIsbn = bookContext.getEffectiveIsbn(); // Unused local variable
                // String googleBookId = bookDataInS3List.path("google_book_id").asText(null); // This was the duplicate
                
                // Extract ISBN information from NYT data
                String currentBookGoogleId = bookDataInS3List.path("google_book_id").asText(null); // ID from S3 list (might be from API or Redis)
                Book preFetchedGoogleBook = (currentBookGoogleId != null) ? fullGoogleBooksDataMap.get(currentBookGoogleId) : null;
                
                // Delegate individual book processing to the new service
                Optional<String> processedBookIdentifierOpt = nytIndividualBookProcessorService.processBook(
                    bookContext.nytBookApiNode(),
                    preFetchedGoogleBook,
                    currentBookGoogleId // Pass the googleBookId known at this stage
                );

                // If the book was processed and an existing S3 key was found (because it was already in Redis),
                // update the current S3 list entry.
                if (processedBookIdentifierOpt.isPresent()) {
                    try {
                        Optional<CachedBook> processedCachedBook = nytIndividualBookProcessorService.findBookByAnyIdentifierAsync(
                            processedBookIdentifierOpt.get().matches("\\d{10,13}") ? null : processedBookIdentifierOpt.get(), // googleId if not ISBN-like
                            processedBookIdentifierOpt.get().length() == 13 ? processedBookIdentifierOpt.get() : null, // isbn13
                            processedBookIdentifierOpt.get().length() == 10 ? processedBookIdentifierOpt.get() : null  // isbn10
                        ).join(); // Blocking call for scheduler context
                        if (processedCachedBook.isPresent() && processedCachedBook.get().getS3Key() != null) {
                             if (!bookDataInS3List.hasNonNull("cached_s3_key") || !bookDataInS3List.get("cached_s3_key").asText().equals(processedCachedBook.get().getS3Key())) {
                                bookDataInS3List.put("cached_s3_key", processedCachedBook.get().getS3Key());
                                bookContext.setListModifiedFlag();
                                logger.debug("Updated S3 list entry for NYT book '{}' with cached_s3_key: {}", bookContext.nytBookApiNode().path("title").asText(), processedCachedBook.get().getS3Key());
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Error finding book by identifier for NYT book: {}", e.getMessage());
                    }
                } else {
                     logger.warn("Processing failed for NYT book: {}", bookContext.nytBookApiNode().path("title").asText("N/A"));
                }
            }

            // Step 6: Save all modified S3 list JSONs and persist to Redis
            allNytBooksToProcess.stream()
                .filter(NytBookWithContext::isListModified)
                .map(NytBookWithContext::getS3ListKey)
                .distinct()
                .forEach(s3ListKeyToSave -> {
                    JsonNode listJsonNodeToSave = listJsonCache.get(s3ListKeyToSave); // Get from our temporary cache
                    if (listJsonNodeToSave instanceof ObjectNode listJsonToSave) { // Check type and cast
                        try {
                            String listJsonString = objectMapper.writeValueAsString(listJsonToSave);
                            s3StorageService.uploadGenericJsonAsync(s3ListKeyToSave, listJsonString, false).join();
                            logger.info("Successfully uploaded updated NYT list to S3: {}", s3ListKeyToSave);
                            
                            // Persist NYT list to Redis
                            persistNytListToRedis(s3ListKeyToSave, listJsonToSave);
                        } catch (IOException e) {
                            logger.error("Error uploading updated NYT list {} to S3: {}", s3ListKeyToSave, e.getMessage(), e);
                        }
                    }
                });

            logger.info("Finished New York Times Bestseller processing job.");

        } catch (Exception e) {
            logger.error("Error during New York Times Bestseller processing job: {}", e.getMessage(), e);
            // This single catch block will handle all exceptions from the try block.
        } finally {
            isProcessing.set(false);
        }
    }
    
    @PreDestroy
    public void shutdown() {
        if (isProcessing.get()) {
            logger.warn("New York Times Bestseller processing is in progress during shutdown. Waiting for completion...");
            // Give the process some time to complete
            int waitSeconds = 30;
            while (isProcessing.get() && waitSeconds > 0) {
                try {
                    Thread.sleep(1000);
                    waitSeconds--;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Interrupted while waiting for NYT processing to complete during shutdown");
                    break;
                }
            }
            if (isProcessing.get()) {
                logger.error("NYT processing did not complete within 30 seconds during shutdown");
            }
        }
    }
    
    private ObjectNode loadOrCreateS3ListJson(String s3ListKey, JsonNode listNodeFromApi, String bestsellersDate, String currentPublishedDate, Map<String, JsonNode> listJsonCache) {
        if (listJsonCache.containsKey(s3ListKey)) {
            return (ObjectNode) listJsonCache.get(s3ListKey);
        }

        try {
            S3FetchResult<String> s3FetchResult = s3StorageService.fetchGenericJsonAsync(s3ListKey).join();
            if (s3FetchResult.isSuccess() && s3FetchResult.getData().isPresent()) {
                ObjectNode loadedNode = (ObjectNode) objectMapper.readTree(s3FetchResult.getData().get());
                listJsonCache.put(s3ListKey, loadedNode);
                logger.info("Loaded existing list {} from S3.", s3ListKey);
                return loadedNode;
            }
        } catch (IOException e) {
            logger.error("Error reading/parsing existing S3 list {}: {}. Creating new.", s3ListKey, e.getMessage());
        } catch (Exception e) { // Catch other potential exceptions from join()
             logger.error("Unexpected error loading S3 list {}: {}. Creating new.", s3ListKey, e.getMessage());
        }
        
        // Create new structure if not found or error
        ObjectNode newListJson = objectMapper.createObjectNode();
        newListJson.put("bestsellers_date", bestsellersDate);
        newListJson.put("published_date", currentPublishedDate);
        newListJson.put("list_name", listNodeFromApi.path("list_name").asText());
        newListJson.put("list_name_encoded", listNodeFromApi.path("list_name_encoded").asText());
        newListJson.put("display_name", listNodeFromApi.path("display_name").asText());
        newListJson.put("updated_frequency", listNodeFromApi.path("updated").asText());
        newListJson.putArray("books");
        listJsonCache.put(s3ListKey, newListJson);
        logger.info("Creating new structure for list {} in S3.", s3ListKey);
        return newListJson;
    }

    private ObjectNode findBookInS3ListJson(ObjectNode listJsonForS3, String effectiveIsbn) {
        if (effectiveIsbn == null || !listJsonForS3.has("books")) return null;
        ArrayNode booksArray = (ArrayNode) listJsonForS3.get("books");
        for (JsonNode bookNode : booksArray) {
            String isbn13 = bookNode.path("primary_isbn13").asText(null);
            String isbn10 = bookNode.path("primary_isbn10").asText(null);
            if (effectiveIsbn.equals(isbn13) || effectiveIsbn.equals(isbn10)) {
                return (ObjectNode) bookNode;
            }
        }
        return null;
    }
    
    private ObjectNode findOrCreateBookInS3ListJson(ObjectNode listJsonForS3, JsonNode nytBookApiNode, String effectiveIsbn) {
        ObjectNode existingBookNode = findBookInS3ListJson(listJsonForS3, effectiveIsbn);
        if (existingBookNode != null) {
            return existingBookNode;
        }
        // If not found, create it from nytBookApiNode and add to the listJsonForS3's "books" array
        ObjectNode newBookNode = objectMapper.valueToTree(nytBookApiNode);
        ((ArrayNode) listJsonForS3.path("books")).add(newBookNode);
        return newBookNode;
    }

        // Helper record to carry context along with NYT book data
    private static class NytBookWithContext {
        private final JsonNode nytBookApiNode; // Raw NYT book data from current API call
        private final ObjectNode listJsonForS3; // The S3 JSON object for the list this book belongs to
        private final String s3ListKey;
        private boolean listModified = false;


        public NytBookWithContext(JsonNode nytBookApiNode, ObjectNode listJsonForS3, String s3ListKey) {
            this.nytBookApiNode = nytBookApiNode;
            this.listJsonForS3 = listJsonForS3;
            this.s3ListKey = s3ListKey;
        }

        public JsonNode nytBookApiNode() { return nytBookApiNode; }
        public ObjectNode listJsonForS3() { return listJsonForS3; }
        public String getEffectiveIsbn() {
            String isbn13 = nytBookApiNode.path("primary_isbn13").asText(null);
            return isbn13 != null ? isbn13 : nytBookApiNode.path("primary_isbn10").asText(null);
        }
        public String getS3ListKey() { return s3ListKey; }
        public boolean isListModified() { return listModified; }
        public void setListModifiedFlag() { this.listModified = true; }
    }

    // Methods related to individual book processing (generateConsistentS3Key, findBookByAnyIdentifier, persistBookToRedis, enrichWithNytData)
    // have been moved to NytIndividualBookProcessorService.
    // The scheduler will now call that service.
    
    /**
     * Persists NYT list metadata to Redis
     * @param listKey The Redis key for the list
     * @param listData The list data as ObjectNode
     */
    private void persistNytListToRedis(String s3ListKey, ObjectNode listData) {
        boolean isRedisActuallyAvailableForListPersistence = false;
        try {
            isRedisActuallyAvailableForListPersistence = redisCacheService.isRedisAvailableAsync().get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Redis availability check (for NYT list persistence) was interrupted for S3 key: {}", s3ListKey, e);
            Thread.currentThread().interrupt();
            return; // Exit if interrupted
        } catch (TimeoutException e) {
            logger.warn("Redis availability check (for NYT list persistence) timed out for S3 key: {}", s3ListKey, e);
            return; // Exit on timeout
        } catch (ExecutionException e) {
            logger.warn("Failed to check Redis availability (for NYT list persistence) for S3 key: {}: {}", s3ListKey, e.getMessage(), e);
            return; // Exit on other failures
        }

        if (!isRedisActuallyAvailableForListPersistence) {
            logger.warn("Redis not available or check failed, skipping NYT list persistence for S3 key: {}", s3ListKey);
            return;
        }
        logger.debug("Redis is available. Attempting to persist NYT list from S3 key {} to Redis.", s3ListKey);
        
        try {
            // Extract list metadata from S3 key and data
            // Format: lists/nyt/{list_name_encoded}/{published_date}.json
            String[] parts = s3ListKey.split("/");
            if (parts.length >= 4) {
                String listNameEncoded = parts[2];
                String publishedDate = parts[3].replace(".json", "");
                
                // Create Redis key for the list
                String redisListKey = String.format("nyt_list:%s:%s", listNameEncoded, publishedDate);
                logger.debug("Persisting NYT list to Redis key: {}", redisListKey);
                
                // Store the entire list JSON
                redisCacheService.cacheStringAsync(redisListKey, objectMapper.writeValueAsString(listData)).join(); // cacheStringAsync has its own logging
                
                // Also store a reference to the latest list for this category
                String latestListKey = String.format("nyt_list:latest:%s", listNameEncoded);
                logger.debug("Updating latest list reference for '{}' to: {}", listNameEncoded, redisListKey);
                redisCacheService.cacheStringAsync(latestListKey, redisListKey).join();
                
                // Extract and store book IDs separately for easy access
                if (listData.has("books")) {
                    ArrayNode books = (ArrayNode) listData.get("books");
                    List<String> bookIdentifiers = new ArrayList<>(); // Changed name for clarity
                    for (JsonNode book : books) {
                        String googleBookId = book.path("google_book_id").asText(null);
                        if (googleBookId != null && !googleBookId.isEmpty()) {
                            bookIdentifiers.add("gbid:" + googleBookId);
                        } else {
                            // Fallback to ISBN
                            String isbn13 = book.path("primary_isbn13").asText(null);
                            if (isbn13 != null && !isbn13.isEmpty()) {
                                bookIdentifiers.add("isbn13:" + isbn13);
                            } else {
                                String isbn10 = book.path("primary_isbn10").asText(null);
                                if (isbn10 != null && !isbn10.isEmpty()) {
                                     bookIdentifiers.add("isbn10:" + isbn10);
                                } else {
                                    logger.warn("NYT book entry in list {} (date {}) has no Google ID or ISBN. Title: {}", listNameEncoded, publishedDate, book.path("title").asText("N/A"));
                                }
                            }
                        }
                    }
                    
                    // Store the list of book IDs
                    String bookIdentifiersKey = String.format("nyt_list:book_identifiers:%s:%s", listNameEncoded, publishedDate);
                    logger.debug("Storing {} book identifiers for list key: {}", bookIdentifiers.size(), bookIdentifiersKey);
                    redisCacheService.cacheStringAsync(bookIdentifiersKey, objectMapper.writeValueAsString(bookIdentifiers)).join();
                }
                
                logger.info("Successfully persisted NYT list to Redis - List: {}, Date: {}, Main Redis Key: {}", listNameEncoded, publishedDate, redisListKey);
            } else {
                logger.warn("Could not parse S3 key '{}' to extract NYT list metadata for Redis persistence.", s3ListKey);
            }
        } catch (Exception e) {
            logger.error("Error persisting NYT list (from S3 key {}) to Redis: {}", s3ListKey, e.getMessage(), e);
        }
    }
}
