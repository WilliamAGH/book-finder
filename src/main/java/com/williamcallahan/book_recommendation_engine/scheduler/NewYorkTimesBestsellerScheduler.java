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
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookDataAggregatorService;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.NewYorkTimesService;
import com.williamcallahan.book_recommendation_engine.service.OpenLibraryBookDataService;
import com.williamcallahan.book_recommendation_engine.service.S3StorageService;
import com.williamcallahan.book_recommendation_engine.service.s3.S3FetchResult;
// import org.springframework.jdbc.core.JdbcTemplate; // removed if not used directly

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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

@Component
public class NewYorkTimesBestsellerScheduler {

    private static final Logger logger = LoggerFactory.getLogger(NewYorkTimesBestsellerScheduler.class);
    private static final int ISBN_PROCESSING_BATCH_SIZE = 5; // Used for estimating calls

    private final NewYorkTimesService newYorkTimesService;
    private final GoogleBooksService googleBooksService;
    private final S3StorageService s3StorageService;
    private final ObjectMapper objectMapper;
    private final BookDataAggregatorService bookDataAggregatorService;
    private final OpenLibraryBookDataService openLibraryBookDataService;
    // private final JdbcTemplate jdbcTemplate;

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
     * @param bookDataAggregatorService Service for merging book data from multiple sources
     * @param openLibraryBookDataService Service for OpenLibrary data
     */
    public NewYorkTimesBestsellerScheduler(NewYorkTimesService newYorkTimesService,
                                           GoogleBooksService googleBooksService,
                                           S3StorageService s3StorageService,
                                           ObjectMapper objectMapper,
                                           BookDataAggregatorService bookDataAggregatorService,
                                           OpenLibraryBookDataService openLibraryBookDataService) {
        this.newYorkTimesService = newYorkTimesService;
        this.googleBooksService = googleBooksService;
        this.s3StorageService = s3StorageService;
        this.objectMapper = objectMapper;
        this.bookDataAggregatorService = bookDataAggregatorService;
        this.openLibraryBookDataService = openLibraryBookDataService;
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

        logger.info("Starting New York Times Bestseller processing job.");
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

            Map<String, String> isbnToGoogleIdMap = new HashMap<>();
            if (!isbnsToFetchGoogleId.isEmpty() && googleBooksApiCallsThisRun < googleBooksApiMaxCallsPerJob) {
                logger.info("Attempting to batch fetch Google Book IDs for {} unique ISBNs.", isbnsToFetchGoogleId.size());
                try {
                    // Estimate calls for this batch operation for the job's total call count.
                    // Actual API calls and rate limiting are handled within GoogleBooksService.
                    int estimatedCallsForThisBatch = Math.min(isbnsToFetchGoogleId.size() / ISBN_PROCESSING_BATCH_SIZE + 1, googleBooksApiMaxCallsPerJob - googleBooksApiCallsThisRun); // Rough estimate
                    
                    // GoogleBooksService.fetchGoogleBookIdsForMultipleIsbns internally uses Resilience4j via its calls to searchBooks 
                    // and manages delays between its own batches.
                    isbnToGoogleIdMap = googleBooksService.fetchGoogleBookIdsForMultipleIsbns(isbnsToFetchGoogleId, null)
                                                       .blockOptional(java.time.Duration.ofMinutes(5)) // Generous timeout for batch
                                                       .orElse(Collections.emptyMap());
                    googleBooksApiCallsThisRun += estimatedCallsForThisBatch; // Update job's call count
                    logger.info("Batch fetched {} Google Book IDs. Estimated API calls for this step: {}", isbnToGoogleIdMap.size(), estimatedCallsForThisBatch);

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
            if (!googleBookIdsForFullFetch.isEmpty() && googleBooksApiCallsThisRun < googleBooksApiMaxCallsPerJob) {
                logger.info("Attempting to batch fetch full data for {} unique Google Book IDs.", googleBookIdsForFullFetch.size());
                try {
                    // The fetchMultipleBooksByIdsTiered uses BookDataOrchestrator which has its own resilience
                    // The number of actual API calls depends on S3 cache hits within the orchestrator
                    List<Book> fetchedBooks = googleBooksService.fetchMultipleBooksByIdsTiered(new ArrayList<>(googleBookIdsForFullFetch))
                                                              .collectList()
                                                              .blockOptional(java.time.Duration.ofMinutes(10)) // Generous timeout
                                                              .orElse(Collections.emptyList());
                    fetchedBooks.forEach(book -> fullGoogleBooksDataMap.put(book.getId(), book));
                    
                    // Estimate calls for this operation for the job's total call count.
                    // Actual API calls and rate limiting are handled by BookDataOrchestrator used by fetchMultipleBooksByIdsTiered.
                    int estimatedCallsForFullData = Math.min(googleBookIdsForFullFetch.size(), googleBooksApiMaxCallsPerJob - googleBooksApiCallsThisRun); // Each ID could be a cache miss
                    googleBooksApiCallsThisRun += estimatedCallsForFullData;
                    logger.info("Batch fetched full data for {} Google Books. Estimated API calls for this step: {}", fullGoogleBooksDataMap.size(), estimatedCallsForFullData);

                } catch (Exception e) {
                    logger.error("Error during batch fetching full Google Books data: {}", e.getMessage(), e);
                }
            }

            // Step 5: Process each NYT book: merge with full Google data, save individual S3 book JSON
            for (NytBookWithContext bookContext : allNytBooksToProcess) {
                ObjectNode bookDataInS3List = findBookInS3ListJson(bookContext.listJsonForS3(), bookContext.getEffectiveIsbn());
                if (bookDataInS3List == null) continue; // Should not happen if findOrCreateBookInS3ListJson was used
                
                String effectiveIsbn = bookContext.getEffectiveIsbn(); // Define effectiveIsbn here for the current bookContext
                String googleBookId = bookDataInS3List.path("google_book_id").asText(null);

                if (googleBookId != null && !googleBookId.isEmpty()) {
                    Book fullGoogleBook = fullGoogleBooksDataMap.get(googleBookId);
                    JsonNode googleBookJsonNode = null;
                    if (fullGoogleBook != null) {
                        try {
                            googleBookJsonNode = (fullGoogleBook.getRawJsonResponse() != null) ?
                                objectMapper.readTree(fullGoogleBook.getRawJsonResponse()) :
                                objectMapper.valueToTree(fullGoogleBook);
                        } catch (IOException e) {
                            logger.error("Error parsing raw JSON for Google Book ID {}: {}", googleBookId, e.getMessage());
                            googleBookJsonNode = objectMapper.valueToTree(fullGoogleBook);
                        }
                    } else {
                        logger.warn("Full Google Books data not found for ID {} in fetched map. Merging NYT data with an empty base.", googleBookId);
                    }
                    
                    ObjectNode mergedBookData = bookDataAggregatorService.prepareEnrichedBookJson(googleBookJsonNode, bookContext.nytBookApiNode(), googleBookId); // This might need to change to use aggregateBookDataSources
                    String s3KeyForIndividualBook = String.format("books/v1/%s.json", googleBookId);
                    try {
                        s3StorageService.uploadGenericJsonAsync(s3KeyForIndividualBook, objectMapper.writeValueAsString(mergedBookData), false).join();
                        logger.info("Updated individual book JSON in S3: {}", s3KeyForIndividualBook);
                    } catch (Exception e) {
                        logger.error("Failed to update individual book JSON {} in S3: {}", s3KeyForIndividualBook, e.getMessage(), e);
                    }
                } else if (effectiveIsbn != null) { // No Google ID found, try OpenLibrary by ISBN
                    logger.info("No Google Book ID found for ISBN {}. Attempting fallback to OpenLibrary.", effectiveIsbn);
                    List<JsonNode> fallbackSources = new ArrayList<>();
                    fallbackSources.add(bookContext.nytBookApiNode()); // Always include NYT data

                    try {
                        Book olBook = openLibraryBookDataService.fetchBookByIsbn(effectiveIsbn).block(java.time.Duration.ofSeconds(10));
                        if (olBook != null) {
                            logger.info("Fetched data from OpenLibrary for ISBN {}", effectiveIsbn);
                            fallbackSources.add(objectMapper.valueToTree(olBook));
                        }
                    } catch (Exception e) {
                        logger.warn("Error fetching from OpenLibrary for ISBN {}: {}", effectiveIsbn, e.getMessage());
                    }
                    // Only try Longitood if OpenLibrary didn't yield much (or specific criteria)
                    // For now, let's assume we try it if OL book is null or has minimal data (e.g. just ISBN)
                    // boolean tryLongitood = fallbackSources.size() == 1 || (fallbackSources.size() > 1 && fallbackSources.get(1).size() < 5); // Simple heuristic
                    // Longitood fetch logic removed as it's cover-only

                    // if (tryLongitood) {
                    //     try {
                    //         Book ltBook = longitoodBookDataService.fetchBookByIsbn(effectiveIsbn).block(java.time.Duration.ofSeconds(10));
                    //         if (ltBook != null) {
                    //             logger.info("Fetched data from Longitood for ISBN {}", effectiveIsbn);
                    //             fallbackSources.add(objectMapper.valueToTree(ltBook));
                    //         }
                    //     } catch (Exception e) {
                    //         logger.warn("Error fetching from Longitood for ISBN {}: {}", effectiveIsbn, e.getMessage());
                    //     }
                    // }
                    
                    if (fallbackSources.size() > 1) { // More than just NYT data (i.e., OpenLibrary data was added)
                        // Use ISBN as the primary ID for aggregation if Google ID is missing
                        ObjectNode aggregatedFallbackData = bookDataAggregatorService.aggregateBookDataSources(effectiveIsbn, "id", fallbackSources.toArray(new JsonNode[0]));
                        String s3KeyForIndividualBook = String.format("books/v1/isbn/%s.json", effectiveIsbn); // Store by ISBN
                        try {
                            s3StorageService.uploadGenericJsonAsync(s3KeyForIndividualBook, objectMapper.writeValueAsString(aggregatedFallbackData), false).join();
                            logger.info("Updated individual book JSON (via ISBN fallback) in S3: {}", s3KeyForIndividualBook);
                            // Update the S3 list entry with a reference or key fields from aggregated data
                            bookDataInS3List.put("fallback_s3_key", s3KeyForIndividualBook);
                            bookDataInS3List.put("title", aggregatedFallbackData.path("title").asText(bookDataInS3List.path("title").asText())); // Update title if better
                            // Potentially update authors, etc.
                            bookContext.setListModifiedFlag();
                        } catch (Exception e) {
                            logger.error("Failed to update individual book JSON (via ISBN fallback) {} in S3: {}", s3KeyForIndividualBook, e.getMessage(), e);
                        }
                    }
                }
            }

            // Step 6: Save all modified S3 list JSONs
            allNytBooksToProcess.stream()
                .filter(NytBookWithContext::isListModified)
                .map(NytBookWithContext::getS3ListKey)
                .distinct()
                .forEach(s3ListKeyToSave -> {
                    JsonNode listJsonNodeToSave = listJsonCache.get(s3ListKeyToSave); // Get from our temporary cache
                    if (listJsonNodeToSave instanceof ObjectNode listJsonToSave) { // Check type and cast
                        try {
                            s3StorageService.uploadGenericJsonAsync(s3ListKeyToSave, objectMapper.writeValueAsString(listJsonToSave), false).join();
                            logger.info("Successfully uploaded updated NYT list to S3: {}", s3ListKeyToSave);
                            // Persist to Postgres as source of truth
                            persistListToDb(s3ListKeyToSave, listJsonToSave);
                        } catch (IOException e) {
                            logger.error("Error uploading updated NYT list {} to S3: {}", s3ListKeyToSave, e.getMessage(), e);
                            // Attempt DB persist even if S3 upload failed
                            try { persistListToDb(s3ListKeyToSave, listJsonToSave); } catch (Exception ignored) {}
                        }
                    }
                });

            logger.info("Finished New York Times Bestseller processing job.");

        } catch (Exception e) {
            logger.error("Error during New York Times Bestseller processing job: {}", e.getMessage(), e);
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
        // private final String listNameEncoded; // Unused
        // private final String publishedDate; // of the list, Unused
        // private final String displayName; // of the list, Unused
        private final String s3ListKey;
        private boolean listModified = false;


        public NytBookWithContext(JsonNode nytBookApiNode, ObjectNode listJsonForS3, String s3ListKey) { // Removed unused constructor parameters
            this.nytBookApiNode = nytBookApiNode;
            this.listJsonForS3 = listJsonForS3;
            // this.listNameEncoded = listNameEncoded;
            // this.publishedDate = publishedDate;
            // this.displayName = displayName;
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

    private void persistListToDb(String s3ListKey, ObjectNode listJson) {
        // DB persistence disabled in this pass; will be implemented with a dedicated DAO/service.
        if (listJson == null) return;
        try {
            // Placeholder: real DB upsert handled in a dedicated service in next step.
        } catch (Exception e) {
            logger.warn("Failed to persist list {} to DB: {}", s3ListKey, e.getMessage());
        }
    }
}
