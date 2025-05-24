/**
 * Service for managing book IDs in S3 for sitemap generation
 * 
 * @author William Callahan
 * 
 * Key responsibilities:
 * - Fetch accumulated book IDs from S3 asynchronously
 * - Update S3 with new book IDs from cache asynchronously
 * - Handle errors gracefully for sitemap generation
 */

package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Service
public class BookSitemapService {

    private static final Logger logger = LoggerFactory.getLogger(BookSitemapService.class);

    private final S3StorageService s3StorageService;
    private final BookCacheFacadeService bookCacheFacadeService;
    private final ObjectMapper objectMapper;
    private final String accumulatedIdsS3Key;

    public BookSitemapService(S3StorageService s3StorageService,
                              BookCacheFacadeService bookCacheFacadeService,
                              ObjectMapper objectMapper,
                              @Value("${sitemap.s3.accumulated-ids-key}") String accumulatedIdsS3Key) {
        this.s3StorageService = s3StorageService;
        this.bookCacheFacadeService = bookCacheFacadeService;
        this.objectMapper = objectMapper;
        this.accumulatedIdsS3Key = accumulatedIdsS3Key;
    }

    /**
     * Internal method to asynchronously fetch and parse book IDs from S3.
     */
    private CompletableFuture<Set<String>> fetchAndParseBookIdsFromS3InternalAsync() {
        return s3StorageService.fetchGenericJsonAsync(accumulatedIdsS3Key)
            .thenApply(s3Result -> {
                if (s3Result.isSuccess() && s3Result.getData().isPresent()) {
                    try {
                        String jsonContent = s3Result.getData().get();
                        if (jsonContent.trim().isEmpty()) {
                            logger.info("S3 file '{}' is empty. Returning new HashSet.", accumulatedIdsS3Key);
                            return new HashSet<String>();
                        }
                        Set<String> bookIds = objectMapper.readValue(jsonContent, new TypeReference<Set<String>>() {});
                        return bookIds != null ? bookIds : new HashSet<String>();
                    } catch (IOException e) {
                        logger.error("IOException parsing S3 content for key '{}': {}. Returning empty set.", accumulatedIdsS3Key, e.getMessage());
                        return new HashSet<String>();
                    }
                } else if (s3Result.isNotFound()) {
                    logger.info("Accumulated book IDs file not found in S3 ('{}'). Returning new HashSet for initial creation.", accumulatedIdsS3Key);
                    return new HashSet<String>();
                } else {
                    logger.error("Failed to fetch S3 content for key '{}': {}. Returning empty set.", accumulatedIdsS3Key, s3Result.getErrorMessage().orElse("Unknown S3 error"));
                    return new HashSet<String>();
                }
            })
            .exceptionally(ex -> {
                logger.error("Unexpected exception fetching/parsing '{}' from S3: {}. Returning empty set.", accumulatedIdsS3Key, ex.getMessage(), ex);
                return new HashSet<String>();
            });
    }

    /**
     * Asynchronously get accumulated book IDs from S3 for sitemap generation.
     */
    public CompletableFuture<Set<String>> getAccumulatedBookIdsFromS3Async() {
        return fetchAndParseBookIdsFromS3InternalAsync();
    }

    /**
     * Asynchronously update accumulated book IDs in S3 with new IDs from cache.
     */
    public CompletableFuture<Void> updateAccumulatedBookIdsInS3Async() {
        logger.info("Starting ASYNC update of accumulated book IDs in S3 (key: {}).", accumulatedIdsS3Key);
        return fetchAndParseBookIdsFromS3InternalAsync()
            .thenComposeAsync(accumulatedIds -> {
                int initialSize = accumulatedIds.size();
                logger.info("Fetched {} existing book IDs from S3.", initialSize);
                return bookCacheFacadeService.getAllCachedBookIds()
                    .thenComposeAsync(currentCachedIdsOpt -> {
                        Set<String> currentCachedIds = currentCachedIdsOpt != null ? currentCachedIdsOpt : Collections.emptySet();
                        if (currentCachedIdsOpt == null) {
                            logger.warn("bookCacheFacadeService.getAllCachedBookIds() returned null. Using empty set for current update cycle.");
                        }
                        logger.info("Fetched {} book IDs from current cache.", currentCachedIds.size());
                        boolean newIdsAdded = accumulatedIds.addAll(currentCachedIds);
                        if (newIdsAdded || accumulatedIds.size() != initialSize) {
                            logger.info("New book IDs found or size changed. Total accumulated IDs: {}. Sorting and uploading to S3.", accumulatedIds.size());
                            try {
                                List<String> sortedIds = new ArrayList<>(accumulatedIds);
                                Collections.sort(sortedIds);
                                String updatedJson = objectMapper.writeValueAsString(sortedIds);
                                return s3StorageService.uploadGenericJsonAsync(accumulatedIdsS3Key, updatedJson, false)
                                    .thenRun(() -> logger.info("Successfully uploaded {} sorted accumulated book IDs to S3 (key: {}).", sortedIds.size(), accumulatedIdsS3Key))
                                    .exceptionally(ex -> {
                                        logger.error("Error uploading sorted accumulated book IDs to S3 (key: {}): {}", accumulatedIdsS3Key, ex.getMessage(), ex);
                                        return null;
                                    });
                            } catch (IOException e) {
                                logger.error("Error serializing accumulated book IDs for S3 (key: {}): {}", accumulatedIdsS3Key, e.getMessage(), e);
                                return CompletableFuture.failedFuture(e);
                            }
                        } else {
                            logger.info("No new book IDs to add from cache, and size unchanged. S3 file (key: {}) remains unchanged with {} IDs.", accumulatedIdsS3Key, accumulatedIds.size());
                            return CompletableFuture.completedFuture(null);
                        }
                    })
                    .exceptionally(ex -> {
                        logger.error("Error fetching IDs from BookCacheFacadeService during S3 update: {}", ex.getMessage(), ex);
                        return null;
                    });
            })
            .exceptionally(ex -> {
                logger.error("CRITICAL: Failed to read existing accumulated book IDs from S3 (key: {}). Aborting update cycle. Error: {}", accumulatedIdsS3Key, ex.getMessage(), ex);
                return null;
            });
    }

    // Synchronous wrappers removed in favor of async methods

    // Synchronous wrappers removed in favor of async methods
}
