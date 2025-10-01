package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Encapsulates the S3 cache update heuristics that were previously part of
 * {@link BookDataOrchestrator}. Keeping this logic in a dedicated service lets
 * other components reuse the same decision-making while shrinking the orchestrator.
 */
/**
 * @deprecated Replaced by Postgres-first persistence. S3 JSON cache updates are no longer used
 * for book metadata and will be removed in version 1.0. This deprecation does NOT affect
 * S3 image persistence, which remains vital for book cover storage and delivery.
 */
@Deprecated
@Component
public class BookS3CacheService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BookS3CacheService.class);

    private final S3RetryService s3RetryService;
    private final ObjectMapper objectMapper;

    BookS3CacheService(S3RetryService s3RetryService, ObjectMapper objectMapper) {
        this.s3RetryService = s3RetryService;
        this.objectMapper = objectMapper;
    }

    Mono<Book> updateCache(Book bookToCache, JsonNode jsonToCache, String apiTypeContext, String s3Key) {
        if (ValidationUtils.isNullOrBlank(s3Key)) {
            LOGGER.error("BookDataOrchestrator ({}) - S3 Update: S3 key is null or empty for book title: {}. Cannot update S3 cache.", apiTypeContext, bookToCache.getTitle());
            return Mono.just(bookToCache);
        }
        String newRawJson = jsonToCache.toString();

        return Mono.fromCompletionStage(s3RetryService.fetchJsonWithRetry(s3Key))
            .flatMap(s3Result -> {
                Book bookToReturn = bookToCache;

                if (s3Result.isSuccess() && s3Result.getData().isPresent()) {
                    String existingRawJson = s3Result.getData().get();
                    try {
                        JsonNode existingS3JsonNode = objectMapper.readTree(existingRawJson);
                        Book existingBookFromS3 = BookJsonParser.convertJsonToBook(existingS3JsonNode);

                        if (shouldUpdateS3(existingBookFromS3, bookToCache, existingRawJson, newRawJson, s3Key, apiTypeContext)) {
                            return Mono.fromCompletionStage(s3RetryService.uploadJsonWithRetry(s3Key, newRawJson))
                                .doOnSuccess(v -> LOGGER.info("BookDataOrchestrator ({}) - S3 Update: Successfully overwrote S3 for S3 key: {}", apiTypeContext, s3Key))
                                .doOnError(e -> LoggingUtils.error(LOGGER, e,
                                    "BookDataOrchestrator ({}) - S3 Update: Failed to overwrite S3 for S3 key: {}",
                                    apiTypeContext,
                                    s3Key))
                                .thenReturn(bookToCache);
                        } else {
                            LOGGER.info("BookDataOrchestrator ({}) - S3 Update: Existing S3 data for S3 key {} is preferred or identical. Not overwriting.", apiTypeContext, s3Key);
                            bookToReturn = existingBookFromS3;
                            return Mono.just(bookToReturn);
                        }
                    } catch (Exception e) {
                        LoggingUtils.error(LOGGER, e,
                            "BookDataOrchestrator ({}) - S3 Update: Error processing existing S3 data for S3 key {}. Defaulting to overwrite S3 with new data.",
                            apiTypeContext,
                            s3Key);
                        return Mono.fromCompletionStage(s3RetryService.uploadJsonWithRetry(s3Key, newRawJson))
                                .thenReturn(bookToCache);
                    }
                } else {
                    if (s3Result.isNotFound()) {
                        LOGGER.info("BookDataOrchestrator ({}) - S3 Update: No existing S3 data found for S3 key {}. Uploading new data.", apiTypeContext, s3Key);
                    } else if (s3Result.isServiceError()) {
                        LOGGER.warn("BookDataOrchestrator ({}) - S3 Update: S3 service error fetching existing data for S3 key {}. Uploading new data. Error: {}", apiTypeContext, s3Key, s3Result.getErrorMessage().orElse("Unknown S3 Error"));
                    } else if (s3Result.isDisabled()) {
                        LOGGER.info("BookDataOrchestrator ({}) - S3 Update: S3 is disabled. Storing new data for S3 key {}.", apiTypeContext, s3Key);
                    }
                    return Mono.fromCompletionStage(s3RetryService.uploadJsonWithRetry(s3Key, newRawJson))
                        .doOnSuccess(v -> LOGGER.info("BookDataOrchestrator ({}) - S3 Update: Successfully uploaded new data to S3 for S3 key: {}", apiTypeContext, s3Key))
                        .doOnError(e -> LoggingUtils.error(LOGGER, e,
                            "BookDataOrchestrator ({}) - S3 Update: Failed to upload new data to S3 for S3 key: {}",
                            apiTypeContext,
                            s3Key))
                        .thenReturn(bookToCache);
                }
            })
            .onErrorReturn(bookToCache);
    }

    private boolean shouldUpdateS3(Book existingBook,
                                   Book newBook,
                                   String existingRawJson,
                                   String newRawJson,
                                   String s3KeyContext,
                                   String apiTypeContext) {
        if (existingBook == null || ValidationUtils.isNullOrEmpty(existingRawJson)) {
            return true;
        }
        if (newBook == null || ValidationUtils.isNullOrEmpty(newRawJson)) {
            return false;
        }
        if (existingRawJson.equals(newRawJson)) {
            return false;
        }
        String oldDesc = existingBook.getDescription();
        String newDesc = newBook.getDescription();
        if (!ValidationUtils.isNullOrEmpty(newDesc)) {
            if (ValidationUtils.isNullOrEmpty(oldDesc)) {
                return true;
            }
            if (newDesc.length() > oldDesc.length() * 1.1) {
                return true;
            }
        }
        int oldNonNullFields = countNonNullKeyFields(existingBook);
        int newNonNullFields = countNonNullKeyFields(newBook);
        if (newNonNullFields > oldNonNullFields) {
            return true;
        }
        LOGGER.debug("BookDataOrchestrator ({}): Keeping existing data for S3 key {} as heuristics did not favour new data.", apiTypeContext, s3KeyContext);
        return false;
    }

    private int countNonNullKeyFields(Book book) {
        if (book == null) {
            return 0;
        }
        int count = 0;
        if (ValidationUtils.hasText(book.getPublisher())) count++;
        if (book.getPublishedDate() != null) count++;
        if (book.getPageCount() != null && book.getPageCount() > 0) count++;
        if (ValidationUtils.hasText(book.getIsbn10())) count++;
        if (ValidationUtils.hasText(book.getIsbn13())) count++;
        if (!ValidationUtils.isNullOrEmpty(book.getCategories())) count++;
        if (ValidationUtils.hasText(book.getLanguage())) count++;
        return count;
    }
}
