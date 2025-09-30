/**
 * Service for real-time notification of book cover image updates and search progress
 *
 * @author William Callahan
 *
 * Features:
 * - Notifies connected clients of book cover image updates via WebSockets
 * - Transforms BookCoverUpdatedEvent into WebSocket messages
 * - Provides topic-based routing by book ID
 * - Includes image source information in notifications
 * - Uses lazy initialization to prevent circular dependency issues
 * - Handles null/incomplete events gracefully with logging
 * - Supports real-time search result updates and progress notifications
 * - Routes search events to query-specific WebSocket topics
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import com.williamcallahan.book_recommendation_engine.service.event.SearchProgressEvent;
import com.williamcallahan.book_recommendation_engine.service.event.SearchResultsUpdatedEvent;
import com.williamcallahan.book_recommendation_engine.service.event.BookUpsertEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.core.MessageSendingOperations;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Lazy
public class CoverUpdateNotifierService {

    private static final Logger logger = LoggerFactory.getLogger(CoverUpdateNotifierService.class);
    private final MessageSendingOperations<String> messagingTemplate;

    /**
     * Constructs CoverUpdateNotifierService with required dependencies
     * - Uses lazy-loaded messaging template to avoid circular dependencies
     * - Validates WebSocket configuration is properly initialized
     * 
     * @param messagingTemplate Template for sending WebSocket messages
     * @param webSocketConfig WebSocket broker configuration
     */
    public CoverUpdateNotifierService(@Lazy MessageSendingOperations<String> messagingTemplate,
                                      WebSocketMessageBrokerConfigurer webSocketConfig) {
        this.messagingTemplate = messagingTemplate;
        logger.info("CoverUpdateNotifierService initialized, WebSocketConfig should be ready.");
    }

    /**
     * Processes book cover update events and broadcasts to WebSocket clients
     * - Creates topic-specific messages for each book update
     * - Validates event data for completeness
     * - Includes all relevant cover metadata in message payload
     * - Routes messages to book-specific WebSocket topics
     * 
     * @param event The BookCoverUpdatedEvent containing updated cover information
     */
    @EventListener
    public void handleBookCoverUpdated(BookCoverUpdatedEvent event) {
        if (event.getGoogleBookId() == null || event.getNewCoverUrl() == null) {
            logger.warn("Received BookCoverUpdatedEvent with null googleBookId or newCoverUrl. IdentifierKey: {}, URL: {}, GoogleBookId: {}", 
                event.getIdentifierKey(), event.getNewCoverUrl(), event.getGoogleBookId());
            return;
        }

        String destination = "/topic/book/" + event.getGoogleBookId() + "/coverUpdate";
        
        Map<String, Object> payload = new HashMap<>();
        payload.put("googleBookId", event.getGoogleBookId());
        payload.put("newCoverUrl", event.getNewCoverUrl());
        payload.put("identifierKey", event.getIdentifierKey());
        if (event.getSource() != null) {
            payload.put("sourceName", event.getSource().name()); // e.g., GOOGLE_BOOKS
            payload.put("sourceDisplayName", event.getSource().getDisplayName()); // e.g., "Google Books"
        } else {
            payload.put("sourceName", CoverImageSource.UNDEFINED.name());
            payload.put("sourceDisplayName", CoverImageSource.UNDEFINED.getDisplayName());
        }

        logger.info("Sending cover update to {}: URL = {}, Source = {}", destination, event.getNewCoverUrl(), event.getSource() != null ? event.getSource().name() : "UNDEFINED");
        this.messagingTemplate.convertAndSend(destination, payload);
    }

    /**
     * Processes search results update events and broadcasts to WebSocket clients
     * - Creates topic-specific messages for search result updates
     * - Includes new books and source information
     * - Routes messages to query-specific WebSocket topics
     * 
     * @param event The SearchResultsUpdatedEvent containing new search results
     */
    @EventListener
    public void handleSearchResultsUpdated(SearchResultsUpdatedEvent event) {
        if (event.getQueryHash() == null || event.getNewResults() == null) {
            logger.warn("Received SearchResultsUpdatedEvent with null queryHash or newResults. Query: {}, Source: {}", 
                event.getSearchQuery(), event.getSource());
            return;
        }

        String destination = "/topic/search/" + event.getQueryHash() + "/results";
        
        Map<String, Object> payload = new HashMap<>();
        payload.put("searchQuery", event.getSearchQuery());
        payload.put("source", event.getSource());
        payload.put("totalResultsNow", event.getTotalResultsNow());
        payload.put("isComplete", event.isComplete());
        payload.put("newResultsCount", event.getNewResults().size());
        
        // Convert books to a simplified format for WebSocket transmission
        payload.put("newResults", event.getNewResults().stream()
            .map(book -> {
                Map<String, Object> bookData = new HashMap<>();
                bookData.put("id", book.getId());
                bookData.put("title", book.getTitle());
                bookData.put("authors", book.getAuthors());
                bookData.put("s3ImagePath", book.getS3ImagePath());
                bookData.put("externalImageUrl", book.getExternalImageUrl());
                bookData.put("publishedDate", book.getPublishedDate());
                bookData.put("description", book.getDescription());
                bookData.put("pageCount", book.getPageCount());
                bookData.put("categories", book.getCategories());
                bookData.put("isbn10", book.getIsbn10());
                bookData.put("isbn13", book.getIsbn13());
                bookData.put("publisher", book.getPublisher());
                bookData.put("language", book.getLanguage());
                if (book.getCoverImages() != null) {
                    Map<String, String> coverImages = new HashMap<>();
                    coverImages.put("small", book.getCoverImages().getFallbackUrl());
                    coverImages.put("large", book.getCoverImages().getPreferredUrl());
                    bookData.put("coverImages", coverImages);
                }
                return bookData;
            })
            .collect(Collectors.toList()));

        logger.info("Sending search results update to {}: {} new results from {}, complete: {}", 
            destination, event.getNewResults().size(), event.getSource(), event.isComplete());
        this.messagingTemplate.convertAndSend(destination, payload);
    }

    /**
     * Processes search progress events and broadcasts to WebSocket clients
     * - Creates topic-specific messages for search progress updates
     * - Includes status and progress information for beautiful loading indicators
     * - Routes messages to query-specific WebSocket topics
     * 
     * @param event The SearchProgressEvent containing progress information
     */
    @EventListener
    public void handleSearchProgress(SearchProgressEvent event) {
        if (event.getQueryHash() == null) {
            logger.warn("Received SearchProgressEvent with null queryHash. Query: {}, Status: {}", 
                event.getSearchQuery(), event.getStatus());
            return;
        }

        String destination = "/topic/search/" + event.getQueryHash() + "/progress";
        
        Map<String, Object> payload = new HashMap<>();
        payload.put("searchQuery", event.getSearchQuery());
        payload.put("status", event.getStatus().name());
        payload.put("message", event.getMessage());
        payload.put("source", event.getSource());

        logger.debug("Sending search progress to {}: {} - {}", destination, event.getStatus(), event.getMessage());
        this.messagingTemplate.convertAndSend(destination, payload);
    }

    /**
     * Broadcast book upsert notifications to book-specific topics
     */
    @EventListener
    public void handleBookUpsert(BookUpsertEvent event) {
        if (event == null || event.getBookId() == null) {
            logger.warn("Received BookUpsertEvent with null content");
            return;
        }
        String destination = "/topic/book/" + event.getBookId() + "/upsert";
        Map<String, Object> payload = new HashMap<>();
        payload.put("bookId", event.getBookId());
        payload.put("slug", event.getSlug());
        payload.put("title", event.getTitle());
        payload.put("isNew", event.isNew());
        payload.put("context", event.getContext());
        logger.info("Sending book upsert to {}: {} (new={})", destination, event.getTitle(), event.isNew());
        this.messagingTemplate.convertAndSend(destination, payload);
    }
}
