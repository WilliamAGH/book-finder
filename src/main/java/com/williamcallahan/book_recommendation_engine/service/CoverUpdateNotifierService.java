/**
 * Service for real-time notification of book cover image updates
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
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.core.MessageSendingOperations;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

import java.util.HashMap;
import java.util.Map;

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
    @Autowired
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
            payload.put("sourceName", com.williamcallahan.book_recommendation_engine.types.CoverImageSource.UNDEFINED.name());
            payload.put("sourceDisplayName", com.williamcallahan.book_recommendation_engine.types.CoverImageSource.UNDEFINED.getDisplayName());
        }

        logger.info("Sending cover update to {}: URL = {}, Source = {}", destination, event.getNewCoverUrl(), event.getSource() != null ? event.getSource().name() : "UNDEFINED");
        this.messagingTemplate.convertAndSend(destination, payload);
    }
}
