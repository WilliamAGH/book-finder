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

    @Autowired
    public CoverUpdateNotifierService(@Lazy MessageSendingOperations<String> messagingTemplate,
                                      WebSocketMessageBrokerConfigurer webSocketConfig) {
        this.messagingTemplate = messagingTemplate;
        logger.info("CoverUpdateNotifierService initialized, WebSocketConfig should be ready.");
    }

    @EventListener
    public void handleBookCoverUpdated(BookCoverUpdatedEvent event) {
        if (event.getGoogleBookId() == null || event.getNewCoverUrl() == null) {
            logger.warn("Received BookCoverUpdatedEvent with null googleBookId or newCoverUrl. IdentifierKey: {}, URL: {}, GoogleBookId: {}", 
                event.getIdentifierKey(), event.getNewCoverUrl(), event.getGoogleBookId());
            return;
        }

        String destination = "/topic/book/" + event.getGoogleBookId() + "/coverUpdate";
        
        Map<String, String> payload = new HashMap<>();
        payload.put("googleBookId", event.getGoogleBookId());
        payload.put("newCoverUrl", event.getNewCoverUrl());
        payload.put("identifierKey", event.getIdentifierKey()); 

        logger.info("Sending cover update to {}: URL = {}", destination, event.getNewCoverUrl());
        this.messagingTemplate.convertAndSend(destination, payload);
    }
} 