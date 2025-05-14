package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import org.springframework.context.annotation.Lazy;

import java.util.HashMap;
import java.util.Map;

@Service
public class CoverUpdateNotifierService {

    private static final Logger logger = LoggerFactory.getLogger(CoverUpdateNotifierService.class);
    private final SimpMessagingTemplate messagingTemplate;

    @Autowired
    public CoverUpdateNotifierService(@Lazy SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    @EventListener
    public void handleBookCoverUpdated(BookCoverUpdatedEvent event) {
        if (event.getGoogleBookId() == null || event.getNewCoverUrl() == null) {
            logger.warn("Received BookCoverUpdatedEvent with null googleBookId or newCoverUrl. IdentifierKey: {}, URL: {}, GoogleBookId: {}", 
                event.getIdentifierKey(), event.getNewCoverUrl(), event.getGoogleBookId());
            return;
        }

        String destination = "/topic/book/" + event.getGoogleBookId() + "/coverUpdate";
        
        // You can send more structured data if needed, e.g., a map or a custom object
        Map<String, String> payload = new HashMap<>();
        payload.put("googleBookId", event.getGoogleBookId());
        payload.put("newCoverUrl", event.getNewCoverUrl());
        // identifierKey might also be useful for some frontend logic, though googleBookId is usually primary for UI elements
        payload.put("identifierKey", event.getIdentifierKey()); 

        logger.info("Sending cover update to {}: URL = {}", destination, event.getNewCoverUrl());
        messagingTemplate.convertAndSend(destination, payload);
    }
} 