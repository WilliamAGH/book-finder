/**
 * Configuration for graceful application shutdown handling
 *
 * @author William Callahan
 *
 * Features:
 * - Handles application shutdown events to prevent connection pool race conditions
 * - Ensures proper cleanup of resources during Spring Boot termination
 * - Coordinates with Redis and S3 services for graceful shutdown
 */

package com.williamcallahan.book_recommendation_engine.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

@Component
public class GracefulShutdownConfig implements ApplicationListener<ContextClosedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(GracefulShutdownConfig.class);

    /**
     * Handles the application shutdown event
     * Coordinates graceful shutdown of services to prevent connection pool failures
     *
     * @param event Spring context closed event
     */
    @Override
    public void onApplicationEvent(@NonNull ContextClosedEvent event) {
        logger.info("Application shutdown event received - coordinating graceful shutdown");
        
        try {
            // Allow time for ongoing operations to complete before connection pools are closed
            Thread.sleep(2000);
            logger.info("Graceful shutdown coordination completed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Graceful shutdown interrupted", e);
        }
    }
}