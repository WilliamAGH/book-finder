package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.lang.NonNull;

/**
 * WebSocket configuration for real-time communication
 * 
 * @author William Callahan
 *
 * Features:
 * - Enables STOMP messaging with simple in-memory broker
 * - Configures WebSocket endpoints with SockJS fallback
 * - Supports CORS with configurable origin patterns
 * - Defines application destination prefixes
 * - Used for real-time book cover update notifications
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    @Value("${app.cors.allowed-origins:*}")
    private String allowedOrigins;
    
    private final TaskScheduler messageBrokerTaskScheduler;
    
    /**
     * Inject TaskScheduler with @Lazy to avoid circular dependency.
     * TaskScheduler is used for WebSocket heartbeat keepalive.
     */
    @Autowired
    public WebSocketConfig(@Lazy TaskScheduler messageBrokerTaskScheduler) {
        this.messageBrokerTaskScheduler = messageBrokerTaskScheduler;
    }

    /**
     * Configures the message broker for WebSocket communication
     * - Sets up in-memory broker for topic destinations
     * - Defines application destination prefix for client messages
     * - Optimized for simple publish-subscribe messaging patterns
     * 
     * @param config the message broker registry
     */
    @Override
    public void configureMessageBroker(@NonNull MessageBrokerRegistry config) {
        config.enableSimpleBroker("/topic")
            .setHeartbeatValue(new long[]{10000, 20000})  // Server sends every 10s, expects client every 20s
            .setTaskScheduler(messageBrokerTaskScheduler); // Use dedicated scheduler for heartbeats
        config.setApplicationDestinationPrefixes("/app");
    }

    /**
     * Registers STOMP endpoints for WebSocket connections
     * - Creates main WebSocket endpoint at /ws path
     * - Configures SockJS fallback for browsers without WebSocket support
     * - Sets CORS allowed origins from application properties
     * - Enables cross-origin WebSocket connections
     * 
     * @param registry the STOMP endpoint registry
     */
    @Override
    public void registerStompEndpoints(@NonNull StompEndpointRegistry registry) {
        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns(allowedOrigins)
                .withSockJS();
    }
} 