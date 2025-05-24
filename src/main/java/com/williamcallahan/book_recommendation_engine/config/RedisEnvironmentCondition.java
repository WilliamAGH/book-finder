/**
 * Custom condition that checks if Redis environment variables are present
 * This enables Redis services automatically when the required environment variables are available
 *
 * @author William Callahan
 *
 * Features:
 * - Checks for REDIS_SERVER environment variable
 * - Enables Redis-dependent beans when conditions are met
 * - Provides console output indicating Redis availability status
 */
package com.williamcallahan.book_recommendation_engine.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.lang.NonNull;

public class RedisEnvironmentCondition implements Condition {

    private static final Logger logger = LoggerFactory.getLogger(RedisEnvironmentCondition.class);
    private static volatile boolean hasLoggedRedisDetection = false;

    @Override
    public boolean matches(@NonNull ConditionContext context, @NonNull AnnotatedTypeMetadata metadata) {
        Environment env = context.getEnvironment();
        
        // Check for Redis environment variables
        String redisServer = env.getProperty("REDIS_SERVER");
        String redisHost = env.getProperty("spring.redis.host");
        String redisPort = env.getProperty("spring.redis.port");
        
        boolean hasRedisConfig = (redisServer != null && !redisServer.trim().isEmpty()) ||
                                (redisHost != null && !redisHost.trim().isEmpty()) ||
                                (redisPort != null && !redisPort.trim().isEmpty());
        
        if (hasRedisConfig && !hasLoggedRedisDetection) {
            logger.info("Redis environment variables detected - enabling Redis services");
            hasLoggedRedisDetection = true;
        }
        
        return hasRedisConfig;
    }
}
