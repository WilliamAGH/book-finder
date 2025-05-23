/**
 * Custom condition that checks if Redis environment variables are present
 * This enables Redis services automatically when the required environment variables are available
 *
 * @author William Callahan
 *
 * Features:
 * - Checks for REDIS_SERVER environment variable
 * - Alternatively checks for SPRING_REDIS_HOST and SPRING_REDIS_PORT combination
 * - Enables Redis-dependent beans when conditions are met
 * - Provides console output indicating Redis availability status
 */
package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.lang.NonNull;

public class RedisEnvironmentCondition implements Condition {

    @Override
    public boolean matches(@NonNull ConditionContext context, @NonNull AnnotatedTypeMetadata metadata) {
        String redisServer = context.getEnvironment().getProperty("REDIS_SERVER");
        String redisHost = context.getEnvironment().getProperty("SPRING_REDIS_HOST");
        String redisPort = context.getEnvironment().getProperty("SPRING_REDIS_PORT");
        
        // Redis is available if either REDIS_SERVER is set OR both host and port are set
        boolean hasRedisServer = redisServer != null && !redisServer.trim().isEmpty();
        boolean hasHostAndPort = redisHost != null && !redisHost.trim().isEmpty() && 
                                redisPort != null && !redisPort.trim().isEmpty();
        
        boolean hasRequiredVars = hasRedisServer || hasHostAndPort;
        
        if (hasRequiredVars) {
            System.out.println("Redis environment variables detected - enabling Redis services");
        } else {
            System.out.println("Redis environment variables not found - Redis services will be disabled");
        }
        
        return hasRequiredVars;
    }
}
