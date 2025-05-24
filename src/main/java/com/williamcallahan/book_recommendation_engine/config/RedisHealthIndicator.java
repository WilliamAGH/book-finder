/**
 * Standard health indicator for Redis
 * 
 * @author William Callahan
 * 
 * Reports the health of the Redis connection
 */

package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Conditional;
import redis.clients.jedis.JedisPooled;
import org.springframework.stereotype.Component;

@Component("redisHealthIndicator")
@Conditional(RedisEnvironmentCondition.class)
public class RedisHealthIndicator implements HealthIndicator {

    private final JedisPooled jedisPooled;

    /**
     * Constructs health indicator with required Redis connection pool
     *
     * @param jedisPooled Pooled Redis connection
     */
    public RedisHealthIndicator(JedisPooled jedisPooled) {
        this.jedisPooled = jedisPooled;
    }

    /**
     * Checks Redis connection status.
     * - Attempts to connect and ping Redis.
     * - Reports UP if Redis is available and responsive.
     * - Reports DOWN if Redis is unavailable or an error occurs.
     *
     * @return Health status with Redis connection details
     */
    @Override
    public Health health() {
        try {
            String response = jedisPooled.ping();
            if ("PONG".equalsIgnoreCase(response)) {
                return Health.up().withDetail("redis_status", "available").withDetail("response", response).build();
            } else {
                return Health.down().withDetail("redis_status", "unknown_response").withDetail("response", response).build();
            }
        } catch (Exception ex) {
            return Health.down()
                    .withDetail("redis_status", "unavailable")
                    .withDetail("error", ex.getClass().getName())
                    .withDetail("message", ex.getMessage())
                    .build();
        }
    }
}
