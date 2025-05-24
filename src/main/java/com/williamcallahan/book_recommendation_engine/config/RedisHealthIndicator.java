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
import org.springframework.beans.factory.annotation.Value;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component("redisHealthIndicator")
@Conditional(RedisEnvironmentCondition.class)
public class RedisHealthIndicator implements HealthIndicator {

    private final JedisPooled jedisPooled;
    
    // Circuit breaker state tracking
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicLong lastSuccessTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong circuitOpenTime = new AtomicLong(0);
    
    @Value("${circuit-breaker.redis.failure-threshold:3}")
    private int failureThreshold;
    
    @Value("${circuit-breaker.redis.timeout-ms:30000}")
    private long circuitBreakerTimeout;

    /**
     * Constructs health indicator with required Redis connection pool
     *
     * @param jedisPooled Pooled Redis connection
     */
    public RedisHealthIndicator(JedisPooled jedisPooled) {
        this.jedisPooled = jedisPooled;
    }

    /**
     * Checks Redis connection status with circuit breaker pattern.
     * - Attempts to connect and ping Redis.
     * - Reports UP if Redis is available and responsive.
     * - Reports DOWN if Redis is unavailable or an error occurs.
     * - Implements circuit breaker to prevent excessive failed connection attempts.
     *
     * @return Health status with Redis connection details
     */
    @Override
    public Health health() {
        // Check if circuit is open
        if (isCircuitOpen()) {
            return Health.down()
                    .withDetail("redis_status", "circuit_open")
                    .withDetail("consecutive_failures", consecutiveFailures.get())
                    .withDetail("circuit_open_duration_ms", System.currentTimeMillis() - circuitOpenTime.get())
                    .build();
        }
        
        try {
            String response = jedisPooled.ping();
            if ("PONG".equalsIgnoreCase(response)) {
                // Reset failure counter on success
                consecutiveFailures.set(0);
                lastSuccessTime.set(System.currentTimeMillis());
                circuitOpenTime.set(0);
                
                return Health.up()
                    .withDetail("redis_status", "available")
                    .withDetail("response", response)
                    .withDetail("last_success_ms_ago", 0)
                    .build();
            } else {
                recordFailure();
                return Health.down()
                    .withDetail("redis_status", "unknown_response")
                    .withDetail("response", response)
                    .withDetail("consecutive_failures", consecutiveFailures.get())
                    .build();
            }
        } catch (Exception ex) {
            recordFailure();
            return Health.down()
                    .withDetail("redis_status", "unavailable")
                    .withDetail("error", ex.getClass().getName())
                    .withDetail("message", ex.getMessage())
                    .withDetail("consecutive_failures", consecutiveFailures.get())
                    .withDetail("last_success_ms_ago", System.currentTimeMillis() - lastSuccessTime.get())
                    .build();
        }
    }
    
    /**
     * Checks if the circuit breaker is currently open
     */
    private boolean isCircuitOpen() {
        if (circuitOpenTime.get() == 0) {
            return false;
        }
        
        // Check if timeout has elapsed
        if (System.currentTimeMillis() - circuitOpenTime.get() > circuitBreakerTimeout) {
            // Try to close the circuit
            circuitOpenTime.set(0);
            consecutiveFailures.set(0);
            return false;
        }
        
        return true;
    }
    
    /**
     * Records a failure and potentially opens the circuit
     */
    private void recordFailure() {
        int failures = consecutiveFailures.incrementAndGet();
        if (failures >= failureThreshold && circuitOpenTime.get() == 0) {
            // Open the circuit
            circuitOpenTime.set(System.currentTimeMillis());
        }
    }
    
    /**
     * Get current circuit breaker state for external monitoring
     */
    public boolean isHealthy() {
        return !isCircuitOpen() && consecutiveFailures.get() == 0;
    }
}
