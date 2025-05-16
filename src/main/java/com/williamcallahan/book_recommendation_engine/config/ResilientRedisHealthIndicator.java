package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Health indicator for Redis that reports application as UP even when Redis is down
 * 
 * @author William Callahan
 * 
 * Provides resilience for applications using Redis by not reporting overall
 * health as DOWN when Redis connectivity issues occur
 */
@Component("redisHealthIndicator")
public class ResilientRedisHealthIndicator implements ReactiveHealthIndicator {

    private final ReactiveRedisConnectionFactory redisConnectionFactory;

    /**
     * Constructs health indicator with required Redis connection factory
     *
     * @param redisConnectionFactory Factory for creating Redis connections
     */
    public ResilientRedisHealthIndicator(ReactiveRedisConnectionFactory redisConnectionFactory) {
        this.redisConnectionFactory = redisConnectionFactory;
    }

    /**
     * Checks Redis connection status without failing the overall health check
     * - Attempts to connect and ping Redis
     * - Reports UP with detailed status even when Redis is unavailable
     * - Handles connection errors and timeouts gracefully
     * - Sets appropriate status indicators for monitoring
     *
     * @return Mono containing Health status with Redis connection details
     */
    @Override
    public Mono<Health> health() {
        return Mono.usingWhen(
            Mono.fromCallable(redisConnectionFactory::getReactiveConnection),
            connection -> connection.ping(),
            connection -> Mono.fromRunnable(connection::close),
            (connection, err) -> Mono.fromRunnable(connection::close),
            connection -> Mono.fromRunnable(connection::close)
        )
        .map(response -> {
            if ("PONG".equalsIgnoreCase(response)) {
                return Health.up().withDetail("redis_status", "available").withDetail("response", response).build();
            } else {
                return Health.up().withDetail("redis_status", "unknown_response").withDetail("response", response).build();
            }
        })
        .onErrorResume(ex -> 
            Mono.just(
                Health.up()
                    .withDetail("redis_status", "unavailable")
                    .withDetail("error", ex.getClass().getName())
                    .withDetail("message", ex.getMessage())
                    .build()
            )
        )
        .timeout(java.time.Duration.ofSeconds(2),
            Mono.just(
                Health.up()
                    .withDetail("redis_status", "timeout")
                    .withDetail("message", "Redis ping timed out after 2 seconds.")
                    .build()
            )
        );
    }
} 