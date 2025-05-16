package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component("redisHealthIndicator") // Give it a specific name to distinguish from the default
public class ResilientRedisHealthIndicator implements ReactiveHealthIndicator {

    private final ReactiveRedisConnectionFactory redisConnectionFactory;

    public ResilientRedisHealthIndicator(ReactiveRedisConnectionFactory redisConnectionFactory) {
        this.redisConnectionFactory = redisConnectionFactory;
    }

    @Override
    public Mono<Health> health() {
        return Mono.fromCallable(() -> {
            // This can throw if the factory has issues or cannot create a connection proxy
            return redisConnectionFactory.getReactiveConnection();
        })
        .flatMap(connection -> connection.ping()) // If getReactiveConnection() succeeds, then ping
        .map(response -> {
            if ("PONG".equalsIgnoreCase(response)) {
                return Health.up().withDetail("redis_status", "available").withDetail("response", response).build();
            } else {
                // This case should ideally not happen if ping() succeeds but is not "PONG"
                return Health.up().withDetail("redis_status", "unknown_response").withDetail("response", response).build();
            }
        })
        .onErrorResume(ex ->  // This should now catch exceptions from getReactiveConnection() or ping()
            // If Redis is down, we still report overall UP, but detail the Redis issue.
            // This allows the main application health check (e.g., for Docker) to pass.
            Mono.just(
                Health.up() // Crucially, report UP for the application's resilience
                    .withDetail("redis_status", "unavailable")
                    .withDetail("error", ex.getClass().getName())
                    .withDetail("message", ex.getMessage())
                    .build()
            )
        )
        // If the ping itself takes too long, we can also consider it unavailable for health check purposes
        // but still keep the application UP.
        .timeout(java.time.Duration.ofSeconds(2),
            Mono.just( // Simpler fallback for timeout, no need to defer Mono.just() itself here
                Health.up() // Report overall UP
                    .withDetail("redis_status", "timeout")
                    .withDetail("message", "Redis ping timed out after 2 seconds.")
                    .build()
            )
        );
    }
} 