/**
 * Service for tracking application metrics and operational health
 * Provides counters, gauges, and timers for monitoring
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.monitoring;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class MetricsService {
    
    private final MeterRegistry meterRegistry;
    
    // Counters
    private final Counter redisTimeouts;
    private final Counter redisErrors;
    private final Counter s3Timeouts;
    private final Counter s3Errors;
    private final Counter apiRateLimits;
    private final Counter circuitBreakerTrips;
    
    // Gauges
    private final AtomicInteger activeAsyncTasks = new AtomicInteger(0);
    private final AtomicInteger redisConnectionPoolSize = new AtomicInteger(0);
    private final AtomicLong lastRedisError = new AtomicLong(0);
    
    // Timers
    private final Timer redisOperationTimer;
    private final Timer s3OperationTimer;
    private final Timer apiCallTimer;
    
    public MetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize counters
        this.redisTimeouts = Counter.builder("redis.timeouts")
            .description("Number of Redis operation timeouts")
            .register(meterRegistry);
            
        this.redisErrors = Counter.builder("redis.errors")
            .description("Number of Redis operation errors")
            .register(meterRegistry);
            
        this.s3Timeouts = Counter.builder("s3.timeouts")
            .description("Number of S3 operation timeouts")
            .register(meterRegistry);
            
        this.s3Errors = Counter.builder("s3.errors")
            .description("Number of S3 operation errors")
            .register(meterRegistry);
            
        this.apiRateLimits = Counter.builder("api.rate_limits")
            .description("Number of API rate limit hits")
            .register(meterRegistry);
            
        this.circuitBreakerTrips = Counter.builder("circuit_breaker.trips")
            .description("Number of circuit breaker trips")
            .register(meterRegistry);
        
        // Initialize gauges
        Gauge.builder("async.tasks.active", activeAsyncTasks, AtomicInteger::get)
            .description("Number of active async tasks")
            .register(meterRegistry);
            
        Gauge.builder("redis.pool.size", redisConnectionPoolSize, AtomicInteger::get)
            .description("Redis connection pool size")
            .register(meterRegistry);
            
        Gauge.builder("redis.last_error_timestamp", lastRedisError, AtomicLong::get)
            .description("Timestamp of last Redis error")
            .register(meterRegistry);
        
        // Initialize timers
        this.redisOperationTimer = Timer.builder("redis.operation.duration")
            .description("Redis operation duration")
            .register(meterRegistry);
            
        this.s3OperationTimer = Timer.builder("s3.operation.duration")
            .description("S3 operation duration")
            .register(meterRegistry);
            
        this.apiCallTimer = Timer.builder("api.call.duration")
            .description("External API call duration")
            .register(meterRegistry);
    }
    
    // Counter methods
    public void incrementRedisTimeout() {
        redisTimeouts.increment();
    }
    
    public void incrementRedisError() {
        redisErrors.increment();
        lastRedisError.set(System.currentTimeMillis());
    }
    
    public void incrementS3Timeout() {
        s3Timeouts.increment();
    }
    
    public void incrementS3Error() {
        s3Errors.increment();
    }
    
    public void incrementApiRateLimit() {
        apiRateLimits.increment();
    }
    
    public void incrementCircuitBreakerTrip() {
        circuitBreakerTrips.increment();
    }
    
    // Gauge methods
    public void setActiveAsyncTasks(int count) {
        activeAsyncTasks.set(count);
    }
    
    public void incrementActiveAsyncTasks() {
        activeAsyncTasks.incrementAndGet();
    }
    
    public void decrementActiveAsyncTasks() {
        activeAsyncTasks.decrementAndGet();
    }
    
    public void setRedisConnectionPoolSize(int size) {
        redisConnectionPoolSize.set(size);
    }
    
    // Timer methods
    public Timer.Sample startRedisTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void stopRedisTimer(Timer.Sample sample) {
        sample.stop(redisOperationTimer);
    }
    
    public Timer.Sample startS3Timer() {
        return Timer.start(meterRegistry);
    }
    
    public void stopS3Timer(Timer.Sample sample) {
        sample.stop(s3OperationTimer);
    }
    
    public Timer.Sample startApiTimer() {
        return Timer.start(meterRegistry);
    }
    
    public void stopApiTimer(Timer.Sample sample) {
        sample.stop(apiCallTimer);
    }
    
    // Convenience method for recording timed operations
    public <T> T recordTimed(Timer timer, java.util.function.Supplier<T> operation) {
        return timer.record(operation);
    }
}