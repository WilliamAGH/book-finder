/**
 * Configuration for retry mechanisms to improve resilience
 *
 * @author William Callahan
 *
 * Features:
 * - Configures retry behavior for Redis and S3 operations
 * - Implements exponential backoff for transient failures
 * - Provides circuit breaker patterns for connection failures
 */

package com.williamcallahan.book_recommendation_engine.config;

import com.williamcallahan.book_recommendation_engine.service.PoolShutdownException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.BackOffContext;
import org.springframework.retry.backoff.BackOffInterruptedException;
import org.springframework.retry.RetryContext;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import redis.clients.jedis.exceptions.JedisConnectionException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableRetry
public class RetryConfig {
    
    // Global retry defaults
    @Value("${app.retry.default.max-attempts:3}")
    private int defaultMaxAttempts;
    
    @Value("${app.retry.default.initial-backoff-ms:1000}")
    private long defaultInitialBackoff;
    
    @Value("${app.retry.default.max-backoff-ms:30000}")
    private long defaultMaxBackoff;
    
    @Value("${app.retry.default.backoff-multiplier:2.0}")
    private double defaultBackoffMultiplier;
    
    @Value("${app.retry.default.jitter-factor:0.5}")
    private double defaultJitterFactor;
    
    // Service-specific configurations
    @Value("${app.retry.redis.max-attempts:2}")
    private int redisMaxAttempts;
    
    @Value("${app.retry.redis.initial-backoff-ms:500}")
    private long redisInitialBackoff;
    
    @Value("${app.retry.redis.backoff-multiplier:1.5}")
    private double redisBackoffMultiplier;
    
    @Value("${app.retry.s3.max-attempts:3}")
    private int s3MaxAttempts;
    
    @Value("${app.retry.s3.initial-backoff-ms:200}")
    private long s3InitialBackoff;
    
    @Value("${app.retry.s3.backoff-multiplier:2.0}")
    private double s3BackoffMultiplier;
    
    @Value("${app.retry.google-api.max-attempts:3}")
    private int googleApiMaxAttempts;
    
    @Value("${app.retry.google-api.initial-backoff-ms:2000}")
    private long googleApiInitialBackoff;
    
    @Value("${app.retry.google-api.backoff-multiplier:2.0}")
    private double googleApiBackoffMultiplier;
    
    // Timeout configurations
    @Value("${app.timeout.redis.ping-ms:1000}")
    private long redisPingTimeout;
    
    @Value("${app.timeout.redis.operation-ms:5000}")
    private long redisOperationTimeout;
    
    @Value("${app.timeout.s3.operation-ms:30000}")
    private long s3OperationTimeout;
    
    @Value("${app.timeout.google-api.operation-ms:60000}")
    private long googleApiOperationTimeout;
    
    /**
     * Custom ExponentialBackOffPolicy with jitter to prevent thundering herd
     */
    private static class ExponentialBackOffWithJitterPolicy implements BackOffPolicy {
        private static final Logger logger = LoggerFactory.getLogger(ExponentialBackOffWithJitterPolicy.class);
        private long initialInterval = 1000;
        private double multiplier = 2.0;
        private long maxInterval = 10000;
        private final double jitterFactor = 0.2; // 20% jitter

        public void setInitialInterval(long initialInterval) {
            this.initialInterval = initialInterval;
        }

        public void setMultiplier(double multiplier) {
            this.multiplier = multiplier;
        }

        public void setMaxInterval(long maxInterval) {
            this.maxInterval = maxInterval;
        }

        private static class BackOffContextImpl implements BackOffContext {
            long currentInterval;
        }

        @Override
        public BackOffContext start(RetryContext context) {
            BackOffContextImpl ctx = new BackOffContextImpl();
            ctx.currentInterval = this.initialInterval;
            return ctx;
        }

        @Override
        public void backOff(BackOffContext backOffContext) throws BackOffInterruptedException {
            BackOffContextImpl ctx = (BackOffContextImpl) backOffContext;
            long sleepTime = ctx.currentInterval;
            long jitter = (long) (sleepTime * jitterFactor * (2 * Math.random() - 1));
            sleepTime = Math.max(1, sleepTime + jitter);
            if (logger.isDebugEnabled()) {
                logger.debug("Backing off for {}ms (with jitter)", sleepTime);
            }
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new BackOffInterruptedException("Thread interrupted while backing off", e);
            }
            long nextInterval = (long) (ctx.currentInterval * multiplier);
            ctx.currentInterval = Math.min(nextInterval, maxInterval);
        }
    }

    /**
     * Creates a retry template for Redis operations
     * Configured to handle connection failures with exponential backoff
     *
     * @return RetryTemplate for Redis operations
     */
    @Bean("redisRetryTemplate")
    public RetryTemplate redisRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        
        // Configure retry policy
        Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        retryableExceptions.put(JedisConnectionException.class, true);
        retryableExceptions.put(PoolShutdownException.class, true); // Connection pool shutdown
        
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(redisMaxAttempts, retryableExceptions);
        retryTemplate.setRetryPolicy(retryPolicy);
        
        // Configure exponential backoff with jitter
        ExponentialBackOffWithJitterPolicy backOffPolicy = new ExponentialBackOffWithJitterPolicy();
        backOffPolicy.setInitialInterval(redisInitialBackoff);
        backOffPolicy.setMultiplier(redisBackoffMultiplier);
        backOffPolicy.setMaxInterval(defaultMaxBackoff);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        
        return retryTemplate;
    }

    /**
     * Creates a retry template for S3 operations
     * Configured to handle connection failures and service exceptions
     *
     * @return RetryTemplate for S3 operations
     */
    @Bean("s3RetryTemplate")
    public RetryTemplate s3RetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        
        // Configure retry policy
        Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
        retryableExceptions.put(S3Exception.class, true);
        retryableExceptions.put(PoolShutdownException.class, true); // Connection pool shutdown
        
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(s3MaxAttempts, retryableExceptions);
        retryTemplate.setRetryPolicy(retryPolicy);
        
        // Configure exponential backoff with jitter
        ExponentialBackOffWithJitterPolicy backOffPolicy = new ExponentialBackOffWithJitterPolicy();
        backOffPolicy.setInitialInterval(s3InitialBackoff);
        backOffPolicy.setMultiplier(s3BackoffMultiplier);
        backOffPolicy.setMaxInterval(defaultMaxBackoff);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        
        return retryTemplate;
    }
    
    /**
     * Creates a default retry template using global configuration
     * @return RetryTemplate with default configuration
     */
    @Bean("defaultRetryTemplate")
    public RetryTemplate defaultRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();
        
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(defaultMaxAttempts);
        retryTemplate.setRetryPolicy(retryPolicy);
        
        ExponentialBackOffWithJitterPolicy backOffPolicy = new ExponentialBackOffWithJitterPolicy();
        backOffPolicy.setInitialInterval(defaultInitialBackoff);
        backOffPolicy.setMultiplier(defaultBackoffMultiplier);
        backOffPolicy.setMaxInterval(defaultMaxBackoff);
        retryTemplate.setBackOffPolicy(backOffPolicy);
        
        return retryTemplate;
    }
    
    // Getters for configuration values to be used by other components
    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public long getDefaultInitialBackoff() { return defaultInitialBackoff; }
    public long getDefaultMaxBackoff() { return defaultMaxBackoff; }
    public double getDefaultBackoffMultiplier() { return defaultBackoffMultiplier; }
    public double getDefaultJitterFactor() { return defaultJitterFactor; }
    public int getRedisMaxAttempts() { return redisMaxAttempts; }
    public int getS3MaxAttempts() { return s3MaxAttempts; }
    public int getGoogleApiMaxAttempts() { return googleApiMaxAttempts; }
    public long getRedisPingTimeout() { return redisPingTimeout; }
    public long getRedisOperationTimeout() { return redisOperationTimeout; }
    public long getS3OperationTimeout() { return s3OperationTimeout; }
    public long getGoogleApiOperationTimeout() { return googleApiOperationTimeout; }
}