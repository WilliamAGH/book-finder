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
        
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(3, retryableExceptions);
        retryTemplate.setRetryPolicy(retryPolicy);
        
        // Configure exponential backoff with jitter
        ExponentialBackOffWithJitterPolicy backOffPolicy = new ExponentialBackOffWithJitterPolicy();
        backOffPolicy.setInitialInterval(1000); // 1 second
        backOffPolicy.setMultiplier(2.0);
        backOffPolicy.setMaxInterval(10000); // 10 seconds
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
        
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(3, retryableExceptions);
        retryTemplate.setRetryPolicy(retryPolicy);
        
        // Configure exponential backoff with jitter
        ExponentialBackOffWithJitterPolicy backOffPolicy = new ExponentialBackOffWithJitterPolicy();
        backOffPolicy.setInitialInterval(1000); // 1 second
        backOffPolicy.setMultiplier(2.0);
        backOffPolicy.setMaxInterval(15000); // 15 seconds
        retryTemplate.setBackOffPolicy(backOffPolicy);
        
        return retryTemplate;
    }
}