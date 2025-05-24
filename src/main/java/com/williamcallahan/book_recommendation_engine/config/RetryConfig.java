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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
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
        
        // Configure exponential backoff
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
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
        
        // Configure exponential backoff
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(1000); // 1 second
        backOffPolicy.setMultiplier(2.0);
        backOffPolicy.setMaxInterval(15000); // 15 seconds
        retryTemplate.setBackOffPolicy(backOffPolicy);
        
        return retryTemplate;
    }
}