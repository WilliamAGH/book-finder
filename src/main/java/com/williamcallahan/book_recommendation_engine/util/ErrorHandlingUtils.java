/**
 * Utility class for standardized error handling across the application
 * Provides consistent error handling patterns for async operations
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.monitoring.MetricsService;
import org.slf4j.Logger;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.util.retry.Retry;
import redis.clients.jedis.exceptions.JedisConnectionException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ErrorHandlingUtils {
    
    /**
     * Standard error categorization for consistent handling
     */
    public enum ErrorCategory {
        TIMEOUT,
        REDIS_CONNECTION,
        S3_CONNECTION,
        SERIALIZATION,
        VALIDATION,
        RATE_LIMIT,
        GENERAL
    }
    
    /**
     * Categorize an exception into standard error types
     */
    public static ErrorCategory categorizeError(Throwable throwable) {
        if (throwable instanceof TimeoutException) {
            return ErrorCategory.TIMEOUT;
        } else if (throwable instanceof JedisConnectionException) {
            return ErrorCategory.REDIS_CONNECTION;
        } else if (throwable instanceof S3Exception) {
            return ErrorCategory.S3_CONNECTION;
        } else if (throwable instanceof IllegalArgumentException) {
            return ErrorCategory.VALIDATION;
        } else if (throwable.getMessage() != null && 
                   throwable.getMessage().contains("Rate limit")) {
            return ErrorCategory.RATE_LIMIT;
        } else if (throwable.getCause() != null) {
            // Unwrap CompletionException
            return categorizeError(throwable.getCause());
        }
        return ErrorCategory.GENERAL;
    }
    
    /**
     * Create a standard error handler for CompletableFuture operations
     * 
     * @param logger The logger to use
     * @param operationName Name of the operation for logging
     * @param metricsService Optional metrics service for tracking
     * @param fallbackValue Value to return on error
     * @return BiFunction suitable for CompletableFuture.handle()
     */
    public static <T> BiFunction<T, Throwable, T> createErrorHandler(
            Logger logger,
            String operationName,
            MetricsService metricsService,
            T fallbackValue) {
        
        return (result, throwable) -> {
            if (throwable != null) {
                ErrorCategory category = categorizeError(throwable);
                
                // Log with appropriate level based on error type
                switch (category) {
                    case TIMEOUT:
                        logger.error("Operation {} timed out: {}", operationName, throwable.getMessage());
                        if (metricsService != null) {
                            metricsService.incrementRedisTimeout();
                        }
                        break;
                    case REDIS_CONNECTION:
                        logger.error("Redis connection error in {}: {}", operationName, throwable.getMessage());
                        if (metricsService != null) {
                            metricsService.incrementRedisError();
                        }
                        break;
                    case S3_CONNECTION:
                        logger.error("S3 connection error in {}: {}", operationName, throwable.getMessage());
                        if (metricsService != null) {
                            metricsService.incrementS3Error();
                        }
                        break;
                    case VALIDATION:
                        logger.warn("Validation error in {}: {}", operationName, throwable.getMessage());
                        break;
                    case RATE_LIMIT:
                        logger.warn("Rate limit hit in {}: {}", operationName, throwable.getMessage());
                        if (metricsService != null) {
                            metricsService.incrementApiRateLimit();
                        }
                        break;
                    default:
                        logger.error("Error in {}: {}", operationName, throwable.getMessage(), throwable);
                }
                
                return fallbackValue;
            }
            return result;
        };
    }
    
    /**
     * Create an exception handler that logs and rethrows
     * Useful for operations that should propagate errors
     */
    public static <T> Function<Throwable, T> createExceptionLogger(
            Logger logger,
            String operationName,
            MetricsService metricsService) {
        
        return throwable -> {
            ErrorCategory category = categorizeError(throwable);
            
            // Log and track metrics
            switch (category) {
                case TIMEOUT:
                    logger.error("Operation {} timed out: {}", operationName, throwable.getMessage());
                    if (metricsService != null) {
                        metricsService.incrementRedisTimeout();
                    }
                    break;
                case REDIS_CONNECTION:
                    logger.error("Redis error in {}: {}", operationName, throwable.getMessage());
                    if (metricsService != null) {
                        metricsService.incrementRedisError();
                    }
                    break;
                case S3_CONNECTION:
                    logger.error("S3 error in {}: {}", operationName, throwable.getMessage());
                    if (metricsService != null) {
                        metricsService.incrementS3Error();
                    }
                    break;
                default:
                    logger.error("Error in {}: {}", operationName, throwable.getMessage());
            }
            
            // Rethrow as CompletionException if not already
            if (throwable instanceof CompletionException) {
                throw (CompletionException) throwable;
            } else {
                throw new CompletionException(operationName + " failed", throwable);
            }
        };
    }
    
    /**
     * Wrap a CompletableFuture with standard error handling
     */
    public static <T> CompletableFuture<T> withErrorHandling(
            CompletableFuture<T> future,
            Logger logger,
            String operationName,
            MetricsService metricsService,
            T fallbackValue) {
        
        return future.handle(createErrorHandler(logger, operationName, metricsService, fallbackValue));
    }
    
    /**
     * Check if an error is retryable
     */
    public static boolean isRetryable(Throwable throwable) {
        ErrorCategory category = categorizeError(throwable);
        return category == ErrorCategory.TIMEOUT ||
               category == ErrorCategory.REDIS_CONNECTION ||
               category == ErrorCategory.S3_CONNECTION ||
               category == ErrorCategory.RATE_LIMIT;
    }
    
    /**
     * Create WebClient retry specification using existing patterns
     * This consolidates the duplicated retry logic from GoogleApiFetcher
     */
    public static Retry createWebClientRetry(Logger logger, String operation) {
        return createWebClientRetry(logger, operation, null);
    }
    
    /**
     * Create WebClient retry specification with optional exhausted callback
     */
    public static Retry createWebClientRetry(Logger logger, String operation, Runnable onExhausted) {
        return createWebClientRetry(logger, operation, onExhausted, 3, Duration.ofSeconds(2));
    }
    
    /**
     * Create WebClient retry specification with full configuration
     * @param logger Logger instance
     * @param operation Operation name for logging
     * @param onExhausted Optional callback when retries exhausted
     * @param maxAttempts Maximum retry attempts
     * @param firstBackoff Initial backoff duration
     */
    public static Retry createWebClientRetry(Logger logger, String operation, Runnable onExhausted, 
                                            int maxAttempts, Duration firstBackoff) {
        return Retry.backoff(maxAttempts, firstBackoff)
                .maxBackoff(Duration.ofSeconds(30))
                .jitter(0.5) // Add jitter to prevent thundering herd
                .filter(throwable -> {
                    if (throwable instanceof WebClientResponseException) {
                        WebClientResponseException wcre = (WebClientResponseException) throwable;
                        // Retry on server errors, rate limits, and timeouts
                        return wcre.getStatusCode().is5xxServerError() || 
                               wcre.getStatusCode().value() == 429 ||
                               wcre.getStatusCode().value() == 408;
                    }
                    return throwable instanceof IOException || 
                           throwable instanceof WebClientRequestException ||
                           throwable instanceof TimeoutException;
                })
                .doBeforeRetry(retrySignal -> {
                    Throwable failure = retrySignal.failure();
                    if (failure instanceof WebClientResponseException) {
                        WebClientResponseException wcre = (WebClientResponseException) failure;
                        logger.warn("Retrying {} after status {}. Attempt #{}/{}. Error: {}",
                                operation, wcre.getStatusCode(), retrySignal.totalRetries() + 1, 
                                maxAttempts, wcre.getMessage());
                    } else {
                        logger.warn("Retrying {} after error. Attempt #{}/{}. Error: {}",
                                operation, retrySignal.totalRetries() + 1, maxAttempts, 
                                failure.getMessage());
                    }
                })
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> {
                    logger.error("All {} retries failed for {}. Final error: {}", 
                            maxAttempts, operation, retrySignal.failure().getMessage());
                    if (onExhausted != null) {
                        onExhausted.run();
                    }
                    return retrySignal.failure();
                });
    }
    
    /**
     * Get suggested retry delay based on error type
     */
    public static long getRetryDelayMillis(Throwable throwable, int attemptNumber) {
        ErrorCategory category = categorizeError(throwable);
        
        // Base delay with exponential backoff
        long baseDelay = 1000L * attemptNumber;
        
        switch (category) {
            case RATE_LIMIT:
                // Longer delay for rate limits
                return Math.min(baseDelay * 10, 60000L);
            case TIMEOUT:
                // Medium delay for timeouts
                return Math.min(baseDelay * 2, 10000L);
            case REDIS_CONNECTION:
            case S3_CONNECTION:
                // Standard exponential backoff for connection issues
                return Math.min(baseDelay, 5000L);
            default:
                return baseDelay;
        }
    }
    
    /**
     * Create a resilient Redis operation wrapper with circuit breaker pattern
     * @param operation The Redis operation to execute
     * @param fallback The fallback value if Redis is unavailable
     * @param logger Logger for operation
     * @param operationName Name for logging
     * @param metricsService Optional metrics service
     */
    public static <T> CompletableFuture<T> withRedisResilience(
            java.util.function.Supplier<CompletableFuture<T>> operation,
            T fallback,
            Logger logger,
            String operationName,
            MetricsService metricsService) {
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                return operation.get();
            } catch (Exception e) {
                ErrorCategory category = categorizeError(e);
                if (category == ErrorCategory.REDIS_CONNECTION) {
                    logger.warn("Redis unavailable for {}, using fallback", operationName);
                    if (metricsService != null) {
                        metricsService.incrementRedisError();
                    }
                    return CompletableFuture.completedFuture(fallback);
                }
                throw new CompletionException(e);
            }
        }).thenCompose(Function.identity())
          .handle(createErrorHandler(logger, operationName, metricsService, fallback));
    }
    
    /**
     * Create a resilient S3 operation wrapper with retry
     * @param operation The S3 operation to execute  
     * @param logger Logger for operation
     * @param operationName Name for logging
     * @param maxRetries Maximum retry attempts
     */
    public static <T> CompletableFuture<T> withS3Resilience(
            java.util.function.Supplier<CompletableFuture<T>> operation,
            Logger logger,
            String operationName,
            int maxRetries) {
        
        return withRetry(operation, logger, operationName, maxRetries, 1000L, 2.0);
    }
    
    /**
     * Generic retry wrapper for any async operation
     */
    private static <T> CompletableFuture<T> withRetry(
            java.util.function.Supplier<CompletableFuture<T>> operation,
            Logger logger,
            String operationName,
            int maxRetries,
            long initialDelayMs,
            double backoffMultiplier) {
        
        return withRetryInternal(operation, logger, operationName, 0, maxRetries, initialDelayMs, backoffMultiplier);
    }
    
    private static <T> CompletableFuture<T> withRetryInternal(
            java.util.function.Supplier<CompletableFuture<T>> operation,
            Logger logger,
            String operationName,
            int attempt,
            int maxRetries,
            long delayMs,
            double backoffMultiplier) {
        
        return operation.get()
            .exceptionallyCompose(throwable -> {
                if (attempt < maxRetries && isRetryable(throwable)) {
                    logger.warn("{} failed on attempt #{}, retrying after {}ms: {}",
                            operationName, attempt + 1, delayMs, throwable.getMessage());
                    
                    return CompletableFuture.supplyAsync(
                        () -> null, 
                        CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS))
                        .thenCompose(ignored -> withRetryInternal(
                            operation, logger, operationName,
                            attempt + 1, maxRetries, 
                            (long)(delayMs * backoffMultiplier), backoffMultiplier));
                }
                return CompletableFuture.failedFuture(throwable);
            });
    }
}