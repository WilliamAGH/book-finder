/**
 * Utility class for standardized error handling across the application
 * Provides consistent error handling patterns for async operations
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.monitoring.MetricsService;
import org.slf4j.Logger;
import redis.clients.jedis.exceptions.JedisConnectionException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
}