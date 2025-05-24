/**
 * Utility class for common async operations and patterns
 * Provides standard error handling, timeouts, and monitoring
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

public class AsyncUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(AsyncUtils.class);
    private static final long DEFAULT_TIMEOUT_SECONDS = 30;
    
    /**
     * Execute an async operation with timeout and error handling
     * 
     * @param operation The async operation to execute
     * @param operationName Name for logging purposes
     * @param timeoutSeconds Timeout in seconds
     * @param fallbackValue Value to return on error
     * @return CompletableFuture with result or fallback value
     */
    public static <T> CompletableFuture<T> withTimeoutAndFallback(
            Supplier<CompletableFuture<T>> operation,
            String operationName,
            long timeoutSeconds,
            T fallbackValue) {
        
        return operation.get()
            .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
            .handle((result, ex) -> {
                if (ex != null) {
                    if (ex instanceof TimeoutException) {
                        logger.error("Operation {} timed out after {} seconds", operationName, timeoutSeconds);
                    } else {
                        logger.error("Operation {} failed: {}", operationName, ex.getMessage());
                    }
                    return fallbackValue;
                }
                return result;
            });
    }
    
    /**
     * Execute an async operation with default timeout
     */
    public static <T> CompletableFuture<T> withTimeout(
            Supplier<CompletableFuture<T>> operation,
            String operationName) {
        
        return withTimeoutAndFallback(operation, operationName, DEFAULT_TIMEOUT_SECONDS, null);
    }
    
    /**
     * Chain async operations with proper error propagation
     */
    public static <T, U> CompletableFuture<U> chain(
            CompletableFuture<T> future,
            Function<T, CompletableFuture<U>> mapper,
            String operationName) {
        
        return future.thenCompose(value -> {
            if (value == null) {
                logger.debug("Null value in chain for operation {}", operationName);
                return CompletableFuture.completedFuture(null);
            }
            return mapper.apply(value);
        }).exceptionally(ex -> {
            logger.error("Chain operation {} failed: {}", operationName, ex.getMessage());
            return null;
        });
    }
    
    /**
     * Execute multiple async operations in parallel with timeout
     */
    public static CompletableFuture<Void> allWithTimeout(
            CompletableFuture<?>[] futures,
            long timeoutSeconds,
            String operationName) {
        
        return CompletableFuture.allOf(futures)
            .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
            .exceptionally(ex -> {
                if (ex instanceof TimeoutException) {
                    logger.error("Parallel operations {} timed out after {} seconds", 
                                operationName, timeoutSeconds);
                } else {
                    logger.error("Parallel operations {} failed: {}", 
                                operationName, ex.getMessage());
                }
                return null;
            });
    }
}