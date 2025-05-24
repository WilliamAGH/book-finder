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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.concurrent.CompletionException;


public class AsyncUtils {
    
    private static final Logger logger = LoggerFactory.getLogger(AsyncUtils.class); // Keep existing logger
    private static final long DEFAULT_TIMEOUT_SECONDS = 30; // Keep existing default
    
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
        // Existing implementation - assumed to be okay for now or refactored separately if needed
        return operation.get()
            .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
            .handle((result, ex) -> {
                if (ex != null) {
                    // Unwrap CompletionException to get the actual cause for logging
                    Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
                    if (cause instanceof TimeoutException) {
                        logger.error("Operation {} timed out after {} seconds", operationName, timeoutSeconds, cause);
                    } else {
                        logger.error("Operation {} failed: {}", operationName, cause.getMessage(), cause);
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
        // Existing implementation
        return withTimeoutAndFallback(operation, operationName, DEFAULT_TIMEOUT_SECONDS, null);
    }
    
    /**
     * Chain async operations with proper error propagation
     */
    public static <T, U> CompletableFuture<U> chain(
            CompletableFuture<T> future,
            Function<T, CompletableFuture<U>> mapper,
            String operationName) {
        // Existing implementation - assumed to be okay for now
        return future.thenCompose(value -> {
            if (value == null) {
                logger.debug("Null value in chain for operation {}", operationName);
                return CompletableFuture.completedFuture(null); // Or handle as error if null is not expected
            }
            return mapper.apply(value);
        }).exceptionally(ex -> {
            logger.error("Chain operation {} failed: {}", operationName, ex.getMessage(), ex);
            // Depending on requirements, might rethrow or return a specific error marker
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
        // Existing implementation - assumed to be okay for now
        return CompletableFuture.allOf(futures)
            .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
            .exceptionally(ex -> {
                Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
                if (cause instanceof TimeoutException) {
                    logger.error("Parallel operations {} timed out after {} seconds", 
                                operationName, timeoutSeconds, cause);
                } else {
                    logger.error("Parallel operations {} failed: {}", 
                                operationName, cause.getMessage(), cause);
                }
                return null;
            });
    }

    /**
     * Executes an asynchronous operation with configurable retry logic, including exponential backoff and jitter.
     * This is the canonical retry utility for CompletableFuture-based operations.
     *
     * @param <T>                  The type of the result.
     * @param operation            Supplier for the CompletableFuture operation to be retried.
     * @param operationName        A descriptive name for the operation, used in logging.
     * @param logger               The SLF4J Logger instance to use for logging retry attempts and failures.
     * @param executor             The Executor to use for scheduling delayed retries.
     * @param maxRetries           The maximum number of retry attempts.
     * @param initialBackoffMs     The initial delay in milliseconds before the first retry.
     * @param maxBackoffMs         The maximum delay in milliseconds for any single backoff period.
     * @param backoffMultiplier    The multiplier for increasing the backoff delay exponentially (e.g., 2.0).
     * @param jitterFactor         A factor (0.0 to 1.0) to introduce randomness to backoff delays, helping to prevent thundering herd.
     *                             0.0 means no jitter. 0.2 means +/- 20% jitter.
     * @param isRetryablePredicate A Predicate to determine if a given Throwable should trigger a retry. Can be null to retry on all exceptions.
     * @return A CompletableFuture that will complete with the result of the operation if successful,
     *         or fail if all retries are exhausted or a non-retryable exception occurs.
     */
    public static <T> CompletableFuture<T> withRetry(
            Supplier<CompletableFuture<T>> operation,
            String operationName,
            Logger logger, // Added logger
            Executor executor,
            int maxRetries,
            long initialBackoffMs,
            long maxBackoffMs, // Added max backoff
            double backoffMultiplier,
            double jitterFactor, // Added jitter factor
            Predicate<Throwable> isRetryablePredicate // Added predicate
    ) {
        return withRetryInternal(operation, operationName, logger, executor, 0, maxRetries, initialBackoffMs, maxBackoffMs, backoffMultiplier, jitterFactor, isRetryablePredicate);
    }

    private static <T> CompletableFuture<T> withRetryInternal(
            Supplier<CompletableFuture<T>> operation,
            String operationName,
            Logger logger,
            Executor executor,
            int attempt,
            int maxRetries,
            long currentBackoffMs,
            long maxBackoffMs,
            double backoffMultiplier,
            double jitterFactor,
            Predicate<Throwable> isRetryablePredicate
    ) {
        return operation.get().exceptionallyCompose(ex -> {
            Throwable actualException = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;

            if (attempt < maxRetries && (isRetryablePredicate == null || isRetryablePredicate.test(actualException))) {
                int nextAttempt = attempt + 1;
                long nextBaseBackoffMs = (long) (currentBackoffMs * backoffMultiplier);
                long cappedBackoffMs = Math.min(nextBaseBackoffMs, maxBackoffMs);
                
                // Apply jitter: currentBackoffMs +/- (currentBackoffMs * jitterFactor * random_between_-1_and_1)
                // Ensure jitterFactor is within reasonable bounds (e.g., 0 to 1)
                double actualJitterFactor = Math.max(0.0, Math.min(1.0, jitterFactor));
                long jitter = (long) (cappedBackoffMs * actualJitterFactor * (Math.random() * 2.0 - 1.0));
                long delayWithJitter = Math.max(0, cappedBackoffMs + jitter); // Ensure delay is not negative

                logger.warn("Operation '{}' failed on attempt {}/{}. Retrying after {}ms (base: {}ms, jitter: {}ms). Error: {}",
                        operationName, nextAttempt, maxRetries +1, delayWithJitter, cappedBackoffMs, jitter, actualException.getMessage());

                CompletableFuture<T> retryFuture = new CompletableFuture<>();
                CompletableFuture.delayedExecutor(delayWithJitter, TimeUnit.MILLISECONDS, executor).execute(() -> {
                    withRetryInternal(operation, operationName, logger, executor, nextAttempt, maxRetries, cappedBackoffMs, maxBackoffMs, backoffMultiplier, jitterFactor, isRetryablePredicate)
                        .whenComplete((result, throwable) -> {
                            if (throwable != null) {
                                retryFuture.completeExceptionally(throwable);
                            } else {
                                retryFuture.complete(result);
                            }
                        });
                });
                return retryFuture;
            } else {
                if (attempt >= maxRetries) {
                    logger.error("Operation '{}' failed after {} retries. Final error: {}",
                            operationName, maxRetries, actualException.getMessage(), actualException);
                } else { // Not retryable
                    logger.error("Operation '{}' failed with a non-retryable exception on attempt {}: {}",
                            operationName, attempt + 1, actualException.getMessage(), actualException);
                }
                return CompletableFuture.failedFuture(actualException);
            }
        });
    }
}
