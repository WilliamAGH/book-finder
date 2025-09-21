/**
 * Circuit breaker service to prevent API calls when rate limits are exceeded
 * This service tracks API failures and temporarily disables API calls when rate limits are hit
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class ApiCircuitBreakerService {

    // Circuit breaker states
    private enum CircuitState {
        CLOSED,    // Normal operation
        OPEN,      // Circuit is open, blocking API calls
        HALF_OPEN  // Testing if service is back up
    }
    
    private final AtomicReference<CircuitState> circuitState = new AtomicReference<>(CircuitState.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicReference<LocalDateTime> lastFailureTime = new AtomicReference<>();
    private final AtomicReference<LocalDateTime> circuitOpenTime = new AtomicReference<>();
    
    // Configuration
    private static final int FAILURE_THRESHOLD = 3; // Open circuit after 3 consecutive 429 errors
    private static final int CIRCUIT_OPEN_DURATION_MINUTES = 60; // Keep circuit open for 1 hour
    private static final int HALF_OPEN_TIMEOUT_MINUTES = 5; // Try half-open after 5 minutes
    
    /**
     * Check if API calls are allowed based on circuit breaker state
     * 
     * @return true if API calls are allowed, false if circuit is open
     */
    public boolean isApiCallAllowed() {
        CircuitState currentState = circuitState.get();
        LocalDateTime now = LocalDateTime.now();
        
        switch (currentState) {
            case CLOSED:
                return true;
                
            case OPEN:
                LocalDateTime openTime = circuitOpenTime.get();
                if (openTime != null && ChronoUnit.MINUTES.between(openTime, now) >= HALF_OPEN_TIMEOUT_MINUTES) {
                    // Try to transition to half-open
                    if (circuitState.compareAndSet(CircuitState.OPEN, CircuitState.HALF_OPEN)) {
                        log.info("Circuit breaker transitioning from OPEN to HALF_OPEN - allowing test API call");
                        return true;
                    }
                }
                log.debug("Circuit breaker is OPEN - blocking API call");
                return false;
                
            case HALF_OPEN:
                log.debug("Circuit breaker is HALF_OPEN - allowing test API call");
                return true;
                
            default:
                return true;
        }
    }
    
    /**
     * Record a successful API call
     * This will reset the circuit breaker if it was in HALF_OPEN state
     */
    public void recordSuccess() {
        CircuitState currentState = circuitState.get();
        
        if (currentState == CircuitState.HALF_OPEN) {
            // Success in half-open state - close the circuit
            if (circuitState.compareAndSet(CircuitState.HALF_OPEN, CircuitState.CLOSED)) {
                failureCount.set(0);
                lastFailureTime.set(null);
                circuitOpenTime.set(null);
                log.info("Circuit breaker SUCCESS in HALF_OPEN state - transitioning to CLOSED");
            }
        } else if (currentState == CircuitState.CLOSED) {
            // Reset failure count on success
            failureCount.set(0);
            lastFailureTime.set(null);
        }
    }
    
    /**
     * Record a rate limit failure (429 error)
     * This may open the circuit breaker if threshold is exceeded
     */
    public void recordRateLimitFailure() {
        LocalDateTime now = LocalDateTime.now();
        lastFailureTime.set(now);
        
        int currentFailures = failureCount.incrementAndGet();
        log.warn("Recorded rate limit failure #{} at {}", currentFailures, now);
        
        CircuitState currentState = circuitState.get();
        
        if (currentState == CircuitState.HALF_OPEN) {
            // Failure in half-open state - immediately open circuit
            if (circuitState.compareAndSet(CircuitState.HALF_OPEN, CircuitState.OPEN)) {
                circuitOpenTime.set(now);
                LoggingUtils.error(log, null,
                    "Circuit breaker FAILURE in HALF_OPEN state - opening circuit for {} minutes",
                    CIRCUIT_OPEN_DURATION_MINUTES);
            }
        } else if (currentState == CircuitState.CLOSED && currentFailures >= FAILURE_THRESHOLD) {
            // Too many failures - open the circuit
            if (circuitState.compareAndSet(CircuitState.CLOSED, CircuitState.OPEN)) {
                circuitOpenTime.set(now);
                LoggingUtils.error(log, null,
                    "Circuit breaker OPENED due to {} consecutive rate limit failures - blocking API calls for {} minutes",
                    currentFailures, CIRCUIT_OPEN_DURATION_MINUTES);
            }
        }
    }
    
    /**
     * Record a general API failure (non-rate-limit)
     * This contributes to failure count but with less weight than rate limit failures
     */
    public void recordGeneralFailure() {
        LocalDateTime now = LocalDateTime.now();
        lastFailureTime.set(now);
        
        // General failures count but don't immediately trigger circuit opening
        int currentFailures = failureCount.incrementAndGet();
        log.debug("Recorded general API failure #{} at {}", currentFailures, now);
        
        // Only open circuit for general failures if we have many more failures
        CircuitState currentState = circuitState.get();
        if (currentState == CircuitState.CLOSED && currentFailures >= FAILURE_THRESHOLD * 2) {
            if (circuitState.compareAndSet(CircuitState.CLOSED, CircuitState.OPEN)) {
                circuitOpenTime.set(now);
                log.warn("Circuit breaker OPENED due to {} consecutive general failures", currentFailures);
            }
        }
    }
    
    /**
     * Get current circuit breaker status for monitoring/debugging
     */
    public String getCircuitStatus() {
        CircuitState state = circuitState.get();
        int failures = failureCount.get();
        LocalDateTime lastFailure = lastFailureTime.get();
        LocalDateTime openTime = circuitOpenTime.get();
        
        StringBuilder status = new StringBuilder();
        status.append("Circuit State: ").append(state);
        status.append(", Failures: ").append(failures);
        
        if (lastFailure != null) {
            status.append(", Last Failure: ").append(lastFailure);
        }
        
        if (openTime != null) {
            long minutesOpen = ChronoUnit.MINUTES.between(openTime, LocalDateTime.now());
            status.append(", Open for: ").append(minutesOpen).append(" minutes");
        }
        
        return status.toString();
    }
    
    /**
     * Manually reset the circuit breaker (for admin/testing purposes)
     */
    public void reset() {
        circuitState.set(CircuitState.CLOSED);
        failureCount.set(0);
        lastFailureTime.set(null);
        circuitOpenTime.set(null);
        log.info("Circuit breaker manually reset to CLOSED state");
    }
}
