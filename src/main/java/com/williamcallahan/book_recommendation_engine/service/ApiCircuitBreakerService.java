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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Service
public class ApiCircuitBreakerService {

    // Circuit breaker states
    private enum CircuitState {
        CLOSED,    // Normal operation
        OPEN       // Circuit is open, blocking API calls until next UTC day
    }
    
    private final AtomicReference<CircuitState> circuitState = new AtomicReference<>(CircuitState.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicReference<LocalDateTime> lastFailureTime = new AtomicReference<>();
    private final AtomicReference<LocalDate> circuitOpenDate = new AtomicReference<>();
    
    // Configuration
    private static final int FAILURE_THRESHOLD = 1; // Open circuit after 1 rate limit error (429)
    // Circuit stays open until next UTC day (Google Books quota resets at UTC midnight)
    
    /**
     * Check if API calls are allowed based on circuit breaker state
     * Resets automatically at UTC midnight (Google Books quota reset time)
     * 
     * @return true if API calls are allowed, false if circuit is open
     */
    public boolean isApiCallAllowed() {
        CircuitState currentState = circuitState.get();
        LocalDate nowUtc = LocalDate.now(ZoneOffset.UTC);
        LocalDate openDate = circuitOpenDate.get();
        
        // Auto-reset at UTC midnight (quota reset)
        if (currentState == CircuitState.OPEN && openDate != null && nowUtc.isAfter(openDate)) {
            if (circuitState.compareAndSet(CircuitState.OPEN, CircuitState.CLOSED)) {
                failureCount.set(0);
                lastFailureTime.set(null);
                circuitOpenDate.set(null);
                log.info("Circuit breaker AUTO-RESET at UTC midnight - quota refresh detected. State: CLOSED");
                return true;
            }
        }
        
        switch (currentState) {
            case CLOSED:
                return true;
                
            case OPEN:
                log.debug("Circuit breaker is OPEN - blocking API call until UTC midnight");
                return false;
                
            default:
                return true;
        }
    }
    
    /**
     * Record a successful API call
     * Resets failure count when circuit is closed
     */
    public void recordSuccess() {
        CircuitState currentState = circuitState.get();
        
        if (currentState == CircuitState.CLOSED) {
            // Reset failure count on success
            failureCount.set(0);
            lastFailureTime.set(null);
        }
        // If circuit is OPEN, success doesn't matter - we wait for UTC reset
    }
    
    /**
     * Record a rate limit failure (429 error)
     * IMMEDIATELY opens circuit breaker for remainder of UTC day (Google Books quota resets at UTC midnight)
     */
    public void recordRateLimitFailure() {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        LocalDate nowUtcDate = LocalDate.now(ZoneOffset.UTC);
        lastFailureTime.set(now);
        
        int currentFailures = failureCount.incrementAndGet();
        log.warn("Recorded rate limit failure #{} at {}", currentFailures, now);
        
        CircuitState currentState = circuitState.get();
        
        if (currentState == CircuitState.CLOSED && currentFailures >= FAILURE_THRESHOLD) {
            // Immediately open the circuit for the rest of the UTC day
            if (circuitState.compareAndSet(CircuitState.CLOSED, CircuitState.OPEN)) {
                circuitOpenDate.set(nowUtcDate);
                LoggingUtils.error(log, null,
                    "Circuit breaker OPENED due to rate limit (429) - blocking ALL authenticated API calls until next UTC day (quota reset). Date: {}",
                    nowUtcDate);
            }
        }
    }
    
    /**
     * Record a general API failure (non-rate-limit)
     * General failures are logged but do NOT trigger circuit breaker
     * Only rate limit (429) errors trigger the circuit breaker
     */
    public void recordGeneralFailure() {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        lastFailureTime.set(now);
        
        // Log but don't open circuit - only 429 errors should trigger the breaker
        log.debug("Recorded general API failure at {} (does not affect circuit breaker)", now);
    }
    
    /**
     * Get current circuit breaker status for monitoring/debugging
     */
    public String getCircuitStatus() {
        CircuitState state = circuitState.get();
        int failures = failureCount.get();
        LocalDateTime lastFailure = lastFailureTime.get();
        LocalDate openDate = circuitOpenDate.get();
        LocalDate nowUtcDate = LocalDate.now(ZoneOffset.UTC);
        
        StringBuilder status = new StringBuilder();
        status.append("Circuit State: ").append(state);
        status.append(", Failures: ").append(failures);
        
        if (lastFailure != null) {
            status.append(", Last Failure: ").append(lastFailure);
        }
        
        if (openDate != null) {
            status.append(", Open Since UTC Date: ").append(openDate);
            status.append(", Current UTC Date: ").append(nowUtcDate);
            if (state == CircuitState.OPEN && !nowUtcDate.isAfter(openDate)) {
                status.append(" (Will reset at next UTC midnight)");
            }
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
        circuitOpenDate.set(null);
        log.info("Circuit breaker manually reset to CLOSED state");
    }
}
