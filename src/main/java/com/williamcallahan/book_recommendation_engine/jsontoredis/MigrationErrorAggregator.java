package com.williamcallahan.book_recommendation_engine.jsontoredis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Error aggregation for comprehensive error reporting during migration operations.
 * Extracted from JsonS3ToRedisService to reduce file size and improve maintainability.
 */
public class MigrationErrorAggregator {
    private static final Logger log = LoggerFactory.getLogger(MigrationErrorAggregator.class);
    
    private final List<ErrorDetail> errors = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, AtomicInteger> errorCountsByType = new ConcurrentHashMap<>();
    
    public void addError(String operation, String key, String errorType, String message, Throwable exception) {
        errors.add(new ErrorDetail(operation, key, errorType, message, exception));
        errorCountsByType.computeIfAbsent(errorType, k -> new AtomicInteger(0)).incrementAndGet();
    }
    
    public List<ErrorDetail> getErrors() {
        return new ArrayList<>(errors);
    }
    
    public Map<String, Integer> getErrorCountsByType() {
        Map<String, Integer> result = new HashMap<>();
        errorCountsByType.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }
    
    public void generateReport() {
        log.error("=== Migration Error Report ===");
        log.error("Total errors: {}", errors.size());
        log.error("Error counts by type:");
        errorCountsByType.forEach((type, count) -> 
            log.error("  {}: {}", type, count.get()));
        
        if (!errors.isEmpty()) {
            log.error("First 10 errors:");
            errors.stream().limit(10).forEach(error -> 
                log.error("  [{}] {} - {}: {}", 
                    error.operation, error.key, error.errorType, error.message));
        }
    }
    
    public static class ErrorDetail {
        public final String operation;
        public final String key;
        public final String errorType;
        public final String message;
        public final Instant timestamp;
        public final String stackTrace;
        
        public ErrorDetail(String operation, String key, String errorType, String message, Throwable exception) {
            this.operation = operation;
            this.key = key;
            this.errorType = errorType;
            this.message = message;
            this.timestamp = Instant.now();
            this.stackTrace = exception != null ? getStackTraceString(exception) : null;
        }
        
        private static String getStackTraceString(Throwable e) {
            StringBuilder sb = new StringBuilder();
            sb.append(e.getClass().getName()).append(": ").append(e.getMessage()).append("\n");
            for (StackTraceElement element : e.getStackTrace()) {
                sb.append("  at ").append(element.toString()).append("\n");
                if (sb.length() > 1000) { // Limit stack trace size
                    sb.append("  ... truncated ...");
                    break;
                }
            }
            return sb.toString();
        }
    }
}