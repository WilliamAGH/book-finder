/**
 * REST controller for monitoring JSON S3 to Redis migration progress
 * Provides real-time migration status and error information
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.jsontoredis.JsonS3ToRedisService;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/migration")
@Profile("jsontoredis")
public class MigrationMonitorController {
    
    private final JsonS3ToRedisService jsonS3ToRedisService;
    
    public MigrationMonitorController(JsonS3ToRedisService jsonS3ToRedisService) {
        this.jsonS3ToRedisService = jsonS3ToRedisService;
    }
    
    /**
     * Get current migration progress
     */
    @GetMapping("/progress")
    public ResponseEntity<Map<String, Object>> getMigrationProgress() {
        JsonS3ToRedisService.MigrationProgress progress = 
            jsonS3ToRedisService.getMigrationProgress();
        
        Map<String, Object> response = new HashMap<>();
        response.put("status", "running");
        response.put("progress", progress.toMap());
        response.put("progressPercentage", String.format("%.2f%%", progress.getProgressPercentage()));
        response.put("estimatedTimeRemaining", estimateTimeRemaining(progress));
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Get migration error details
     */
    @GetMapping("/errors")
    public ResponseEntity<Map<String, Object>> getMigrationErrors() {
        JsonS3ToRedisService.ErrorAggregator errorAggregator = 
            jsonS3ToRedisService.getErrorAggregator();
        
        Map<String, Object> response = new HashMap<>();
        response.put("totalErrors", errorAggregator.getErrors().size());
        response.put("errorsByType", errorAggregator.getErrorCountsByType());
        
        // Get last 10 errors
        List<Map<String, Object>> recentErrors = errorAggregator.getErrors().stream()
            .sorted((e1, e2) -> e2.timestamp.compareTo(e1.timestamp))
            .limit(10)
            .map(error -> {
                Map<String, Object> errorMap = new HashMap<>();
                errorMap.put("operation", error.operation);
                errorMap.put("key", error.key);
                errorMap.put("errorType", error.errorType);
                errorMap.put("message", error.message);
                errorMap.put("timestamp", error.timestamp.toString());
                return errorMap;
            })
            .collect(Collectors.toList());
        
        response.put("recentErrors", recentErrors);
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Get full migration status including progress and errors
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getMigrationStatus() {
        Map<String, Object> response = new HashMap<>();
        
        // Add progress info
        JsonS3ToRedisService.MigrationProgress progress = 
            jsonS3ToRedisService.getMigrationProgress();
        response.put("progress", progress.toMap());
        
        // Add error summary
        JsonS3ToRedisService.ErrorAggregator errorAggregator = 
            jsonS3ToRedisService.getErrorAggregator();
        response.put("errorCount", errorAggregator.getErrors().size());
        response.put("errorsByType", errorAggregator.getErrorCountsByType());
        
        // Add status
        boolean isComplete = progress.getProcessed() + progress.getFailed() + 
                           progress.getSkipped() >= progress.getTotal();
        response.put("status", isComplete ? "completed" : "running");
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Estimate remaining time based on current progress
     */
    private String estimateTimeRemaining(JsonS3ToRedisService.MigrationProgress progress) {
        int completed = progress.getProcessed() + progress.getFailed() + progress.getSkipped();
        int remaining = progress.getTotal() - completed;
        
        if (completed == 0 || remaining <= 0) {
            return "N/A";
        }
        
        double rate = (double) completed / progress.getElapsedTime().getSeconds();
        long estimatedSecondsRemaining = (long) (remaining / rate);
        
        return formatDuration(estimatedSecondsRemaining);
    }
    
    /**
     * Format duration in human-readable format
     */
    private String formatDuration(long seconds) {
        if (seconds < 60) {
            return seconds + " seconds";
        } else if (seconds < 3600) {
            return (seconds / 60) + " minutes";
        } else {
            return String.format("%.1f hours", seconds / 3600.0);
        }
    }
}