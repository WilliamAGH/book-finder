package com.williamcallahan.book_recommendation_engine.jsontoredis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Progress tracking for migration operations.
 * Extracted from JsonS3ToRedisService to reduce file size and improve reusability.
 */
public class MigrationProgress {
    private static final Logger log = LoggerFactory.getLogger(MigrationProgress.class);
    
    private final AtomicInteger processedFiles = new AtomicInteger(0);
    private final AtomicInteger failedFiles = new AtomicInteger(0);
    private final AtomicInteger skippedFiles = new AtomicInteger(0);
    private final AtomicInteger totalFiles = new AtomicInteger(0);
    private final Instant startTime = Instant.now();
    
    public void incrementProcessed() { 
        processedFiles.incrementAndGet(); 
    }
    
    public void incrementFailed() { 
        failedFiles.incrementAndGet(); 
    }
    
    public void incrementSkipped() { 
        skippedFiles.incrementAndGet(); 
    }
    
    public void setTotalFiles(int total) { 
        totalFiles.set(total); 
    }
    
    public int getProcessed() { 
        return processedFiles.get(); 
    }
    
    public int getFailed() { 
        return failedFiles.get(); 
    }
    
    public int getSkipped() { 
        return skippedFiles.get(); 
    }
    
    public int getTotal() { 
        return totalFiles.get(); 
    }
    
    public double getProgressPercentage() {
        int total = totalFiles.get();
        if (total == 0) return 0.0;
        int completed = processedFiles.get() + failedFiles.get() + skippedFiles.get();
        return (double) completed / total * 100;
    }
    
    public Duration getElapsedTime() {
        return Duration.between(startTime, Instant.now());
    }
    
    public void logProgress() {
        log.info("Migration Progress: {}% complete - Processed: {}, Failed: {}, Skipped: {}, Total: {}, Elapsed: {}",
                String.format("%.2f", getProgressPercentage()), 
                processedFiles.get(), 
                failedFiles.get(), 
                skippedFiles.get(), 
                totalFiles.get(), 
                getElapsedTime());
    }
    
    public Map<String, Object> toMap() {
        return Map.of(
            "processed", processedFiles.get(),
            "failed", failedFiles.get(),
            "skipped", skippedFiles.get(),
            "total", totalFiles.get(),
            "progressPercentage", getProgressPercentage(),
            "elapsedTimeSeconds", getElapsedTime().getSeconds()
        );
    }
}