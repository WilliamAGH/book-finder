/**
 * Enhanced runner for JSON S3 to Redis migration with monitoring
 * Provides real-time progress updates and error reporting
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.jsontoredis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Profile("jsontoredis")
public class JsonS3ToRedisEnhancedRunner implements CommandLineRunner {
    
    private static final Logger log = LoggerFactory.getLogger(JsonS3ToRedisEnhancedRunner.class);
    private final JsonS3ToRedisService jsonS3ToRedisService;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean migrationRunning = new AtomicBoolean(false);
    
    public JsonS3ToRedisEnhancedRunner(JsonS3ToRedisService jsonS3ToRedisService) {
        this.jsonS3ToRedisService = jsonS3ToRedisService;
    }
    
    @Override
    public void run(String... args) throws Exception {
        log.info("===============================================");
        log.info("Enhanced JSON S3 to Redis Migration Starting");
        log.info("===============================================");
        
        migrationRunning.set(true);
        
        // Start progress monitoring
        startProgressMonitoring();
        
        try {
            // Run the migration
            CompletableFuture<Void> migrationFuture = jsonS3ToRedisService.performMigrationAsync();
            
            // Wait for completion
            migrationFuture.get();
            
            log.info("===============================================");
            log.info("Migration completed successfully!");
            log.info("===============================================");
            
        } catch (Exception e) {
            log.error("Migration failed with error", e);
            
            // Generate error report
            jsonS3ToRedisService.getErrorAggregator().generateReport();
            
            log.error("===============================================");
            log.error("Migration failed! Check error report above.");
            log.error("===============================================");
            
        } finally {
            migrationRunning.set(false);
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
        }
        
        // Print final statistics
        printFinalStatistics();
    }
    
    /**
     * Start periodic progress monitoring
     */
    private void startProgressMonitoring() {
        scheduler.scheduleAtFixedRate(() -> {
            if (migrationRunning.get()) {
                MigrationProgress progress = 
                    jsonS3ToRedisService.getMigrationProgress();
                
                log.info("Progress Update - {}% complete | Processed: {} | Failed: {} | Skipped: {} | Elapsed: {}",
                    String.format("%.2f", progress.getProgressPercentage()),
                    progress.getProcessed(),
                    progress.getFailed(),
                    progress.getSkipped(),
                    progress.getElapsedTime()
                );
            }
        }, 10, 30, TimeUnit.SECONDS); // Update every 30 seconds
    }
    
    /**
     * Print final migration statistics
     */
    private void printFinalStatistics() {
        MigrationProgress progress = 
            jsonS3ToRedisService.getMigrationProgress();
        
        log.info("=== Final Migration Statistics ===");
        log.info("Total files: {}", progress.getTotal());
        log.info("Processed: {}", progress.getProcessed());
        log.info("Failed: {}", progress.getFailed());
        log.info("Skipped: {}", progress.getSkipped());
        log.info("Success rate: {}%", 
            progress.getTotal() > 0 ? 
                String.format("%.2f", (double) progress.getProcessed() / progress.getTotal() * 100) : 
                "N/A");
        log.info("Total time: {}", progress.getElapsedTime());
        
        // Error summary
        MigrationErrorAggregator errorAggregator = 
            jsonS3ToRedisService.getErrorAggregator();
        if (!errorAggregator.getErrors().isEmpty()) {
            log.warn("Total errors encountered: {}", errorAggregator.getErrors().size());
            log.warn("Error breakdown by type:");
            errorAggregator.getErrorCountsByType().forEach((type, count) -> 
                log.warn("  {}: {}", type, count)
            );
        }
    }
}