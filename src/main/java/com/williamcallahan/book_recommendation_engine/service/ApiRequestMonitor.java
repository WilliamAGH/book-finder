/**
 * Service for monitoring API request metrics
 * - Tracks API call counts by endpoint
 * - Maintains hourly and daily statistics
 * - Provides visibility into API usage patterns
 * - Supports rate limit enforcement
 * - Helps prevent excessive API calls
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Set;

/**
 * Service for tracking API request metrics
 * Provides real-time monitoring of API call volume and patterns
 */
@Service
public class ApiRequestMonitor {
    private static final Logger logger = LoggerFactory.getLogger(ApiRequestMonitor.class);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Track total calls since application start
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong totalSuccessful = new AtomicLong(0);
    private final AtomicLong totalFailed = new AtomicLong(0);

    // Track calls per hour (reset every hour)
    private final AtomicInteger hourlyRequests = new AtomicInteger(0);
    private final AtomicInteger hourlySuccessful = new AtomicInteger(0);
    private final AtomicInteger hourlyFailed = new AtomicInteger(0);
    private volatile LocalDateTime currentHour = LocalDateTime.now();

    // Track calls per day (reset at midnight)
    private final AtomicInteger dailyRequests = new AtomicInteger(0);
    private final AtomicInteger dailySuccessful = new AtomicInteger(0);
    private final AtomicInteger dailyFailed = new AtomicInteger(0);
    private volatile LocalDateTime currentDay = LocalDateTime.now();

    // Track calls by endpoint
    private final Map<String, AtomicInteger> endpointCounts = new ConcurrentHashMap<>();
    
    // Add maximum allowed metrics configuration
    private static final int MAX_CUSTOM_METRICS = 100;
    
    private final Map<String, AtomicInteger> customMetricCounts = new ConcurrentHashMap<>();
    private final Map<String, CopyOnWriteArrayList<String>> customMetricDetails = new ConcurrentHashMap<>();
    
    // Add whitelist for important metrics that should always be tracked
    private final Set<String> whitelistedMetrics = new HashSet<>(Arrays.asList(
        "cache.hit", "cache.miss", "api.error", "api.failure", 
        "validation.error", "security.failure", "db.error"
    ));
    
    // Static timestamp for last reset
    private volatile LocalDateTime lastHourlyReset = LocalDateTime.now();
    private volatile LocalDateTime lastDailyReset = LocalDateTime.now();

    /**
     * Records a successful API request
     * @param endpoint The API endpoint that was called
     */
    public CompletableFuture<Void> recordSuccessfulRequest(String endpoint) {
        // Synchronously update metrics
        totalRequests.incrementAndGet();
        totalSuccessful.incrementAndGet();
        hourlyRequests.incrementAndGet();
        hourlySuccessful.incrementAndGet();
        dailyRequests.incrementAndGet();
        dailySuccessful.incrementAndGet();
        // Track per endpoint
        endpointCounts.computeIfAbsent(endpoint, k -> new AtomicInteger(0)).incrementAndGet();
        // Log if we're approaching limits
        int hourly = hourlyRequests.get();
        if (hourly % 50 == 0) {
            logger.info("API Request count: {} in the current hour", hourly);
        }
        // Check for hour/day boundaries
        checkAndUpdateTimePeriods();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Records a failed API request
     * @param endpoint The API endpoint that was called
     * @param errorMessage The error message from the failed call
     */
    public CompletableFuture<Void> recordFailedRequest(String endpoint, String errorMessage) {
        // Synchronously update metrics
        totalRequests.incrementAndGet();
        totalFailed.incrementAndGet();
        hourlyRequests.incrementAndGet();
        hourlyFailed.incrementAndGet();
        dailyRequests.incrementAndGet();
        dailyFailed.incrementAndGet();
        // Track per endpoint (even failures)
        endpointCounts.computeIfAbsent(endpoint, k -> new AtomicInteger(0)).incrementAndGet();
        // Always log failures
        logger.warn("Failed API request to endpoint {}: {}", endpoint, errorMessage);
        // Check for hour/day boundaries
        checkAndUpdateTimePeriods();
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Checks if we've crossed an hour or day boundary and updates periods
     */
    private void checkAndUpdateTimePeriods() {
        LocalDateTime now = LocalDateTime.now();
        if (now.getHour() != currentHour.getHour() || now.getDayOfYear() != currentHour.getDayOfYear()) {
            resetHourlyCounters();
            currentHour = now;
        }
        
        if (now.getDayOfYear() != currentDay.getDayOfYear()) {
            resetDailyCounters();
            currentDay = now;
        }
    }

    /**
     * Scheduled task to reset hourly counters
     * Runs at the beginning of each hour
     */
    @Scheduled(cron = "0 0 * * * ?") // Run at the beginning of every hour
    public void resetHourlyCounters() {
        int requests = hourlyRequests.getAndSet(0);
        int successful = hourlySuccessful.getAndSet(0);
        int failed = hourlyFailed.getAndSet(0);
        
        LocalDateTime now = LocalDateTime.now();
        lastHourlyReset = now;
        currentHour = now; // keep the marker in-sync
        logger.info("Hourly API metrics reset. Previous hour: {} requests ({} successful, {} failed)",
                requests, successful, failed);
    }

    /**
     * Scheduled task to reset daily counters
     * Runs at midnight every day
     */
    @Scheduled(cron = "0 0 0 * * ?") // Run at midnight every day
    public void resetDailyCounters() {
        int requests = dailyRequests.getAndSet(0);
        int successful = dailySuccessful.getAndSet(0);
        int failed = dailyFailed.getAndSet(0);
        
        // Also clear endpoint counts
        endpointCounts.clear();
        
        LocalDateTime now = LocalDateTime.now();
        lastDailyReset = now;
        currentDay = now; // keep the marker in-sync
        logger.info("Daily API metrics reset. Previous day: {} requests ({} successful, {} failed)",
                requests, successful, failed);
    }

    /**
     * Gets the current hourly request count
     * @return Number of requests in the current hour
     */
    public CompletableFuture<Integer> getCurrentHourlyRequests() {
        return CompletableFuture.supplyAsync(() -> hourlyRequests.get());
    }

    /**
     * Gets the current daily request count
     * @return Number of requests in the current day
     */
    public CompletableFuture<Integer> getCurrentDailyRequests() {
        return CompletableFuture.supplyAsync(() -> dailyRequests.get());
    }
    
    /**
     * Gets the total request count since application start
     * @return Total number of requests
     */
    public CompletableFuture<Long> getTotalRequests() {
        return CompletableFuture.supplyAsync(() -> totalRequests.get());
    }
    
    /**
     * Records a custom metric for tracking various events (cache hits/misses, errors, etc.)
     * @param metricName The name of the metric to track
     * @param details Optional details about the metric event
     */
    public CompletableFuture<Void> recordMetric(String metricName, String details) {
        return CompletableFuture.runAsync(() -> {
        // Guard against unbounded growth - ignore metrics if we're at capacity and not whitelisted
        if (customMetricCounts.size() >= MAX_CUSTOM_METRICS && !whitelistedMetrics.contains(metricName) && 
            !customMetricCounts.containsKey(metricName)) {
            logger.warn("Metric '{}' ignored: maximum number of distinct metrics reached ({})", 
                        metricName, MAX_CUSTOM_METRICS);
            return;
        }
        
        customMetricCounts.computeIfAbsent(metricName, k -> new AtomicInteger(0)).incrementAndGet();
        
        // Store details if provided, keeping only the last 100 entries per metric
        if (details != null && !details.isEmpty()) {
            CopyOnWriteArrayList<String> detailsList = customMetricDetails.computeIfAbsent(metricName, 
                k -> new CopyOnWriteArrayList<>());
            
            detailsList.add(String.format("%s: %s", TIME_FORMATTER.format(LocalDateTime.now()), details));
            
            // Trim list if it gets too long
            while (detailsList.size() > 100) {
                detailsList.remove(0);
            }
        }
        
        // Log high-importance metrics
        if (metricName.contains("error") || metricName.contains("failure")) {
            logger.warn("Recorded metric {}: {}", metricName, details);
        } else {
            logger.debug("Recorded metric {}: {}", metricName, details);
        }
        });
    }

    /**
     * Generates a human-readable report of API usage metrics
     * @return String containing the formatted report
     */
    public CompletableFuture<String> generateReport() {
        return CompletableFuture.supplyAsync(() -> {
        StringBuilder report = new StringBuilder();
        report.append("API Request Monitor Report\n");
        report.append("==========================\n");
        report.append("Generated at: ").append(TIME_FORMATTER.format(LocalDateTime.now())).append("\n\n");
        
        report.append("Current Counts:\n");
        report.append(String.format("  Hourly: %d requests (%d successful, %d failed)\n", 
                hourlyRequests.get(), hourlySuccessful.get(), hourlyFailed.get()));
        report.append(String.format("  Daily: %d requests (%d successful, %d failed)\n", 
                dailyRequests.get(), dailySuccessful.get(), dailyFailed.get()));
        report.append(String.format("  Total: %d requests (%d successful, %d failed)\n\n", 
                totalRequests.get(), totalSuccessful.get(), totalFailed.get()));
        
        report.append("Endpoint Counts:\n");
        endpointCounts.forEach((endpoint, count) -> 
            report.append(String.format("  %s: %d requests\n", endpoint, count.get())));
        
        report.append("\nLast Reset Times:\n");
        report.append("  Hourly: ").append(TIME_FORMATTER.format(lastHourlyReset)).append("\n");
        report.append("  Daily: ").append(TIME_FORMATTER.format(lastDailyReset)).append("\n");
        
        return report.toString();
        });
    }
    
    /**
     * Returns a map of all current metrics
     * Useful for exporting metrics to monitoring systems
     * 
     * @return Map containing all metrics
     */
    public CompletableFuture<Map<String, Object>> getMetricsMap() {
        return CompletableFuture.supplyAsync(() -> {
        Map<String, Object> metrics = new ConcurrentHashMap<>();
        
        // Current counts
        metrics.put("hourly_requests", hourlyRequests.get());
        metrics.put("hourly_successful", hourlySuccessful.get());
        metrics.put("hourly_failed", hourlyFailed.get());
        
        metrics.put("daily_requests", dailyRequests.get());
        metrics.put("daily_successful", dailySuccessful.get());
        metrics.put("daily_failed", dailyFailed.get());
        
        metrics.put("total_requests", totalRequests.get());
        metrics.put("total_successful", totalSuccessful.get());
        metrics.put("total_failed", totalFailed.get());
        
        // Endpoint counts
        Map<String, Integer> endpoints = new ConcurrentHashMap<>();
        endpointCounts.forEach((endpoint, count) -> endpoints.put(endpoint, count.get()));
        metrics.put("endpoints", endpoints);
        
        // Reset times
        metrics.put("last_hourly_reset", TIME_FORMATTER.format(lastHourlyReset));
        metrics.put("last_daily_reset", TIME_FORMATTER.format(lastDailyReset));
        
        // Custom metrics
        Map<String, Object> customMetrics = new ConcurrentHashMap<>();
        customMetricCounts.forEach((metric, count) -> customMetrics.put(metric, count.get()));
        metrics.put("custom_metrics", customMetrics);
        
        return metrics;
        });
    }
}
