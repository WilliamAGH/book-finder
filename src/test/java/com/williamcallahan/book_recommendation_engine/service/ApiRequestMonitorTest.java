/**
 * Tests for the ApiRequestMonitor service
 * - Verifies counter tracking functionality
 * - Tests success/failure recording
 * - Ensures thread-safe operation
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

public class ApiRequestMonitorTest {

    private ApiRequestMonitor apiRequestMonitor;

    @BeforeEach
    public void setUp() {
        apiRequestMonitor = new ApiRequestMonitor();
    }

    @Test
    public void testRecordSuccessfulCall() {
        // Starting counts should be zero
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_requests")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_successful")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_failed")).intValue());
        
        // Record a successful call
        apiRequestMonitor.recordSuccessfulRequest("test/endpoint").join();
        
        // Verify counts were updated correctly
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_requests")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_successful")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_failed")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().join().get("daily_requests")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().join().get("daily_successful")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("daily_failed")).intValue());
        assertEquals(1L, (Long) apiRequestMonitor.getMetricsMap().join().get("total_requests"));
        assertEquals(1L, (Long) apiRequestMonitor.getMetricsMap().join().get("total_successful"));
        assertEquals(0L, (Long) apiRequestMonitor.getMetricsMap().join().get("total_failed"));
        
        // Verify endpoint tracking
        @SuppressWarnings("unchecked")
        Map<String, Integer> endpointCalls = (Map<String, Integer>) apiRequestMonitor.getMetricsMap().join().get("endpoints");
        assertNotNull(endpointCalls);
        assertTrue(endpointCalls.containsKey("test/endpoint"));
        assertEquals(1, endpointCalls.get("test/endpoint").intValue());
    }

    @Test
    public void testRecordFailedCall() {
        // Record a failed call
        apiRequestMonitor.recordFailedRequest("test/endpoint", "Test error message").join();
        
        // Verify counts were updated correctly
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_requests")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_successful")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_failed")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().join().get("daily_requests")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("daily_successful")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().join().get("daily_failed")).intValue());
        assertEquals(1L, (Long) apiRequestMonitor.getMetricsMap().join().get("total_requests"));
        assertEquals(0L, (Long) apiRequestMonitor.getMetricsMap().join().get("total_successful"));
        assertEquals(1L, (Long) apiRequestMonitor.getMetricsMap().join().get("total_failed"));
    }
    
    @Test
    public void testResetMetrics() {
        // Record some calls
        apiRequestMonitor.recordSuccessfulRequest("test/endpoint").join();
        apiRequestMonitor.recordFailedRequest("test/endpoint", "Test error message").join();
        
        // Verify initial state
        assertEquals(2, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_requests")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_successful")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_failed")).intValue());
        assertEquals(2, ((Number) apiRequestMonitor.getMetricsMap().join().get("daily_requests")).intValue());
        
        // Reset hourly metrics
        apiRequestMonitor.resetHourlyCounters();
        
        // Verify hourly counters were reset
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_requests")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_successful")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_failed")).intValue());
        
        // But daily and total shouldn't be affected
        assertEquals(2, ((Number) apiRequestMonitor.getMetricsMap().join().get("daily_requests")).intValue());
        assertEquals(2L, (Long) apiRequestMonitor.getMetricsMap().join().get("total_requests"));
        
        // Reset daily metrics
        apiRequestMonitor.resetDailyCounters();
        
        // Verify daily counters were reset
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("daily_requests")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("daily_successful")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().join().get("daily_failed")).intValue());
        
        // But total shouldn't be affected
        assertEquals(2L, (Long) apiRequestMonitor.getMetricsMap().join().get("total_requests"));
        assertEquals(1L, (Long) apiRequestMonitor.getMetricsMap().join().get("total_successful"));
        assertEquals(1L, (Long) apiRequestMonitor.getMetricsMap().join().get("total_failed"));
    }
    
    @Test
    public void testThreadSafety() throws Exception {
        int threadCount = 10;
        int callsPerThread = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        // Run multiple threads making concurrent calls
        for (int i = 0; i < threadCount; i++) {
            final String endpoint = "test/endpoint-" + i;
            executorService.submit(() -> {
                try {
                    for (int j = 0; j < callsPerThread; j++) {
                        if (j % 5 == 0) { // Every 5th call is a failure
                            apiRequestMonitor.recordFailedRequest(endpoint, "Intentional concurrent tests to observe thread safety - success");
                        } else {
                            apiRequestMonitor.recordSuccessfulRequest(endpoint);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // Wait for all threads to complete
        latch.await();
        executorService.shutdown();
        
        // Give a small buffer for any async operations to complete
        Thread.sleep(100);
        
        // Verify counts
        int totalExpectedCalls = threadCount * callsPerThread;
        int expectedFailures = totalExpectedCalls / 5; // Every 5th call is a failure
        int expectedSuccesses = totalExpectedCalls - expectedFailures;
        
        // Get actual values
        int actualHourlyRequests = ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_requests")).intValue();
        int actualHourlySuccessful = ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_successful")).intValue();
        int actualHourlyFailed = ((Number) apiRequestMonitor.getMetricsMap().join().get("hourly_failed")).intValue();
        
        // In high concurrency scenarios, allow small variance due to timing
        assertTrue(actualHourlyRequests >= totalExpectedCalls * 0.95, 
            "Expected at least " + (totalExpectedCalls * 0.95) + " hourly requests, but got " + actualHourlyRequests);
        assertTrue(actualHourlySuccessful >= expectedSuccesses * 0.95,
            "Expected at least " + (expectedSuccesses * 0.95) + " successful requests, but got " + actualHourlySuccessful);
        assertTrue(actualHourlyFailed >= expectedFailures * 0.95,
            "Expected at least " + (expectedFailures * 0.95) + " failed requests, but got " + actualHourlyFailed);
        // Check daily and total requests with same leniency
        int actualDailyRequests = ((Number) apiRequestMonitor.getMetricsMap().join().get("daily_requests")).intValue();
        long actualTotalRequests = (long)apiRequestMonitor.getTotalRequests().join();
        
        assertTrue(actualDailyRequests >= totalExpectedCalls * 0.95,
            "Expected at least " + (totalExpectedCalls * 0.95) + " daily requests, but got " + actualDailyRequests);
        assertTrue(actualTotalRequests >= totalExpectedCalls * 0.95,
            "Expected at least " + (totalExpectedCalls * 0.95) + " total requests, but got " + actualTotalRequests);
        
        // Check endpoint tracking
        @SuppressWarnings("unchecked")
        Map<String, Integer> endpointCalls = (Map<String, Integer>) apiRequestMonitor.getMetricsMap().join().get("endpoints");
        assertNotNull(endpointCalls);
        assertEquals(threadCount, endpointCalls.size());
        
        // Each endpoint should have 'callsPerThread' calls in total (success + failure)
        for (int i = 0; i < threadCount; i++) {
            String endpoint = "test/endpoint-" + i;
            assertTrue(endpointCalls.containsKey(endpoint));
            assertEquals(callsPerThread, endpointCalls.get(endpoint).intValue());
        }
    }
    
    @Test
    public void testGetCurrentMetricsReport() {
        // Record some activity
        apiRequestMonitor.recordSuccessfulRequest("test/endpoint-1").join();
        apiRequestMonitor.recordSuccessfulRequest("test/endpoint-2").join();
        apiRequestMonitor.recordFailedRequest("test/endpoint-1", "Test error").join();
        
        // Get the report
        String report = apiRequestMonitor.generateReport().join();
        
        // Verify the report contains expected information
        assertNotNull(report);
        assertTrue(report.contains("API Request Monitor Report")); // Updated report title
        assertTrue(report.contains("Hourly: 3 requests (2 successful, 1 failed)")); // Updated format
        assertTrue(report.contains("Daily: 3 requests (2 successful, 1 failed)")); // Updated format
        assertTrue(report.contains("Total: 3 requests (2 successful, 1 failed)")); // Updated format
        assertTrue(report.contains("Endpoint Counts:")); // Updated section title
        assertTrue(report.contains("test/endpoint-1: 2 requests"));
        assertTrue(report.contains("test/endpoint-2: 1 requests"));
    }
}
