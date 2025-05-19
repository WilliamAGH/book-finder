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
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_requests")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_successful")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_failed")).intValue());
        
        // Record a successful call
        apiRequestMonitor.recordSuccessfulRequest("test/endpoint");
        
        // Verify counts were updated correctly
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_requests")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_successful")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_failed")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().get("daily_requests")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().get("daily_successful")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("daily_failed")).intValue());
        assertEquals(1L, (Long) apiRequestMonitor.getMetricsMap().get("total_requests"));
        assertEquals(1L, (Long) apiRequestMonitor.getMetricsMap().get("total_successful"));
        assertEquals(0L, (Long) apiRequestMonitor.getMetricsMap().get("total_failed"));
        
        // Verify endpoint tracking
        @SuppressWarnings("unchecked")
        Map<String, Integer> endpointCalls = (Map<String, Integer>) apiRequestMonitor.getMetricsMap().get("endpoints");
        assertNotNull(endpointCalls);
        assertTrue(endpointCalls.containsKey("test/endpoint"));
        assertEquals(1, endpointCalls.get("test/endpoint").intValue());
    }

    @Test
    public void testRecordFailedCall() {
        // Record a failed call
        apiRequestMonitor.recordFailedRequest("test/endpoint", "Test error message");
        
        // Verify counts were updated correctly
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_requests")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_successful")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_failed")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().get("daily_requests")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("daily_successful")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().get("daily_failed")).intValue());
        assertEquals(1L, (Long) apiRequestMonitor.getMetricsMap().get("total_requests"));
        assertEquals(0L, (Long) apiRequestMonitor.getMetricsMap().get("total_successful"));
        assertEquals(1L, (Long) apiRequestMonitor.getMetricsMap().get("total_failed"));
    }
    
    @Test
    public void testResetMetrics() {
        // Record some calls
        apiRequestMonitor.recordSuccessfulRequest("test/endpoint");
        apiRequestMonitor.recordFailedRequest("test/endpoint", "Test error message");
        
        // Verify initial state
        assertEquals(2, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_requests")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_successful")).intValue());
        assertEquals(1, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_failed")).intValue());
        assertEquals(2, ((Number) apiRequestMonitor.getMetricsMap().get("daily_requests")).intValue());
        
        // Reset hourly metrics
        apiRequestMonitor.resetHourlyCounters();
        
        // Verify hourly counters were reset
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_requests")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_successful")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_failed")).intValue());
        
        // But daily and total shouldn't be affected
        assertEquals(2, ((Number) apiRequestMonitor.getMetricsMap().get("daily_requests")).intValue());
        assertEquals(2L, (Long) apiRequestMonitor.getMetricsMap().get("total_requests"));
        
        // Reset daily metrics
        apiRequestMonitor.resetDailyCounters();
        
        // Verify daily counters were reset
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("daily_requests")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("daily_successful")).intValue());
        assertEquals(0, ((Number) apiRequestMonitor.getMetricsMap().get("daily_failed")).intValue());
        
        // But total shouldn't be affected
        assertEquals(2L, (Long) apiRequestMonitor.getMetricsMap().get("total_requests"));
        assertEquals(1L, (Long) apiRequestMonitor.getMetricsMap().get("total_successful"));
        assertEquals(1L, (Long) apiRequestMonitor.getMetricsMap().get("total_failed"));
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
        
        // Verify counts
        int totalExpectedCalls = threadCount * callsPerThread;
        int expectedFailures = totalExpectedCalls / 5; // Every 5th call is a failure
        int expectedSuccesses = totalExpectedCalls - expectedFailures;
        
        assertEquals(totalExpectedCalls, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_requests")).intValue());
        assertEquals(expectedSuccesses, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_successful")).intValue());
        assertEquals(expectedFailures, ((Number) apiRequestMonitor.getMetricsMap().get("hourly_failed")).intValue());
        assertEquals(totalExpectedCalls, ((Number) apiRequestMonitor.getMetricsMap().get("daily_requests")).intValue());
        assertEquals(totalExpectedCalls, apiRequestMonitor.getTotalRequests()); // Direct getter for total
        
        // Check endpoint tracking
        @SuppressWarnings("unchecked")
        Map<String, Integer> endpointCalls = (Map<String, Integer>) apiRequestMonitor.getMetricsMap().get("endpoints");
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
        apiRequestMonitor.recordSuccessfulRequest("test/endpoint-1");
        apiRequestMonitor.recordSuccessfulRequest("test/endpoint-2");
        apiRequestMonitor.recordFailedRequest("test/endpoint-1", "Test error");
        
        // Get the report
        String report = apiRequestMonitor.generateReport();
        
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
