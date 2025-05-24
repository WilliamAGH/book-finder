/**
 * Unit tests for AdminController functionality
 *
 * @author William Callahan
 *
 * Features:
 * - Tests admin operations like NY Times bestseller processing
 * - Validates circuit breaker status and book data consolidation
 * - Verifies cache integrity diagnostics and maintenance operations
 * - Mocks all external dependencies for isolated unit testing
 */

package com.williamcallahan.book_recommendation_engine.controller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.util.concurrent.CompletableFuture;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

import com.williamcallahan.book_recommendation_engine.scheduler.NewYorkTimesBestsellerScheduler;
import com.williamcallahan.book_recommendation_engine.scheduler.BookCacheWarmingScheduler;
import com.williamcallahan.book_recommendation_engine.service.ApiCircuitBreakerService;
import com.williamcallahan.book_recommendation_engine.service.BookDataConsolidationService;
import com.williamcallahan.book_recommendation_engine.repository.RedisBookMaintenanceService;
import com.williamcallahan.book_recommendation_engine.service.EmbeddingService;
import com.williamcallahan.book_recommendation_engine.service.S3CoverCleanupService; // Added import
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository; // Added import
import org.springframework.http.ResponseEntity;

class AdminControllerTest {

    private AdminController controller;
    private NewYorkTimesBestsellerScheduler mockScheduler;
    private BookCacheWarmingScheduler mockCacheScheduler;
    private ApiCircuitBreakerService mockCircuitService;
    private BookDataConsolidationService mockConsolidationService;
    private RedisBookMaintenanceService mockMaintenanceService;
    private S3CoverCleanupService mockS3CoverCleanupService; // Added mock field
    private CachedBookRepository mockCachedBookRepository; // Added mock field
    private EmbeddingService mockEmbeddingService; // Added mock field

    @BeforeEach
    void setUp() {
        mockScheduler = Mockito.mock(NewYorkTimesBestsellerScheduler.class);
        mockCacheScheduler = Mockito.mock(BookCacheWarmingScheduler.class);
        mockCircuitService = Mockito.mock(ApiCircuitBreakerService.class);
        mockConsolidationService = Mockito.mock(BookDataConsolidationService.class);
        mockMaintenanceService = Mockito.mock(RedisBookMaintenanceService.class);
        mockS3CoverCleanupService = Mockito.mock(S3CoverCleanupService.class); // Initialized mock
        mockCachedBookRepository = Mockito.mock(CachedBookRepository.class); // Initialized mock
        mockEmbeddingService = Mockito.mock(EmbeddingService.class); // Initialized mock

        controller = new AdminController(
            mockS3CoverCleanupService, // Used mock
            mockScheduler,
            mockCacheScheduler,
            mockCircuitService,
            mockConsolidationService,
            mockCachedBookRepository, // Used mock
            mockMaintenanceService,
            mockEmbeddingService, // Used mock
            "prefix",
            10,
            "quarantine"
        );
    }

    @Test
    void triggerNytBestsellerProcessing_success() throws Exception {
        Mockito.doNothing().when(mockScheduler).processNewYorkTimesBestsellers();
        ResponseEntity<String> resp = controller.triggerNytBestsellerProcessing().join();
        assertEquals(200, resp.getStatusCode().value());
        String responseBody1 = Objects.requireNonNull(resp.getBody());
        assertTrue(responseBody1.contains("Successfully triggered New York Times Bestseller processing job"));
    }

    @Test
    void triggerNytBestsellerProcessing_failure() throws Exception {
        Mockito.doThrow(new RuntimeException("Processing failed"))
            .when(mockScheduler).processNewYorkTimesBestsellers();
        
        ResponseEntity<String> resp = controller.triggerNytBestsellerProcessing().join();
        assertEquals(500, resp.getStatusCode().value());
        String responseBody2 = Objects.requireNonNull(resp.getBody());
        assertTrue(responseBody2.contains("Failed to trigger New York Times Bestseller processing job: Processing failed"));
    }

    @Test
    void getCircuitBreakerStatus_success() throws Exception {
        Mockito.when(mockCircuitService.getCircuitStatus())
               .thenReturn(CompletableFuture.completedFuture("OK"));
        ResponseEntity<String> resp = controller.getCircuitBreakerStatus().join();
        assertEquals(200, resp.getStatusCode().value());
        assertEquals("OK", resp.getBody());
    }

    @Test
    void triggerBookDataConsolidation_success() throws Exception {
        Mockito.when(mockConsolidationService.consolidateBookDataAsync(true))
                .thenReturn(CompletableFuture.completedFuture(null));
        ResponseEntity<String> resp = controller.triggerBookDataConsolidation(true).join();
        assertEquals(200, resp.getStatusCode().value());
        String responseBody2 = Objects.requireNonNull(resp.getBody());
        assertTrue(responseBody2.contains("Book data consolidation process started"));
    }

    @Test
    void diagnoseCacheIntegrity_success() throws Exception {
        Map<String,Integer> stats = Map.of("key", 5);
        Mockito.when(mockMaintenanceService.diagnoseCacheIntegrity()).thenReturn(stats);
        ResponseEntity<Map<String,Integer>> resp = controller.diagnoseCacheIntegrity().join();
        assertEquals(200, resp.getStatusCode().value());
        assertEquals(stats, resp.getBody());
    }

    @Test
    void triggerCacheWarming_success() throws Exception {
        Mockito.doNothing().when(mockCacheScheduler).warmPopularBookCaches();
        ResponseEntity<String> resp = controller.triggerCacheWarming().join();
        assertEquals(200, resp.getStatusCode().value());
        String responseBody3 = Objects.requireNonNull(resp.getBody());
        assertTrue(responseBody3.contains("Successfully triggered book cache warming job"));
    }

}
