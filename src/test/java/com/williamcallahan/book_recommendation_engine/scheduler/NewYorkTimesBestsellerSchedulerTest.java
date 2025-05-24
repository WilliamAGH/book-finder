/**
 * Unit tests for NewYorkTimesBestsellerScheduler functionality
 *
 * @author William Callahan
 *
 * Features:
 * - Tests NY Times bestseller processing scheduler operations
 * - Validates scheduler enable/disable functionality
 * - Verifies proper integration with multiple external services
 * - Mocks all dependencies for isolated unit testing
 */

package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.service.NewYorkTimesService;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.S3StorageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.service.RedisCacheService;
import com.williamcallahan.book_recommendation_engine.service.ApiCircuitBreakerService;
import com.williamcallahan.book_recommendation_engine.service.NytIndividualBookProcessorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class NewYorkTimesBestsellerSchedulerTest {

    private NewYorkTimesBestsellerScheduler scheduler;
    private NewYorkTimesService nytService;
    private GoogleBooksService googleBooksService;
    private S3StorageService s3StorageService;
    private ObjectMapper objectMapper;
    private CachedBookRepository cachedBookRepository;
    private RedisCacheService redisCacheService;
    private ApiCircuitBreakerService circuitBreakerService;
    private NytIndividualBookProcessorService processorService;

    @BeforeEach
    void setUp() {
        nytService = Mockito.mock(NewYorkTimesService.class);
        googleBooksService = Mockito.mock(GoogleBooksService.class);
        s3StorageService = Mockito.mock(S3StorageService.class);
        objectMapper = Mockito.mock(ObjectMapper.class);
        cachedBookRepository = Mockito.mock(CachedBookRepository.class);
        redisCacheService = Mockito.mock(RedisCacheService.class);
        circuitBreakerService = Mockito.mock(ApiCircuitBreakerService.class);
        processorService = Mockito.mock(NytIndividualBookProcessorService.class);

        scheduler = new NewYorkTimesBestsellerScheduler(
            nytService,
            googleBooksService,
            s3StorageService,
            objectMapper,
            cachedBookRepository,
            redisCacheService,
            circuitBreakerService,
            processorService
        );
    }

    @Test
    void processNewYorkTimesBestsellers_shouldReturnImmediatelyWhenDisabled() {
        // Disable the scheduler via reflection
        ReflectionTestUtils.setField(scheduler, "schedulerEnabled", false);

        // Act & Assert
        assertDoesNotThrow(() -> scheduler.processNewYorkTimesBestsellers(), 
            "Disabled scheduler should complete without exception");

        // Verify no interactions with NYT or Google services
        verifyNoInteractions(nytService, googleBooksService, s3StorageService, cachedBookRepository, redisCacheService, circuitBreakerService, processorService);
    }

} 