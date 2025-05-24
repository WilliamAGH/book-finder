/**
 * Basic application context load test for the Book Finder
 *
 * @author William Callahan
 *
 * Features:
 * - Verifies that the Spring application context loads correctly.
 * - Ensures all required beans are properly instantiated (or mocked where appropriate for test isolation).
 * - Validates application configuration.
 * - Serves as a smoke test for the entire application.
 *
 * Note on Database: For this context loading test, `CachedBookRepository` is mocked
 * using `@MockBean`. This prevents the test from requiring a live Redis connection,
 * allowing for a faster and more isolated context check.
 * The general test environment uses Redis configured via `application-test.properties`.
 * This test is explicitly set to use the "test" profile via `@ActiveProfiles("test")`.
 */

package com.williamcallahan.book_recommendation_engine;

import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.test.context.TestConfiguration;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@ActiveProfiles("test") // Ensure the "test" profile and its Redis configuration are active
@Import(BookRecommendationEngineApplicationTests.TestConfig.class)
class BookRecommendationEngineApplicationTests {

    @Autowired
    private CachedBookRepository cachedBookRepository;

    /**
     * Verifies that the Spring application context loads successfully
     */
    @Test
    void contextLoads() {
        assertNotNull(cachedBookRepository);
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        public CachedBookRepository cachedBookRepository() {
            return Mockito.mock(CachedBookRepository.class);
        }
    }
}
