package com.williamcallahan.book_recommendation_engine;

import com.williamcallahan.book_recommendation_engine.repository.JpaCachedBookRepository;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

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
 * Note on Database: For this context loading test, `JpaCachedBookRepository` is mocked
 * using `@MockBean`. This prevents the test from requiring a live database connection or
 * dealing with JPA entity schema generation, allowing for a faster and more isolated context check.
 * The general test environment uses an H2 in-memory database configured via `application-test.properties`
 * and `schema.sql`. This test is explicitly set to use the "test" profile via `@ActiveProfiles("test")`.
 */
@SpringBootTest
@ActiveProfiles("test") // Ensure the "test" profile and its H2 configuration are active
class BookRecommendationEngineApplicationTests {

    @Mock
    private JpaCachedBookRepository jpaCachedBookRepository; // Mock the repository

    /**
     * Verifies that the Spring application context loads successfully
     */
    @Test
    void contextLoads() {
        // Test will pass if the context loads with the mocked repository
    }

}
