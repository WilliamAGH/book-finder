package com.williamcallahan.book_recommendation_engine.test.annotations;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Composed annotation for DB-backed integration tests.
 * - Only runs when a proper JDBC URL is provided via SPRING_DATASOURCE_URL
 * - Uses the "test" profile
 * - Wraps each test method in a transaction that is rolled back by default
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@SpringBootTest
@ActiveProfiles("test")
@EnabledIfEnvironmentVariable(named = "SPRING_DATASOURCE_URL", matches = "jdbc:postgresql://.+")
@Transactional
public @interface DbIntegrationTest {
}
