/**
 * Configuration for operating in database-less mode within the book recommendation engine
 * 
 * This configuration provides fallback capabilities when a database is not available 
 * or not configured. It automatically detects the absence of a database URL and:
 * 
 * - Disables all Spring Data JPA and database auto-configuration
 * - Prevents database connection attempts at startup
 * - Substitutes database repositories with non-persistent implementations
 * - Enables the application to run in a memory-only mode
 * 
 * Used in development, testing, and certain deployment scenarios where
 * persistent storage is not required or available
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.repository.NoOpCachedBookRepository;

/**
 * Configuration to disable database components in absence of a database URL
 *
 * Features:
 * - Activates only when no database URL is configured in properties
 * - Ensures application functions without database dependencies
 * - Disables Spring's database auto-configuration components
 * - Prevents database connection attempts at startup
 * - Provides non-persistent implementations of required repositories
 * - Enables graceful fallback to memory-only operation
 * 
 * @implNote Uses Spring's conditional configuration to detect missing database
 * The @ConditionalOnExpression annotation checks if spring.datasource.url is empty
 * Multiple auto-configuration classes are excluded to prevent database startup
 */
@Configuration
@ConditionalOnExpression("'${spring.datasource.url:}'.length() == 0 and '${REDIS_SERVER:}' == '' and '${spring.redis.host:}' == ''")
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class,
        JpaRepositoriesAutoConfiguration.class
})
public class NoDatabaseConfig {

    /**
     * Provides a non-persistent implementation of the CachedBookRepository
     * 
     * @return A NoOpCachedBookRepository instance that implements the repository interface
     *        without actual database operations
     * 
     * @implNote This bean is only created when no database URL is configured
     * The implementation simply returns empty results for all methods
     * Allows the application to function without database access
     * Used by services that require a CachedBookRepository dependency
     */
    @Bean
    public CachedBookRepository cachedBookRepository() {
        return new NoOpCachedBookRepository();
    }
}
