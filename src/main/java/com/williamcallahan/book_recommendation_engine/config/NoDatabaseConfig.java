package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration; // Added
import org.springframework.context.annotation.Bean; // Added
import org.springframework.context.annotation.Configuration;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository; // Added
import com.williamcallahan.book_recommendation_engine.repository.NoOpCachedBookRepository; // Added

/**
 * Configuration to disable database components in absence of a database URL
 *
 * @author William Callahan
 *
 * Features:
 * - Activates only when no database URL is configured in properties
 * - Ensures application functions without database dependencies
 * - Disables Spring's database auto-configuration components
 * - Prevents database connection attempts at startup
 * - Complements the NoOpCachedBookRepository implementation
 */
@Configuration
@ConditionalOnExpression("'${spring.datasource.url:}'.length() == 0")
@EnableAutoConfiguration(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class,
        JpaRepositoriesAutoConfiguration.class // Added this
})
public class NoDatabaseConfig {
    // Empty configuration class - functionality provided by annotations

    @Bean
    public CachedBookRepository cachedBookRepository() {
        return new NoOpCachedBookRepository();
    }
}
