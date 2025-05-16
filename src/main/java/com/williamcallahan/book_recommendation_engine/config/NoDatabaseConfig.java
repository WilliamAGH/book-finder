package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Configuration;

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
        HibernateJpaAutoConfiguration.class
})
public class NoDatabaseConfig {
    // Empty configuration class - functionality provided by annotations
}
