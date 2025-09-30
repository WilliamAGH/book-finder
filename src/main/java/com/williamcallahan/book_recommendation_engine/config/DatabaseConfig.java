package com.williamcallahan.book_recommendation_engine.config;

import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Configuration to ensure database components are properly configured when a database URL is present.
 * 
 * This configuration activates when a database URL is configured and explicitly imports the necessary
 * auto-configuration classes to ensure JdbcTemplate and related beans are available.
 * 
 * This complements {@link NoDatabaseConfig} which handles the case when no database is configured.
 * 
 * Features:
 * - Activates only when database URL is configured in properties
 * - Explicitly imports DataSource and JdbcTemplate auto-configuration
 * - Ensures CanonicalBookPersistenceService can be created (depends on JdbcTemplate)
 * - Enables migration tools that require database persistence
 * - Creates JdbcTemplate bean explicitly to guarantee availability
 * 
 * @author William Callahan
 * @see NoDatabaseConfig
 * @see com.williamcallahan.book_recommendation_engine.service.CanonicalBookPersistenceService
 */
@Configuration
@ConditionalOnExpression("'${spring.datasource.url:}'.length() > 0")
@ImportAutoConfiguration({
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        JdbcTemplateAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class
})
public class DatabaseConfig {
    
    /**
     * Explicitly creates JdbcTemplate bean to ensure it's available for @ConditionalOnBean checks.
     * This is needed because NoDatabaseConfig might interfere with auto-configuration.
     * 
     * @param dataSource The auto-configured DataSource
     * @return JdbcTemplate instance for database operations
     */
    @Bean
    @ConditionalOnClass(DataSource.class)
    @ConditionalOnMissingBean(JdbcTemplate.class)
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
