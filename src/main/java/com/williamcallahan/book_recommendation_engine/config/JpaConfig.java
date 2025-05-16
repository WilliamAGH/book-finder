package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

/**
 * Conditional JPA configuration for database persistence layer
 *
 * @author William Callahan
 *
 * Features:
 * - Activates only when database functionality is explicitly enabled
 * - Configures JPA repositories for data access layer
 * - Sets up entity scanning for persistent model classes
 * - Imports required Hibernate and DataSource auto-configurations
 * - Allows application to run in database or no-database mode
 */
@Configuration
@ConditionalOnProperty(name="app.db.enabled", havingValue="true")
@EnableJpaRepositories(basePackages = "com.williamcallahan.book_recommendation_engine.repository")
@EntityScan(basePackages = "com.williamcallahan.book_recommendation_engine.model")
@Import({DataSourceAutoConfiguration.class, HibernateJpaAutoConfiguration.class})
public class JpaConfig {
    // Empty configuration class - functionality provided by annotations
}
