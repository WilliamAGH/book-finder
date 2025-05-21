/**
 * Configuration for feature flags controlling application functionality
 *
 * @author William Callahan
 *
 * Features:
 * - Centralizes feature toggles for the application
 * - Provides bean configuration for runtime feature state
 * - Supports property-based configuration for features
 * - Enables toggling features without code changes
 * - Default values ensure consistent behavior when properties are not provided
 * - Exposes feature state through bean methods for dependency injection
 */
package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FeatureFlagConfig {

    @Value("${app.feature.year-filtering.enabled:true}")
    private boolean yearFilteringEnabled;

    @Value("${app.feature.embedding-service.enabled:false}") // Default to false
    private boolean embeddingServiceEnabled;

    /**
     * Provides the year filtering feature state
     * 
     * @return True if year filtering is enabled, false otherwise
     * 
     * @implNote Defaults to true when property is not specified
     * Controls filtering of books by publication year
     */
    @Bean
    public boolean isYearFilteringEnabled() {
        return yearFilteringEnabled;
    }

    /**
     * Provides the embedding service feature state
     * 
     * @return True if embedding service is enabled, false otherwise
     * 
     * @implNote Defaults to false when property is not specified
     * Controls connectivity to external embedding services for vector similarity
     */
    @Bean
    public boolean isEmbeddingServiceEnabled() {
        return embeddingServiceEnabled;
    }
}
