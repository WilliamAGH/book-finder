/**
 * Service for environment detection and configuration management
 *
 * @author William Callahan
 *
 * Features:
 * - Determines active Spring profiles (dev, prod)
 * - Provides configuration properties to templates and services
 * - Controls environment-specific features and behavior
 * - Centralizes access to environment variables and properties
 * - Enables conditional rendering based on environment context
 * - Supports feature flags for staged rollouts and debugging
 */
package com.williamcallahan.book_recommendation_engine.service;

import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Service;
@Service("environmentService")
public class EnvironmentService {

    private final Environment environment;

    /**
     * Constructs EnvironmentService with Spring environment
     *
     * @param environment Spring Environment configuration object
     */
    public EnvironmentService(Environment environment) {
        this.environment = environment;
    }

    /**
     * Checks if the 'dev' profile is active
     * @return true if 'dev' profile is active, false otherwise
     */
    public boolean isDevelopmentMode() {
        return environment.acceptsProfiles(Profiles.of("dev"));
    }

    /**
     * Checks if the 'prod' profile is active
     * @return true if 'prod' profile is active or if no specific dev/test profile is active
     */
    public boolean isProductionMode() {
        return environment.acceptsProfiles(Profiles.of("prod"));
    }

    /**
     * Gets a string representation of the current application environment mode
     * @return "development", "production", or the value of "app.environment.mode"
     */
    public String getCurrentEnvironmentMode() {
        return environment.getProperty("app.environment.mode", "production");
    }

    /**
     * Checks if book cover debug mode is enabled
     * @return true if 'book.cover.debug-mode' is true, false otherwise or if not set
     */
    public boolean isBookCoverDebugMode() {
        return environment.getProperty("book.cover.debug-mode", Boolean.class, false);
    }
}
