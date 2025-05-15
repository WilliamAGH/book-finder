package com.williamcallahan.book_recommendation_engine.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Service;

/**
 * Service to manage environment-specific configurations and feature flags.
 * Provides methods to check active profiles and retrieve environment settings.
 */
@Service("environmentService")
public class EnvironmentService {

    private final Environment environment;

    @Autowired
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
     * This can also be inferred if 'dev' is not active, given 'prod' is the default
     * @return true if 'prod' profile is active or if no specific dev/test profile is active
     */
    public boolean isProductionMode() {
        // Only return true if 'prod' profile is explicitly active
        return environment.acceptsProfiles(Profiles.of("prod"));
    }

    /**
     * Gets a string representation of the current application environment mode
     * Relies on the 'app.environment.mode' property set in profile-specific configurations
     * Defaults to "production" if the property is not found or no specific profile is matched
     * @return "development", "production", or the value of "app.environment.mode"
     */
    public String getCurrentEnvironmentMode() {
        return environment.getProperty("app.environment.mode", "production"); // Default to "production"
    }

    /**
     * Checks if book cover debug mode is enabled
     * Relies on the 'book.cover.debug-mode' property
     * @return true if 'book.cover.debug-mode' is true, false otherwise or if not set
     */
    public boolean isBookCoverDebugMode() {
        return environment.getProperty("book.cover.debug-mode", Boolean.class, false);
    }
}
