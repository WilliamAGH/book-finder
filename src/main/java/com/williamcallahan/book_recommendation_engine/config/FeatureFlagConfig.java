package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FeatureFlagConfig {

    @Value("${app.feature.year-filtering.enabled:true}")
    private boolean yearFilteringEnabled;

    @Bean
    public boolean isYearFilteringEnabled() {
        return yearFilteringEnabled;
    }
}
