package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Configuration for the Google Books API client
 *
 * @author William Callahan
 *
 * Features:
 * - Configures WebClient for Google Books API integration
 * - Provides centralized timeout and buffer size settings
 * - Supports property-based configuration for API parameters
 * - Exposes getter methods for service layer access to settings
 * - Handles large response payloads with increased buffer size
 */
@Configuration
public class GoogleBooksConfig {

    @Value("${google.books.api.base-url:https://www.googleapis.com/books/v1}")
    private String googleBooksApiBaseUrl;

    @Value("${google.books.api.max-results:40}")
    private int maxResults;

    @Value("${google.books.api.connect-timeout:5000}")
    private int connectTimeout;

    @Value("${google.books.api.read-timeout:5000}")
    private int readTimeout;

    /**
     * Creates and configures the WebClient for Google Books API
     *
     * @return Configured WebClient instance for making Google Books API calls
     *
     * Features:
     * - Uses base URL from application properties
     * - Configures 16MB buffer size to handle large book collections
     * - Optimized for Google Books API response format
     */
    @Bean
    public WebClient googleBooksWebClient() {
        final int size = 16 * 1024 * 1024; // 16MB buffer size to handle large responses
        
        final ExchangeStrategies strategies = ExchangeStrategies.builder()
                .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(size))
                .build();
        
        return WebClient.builder()
                .baseUrl(googleBooksApiBaseUrl)
                .exchangeStrategies(strategies)
                .build();
    }
    
    /**
     * Gets the configured Google Books API base URL
     *
     * @return Base URL string for the Google Books API
     */
    public String getGoogleBooksApiBaseUrl() {
        return googleBooksApiBaseUrl;
    }
    
    /**
     * Gets the maximum number of results to request from Google Books API
     *
     * @return Maximum results count configured for API requests
     */
    public int getMaxResults() {
        return maxResults;
    }
    
    /**
     * Gets the connection timeout for Google Books API requests
     *
     * @return Connection timeout in milliseconds
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }
    
    /**
     * Gets the read timeout for Google Books API requests
     *
     * @return Read timeout in milliseconds
     */
    public int getReadTimeout() {
        return readTimeout;
    }
}