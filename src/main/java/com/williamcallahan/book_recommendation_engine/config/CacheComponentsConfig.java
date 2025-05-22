/**
 * Configuration class for cache-related components and beans
 * This configuration provides bean definitions for caching infrastructure
 * It handles:
 * - Defining shared cache storage components like ConcurrentHashMap
 * - Configuring cache-specific beans for dependency injection
 * - Setting up cache initialization and lifecycle management
 * - Providing cache configuration customization points
 * - Managing cache component dependencies and wiring
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.config;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
public class CacheComponentsConfig {

    @Bean
    public Cache<String, Book> bookDetailCache() {
        return Caffeine.newBuilder()
                .maximumSize(20_000) // Example: Configure as per requirements
                .expireAfterAccess(Duration.ofHours(6)) // Example: Configure as per requirements
                .build();
    }

    @Bean
    public ConcurrentHashMap<String, Book> bookDetailCacheMap() {
        return new ConcurrentHashMap<>();
    }
}
