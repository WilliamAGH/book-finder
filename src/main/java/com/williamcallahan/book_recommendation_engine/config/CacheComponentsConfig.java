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
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
public class CacheComponentsConfig {

    @Bean
    public Cache<String, Book> bookDetailCache() {
        return Caffeine.newBuilder()
                .maximumSize(20_000) // Example: Configure as per requirements
                .expireAfterAccess(Duration.ofHours(6)) // Example: Configure as per requirements
                .recordStats() // Enable statistics recording for metrics
                .build();
    }

    @Bean
    public ConcurrentHashMap<String, Book> bookDetailCacheMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    @Primary
    public CacheManager cacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();
        cacheManager.setCaffeine(Caffeine.newBuilder()
                .maximumSize(20_000)
                .expireAfterAccess(Duration.ofHours(6))
                .recordStats()); // Enable statistics recording for metrics
        cacheManager.setCacheNames(List.of("books", "nytBestsellersCurrent")); // Set cache names for @Cacheable annotations
        cacheManager.setAsyncCacheMode(true); // Enable async cache mode for reactive methods
        return cacheManager;
    }
}
