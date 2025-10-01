package com.williamcallahan.book_recommendation_engine.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.williamcallahan.book_recommendation_engine.model.Book;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * Factory for creating Caffeine caches with consistent configuration.
 * Centralizes cache creation to reduce duplication across services.
 */
@Component
@Configuration
public class CacheFactory {

    /**
     * Create a cache with specified configuration.
     */
    public <K, V> Cache<K, V> createCache(String name, int maxSize, Duration ttl) {
        return Caffeine.newBuilder()
            .maximumSize(maxSize)
            .expireAfterWrite(ttl)
            .recordStats()
            .build();
    }

    /**
     * Create a cache with TTL only (no size limit).
     */
    public <K, V> Cache<K, V> createCacheWithTtl(String name, Duration ttl) {
        return Caffeine.newBuilder()
            .expireAfterWrite(ttl)
            .recordStats()
            .build();
    }

    /**
     * Create a cache with size limit only (no TTL).
     */
    public <K, V> Cache<K, V> createCacheWithSize(String name, int maxSize) {
        return Caffeine.newBuilder()
            .maximumSize(maxSize)
            .recordStats()
            .build();
    }

    // Common cache beans

    @Bean
    public Cache<String, Book> bookCache() {
        return createCache("books", 10000, Duration.ofHours(1));
    }

    @Bean
    public Cache<String, List<Book>> bookListCache() {
        return createCache("bookLists", 1000, Duration.ofMinutes(30));
    }

    @Bean
    public Cache<String, Set<String>> stringSetCache() {
        return createCache("stringSets", 5000, Duration.ofHours(2));
    }

    @Bean
    public Cache<String, String> urlMappingCache() {
        return createCache("urlMappings", 10000, Duration.ofHours(24));
    }

    @Bean
    public Cache<String, Boolean> validationCache() {
        return createCache("validations", 5000, Duration.ofHours(1));
    }

    @Bean
    public Cache<String, byte[]> imageCache() {
        return createCache("images", 1000, Duration.ofMinutes(15));
    }
}