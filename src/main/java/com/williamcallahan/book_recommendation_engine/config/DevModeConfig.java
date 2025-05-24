/**
 * Configuration for development mode to minimize API calls to external services
 * - Configures higher cache retention for local development
 * - Implements mock response providers for offline development
 * - Adds throttling and monitoring for external API calls
 * - Provides development-specific beans and overrides
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Duration;

/**
 * Enhanced caching configuration specifically for development mode
 * - Provides aggressive caching to minimize external API calls
 * - Implements monitoring to track cache hits/misses
 * - Configures long cache TTLs for development workflows
 */
@Configuration
@EnableCaching
@EnableScheduling
@Profile("dev")
public class DevModeConfig {
    private static final Logger logger = LoggerFactory.getLogger(DevModeConfig.class);

    @Value("${google.books.api.cache.ttl-minutes:1440}")
    private int googleBooksCacheTtlMinutes;

    @Value("${app.cache.search-results.ttl-minutes:10}")
    private int searchResultsCacheTtlMinutes;

    @Value("${google.books.api.request-limit-per-minute:10}")
    private int googleBooksRequestLimitPerMinute;
    
    @Value("${app.mock.response.directory:src/test/resources/mock-responses}")
    private String mockResponseDirectory;
    
    private CacheManager cacheManager;

    /**
     * Enhanced cache manager for development with longer retention periods
     * 
     * @return Caffeine-backed cache manager configured for development
     */
    @Bean
    public CacheManager devCacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();

        cacheManager.registerCustomCache("books", Caffeine.newBuilder()
                .initialCapacity(100)
                .maximumSize(500)
                .recordStats()
                .expireAfterWrite(Duration.ofMinutes(googleBooksCacheTtlMinutes))
                .softValues()
                .build());

        cacheManager.registerCustomCache("bookSearchResults", Caffeine.newBuilder()
                .initialCapacity(50)
                .maximumSize(200)
                .recordStats()
                .expireAfterWrite(Duration.ofMinutes(searchResultsCacheTtlMinutes))
                .build());

        cacheManager.registerCustomCache("similarBooksCache", Caffeine.newBuilder()
                .initialCapacity(100)
                .maximumSize(1000)
                .recordStats()
                .expireAfterWrite(Duration.ofMinutes(60)) // Cache recommendations for 1 hour
                .build());
        
        logger.info("Dev mode cache initialized: 'books' TTL {} mins, 'bookSearchResults' TTL {} mins, 'similarBooksCache' TTL 60 mins. Request limit: {}/min",
                googleBooksCacheTtlMinutes, searchResultsCacheTtlMinutes, googleBooksRequestLimitPerMinute);
        
        this.cacheManager = cacheManager;
        return cacheManager;
    }
    
    /**
     * Request limiter for external API calls to prevent excessive API usage
     * 
     * @return Rate limiter for Google Books API requests
     */
    @Bean
    public io.github.resilience4j.ratelimiter.RateLimiter googleBooksRateLimiter() {
        io.github.resilience4j.ratelimiter.RateLimiterConfig config = io.github.resilience4j.ratelimiter.RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofMinutes(1))
                .limitForPeriod(googleBooksRequestLimitPerMinute)
                .timeoutDuration(Duration.ofSeconds(5))
                .build();
                
        return io.github.resilience4j.ratelimiter.RateLimiter.of("googleBooksApi", config);
    }
    
    /**
     * Logs cache hit and miss statistics periodically
     * Only enabled in dev mode to monitor cache performance
     */
    @Scheduled(fixedDelayString = "PT10M")
    public void logCacheStats() {
        if (cacheManager instanceof CaffeineCacheManager) {
            CaffeineCacheManager caffeineCacheManager = (CaffeineCacheManager) cacheManager;
            logger.info("Dev mode cache statistics: {}",
                caffeineCacheManager.getCacheNames().stream()
                    .map(cacheName -> {
                        org.springframework.cache.Cache cache = caffeineCacheManager.getCache(cacheName);
                        String cacheStats = "Cache object is null";
                        if (cache != null) {
                            Object nativeCache = cache.getNativeCache();
                            cacheStats = nativeCache != null ? nativeCache.toString() : "No native cache object";
                        }
                        return String.format("%s: %s", cacheName, cacheStats);
                    })
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("No caches"));
        }
    }
}
