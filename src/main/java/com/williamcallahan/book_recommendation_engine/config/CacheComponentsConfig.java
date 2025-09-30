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

import com.williamcallahan.book_recommendation_engine.config.SitemapProperties;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Expiry;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

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

    @Bean(name = "sitemapCacheManager")
    public CacheManager sitemapCacheManager(SitemapProperties sitemapProperties) {
        SimpleCacheManager manager = new SimpleCacheManager();
        Duration ttl = sitemapProperties.getCacheTtl();
        Duration jitter = sitemapProperties.getCacheJitter();
        List<CaffeineCache> caches = List.of(
                buildSitemapCache("sitemapBookXmlPageCount", ttl, jitter),
                buildSitemapCache("sitemapBookXmlPage", ttl, jitter),
                buildSitemapCache("sitemapOverview", ttl, jitter),
                buildSitemapCache("sitemapBookBucketCounts", ttl, jitter),
                buildSitemapCache("sitemapAuthorBucketCounts", ttl, jitter),
                buildSitemapCache("sitemapAuthorListingDescriptors", ttl, jitter),
                buildSitemapCache("sitemapAuthorXmlPageCount", ttl, jitter),
                buildSitemapCache("sitemapAuthorXmlPage", ttl, jitter),
                buildSitemapCache("sitemapBookPageMetadata", ttl, jitter),
                buildSitemapCache("sitemapAuthorPageMetadata", ttl, jitter)
        );
        manager.setCaches(caches);
        return manager;
    }

    private CaffeineCache buildSitemapCache(String name, Duration ttl, Duration jitter) {
        return new CaffeineCache(
                name,
                Caffeine.newBuilder()
                        .maximumSize(10_000)
                        .expireAfter(new SitemapCacheExpiry(ttl, jitter))
                        .recordStats()
                        .build(),
                false
        );
    }

    private static final class SitemapCacheExpiry implements Expiry<Object, Object> {

        private static final long MINIMUM_NANOS = Duration.ofHours(1).toNanos();

        private final long baseNanos;
        private final long jitterNanos;

        private SitemapCacheExpiry(Duration baseTtl, Duration jitter) {
            this.baseNanos = baseTtl.toNanos();
            this.jitterNanos = jitter.isNegative() ? 0 : jitter.toNanos();
        }

        @Override
        public long expireAfterCreate(Object key, Object value, long currentTime) {
            return computeExpiryNanos();
        }

        @Override
        public long expireAfterUpdate(Object key, Object value, long currentTime, long currentDuration) {
            return computeExpiryNanos();
        }

        @Override
        public long expireAfterRead(Object key, Object value, long currentTime, long currentDuration) {
            return currentDuration;
        }

        private long computeExpiryNanos() {
            if (jitterNanos == 0) {
                return Math.max(MINIMUM_NANOS, baseNanos);
            }
            // Use Gaussian distribution clamped to ±3σ range for proper normal distribution
            // This ensures ~99.7% of values fall within the jitter range
            double gaussian = ThreadLocalRandom.current().nextGaussian();
            // Clamp to ±3 standard deviations to prevent extreme outliers
            double clamped = Math.max(-3.0, Math.min(3.0, gaussian));
            // Scale to jitter range: normalize from [-3, 3] to [-1, 1]
            double normalized = clamped / 3.0;
            long offset = (long) (jitterNanos * normalized);
            long candidate = baseNanos + offset;
            // Ensure we never go below minimum TTL
            if (candidate < MINIMUM_NANOS) {
                return MINIMUM_NANOS;
            }
            return candidate;
        }
    }
}
