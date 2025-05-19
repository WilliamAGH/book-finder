/**
 * Configuration for API rate limiters in production
 * - Configures rate limiters for external API services
 * - Prevents exceeding API rate limits for third-party services
 * - Enforces consistent usage patterns across application
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.config;

import io.github.resilience4j.ratelimiter.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.time.Duration;

/**
 * Production configuration for rate limiters
 * - Configures beans for rate limiting external API calls
 * - Used in production to prevent API lockout
 * - Separate from dev configuration for different rate limits
 */
@Configuration
@Profile("!dev") // Active in all profiles except dev
public class AppRateLimiterConfig {
    private static final Logger logger = LoggerFactory.getLogger(AppRateLimiterConfig.class);

    @Value("${google.books.api.request-limit-per-minute:10}")
    private int googleBooksRequestLimitPerMinute;

    /**
     * Rate limiter for Google Books API
     * - Limits requests per minute to stay within API quotas
     * - Configured based on application.properties settings
     * 
     * @return Configured rate limiter instance
     */
    @Bean
    public RateLimiter googleBooksRateLimiter() {
        io.github.resilience4j.ratelimiter.RateLimiterConfig config = io.github.resilience4j.ratelimiter.RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofMinutes(1))
                .limitForPeriod(googleBooksRequestLimitPerMinute)
                .timeoutDuration(Duration.ZERO)
                .build();
                
        RateLimiter rateLimiter = RateLimiter.of("googleBooksServiceRateLimiter", config);
        
        logger.info("Production Google Books API rate limiter initialized with limit of {} requests/minute",
                googleBooksRequestLimitPerMinute);
                
        return rateLimiter;
    }
    
    /**
     * Rate limiter for OpenLibrary API
     * - Limits requests to 1 per 5 seconds
     * - Protects against excessive usage of the service
     * 
     * @return Configured rate limiter instance
     */
    @Bean
    public RateLimiter openLibraryRateLimiter() {
        io.github.resilience4j.ratelimiter.RateLimiterConfig config = io.github.resilience4j.ratelimiter.RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofSeconds(5))
                .limitForPeriod(1)
                .timeoutDuration(Duration.ZERO)
                .build();
                
        RateLimiter rateLimiter = RateLimiter.of("openLibraryServiceRateLimiter", config);
        
        logger.info("Production OpenLibrary API rate limiter initialized with limit of 1 request per 5 seconds");
                
        return rateLimiter;
    }
    
    /**
     * Rate limiter for Longitood API
     * - Limits requests to 100 per minute
     * - Ensures safe usage of the image service
     * 
     * @return Configured rate limiter instance
     */
    @Bean
    public RateLimiter longitoodRateLimiter() {
        io.github.resilience4j.ratelimiter.RateLimiterConfig config = io.github.resilience4j.ratelimiter.RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofMinutes(1))
                .limitForPeriod(100)
                .timeoutDuration(Duration.ZERO)
                .build();
                
        RateLimiter rateLimiter = RateLimiter.of("longitoodServiceRateLimiter", config);
        
        logger.info("Production Longitood API rate limiter initialized with limit of 100 requests per minute");
                
        return rateLimiter;
    }
}
