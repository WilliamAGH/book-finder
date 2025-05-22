/**
 * Configuration for Redis connection setup for JSON data migration
 *
 * @author William Callahan
 *
 * Features:
 * - Configures Redis connection using Jedis client library
 * - Supports connection via URL string or host/port configuration
 * - Handles authentication with password when provided
 * - Provides secure URL logging with credential masking
 * - Implements connection error handling with descriptive messages
 * - Creates a qualified bean for use with jsonS3ToRedis module
 * - Excludes itself from test profiles to prevent unwanted connections
 */
package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;


@Configuration
@Profile("!test") // Exclude from tests unless specifically needed
public class RedisConfig {

    // private static final Logger log = LoggerFactory.getLogger(RedisConfig.class); // Unused

    @Value("${spring.redis.host:localhost}")
    private String redisHost;

    @Value("${spring.redis.port:6379}")
    private int redisPort;

    @Value("${spring.redis.password:#{null}}")
    private String redisPassword;

    @Value("${REDIS_SERVER:#{null}}")
    private String redisUrl;

    @Value("${spring.redis.ssl:false}")
    private boolean useSsl;

    @Value("${spring.redis.timeout:2000}")
    private int timeout;

}
