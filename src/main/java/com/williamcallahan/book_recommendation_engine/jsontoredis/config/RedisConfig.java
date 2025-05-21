/**
 * Redis configuration for the JSON S3 to Redis migration module
 *
 * @author William Callahan
 *
 * Features:
 * - Provides Redis connection configuration for the jsontoredis module
 * - Supports connection via environment variable URL or standard properties
 * - Parses connection details from Redis URL strings
 * - Handles password authentication with security masking in logs
 * - Implements fallback mechanisms for connection parameter errors
 * - Creates named bean with appropriate destroy method
 * - Only active when jsontoredis profile is enabled
 */
package com.williamcallahan.book_recommendation_engine.jsontoredis.config;

import redis.clients.jedis.JedisPooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Profile("jsontoredis")
@Configuration("jsonS3ToRedis_RedisConfig")
public class RedisConfig {

    private static final Logger log = LoggerFactory.getLogger(RedisConfig.class);
    private static final int DEFAULT_REDIS_PORT = 6379;

    @Value("${REDIS_SERVER:#{null}}")
    private String redisUrl;

    @Value("${spring.redis.host:localhost}")
    private String redisHost;

    @Value("${spring.redis.port:0}") // Default to 0 to allow DEFAULT_REDIS_PORT logic
    private int redisPort;

    @Value("${spring.redis.password:#{null}}")
    private String redisPassword;

    // @Value("${spring.redis.ssl:false}") // SSL not explicitly used here, but could be added if needed for Jedis
    // private boolean useSsl;

    // @Value("${spring.redis.database:0}") // JedisPooled typically handles database selection differently or via URL
    // private int redisDatabase; // This might not be directly used with JedisPooled constructor in this simple form

    /**
     * Provides a JedisPooled client for Redis access, specifically for the JSON S3 to Redis utility
     * 
     * @return A JedisPooled client configured for the Redis server
     * @throws IllegalArgumentException if the Redis URL format is invalid
     */
    @Bean(name = "jsonS3ToRedisJedisPooled", destroyMethod = "close")
    public JedisPooled jedisPooled() {
        String host = this.redisHost;
        int port = (this.redisPort == 0 ? DEFAULT_REDIS_PORT : this.redisPort);
        String password = this.redisPassword;
        // boolean ssl = this.useSsl; // If SSL is needed, JedisPooled constructor might need different handling

        // Note: REDIS_SERVER URL parsing for Jedis might be more complex if it includes SSL or specific database
        // This example prioritizes host/port/password
        if (this.redisUrl != null && !this.redisUrl.isEmpty()) {
            // Basic parsing for redis://[password@]host:port[/database]
            // This is a simplified parser. For production, a robust URI parser is recommended
            String urlToLog = this.redisUrl.replaceAll("redis(s)?://[^@]*@", "redis$1://****@");
            log.info("jsonS3ToRedis_RedisConfig: Connecting to Redis using REDIS_SERVER URL (Jedis): {}", urlToLog);
            try {
                java.net.URI uri = java.net.URI.create(this.redisUrl);
                host = uri.getHost();
                port = uri.getPort() == -1 ? DEFAULT_REDIS_PORT : uri.getPort();
                if (uri.getUserInfo() != null) {
                    String userInfoStr = uri.getUserInfo();
                    int colonIdx = userInfoStr.indexOf(':');
                    if (colonIdx != -1) { // Format is user:password or just :password
                        password = userInfoStr.substring(colonIdx + 1);
                    } else { // No colon, so the entire userInfo is the password
                        password = userInfoStr;
                    }
                }
                // SSL and database from URL would require more complex parsing or a different Jedis constructor
            } catch (IllegalArgumentException e) {
                log.error("Invalid REDIS_SERVER URL format: {}. Falling back to host/port.", this.redisUrl, e);
                // Fallback to default host/port if URL parsing fails
                host = this.redisHost;
                port = (this.redisPort == 0 ? DEFAULT_REDIS_PORT : this.redisPort);
                password = this.redisPassword;
            }
        } else {
            log.info("jsonS3ToRedis_RedisConfig: Connecting to Redis using host/port (Jedis): {}:{}", host, port);
        }

        // For Jedis 6.0.0, the constructor with HostAndPort and password might not exist
        // Trying new JedisPooled(String host, int port, String user, String password)
        // Assuming 'user' can be null for standard Redis password authentication
        if (password != null && !password.isEmpty()) {
            // It's common for Redis to not require a username, so passing null
            // If a username is required by your setup (e.g. Redis ACLs), this needs adjustment
            return new JedisPooled(host, port, null, password);
        } else {
            return new JedisPooled(host, port);
        }
    }
}
