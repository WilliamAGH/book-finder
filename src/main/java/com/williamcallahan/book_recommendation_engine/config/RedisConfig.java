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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.beans.factory.annotation.Qualifier;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.DefaultJedisClientConfig;


@Configuration
@Profile("!test") // Exclude from tests unless specifically needed
public class RedisConfig {

    private static final Logger log = LoggerFactory.getLogger(RedisConfig.class);

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

    /**
     * Creates a JedisPooled instance for Redis connections
     * 
     * @return A configured JedisPooled instance for Redis operations
     * @throws RuntimeException if Redis connection initialization fails
     */
    @Bean("jsonS3ToRedis_JedisPooled")
    @Qualifier("jsonS3ToRedis_RedisJsonService")
    public JedisPooled jedisPooled() {
        try {
            // If URL is provided, parse it
            if (redisUrl != null && !redisUrl.isEmpty()) {
                String logSafeUrl = redisUrl.replaceAll("://([^@]+)@", "://****@");
                log.info("Attempting to connect to Redis using URL: {}", logSafeUrl);
                try {
                    java.net.URI uri = java.net.URI.create(redisUrl);
                    String host = uri.getHost();
                    int port = uri.getPort();
                    boolean ssl = "rediss".equalsIgnoreCase(uri.getScheme());
                    String password = null;

                    if (uri.getUserInfo() != null) {
                        // Jedis expects password to be after a colon if user is present,
                        // or just the userInfo if no user.
                        // If format is rediss://password@host... userinfo is the password.
                        password = uri.getUserInfo();
                    } else {
                        // Check if password is embedded in a way that UserInfo is null
                        // e.g. rediss://:password@... (not typical for rediss://password@...)
                        // This part might need refinement based on exact Jedis behavior for rediss://password@host
                        // For now, we assume if UserInfo is null, no password was parsed by URI class this way.
                        // However, your URL format rediss://ACTUAL_PASSWORD@host means UserInfo IS the password.
                    }
                    
                    // If password was in UserInfo (e.g. rediss://password@host)
                    // For JedisPooled with HostAndPort and JedisClientConfig
                    if (host != null && port != -1) {
                        HostAndPort hostAndPort = new HostAndPort(host, port);
                        DefaultJedisClientConfig.Builder configBuilder = DefaultJedisClientConfig.builder();
                        if (ssl) {
                            configBuilder.ssl(true);
                        }
                        if (password != null && !password.isEmpty()) {
                            configBuilder.password(password);
                        }
                        JedisClientConfig clientConfig = configBuilder.build();
                        log.info("Connecting to Redis at {}:{} with SSL: {} and password provided: {}", host, port, ssl, (password != null && !password.isEmpty()));
                        return new JedisPooled(hostAndPort, clientConfig);
                    } else {
                        log.error("Could not parse host/port from Redis URL: {}", logSafeUrl);
                         // Fall through to host/port properties
                    }

                } catch (Exception e) {
                    log.error("Failed to parse or connect with Redis URL {}: {}. Falling back to host/port properties.",
                            logSafeUrl, e.getMessage());
                    // Fall through to host/port properties
                }
            }

            // Fall back to host/port/password/ssl properties
            log.info("Connecting to Redis using properties: Host={}, Port={}, SSL from property: {}, Password Provided={}",
                    redisHost, redisPort, this.useSsl, (this.redisPassword != null && !this.redisPassword.isEmpty()));

            HostAndPort hostAndPort = new HostAndPort(this.redisHost, this.redisPort);
            DefaultJedisClientConfig.Builder configBuilder = DefaultJedisClientConfig.builder();
            if (this.useSsl) { // Use the class field 'useSsl' read from properties
                configBuilder.ssl(true);
            }
            if (this.redisPassword != null && !this.redisPassword.isEmpty()) {
                configBuilder.password(this.redisPassword);
            }
            JedisClientConfig clientConfig = configBuilder.build();
            return new JedisPooled(hostAndPort, clientConfig);

        } catch (Exception e) {
            log.error("Failed to create Redis connection: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize Redis connection", e);
        }
    }
}
