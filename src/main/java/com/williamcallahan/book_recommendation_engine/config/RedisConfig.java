/**
 * Redis configuration for book recommendation engine using Jedis directly
 *
 * @author William Callahan
 *
 * Features:
 * - Configures Redis connection using Jedis directly
 * - Supports both local and cloud Redis instances
 * - Handles SSL connections for production environments
 * - Optimized connection pooling for async/reactive workloads
 * - Provides JedisPooled instance for all Redis operations
 */

package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPooled;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;

@Configuration
@Profile("!test")
@Conditional(RedisEnvironmentCondition.class)
public class RedisConfig {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RedisConfig.class);

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

    @Value("${spring.redis.timeout:10000}")
    private int timeout;

    @Value("${spring.redis.jedis.pool.max-active:16}")
    private int maxActive;

    @Value("${spring.redis.jedis.pool.max-idle:8}")
    private int maxIdle;

    @Value("${spring.redis.jedis.pool.min-idle:2}")
    private int minIdle;

    @Value("${spring.redis.jedis.pool.max-wait:5000}")
    private int maxWait;

    /**
     * Creates JedisPooled instance for all Redis operations
     * Note: Shutdown is handled by GracefulShutdownConfig to ensure proper ordering
     *
     * @return Configured JedisPooled instance
     */
    @Bean
    @Primary
    public JedisPooled jedisPooled() {
        HostAndPort hostAndPort = createHostAndPort();
        DefaultJedisClientConfig clientConfig = createClientConfig();
        GenericObjectPoolConfig<Connection> poolConfig = jedisPoolConfig();
        
        logger.info("Creating JedisPooled bean: host={}, port={}, pool(maxTotal={}, maxIdle={}, minIdle={}), ssl={}, passwordProvided={}",
            hostAndPort.getHost(), hostAndPort.getPort(),
            poolConfig.getMaxTotal(), poolConfig.getMaxIdle(), poolConfig.getMinIdle(),
            clientConfig.isSsl(), clientConfig.getPassword() != null);
        
        // Create the pooled client and validate connectivity with a ping
        JedisPooled jedis = new JedisPooled(hostAndPort, clientConfig, poolConfig);
        try {
            String pong = jedis.ping();
            logger.info("Redis ping successful on startup: {}", pong);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to ping Redis during startup: " + e.getMessage(), e);
        }
        return jedis;
    }

    private HostAndPort createHostAndPort() {
        if (redisUrl != null && !redisUrl.isEmpty()) {
            try {
                URI uri = new URI(redisUrl);
                return new HostAndPort(uri.getHost(), uri.getPort() != -1 ? uri.getPort() : 6379);
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Invalid Redis URL: " + maskCredentials(redisUrl), e);
            }
        } else {
            return new HostAndPort(redisHost, redisPort);
        }
    }

    private DefaultJedisClientConfig createClientConfig() {
        DefaultJedisClientConfig.Builder builder = DefaultJedisClientConfig.builder()
            .connectionTimeoutMillis(timeout)
            .socketTimeoutMillis(timeout);

        // Handle password from URL or configuration
        String password = extractPassword();
        if (password != null && !password.isEmpty()) {
            builder.password(password);
        }

        // Handle SSL
        boolean effectiveSsl = useSsl || (redisUrl != null && redisUrl.startsWith("rediss://"));
        if (effectiveSsl) {
            builder.ssl(true);
        }

        return builder.build();
    }

    private String extractPassword() {
        if (redisUrl != null && !redisUrl.isEmpty()) {
            try {
                URI uri = new URI(redisUrl);
                if (uri.getUserInfo() != null) {
                    String[] userInfo = uri.getUserInfo().split(":", 2);
                    if (userInfo.length > 1) {
                        return userInfo[1];
                    }
                }
            } catch (URISyntaxException e) {
                logger.warn("Failed to parse Redis URL for password extraction: {}", e.getMessage());
            }
        }
        return redisPassword;
    }

    private GenericObjectPoolConfig<Connection> jedisPoolConfig() {
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        // Pool sizing from application.yml configuration
        poolConfig.setMaxTotal(maxActive);
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMinIdle(minIdle);
        // Connection validation for reliability
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        // Async-friendly blocking behavior
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWait(Duration.ofMillis(maxWait)); // Timeout from application.yml
        // Optimized eviction for connection stability with async workloads
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(60000)); // Reduced frequency from 30s to 60s
        poolConfig.setMinEvictableIdleDuration(Duration.ofMillis(120000)); // Increased from 60s to 120s
        poolConfig.setNumTestsPerEvictionRun(3); // Limit tests per eviction run for performance
        return poolConfig;
    }
    
    private String maskCredentials(String url) {
        try {
            URI uri = new URI(url);
            if (uri.getUserInfo() != null) {
                return url.replace(uri.getUserInfo(), "******");
            }
        } catch (URISyntaxException e) {
            // Ignore
        }
        return url;
    }
}
