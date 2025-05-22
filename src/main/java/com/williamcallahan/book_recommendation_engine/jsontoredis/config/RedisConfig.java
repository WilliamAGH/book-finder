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
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.DefaultJedisClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

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
    private int redisPortProperty; // Renamed to avoid conflict with local var

    @Value("${spring.redis.password:#{null}}")
    private String redisPasswordProperty;

    @Value("${spring.redis.ssl:false}")
    private boolean useSslProperty;

    @Value("${spring.redis.timeout:2000}") // Default connection and socket timeout
    private int redisTimeoutProperty;

    // @Value("${spring.redis.database:0}") // JedisPooled typically handles database selection differently or via URL
    // private int redisDatabase;

    /**
     * Provides a JedisPooled client for Redis access, specifically for the JSON S3 to Redis utility.
     * Configuration prioritizes REDIS_SERVER URL, then falls back to individual spring.redis.* properties.
     * 
     * @return A JedisPooled client configured for the Redis server
     * @throws IllegalArgumentException if the Redis URL format is invalid
     */
    @Bean(name = "jsonS3ToRedisJedisPooled", destroyMethod = "close")
    public JedisPooled jedisPooled() {
        String host = this.redisHost; // Default from properties
        int port = (this.redisPortProperty == 0 ? DEFAULT_REDIS_PORT : this.redisPortProperty); // Default from properties
        String password = this.redisPasswordProperty; // Default from properties
        boolean ssl = this.useSslProperty; // Default from properties
        int timeout = this.redisTimeoutProperty; // Default from properties

        if (this.redisUrl != null && !this.redisUrl.isEmpty()) {
            String urlToLog = this.redisUrl.replaceAll("redis(s)?://[^@]*@", "redis$1://****@");
            log.info("jsonS3ToRedis_RedisConfig: Attempting to connect to Redis using REDIS_SERVER URL (Jedis): {}", urlToLog);
            try {
                java.net.URI uri = java.net.URI.create(this.redisUrl);
                if (uri.getHost() != null) {
                    host = uri.getHost();
                    port = uri.getPort() == -1 ? DEFAULT_REDIS_PORT : uri.getPort();
                    ssl = "rediss".equalsIgnoreCase(uri.getScheme());

                    if (uri.getUserInfo() != null) {
                        String userInfoStr = uri.getUserInfo();
                        // Jedis password is the part after ':' if present, or the whole userInfo.
                        // For redis://user:pass@... or redis://:pass@...
                        int colonIdx = userInfoStr.indexOf(':');
                        if (colonIdx != -1) {
                            password = userInfoStr.substring(colonIdx + 1);
                        } else { // No colon, implies format like redis://password@... or redis://user@... (user as password)
                            password = userInfoStr;
                        }
                    } else {
                        // No user info in URL, password might still be from properties if not in URL
                        password = this.redisPasswordProperty; 
                    }
                    log.info("Parsed from URL: host={}, port={}, ssl={}, password_present={}", host, port, ssl, (password != null && !password.isEmpty()));
                } else {
                     log.warn("REDIS_SERVER URL ({}) provided but could not parse host. Falling back to properties.", urlToLog);
                }
            } catch (IllegalArgumentException e) {
                log.error("Invalid REDIS_SERVER URL format: {}. Falling back to host/port properties.", this.redisUrl, e);
                // Reset to property-based values if URL parsing failed mid-way
                host = this.redisHost;
                port = (this.redisPortProperty == 0 ? DEFAULT_REDIS_PORT : this.redisPortProperty);
                password = this.redisPasswordProperty;
                ssl = this.useSslProperty;
            }
        } else {
            log.info("jsonS3ToRedis_RedisConfig: Connecting to Redis using host/port properties (Jedis): {}:{}, ssl: {}, timeout: {}ms", host, port, ssl, timeout);
        }

        DefaultJedisClientConfig.Builder clientConfigBuilder = DefaultJedisClientConfig.builder()
                .ssl(ssl)
                .connectionTimeoutMillis(timeout)
                .socketTimeoutMillis(timeout);

        if (ssl) { // Restore custom SSL context
            try {
                SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
                sslContext.init(null, new TrustManager[]{
                        new X509TrustManager() {
                            public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                            public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                        }
                }, new SecureRandom());
                clientConfigBuilder
                        .sslSocketFactory(sslContext.getSocketFactory())
                        .hostnameVerifier((hostname, session) -> true);
            } catch (Exception e) {
                log.warn("Failed to initialize custom SSL context for Redis, proceeding with default SSL settings: {}", e.getMessage(), e);
            }
        }

        if (password != null && !password.isEmpty()) {
            clientConfigBuilder.password(password);
        }

        JedisClientConfig clientConfig = clientConfigBuilder.build();
        HostAndPort hostAndPort = new HostAndPort(host, port);
        
        log.info("Creating JedisPooled with: Host={}, Port={}, SSL={}, Timeout={}, PasswordProvided={}",
                hostAndPort.getHost(), hostAndPort.getPort(), clientConfig.isSsl(),
                clientConfig.getConnectionTimeoutMillis(), (clientConfig.getPassword() != null));

        return new JedisPooled(hostAndPort, clientConfig);
    }
}
