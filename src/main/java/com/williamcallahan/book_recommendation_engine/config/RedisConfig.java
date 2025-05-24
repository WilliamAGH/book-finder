/**
 * Configuration for Redis connection setup
 *
 * @author William Callahan
 *
 * Features:
 * - Configures Redis connection using Jedis client library via JedisConnectionFactory
 * - Supports connection via a Redis URL string or separate host/port properties
 * - Handles authentication with a password if provided either in the URL or as a separate property
 * - Provides secure URL logging by masking credentials in case of URI syntax errors
 * - Connection timeout is configurable
 * - SSL usage is configurable
 * - Excludes itself from 'test' profiles to prevent unwanted connections during testing by default
 */
package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;

@Configuration
@Profile("!test") // Exclude from tests unless specifically needed
public class RedisConfig {

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

    @Bean
    @Primary
    public JedisConnectionFactory jedisConnectionFactory() {
        // Use the helper method to create Redis configuration
        RedisStandaloneConfiguration redisConfig = createRedisStandaloneConfiguration();
        
        JedisClientConfiguration.JedisClientConfigurationBuilder builder = JedisClientConfiguration.builder();
        builder.connectTimeout(Duration.ofMillis(timeout));
        
        // Enable SSL if either property flag OR scheme dictates
        boolean effectiveSsl = useSsl || "rediss".equalsIgnoreCase(redisUrl != null ? URI.create(redisUrl).getScheme() : null);
        if (effectiveSsl) {
            builder.useSsl();
        }
        
        return new JedisConnectionFactory(redisConfig, builder.build());
    }

    @Bean
    public ReactiveRedisConnectionFactory reactiveRedisConnectionFactory() {
        // Create the same Redis configuration as used for Jedis
        RedisStandaloneConfiguration redisConfig = createRedisStandaloneConfiguration();
        
        LettuceClientConfiguration.LettuceClientConfigurationBuilder builder = LettuceClientConfiguration.builder();
        builder.commandTimeout(Duration.ofMillis(timeout));
        
        // Enable SSL if either property flag OR scheme dictates
        boolean effectiveSsl = useSsl || "rediss".equalsIgnoreCase(redisUrl != null ? URI.create(redisUrl).getScheme() : null);
        if (effectiveSsl) {
            builder.useSsl();
        }
        
        return new LettuceConnectionFactory(redisConfig, builder.build());
    }

    // Helper method to create Redis configuration (DRY principle)
    private RedisStandaloneConfiguration createRedisStandaloneConfiguration() {
        RedisStandaloneConfiguration redisConfig = new RedisStandaloneConfiguration();
        
        if (redisUrl != null && !redisUrl.isEmpty()) {
            try {
                // Parse URL configuration
                URI uri = new URI(redisUrl);
                redisConfig.setHostName(uri.getHost());
                redisConfig.setPort(uri.getPort() != -1 ? uri.getPort() : 6379);
                
                if (uri.getUserInfo() != null) {
                    String[] userInfo = uri.getUserInfo().split(":", 2);
                    if (userInfo.length > 1) {
                        redisConfig.setPassword(userInfo[1]);
                    }
                } else if (redisPassword != null && !redisPassword.isEmpty()) {
                    redisConfig.setPassword(redisPassword);
                }
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Invalid Redis URL: " + maskCredentials(redisUrl), e);
            }
        } else {
            // Use host/port configuration
            redisConfig.setHostName(redisHost);
            redisConfig.setPort(redisPort);
            
            if (redisPassword != null && !redisPassword.isEmpty()) {
                redisConfig.setPassword(redisPassword);
            }
        }
        
        return redisConfig;
    }
    
    // Utility method to mask credentials in logs
    private String maskCredentials(String url) {
        try {
            URI uri = new URI(url);
            if (uri.getUserInfo() != null) {
                return url.replace(uri.getUserInfo(), "******");
            }
        } catch (URISyntaxException e) {
            // Ignore, return original URL if it's malformed for URI parsing
        }
        return url;
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(JedisConnectionFactory jedisConnectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(jedisConnectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public StringRedisTemplate stringRedisTemplate(JedisConnectionFactory jedisConnectionFactory) {
        StringRedisTemplate template = new StringRedisTemplate();
        template.setConnectionFactory(jedisConnectionFactory);
        template.afterPropertiesSet();
        return template;
    }
}
