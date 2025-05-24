/**
 * Custom Spring Boot application configuration for testing
 *
 * @author William Callahan
 *
 * Features:
 * - Disables JPA repositories auto-configuration to prevent conflicts
 * - Provides clean test environment for Redis-based repositories
 * - Enables component scanning for main application packages
 * - Allows tests to provide specific mocks and configurations
 */

package com.williamcallahan.book_recommendation_engine.test.config;

import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.AsyncTaskExecutor;
import redis.clients.jedis.JedisPooled;
@SpringBootApplication(exclude = JpaRepositoriesAutoConfiguration.class)
@ComponentScan(basePackages = "com.williamcallahan.book_recommendation_engine")
public class TestApplicationConfig {

    @Bean("mvcTaskExecutor")
    public AsyncTaskExecutor mvcTaskExecutor() {
        return Mockito.mock(AsyncTaskExecutor.class);
    }

    @Bean("imageProcessingExecutor")
    public AsyncTaskExecutor imageProcessingExecutor() {
        return Mockito.mock(AsyncTaskExecutor.class);
    }

    @Bean
    @Primary
    public JedisPooled jedisPooled() {
        JedisPooled mockJedisPooled = Mockito.mock(JedisPooled.class);
        Mockito.when(mockJedisPooled.ping()).thenReturn("PONG");
        // Mock common Redis operations
        Mockito.when(mockJedisPooled.get(Mockito.anyString())).thenReturn(null);
        Mockito.when(mockJedisPooled.set(Mockito.anyString(), Mockito.anyString())).thenReturn("OK");
        Mockito.when(mockJedisPooled.del(Mockito.anyString())).thenReturn(1L);
        Mockito.when(mockJedisPooled.exists(Mockito.anyString())).thenReturn(false);
        return mockJedisPooled;
    }
}
