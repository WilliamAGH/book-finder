/**
 * Test for graceful shutdown handling
 * - Verifies executors are shut down on context close
 * - Verifies Redis connections are closed
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.config;

import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import redis.clients.jedis.JedisPooled;
import org.springframework.test.util.ReflectionTestUtils;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.Mock;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GracefulShutdownTest {

    @Mock
    private JedisPooled jedisPooled;

    @Mock
    private ThreadPoolTaskExecutor taskExecutor;

    @Mock
    private ThreadPoolTaskExecutor mvcTaskExecutor;

    @Mock
    private ThreadPoolTaskExecutor imageProcessingExecutor;

    @Mock
    private ApplicationContext applicationContext;

    @Test
    void testGracefulShutdown() {
        // Given
        GracefulShutdownConfig shutdownConfig = spy(new GracefulShutdownConfig());
        
        // Inject mocks using ReflectionTestUtils
        ReflectionTestUtils.setField(shutdownConfig, "applicationContext", applicationContext);
        ReflectionTestUtils.setField(shutdownConfig, "taskExecutor", taskExecutor);
        ReflectionTestUtils.setField(shutdownConfig, "mvcTaskExecutor", mvcTaskExecutor);
        ReflectionTestUtils.setField(shutdownConfig, "imageProcessingExecutor", imageProcessingExecutor);
        ReflectionTestUtils.setField(shutdownConfig, "jedisPooled", jedisPooled);
        
        // Mock executor behavior
        when(taskExecutor.getActiveCount()).thenReturn(0);
        when(mvcTaskExecutor.getActiveCount()).thenReturn(0);
        when(imageProcessingExecutor.getActiveCount()).thenReturn(0);
        
        // When
        assertFalse(GracefulShutdownConfig.isShuttingDown());
        
        ContextClosedEvent event = new ContextClosedEvent(applicationContext);
        shutdownConfig.onApplicationEvent(event);
        
        // Then
        assertTrue(GracefulShutdownConfig.isShuttingDown());
        
        // Verify executors were shut down
        verify(taskExecutor).shutdown();
        verify(mvcTaskExecutor).shutdown();
        verify(imageProcessingExecutor).shutdown();
        
        // Verify Redis was closed
        verify(jedisPooled).close();
    }
}