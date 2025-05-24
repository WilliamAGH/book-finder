/**
 * Configuration for graceful application shutdown handling
 *
 * @author William Callahan
 *
 * Features:
 * - Handles application shutdown events to prevent connection pool race conditions
 * - Ensures proper cleanup of resources during Spring Boot termination
 * - Coordinates with Redis and S3 services for graceful shutdown
 */

package com.williamcallahan.book_recommendation_engine.config;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import redis.clients.jedis.JedisPooled;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class GracefulShutdownConfig implements ApplicationListener<ContextClosedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(GracefulShutdownConfig.class);
    private static final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
    private static final CountDownLatch executorShutdownLatch = new CountDownLatch(1);
    
    @Autowired(required = false)
    @Qualifier("taskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;
    
    @Autowired(required = false)
    @Qualifier("mvcTaskExecutor")
    private ThreadPoolTaskExecutor mvcTaskExecutor;
    
    @Autowired(required = false)
    @Qualifier("imageProcessingExecutor")
    private ThreadPoolTaskExecutor imageProcessingExecutor;
    
    @Autowired(required = false)
    private JedisPooled jedisPooled;
    
    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Check if shutdown has been initiated
     */
    public static boolean isShuttingDown() {
        return shutdownInitiated.get();
    }
    
    /**
     * Wait for executors to shutdown before closing Redis
     */
    public static void waitForExecutorShutdown(long timeout, TimeUnit unit) throws InterruptedException {
        executorShutdownLatch.await(timeout, unit);
    }

    /**
     * Handles the application shutdown event
     * Coordinates graceful shutdown of services to prevent connection pool failures
     *
     * @param event Spring context closed event
     */
    @Override
    public void onApplicationEvent(@NonNull ContextClosedEvent event) {
        // Only process if this is our application context
        if (event.getApplicationContext() != applicationContext) {
            return;
        }
        
        if (shutdownInitiated.compareAndSet(false, true)) {
            logger.info("Application shutdown event received - initiating graceful shutdown");
            
            // Phase 1: Stop accepting new work
            logger.info("Phase 1: Stopping acceptance of new work");
            stopAcceptingWork();
            
            // Phase 2: Wait for async executors to complete
            logger.info("Phase 2: Waiting for async executors to complete tasks");
            waitForExecutorsToComplete();
            
            // Phase 3: Signal that executors are done
            executorShutdownLatch.countDown();
            
            // Phase 4: Close Redis connections gracefully
            logger.info("Phase 3: Closing Redis connections");
            closeRedisConnections();
            
            logger.info("Graceful shutdown completed successfully");
        }
    }
    
    /**
     * Stop accepting new work on all executors
     */
    private void stopAcceptingWork() {
        try {
            if (taskExecutor != null) {
                taskExecutor.shutdown();
                logger.debug("Task executor shutdown initiated");
            }
            if (mvcTaskExecutor != null) {
                mvcTaskExecutor.shutdown();
                logger.debug("MVC task executor shutdown initiated");
            }
            if (imageProcessingExecutor != null) {
                imageProcessingExecutor.shutdown();
                logger.debug("Image processing executor shutdown initiated");
            }
        } catch (Exception e) {
            logger.error("Error stopping executor acceptance of new work", e);
        }
    }
    
    /**
     * Wait for all executors to complete their tasks
     */
    private void waitForExecutorsToComplete() {
        long startTime = System.currentTimeMillis();
        
        // Monitor executor states
        boolean allTerminated = false;
        long timeout = 30000; // 30 seconds total timeout
        
        while (!allTerminated && (System.currentTimeMillis() - startTime) < timeout) {
            allTerminated = true;
            
            if (taskExecutor != null) {
                int active = taskExecutor.getActiveCount();
                int queued = 0;
                if (taskExecutor.getThreadPoolExecutor() != null) {
                    queued = taskExecutor.getThreadPoolExecutor().getQueue().size();
                }
                if (active > 0 || queued > 0) {
                    logger.info("Task executor: {} active, {} queued", active, queued);
                    allTerminated = false;
                }
            }
            
            if (mvcTaskExecutor != null) {
                int active = mvcTaskExecutor.getActiveCount();
                int queued = 0;
                if (mvcTaskExecutor.getThreadPoolExecutor() != null) {
                    queued = mvcTaskExecutor.getThreadPoolExecutor().getQueue().size();
                }
                if (active > 0 || queued > 0) {
                    logger.info("MVC executor: {} active, {} queued", active, queued);
                    allTerminated = false;
                }
            }
            
            if (imageProcessingExecutor != null) {
                int active = imageProcessingExecutor.getActiveCount();
                int queued = 0;
                if (imageProcessingExecutor.getThreadPoolExecutor() != null) {
                    queued = imageProcessingExecutor.getThreadPoolExecutor().getQueue().size();
                }
                if (active > 0 || queued > 0) {
                    logger.info("Image executor: {} active, {} queued", active, queued);
                    allTerminated = false;
                }
            }
            
            if (!allTerminated) {
                try {
                    Thread.sleep(1000); // Check every second
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Interrupted while waiting for executors", e);
                    break;
                }
            }
        }
        
        if (!allTerminated) {
            logger.warn("Executors did not complete within timeout, proceeding with shutdown");
        } else {
            logger.info("All executors completed successfully");
        }
    }
    
    /**
     * Close Redis connections gracefully
     */
    private void closeRedisConnections() {
        try {
            if (jedisPooled != null) {
                logger.info("Closing Redis connection pool");
                jedisPooled.close();
                logger.info("Redis connection pool closed successfully");
            }
        } catch (Exception e) {
            logger.error("Error closing Redis connections", e);
        }
    }
    
    /**
     * Pre-destroy hook for additional cleanup
     */
    @PreDestroy
    public void preDestroy() {
        logger.debug("Pre-destroy hook called");
    }
}