/**
 * Configuration for asynchronous request handling in the Spring MVC framework
 *
 * @author William Callahan
 *
 * Features:
 * - Configures thread pool for handling asynchronous HTTP requests
 * - Sets appropriate timeout values for long-running operations
 * - Optimizes thread usage with bounded queue capacity
 * - Implements custom thread naming for easier debugging
 */

package com.williamcallahan.book_recommendation_engine.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.lang.NonNull;

import java.lang.reflect.Method;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableAsync
public class AsyncConfig implements WebMvcConfigurer, AsyncConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(AsyncConfig.class);

    /**
     * Configures asynchronous request handling for Spring MVC
     *
     * @param configurer Spring's async support configurer object
     *
     * Features:
     * - Sets default timeout to 60 seconds for async requests
     * - Assigns custom task executor with optimized thread pool
     */
    @Override
    public void configureAsyncSupport(@NonNull AsyncSupportConfigurer configurer) {
        configurer.setDefaultTimeout(60000);
        configurer.setTaskExecutor(mvcTaskExecutor());
    }

    /**
     * Creates and configures the thread pool task executor for MVC async processing
     *
     * @return Configured AsyncTaskExecutor for processing asynchronous requests
     *
     * Features:
     * - Core pool of 20 threads for handling typical load
     * - Maximum pool of 100 threads for high load periods
     * - Queue capacity of 500 tasks before rejecting new requests
     * - Descriptive thread naming pattern for monitoring
     * - Fallback to caller thread when saturated (CallerRunsPolicy)
     */
    @Bean("mvcTaskExecutor")
    public AsyncTaskExecutor mvcTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(20);
        executor.setMaxPoolSize(100);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("mvc-async-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60); // Wait up to 60 seconds for tasks to complete
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

    /**
     * Creates and configures a dedicated thread pool task executor for CPU-intensive image processing
     *
     * @return Configured AsyncTaskExecutor for image processing tasks
     *
     * Features:
     * - Core pool size based on available processors
     * - Max pool size also based on available processors (can be slightly higher for burst)
     * - Smaller queue capacity as tasks are expected to be CPU-bound and long-running
     * - Descriptive thread naming pattern for monitoring
     * - Fallback to caller thread when saturated (CallerRunsPolicy)
     */
    @Bean("imageProcessingExecutor")
    public AsyncTaskExecutor imageProcessingExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        int processors = Runtime.getRuntime().availableProcessors();
        executor.setCorePoolSize(processors > 1 ? processors : 2); // At least 2 threads
        executor.setMaxPoolSize(processors > 1 ? processors * 2 : 4); // Allow some burst
        executor.setQueueCapacity(100); // Smaller queue for CPU-bound tasks
        executor.setThreadNamePrefix("image-proc-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60); // Wait up to 60 seconds for tasks to complete
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

    /**
     * Dedicated thread pool task executor for migration tasks
     * Features:
     * - Core pool of 20 threads for migration workloads
     * - Max pool of 50 threads for high throughput
     * - Queue capacity of 500 tasks before rejecting new requests
     * - Descriptive thread naming for monitoring
     * - Fallback to caller thread when saturated (CallerRunsPolicy)
     */
    @Bean("migrationTaskExecutor")
    public AsyncTaskExecutor migrationTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(20); // Increased from 5 to 20
        executor.setMaxPoolSize(50); // Increased from 10 to 50
        executor.setQueueCapacity(500); // Increased from 200 to 500
        executor.setThreadNamePrefix("migration-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

    /**
     * Primary executor for @Async methods. Named 'taskExecutor' so Spring picks it up by default.
     *
     * Features:
     * - Core pool of 10 threads for handling typical load
     * - Maximum pool of 50 threads for high load periods
     * - Queue capacity of 100 tasks before rejecting new requests
     * - Descriptive thread naming pattern for monitoring
     * - Fallback to caller thread when saturated (CallerRunsPolicy)
     */
    @Override
    @Bean(name = "taskExecutor")
    public Executor getAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(10);
        executor.setMaxPoolSize(50);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("Async-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

    /**
     * Handles uncaught exceptions thrown from @Async void methods
     */
    @Override
    public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new AsyncUncaughtExceptionHandler() {
            @Override
            public void handleUncaughtException(@NonNull Throwable ex, @NonNull Method method, @NonNull Object... params) {
                logger.error("Uncaught async exception in method {} with params {}", method.getName(), params, ex);
            }
        };
    }
}
