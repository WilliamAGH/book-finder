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

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.lang.NonNull;

@Configuration
public class AsyncConfig implements WebMvcConfigurer {

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
        executor.initialize();
        return executor;
    }

    /**
     * Creates and configures a dedicated thread pool task executor for CPU-intensive image processing.
     *
     * @return Configured AsyncTaskExecutor for image processing tasks.
     *
     * Features:
     * - Core pool size based on available processors.
     * - Max pool size also based on available processors (can be slightly higher for burst).
     * - Smaller queue capacity as tasks are expected to be CPU-bound and long-running.
     * - Descriptive thread naming pattern for monitoring.
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
        executor.initialize();
        return executor;
    }
}
