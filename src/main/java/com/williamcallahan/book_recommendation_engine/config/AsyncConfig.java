package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.lang.NonNull;

/**
 * Configuration for MVC asynchronous request handling.
 */
@Configuration
public class AsyncConfig implements WebMvcConfigurer {

    @Override
    public void configureAsyncSupport(@NonNull AsyncSupportConfigurer configurer) {
        // Set default timeout (ms) to match spring.mvc.async.request-timeout
        configurer.setDefaultTimeout(60000);
        configurer.setTaskExecutor(mvcTaskExecutor());
    }

    /**
     * Task executor for MVC async processing with bounded pool.
     */
    @Bean("mvcTaskExecutor")
    public AsyncTaskExecutor mvcTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(20);
        executor.setMaxPoolSize(100);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("mvc-async-");
        executor.initialize();
        return executor;
    }
} 