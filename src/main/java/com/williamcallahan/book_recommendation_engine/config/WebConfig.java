package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.nio.file.Path;
import java.nio.file.Paths;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Value("${app.cover-cache.dir:covers}")
    private String coverCacheDirName; // This should match the directory name used in URLs, e.g., "covers"

    @Override
    public void addResourceHandlers(@NonNull ResourceHandlerRegistry registry) {
        // Resolve the absolute path to the cache directory.
        // This assumes 'coverCacheDirName' is a relative path from the application's working directory.
        Path cachePath = Paths.get(coverCacheDirName).toAbsolutePath();
        String resourceLocation = "file:" + cachePath.toString() + "/";

        // Serve files from the dynamically created cache directory.
        // For example, if coverCacheDirName is "covers", this handles "/covers/**".
        registry.addResourceHandler("/" + coverCacheDirName + "/**")
                .addResourceLocations(resourceLocation)
                .setCachePeriod(3600 * 24 * 30); // Cache for 30 days in browser

        // It's good practice to also explicitly register the default static resource handlers
        // if you're adding custom ones, to ensure they continue to work as expected.
        // Spring Boot's auto-configuration usually handles these, but being explicit can prevent surprises.
        if (!registry.hasMappingForPattern("/webjars/**")) {
            registry.addResourceHandler("/webjars/**")
                    .addResourceLocations("classpath:/META-INF/resources/webjars/");
        }
        if (!registry.hasMappingForPattern("/**")) {
            registry.addResourceHandler("/**")
                    .addResourceLocations("classpath:/META-INF/resources/",
                                        "classpath:/resources/",
                                        "classpath:/static/",
                                        "classpath:/public/");
        }
    }
} 