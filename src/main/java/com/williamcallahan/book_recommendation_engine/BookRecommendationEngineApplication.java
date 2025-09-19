/**
 * Main application class for Book Finder
 *
 * @author William Callahan
 *
 * Features:
 * - Excludes default database auto-configurations to allow conditional DB setup
 * - Enables caching for improved performance
 * - Supports asynchronous operations for non-blocking API calls
 * - Entry point for Spring Boot application
 */

package com.williamcallahan.book_recommendation_engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.ai.model.openai.autoconfigure.OpenAiAudioSpeechAutoConfiguration;
import org.springframework.ai.model.openai.autoconfigure.OpenAiAudioTranscriptionAutoConfiguration;
import org.springframework.ai.model.openai.autoconfigure.OpenAiChatAutoConfiguration;
import org.springframework.ai.model.openai.autoconfigure.OpenAiEmbeddingAutoConfiguration;
import org.springframework.ai.model.openai.autoconfigure.OpenAiImageAutoConfiguration;
import org.springframework.ai.model.openai.autoconfigure.OpenAiModerationAutoConfiguration;

@SpringBootApplication(exclude = {
    OpenAiAudioSpeechAutoConfiguration.class,
    OpenAiAudioTranscriptionAutoConfiguration.class,
    OpenAiChatAutoConfiguration.class,
    OpenAiEmbeddingAutoConfiguration.class,
    OpenAiImageAutoConfiguration.class,
    OpenAiModerationAutoConfiguration.class,
    // Disable default Spring Security auto-configuration to allow public access
    org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration.class,
    org.springframework.boot.autoconfigure.security.servlet.SecurityFilterAutoConfiguration.class,
    // Disable reactive security auto-configuration for WebFlux endpoints
    org.springframework.boot.autoconfigure.security.reactive.ReactiveSecurityAutoConfiguration.class
})
@EnableCaching
@EnableAsync
@EnableScheduling
@EnableRetry
public class BookRecommendationEngineApplication implements ApplicationRunner {

    /**
     * Main method that starts the Spring Boot application
     *
     * @param args Command line arguments passed to the application
     */
    public static void main(String[] args) {
        SpringApplication.run(BookRecommendationEngineApplication.class, args);
    }

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator bookDataOrchestrator;

    @Override
    public void run(ApplicationArguments args) {
        if (args.containsOption("migrate.s3.books")) {
            String prefix = args.containsOption("migrate.prefix") ? args.getOptionValues("migrate.prefix").get(0) : "books/v1/";
            int max = parseIntArg(args, "migrate.max", 0);
            int skip = parseIntArg(args, "migrate.skip", 0);
            if (bookDataOrchestrator != null) {
                bookDataOrchestrator.migrateBooksFromS3(prefix, max, skip);
            }
        }

        if (args.containsOption("migrate.s3.lists")) {
            String provider = args.containsOption("migrate.lists.provider") ? args.getOptionValues("migrate.lists.provider").get(0) : "NYT";
            String listPrefix = args.containsOption("migrate.lists.prefix") ? args.getOptionValues("migrate.lists.prefix").get(0) : ("lists/" + provider.toLowerCase() + "/");
            int maxLists = parseIntArg(args, "migrate.lists.max", 0);
            int skipLists = parseIntArg(args, "migrate.lists.skip", 0);
            if (bookDataOrchestrator != null) {
                bookDataOrchestrator.migrateListsFromS3(provider, listPrefix, maxLists, skipLists);
            }
        }
    }

    private int parseIntArg(ApplicationArguments args, String name, int defaultValue) {
        try {
            if (args.containsOption(name)) {
                return Integer.parseInt(args.getOptionValues(name).get(0));
            }
        } catch (Exception ignored) { }
        return defaultValue;
    }
}
