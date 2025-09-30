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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import com.williamcallahan.book_recommendation_engine.util.S3Paths;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    org.springframework.boot.autoconfigure.security.reactive.ReactiveSecurityAutoConfiguration.class,
    // Disable SQL initialization to prevent automatic schema.sql execution
    org.springframework.boot.autoconfigure.sql.init.SqlInitializationAutoConfiguration.class
})
@EnableCaching
@EnableAsync
@EnableScheduling
@EnableRetry
public class BookRecommendationEngineApplication implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(BookRecommendationEngineApplication.class);

    private final com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator bookDataOrchestrator;

    /**
     * Constructor injection for BookDataOrchestrator
     * Required for proper CGLIB proxy support when BookDataOrchestrator has @Async methods
     *
     * @param bookDataOrchestrator The book data orchestrator service (optional, lazy-loaded)
     */
    public BookRecommendationEngineApplication(
            @org.springframework.beans.factory.annotation.Autowired(required = false)
            @org.springframework.context.annotation.Lazy
            com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator bookDataOrchestrator) {
        this.bookDataOrchestrator = bookDataOrchestrator;
    }

    /**
     * Main method that starts the Spring Boot application
     *
     * @param args Command line arguments passed to the application
     */
    public static void main(String[] args) {
        // Load .env file first
        loadDotEnvFile();
        normalizeDatasourceUrlFromEnv();
        SpringApplication.run(BookRecommendationEngineApplication.class, args);
    }

    private static void loadDotEnvFile() {
        try {
            // Check if .env file exists and load it
            java.nio.file.Path envFile = java.nio.file.Paths.get(".env");
            if (java.nio.file.Files.exists(envFile)) {
                java.util.Properties props = new java.util.Properties();
                try (java.io.InputStream is = java.nio.file.Files.newInputStream(envFile)) {
                    props.load(is);
                }
                // Set as system properties only if not already set as environment variables
                for (String key : props.stringPropertyNames()) {
                    if (System.getenv(key) == null) {
                        System.setProperty(key, props.getProperty(key));
                    }
                }
            }
        } catch (IOException | SecurityException e) {
            // Silently continue if .env loading fails
        }
    }

    private static void normalizeDatasourceUrlFromEnv() {
        try {
            // Check environment variable first, then system property (from .env)
            String url = System.getenv("SPRING_DATASOURCE_URL");
            if (ValidationUtils.isNullOrBlank(url)) {
                url = System.getProperty("SPRING_DATASOURCE_URL");
            }
            if (ValidationUtils.isNullOrBlank(url)) return;

            java.util.Optional<com.williamcallahan.book_recommendation_engine.config.DatabaseUrlEnvironmentPostProcessor.JdbcParseResult> parsed =
                com.williamcallahan.book_recommendation_engine.config.DatabaseUrlEnvironmentPostProcessor.normalizePostgresUrl(url);
            if (parsed.isEmpty()) return;

            var result = parsed.get();
            String jdbcUrl = result.jdbcUrl;
            // Set Spring + Hikari properties before the context initializes
            System.setProperty("spring.datasource.url", jdbcUrl);
            System.setProperty("spring.datasource.jdbc-url", jdbcUrl);
            System.setProperty("spring.datasource.hikari.jdbc-url", jdbcUrl);
            System.setProperty("spring.datasource.driver-class-name", "org.postgresql.Driver");

            // Set username and password if extracted and not already provided
            String existingUser = System.getenv("SPRING_DATASOURCE_USERNAME");
            if (ValidationUtils.isNullOrBlank(existingUser)) {
                existingUser = System.getProperty("SPRING_DATASOURCE_USERNAME");
            }
            if (ValidationUtils.isNullOrBlank(existingUser)) {
                existingUser = System.getProperty("spring.datasource.username");
            }

            String existingPass = System.getenv("SPRING_DATASOURCE_PASSWORD");
            if (ValidationUtils.isNullOrBlank(existingPass)) {
                existingPass = System.getProperty("SPRING_DATASOURCE_PASSWORD");
            }
            if (ValidationUtils.isNullOrBlank(existingPass)) {
                existingPass = System.getProperty("spring.datasource.password");
            }

            String decodedUser = decodeUrlComponent(result.username);
            String decodedPass = decodeUrlComponent(result.password);

            if (ValidationUtils.isNullOrBlank(existingUser) && ValidationUtils.hasText(decodedUser)) {
                System.setProperty("spring.datasource.username", decodedUser);
            }
            if (ValidationUtils.isNullOrBlank(existingPass) && ValidationUtils.hasText(decodedPass)) {
                System.setProperty("spring.datasource.password", decodedPass);
            }

            // Echo minimal confirmation to stdout (password omitted)
            String safeUrl = jdbcUrl.replaceAll("://[^@]+@", "://***:***@");
            System.out.println("[DB] Normalized SPRING_DATASOURCE_URL to JDBC: " + safeUrl);
        } catch (RuntimeException e) {
            // If parsing fails, leave as-is; Spring will surface the connection error
            System.err.println("[DB] Failed to normalize SPRING_DATASOURCE_URL: " + e.getMessage());
        }
    }

    @Override
    public void run(ApplicationArguments args) {
        if (args.containsOption("migrate.s3.books")) {
            assertOrchestratorAvailable("--migrate.s3.books");

            String rawPrefix = firstOptionValue(args, "migrate.prefix");
            String prefix = S3Paths.ensureTrailingSlash(rawPrefix);
            int max = parseIntArg(args, "migrate.max", 0);
            int skip = parseIntArg(args, "migrate.skip", 0);
            bookDataOrchestrator.migrateBooksFromS3(prefix, max, skip);
        }

        if (args.containsOption("migrate.s3.lists")) {
            assertOrchestratorAvailable("--migrate.s3.lists");

            String provider = java.util.Optional.ofNullable(firstOptionValue(args, "migrate.lists.provider"))
                    .filter(ValidationUtils::hasText)
                    .orElse("NYT");
            String defaultPrefix = "lists/" + provider.toLowerCase(Locale.ROOT) + "/";
            String rawListPrefix = firstOptionValue(args, "migrate.lists.prefix");
            String listPrefix = S3Paths.ensureTrailingSlash(rawListPrefix, defaultPrefix);
            int maxLists = parseIntArg(args, "migrate.lists.max", 0);
            int skipLists = parseIntArg(args, "migrate.lists.skip", 0);
            bookDataOrchestrator.migrateListsFromS3(provider, listPrefix, maxLists, skipLists);
        }
    }

    private void assertOrchestratorAvailable(String triggerOption) {
        if (bookDataOrchestrator == null) {
            String message = "BookDataOrchestrator bean is required for " + triggerOption + " but is not initialized.";
            log.error(message);
            throw new IllegalStateException(message);
        }
    }

    private static String decodeUrlComponent(String value) {
        if (value == null) {
            return null;
        }
        try {
            return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ex) {
            log.warn("Failed to URL-decode datasource credential component; using raw value", ex);
            return value;
        }
    }

    private String firstOptionValue(ApplicationArguments args, String name) {
        java.util.List<String> values = args.getOptionValues(name);
        return (values == null || values.isEmpty()) ? null : values.get(0);
    }

    private int parseIntArg(ApplicationArguments args, String name, int defaultValue) {
        try {
            String value = firstOptionValue(args, name);
            if (ValidationUtils.hasText(value)) {
                return Integer.parseInt(value);
            }
        } catch (Exception ignored) { }
        return defaultValue;
    }
}
