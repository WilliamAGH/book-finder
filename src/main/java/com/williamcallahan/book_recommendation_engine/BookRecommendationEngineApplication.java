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
            String url = firstText(
                System.getenv("SPRING_DATASOURCE_URL"),
                System.getProperty("SPRING_DATASOURCE_URL")
            );
            if (!ValidationUtils.hasText(url)) {
                return;
            }

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
            String existingUser = firstText(
                System.getenv("SPRING_DATASOURCE_USERNAME"),
                System.getProperty("SPRING_DATASOURCE_USERNAME"),
                System.getProperty("spring.datasource.username")
            );

            String existingPass = firstText(
                System.getenv("SPRING_DATASOURCE_PASSWORD"),
                System.getProperty("SPRING_DATASOURCE_PASSWORD"),
                System.getProperty("spring.datasource.password")
            );

            String decodedUser = decodeUrlComponent(result.username);
            String decodedPass = decodeUrlComponent(result.password);

            if (!ValidationUtils.hasText(existingUser) && ValidationUtils.hasText(decodedUser)) {
                System.setProperty("spring.datasource.username", decodedUser);
            }
            if (!ValidationUtils.hasText(existingPass) && ValidationUtils.hasText(decodedPass)) {
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

    private static String firstText(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (ValidationUtils.hasText(value)) {
                return value;
            }
        }
        return null;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (args.containsOption("migrate.s3.books")) {
            log.error("--migrate.s3.books has been removed. Run the manual SQL migration instead (see AGENTS.md manual)." );
            throw new IllegalStateException("S3-backed book migrations are no longer automated; run manual SQL steps instead.");
        }

        if (args.containsOption("migrate.s3.lists")) {
            log.error("--migrate.s3.lists has been removed. Run the manual SQL migration instead (see AGENTS.md manual)." );
            throw new IllegalStateException("S3-backed list migrations are no longer automated; run manual SQL steps instead.");
        }
    }

    private static String decodeUrlComponent(String value) {
        if (value == null) {
            return null;
        }
        // Only attempt URL decoding if the value contains percent-encoded sequences
        // This prevents corruption of passwords with literal '+' characters
        if (!value.contains("%")) {
            return value;
        }
        try {
            // Pre-escape literal '+' characters to preserve them during decoding
            // URLDecoder treats '+' as space, but in passwords it should be literal
            String prepared = value.replace("+", "%2B");
            return java.net.URLDecoder.decode(prepared, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException ex) {
            // Bail out to original value if decoding fails (malformed % sequences)
            log.warn("Failed to URL-decode datasource credential component (malformed encoding); using raw value", ex);
            return value;
        }
    }

}
