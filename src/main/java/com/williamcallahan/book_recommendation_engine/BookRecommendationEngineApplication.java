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
        } catch (Exception e) {
            // Silently continue if .env loading fails
        }
    }

    private static void normalizeDatasourceUrlFromEnv() {
        try {
            // Check environment variable first, then system property (from .env)
            String url = System.getenv("SPRING_DATASOURCE_URL");
            if (url == null || url.isBlank()) {
                url = System.getProperty("SPRING_DATASOURCE_URL");
            }
            if (url == null || url.isBlank()) return;
            String lower = url.toLowerCase();
            if (!(lower.startsWith("postgres://") || lower.startsWith("postgresql://"))) return;

            // Manual parsing to handle postgres:// format properly
            // Format: postgres://username:password@host:port/database?params
            String withoutScheme = url.substring(url.indexOf("://") + 3);

            String username = null;
            String password = null;
            String hostPart;

            // Check if credentials are present
            if (withoutScheme.contains("@")) {
                String[] parts = withoutScheme.split("@", 2);
                String userInfo = parts[0];
                hostPart = parts[1];

                // Extract username and password
                if (userInfo.contains(":")) {
                    int colonIndex = userInfo.indexOf(":");
                    username = userInfo.substring(0, colonIndex);
                    password = userInfo.substring(colonIndex + 1);
                } else {
                    username = userInfo;
                }
            } else {
                hostPart = withoutScheme;
            }

            // Parse host, port, database, and query params
            String host;
            int port = 5432;
            String database = "postgres";
            String query = null;

            // Split by ? to separate query params
            if (hostPart.contains("?")) {
                String[] parts = hostPart.split("\\?", 2);
                hostPart = parts[0];
                query = parts[1];
            }

            // Split by / to separate database
            if (hostPart.contains("/")) {
                String[] parts = hostPart.split("/", 2);
                String hostPortPart = parts[0];
                database = parts[1];

                // Extract host and port
                if (hostPortPart.contains(":")) {
                    String[] hostPortSplit = hostPortPart.split(":", 2);
                    host = hostPortSplit[0];
                    try {
                        port = Integer.parseInt(hostPortSplit[1]);
                    } catch (NumberFormatException e) {
                        port = 5432;
                    }
                } else {
                    host = hostPortPart;
                }
            } else {
                // No database specified in URL
                if (hostPart.contains(":")) {
                    String[] hostPortSplit = hostPart.split(":", 2);
                    host = hostPortSplit[0];
                    try {
                        port = Integer.parseInt(hostPortSplit[1]);
                    } catch (NumberFormatException e) {
                        port = 5432;
                    }
                } else {
                    host = hostPart;
                }
            }

            // Build JDBC URL
            StringBuilder jdbc = new StringBuilder()
                    .append("jdbc:postgresql://")
                    .append(host)
                    .append(":")
                    .append(port)
                    .append("/")
                    .append(database);
            if (query != null && !query.isBlank()) {
                jdbc.append("?").append(query);
            }

            String jdbcUrl = jdbc.toString();
            // Set Spring + Hikari properties before the context initializes
            System.setProperty("spring.datasource.url", jdbcUrl);
            System.setProperty("spring.datasource.jdbc-url", jdbcUrl);
            System.setProperty("spring.datasource.hikari.jdbc-url", jdbcUrl);
            System.setProperty("spring.datasource.driver-class-name", "org.postgresql.Driver");

            // Set username and password if extracted and not already provided
            String existingUser = System.getenv("SPRING_DATASOURCE_USERNAME");
            if (existingUser == null || existingUser.isBlank()) {
                existingUser = System.getProperty("SPRING_DATASOURCE_USERNAME");
            }
            if (existingUser == null || existingUser.isBlank()) {
                existingUser = System.getProperty("spring.datasource.username");
            }

            String existingPass = System.getenv("SPRING_DATASOURCE_PASSWORD");
            if (existingPass == null || existingPass.isBlank()) {
                existingPass = System.getProperty("SPRING_DATASOURCE_PASSWORD");
            }
            if (existingPass == null || existingPass.isBlank()) {
                existingPass = System.getProperty("spring.datasource.password");
            }

            if ((existingUser == null || existingUser.isBlank()) && username != null && !username.isBlank()) {
                System.setProperty("spring.datasource.username", username);
            }
            if ((existingPass == null || existingPass.isBlank()) && password != null && !password.isBlank()) {
                System.setProperty("spring.datasource.password", password);
            }

            // Echo minimal confirmation to stdout (password omitted)
            String safeUrl = jdbcUrl.replaceAll("://[^@]+@", "://***:***@");
            System.out.println("[DB] Normalized SPRING_DATASOURCE_URL to JDBC: " + safeUrl);
        } catch (Exception e) {
            // If parsing fails, leave as-is; Spring will surface the connection error
            System.err.println("[DB] Failed to normalize SPRING_DATASOURCE_URL: " + e.getMessage());
        }
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
