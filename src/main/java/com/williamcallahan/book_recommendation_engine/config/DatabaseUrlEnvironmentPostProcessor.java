package com.williamcallahan.book_recommendation_engine.config;

import java.util.Locale;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;

import java.util.HashMap;
import java.util.Map;

/**
 * Normalizes SPRING_DATASOURCE_URL values provided as Postgres URI (postgres://...)
 * into a JDBC URL (jdbc:postgresql://...).
 *
 * Also sets spring.datasource.username and spring.datasource.password from the URI
 * user-info if not already provided by higher-precedence sources.
 */
public final class DatabaseUrlEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    private static final String DS_URL = "spring.datasource.url";
    private static final String ENV_DS_URL = "SPRING_DATASOURCE_URL";
    private static final String DS_JDBC_URL = "spring.datasource.jdbc-url";
    private static final String HIKARI_JDBC_URL = "spring.datasource.hikari.jdbc-url";
    private static final String DS_USERNAME = "spring.datasource.username";
    private static final String DS_PASSWORD = "spring.datasource.password";
    private static final String DS_DRIVER = "spring.datasource.driver-class-name";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        String url = environment.getProperty(DS_URL);
        if (url == null || url.isBlank()) {
            // Fallback to raw env var if application.yml hasn't mapped it yet
            url = environment.getProperty(ENV_DS_URL);
        }
        if (url == null || url.isBlank()) {
            return;
        }

        String lower = url.toLowerCase(Locale.ROOT);
        if (!(lower.startsWith("postgres://") || lower.startsWith("postgresql://"))) {
            return; // already JDBC or some other supported format
        }

        try {
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
                String[] queryParts = hostPart.split("\\?", 2);
                hostPart = queryParts[0];
                query = queryParts[1];
            }

            // Split by / to separate database
            if (hostPart.contains("/")) {
                String[] dbParts = hostPart.split("/", 2);
                String hostPortPart = dbParts[0];
                database = dbParts[1];

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

            Map<String, Object> overrides = new HashMap<>();
            String jdbcUrl = jdbc.toString();
            overrides.put(DS_URL, jdbcUrl);
            // Also set commonly used aliases so Hikari picks up the normalized URL reliably
            overrides.put(DS_JDBC_URL, jdbcUrl);
            overrides.put(HIKARI_JDBC_URL, jdbcUrl);
            overrides.put(DS_DRIVER, "org.postgresql.Driver");

            // Set username and password if extracted and not already provided
            String existingUser = environment.getProperty(DS_USERNAME);
            String existingPass = environment.getProperty(DS_PASSWORD);
            if ((existingUser == null || existingUser.isBlank()) && username != null && !username.isBlank()) {
                overrides.put(DS_USERNAME, username);
            }
            if ((existingPass == null || existingPass.isBlank()) && password != null && !password.isBlank()) {
                overrides.put(DS_PASSWORD, password);
            }

            MutablePropertySources sources = environment.getPropertySources();
            // Highest precedence so these values win over application.yml
            sources.addFirst(new MapPropertySource("databaseUrlProcessor", overrides));
        } catch (RuntimeException e) {
            // Leave the value as-is; Spring will surface connection errors if invalid
        }
    }

    @Override
    public int getOrder() {
        // Run early but after default property source setup
        return Ordered.HIGHEST_PRECEDENCE;
    }
}


