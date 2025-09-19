package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;

import java.net.URI;
import java.net.URISyntaxException;
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
    private static final String DS_USERNAME = "spring.datasource.username";
    private static final String DS_PASSWORD = "spring.datasource.password";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        String url = environment.getProperty(DS_URL);
        if (url == null || url.isBlank()) {
            return;
        }

        String lower = url.toLowerCase();
        if (!(lower.startsWith("postgres://") || lower.startsWith("postgresql://"))) {
            return; // already JDBC or some other supported format
        }

        try {
            URI uri = new URI(url);
            String host = (uri.getHost() != null) ? uri.getHost() : "localhost";
            int port = (uri.getPort() == -1) ? 5432 : uri.getPort();
            String path = (uri.getPath() != null) ? uri.getPath() : "/";
            String database = path.startsWith("/") ? path.substring(1) : path;
            String query = uri.getQuery();

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
            overrides.put(DS_URL, jdbc.toString());

            String existingUser = environment.getProperty(DS_USERNAME);
            String existingPass = environment.getProperty(DS_PASSWORD);
            String userInfo = uri.getUserInfo();
            if (userInfo != null && !userInfo.isEmpty()) {
                int idx = userInfo.indexOf(':');
                String user = (idx >= 0) ? userInfo.substring(0, idx) : userInfo;
                String pass = (idx >= 0 && idx + 1 < userInfo.length()) ? userInfo.substring(idx + 1) : null;
                if ((existingUser == null || existingUser.isBlank()) && user != null && !user.isBlank()) {
                    overrides.put(DS_USERNAME, user);
                }
                if ((existingPass == null || existingPass.isBlank()) && pass != null && !pass.isBlank()) {
                    overrides.put(DS_PASSWORD, pass);
                }
            }

            MutablePropertySources sources = environment.getPropertySources();
            // Highest precedence so these values win over application.yml
            sources.addFirst(new MapPropertySource("databaseUrlProcessor", overrides));
        } catch (URISyntaxException e) {
            // Leave the value as-is; Spring will surface connection errors if invalid
        }
    }

    @Override
    public int getOrder() {
        // Run early but after default property source setup
        return Ordered.HIGHEST_PRECEDENCE;
    }
}


