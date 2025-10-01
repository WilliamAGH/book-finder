package com.williamcallahan.book_recommendation_engine.testutil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Utility for loading canonical Postgres fixtures used across controller and integration tests.
 */
public final class PostgresFixtures {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());
    private static final String FIXTURE_ROOT = "/fixtures/postgres/";

    private PostgresFixtures() {
    }

    /**
     * Loads the named fixture as a {@link JsonNode} tree. The fixture file must live under
     * {@code src/test/resources/fixtures/postgres} and be named {@code <name>.json}.
     */
    public static JsonNode loadNode(String name) {
        try (InputStream inputStream = openFixtureStream(name)) {
            return OBJECT_MAPPER.readTree(inputStream);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load Postgres fixture '%s'".formatted(name), e);
        }
    }

    /**
     * Loads the named fixture and maps it to the requested type using the shared {@link ObjectMapper}.
     */
    public static <T> T load(String name, Class<T> targetType) {
        try (InputStream inputStream = openFixtureStream(name)) {
            return OBJECT_MAPPER.readValue(inputStream, targetType);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to map Postgres fixture '%s' to %s".formatted(name, targetType.getSimpleName()), e);
        }
    }

    /**
     * Convenience helper for tests that just need raw JSON text.
     */
    public static String loadRawJson(String name) {
        try (InputStream inputStream = openFixtureStream(name)) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read Postgres fixture '%s'".formatted(name), e);
        }
    }

    private static InputStream openFixtureStream(String name) {
        String resourceName = name.endsWith(".json") ? name : name + ".json";
        InputStream inputStream = PostgresFixtures.class.getResourceAsStream(FIXTURE_ROOT + resourceName);
        return Objects.requireNonNull(inputStream, "Fixture '%s' not found under %s".formatted(resourceName, FIXTURE_ROOT));
    }
}
