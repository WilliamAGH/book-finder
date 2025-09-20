package com.williamcallahan.book_recommendation_engine.testutil;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresFixturesTest {

    @Test
    void loadNode_returnsFixtureTree() {
        JsonNode fixture = PostgresFixtures.loadNode("book_with_collections_and_tags");
        assertThat(fixture.get("book").get("title").asText()).isEqualTo("Fixture Book of Secrets");
        assertThat(fixture.get("collections")).isNotNull();
        assertThat(fixture.get("tags")).isNotNull();
    }

    @Test
    void loadRawJson_returnsFixtureText() {
        String raw = PostgresFixtures.loadRawJson("book_with_collections_and_tags");
        assertThat(raw).contains("Fixture Book of Secrets");
    }
}
