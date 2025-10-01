package com.williamcallahan.book_recommendation_engine.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

class GoogleBooksMapperTest {

    private GoogleBooksMapper mapper;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        mapper = new GoogleBooksMapper();
        objectMapper = new ObjectMapper();
    }

    @Test
    void map_populatesCoreFieldsFromVolumeInfo() {
        JsonNode json = loadFixture("/fixtures/google-books-sample.json");

        BookAggregate aggregate = mapper.map(json);

        assertThat(aggregate).isNotNull();
        assertThat(aggregate.getTitle()).isEqualTo("The Google Story (2018 Updated Edition)");
        assertThat(aggregate.getSubtitle()).isEqualTo("Inside the Hottest Business, Media, and Technology Success of Our Time");
        assertThat(aggregate.getPublisher()).isEqualTo("Random House Publishing Group");
        assertThat(aggregate.getPageCount()).isEqualTo(384);
        assertThat(aggregate.getSlugBase()).contains("google-story");
    }

    @Test
    void map_extractsIdsAuthorsAndCategories() {
        JsonNode json = loadFixture("/fixtures/google-books-sample.json");

        BookAggregate aggregate = mapper.map(json);

        assertThat(aggregate.getIsbn13()).isEqualTo("9780440335702");
        assertThat(aggregate.getIsbn10()).isEqualTo("0440335701");
        assertThat(aggregate.getAuthors())
            .containsExactly("David A. Vise", "Mark Malseed");
        assertThat(aggregate.getCategories())
            .contains("Business & Economics / Entrepreneurship")
            .hasSize(3);
    }

    @Test
    void map_populatesExternalIdentifiersAndImages() {
        JsonNode json = loadFixture("/fixtures/google-books-sample.json");

        BookAggregate aggregate = mapper.map(json);
        BookAggregate.ExternalIdentifiers ids = aggregate.getIdentifiers();

        assertThat(ids).isNotNull();
        assertThat(ids.getSource()).isEqualTo("GOOGLE_BOOKS");
        assertThat(ids.getExternalId()).isEqualTo("zyTCAlFPjgYC");
        assertThat(ids.getImageLinks())
            .containsKeys("smallThumbnail", "thumbnail", "large");
        assertThat(ids.getAverageRating()).isEqualTo(4.0);
        assertThat(ids.getRatingsCount()).isEqualTo(4);
    }

    @Test
    void map_extractsDimensionsWhenPresent() {
        JsonNode json = loadFixture("/fixtures/google-books-sample.json");

        BookAggregate aggregate = mapper.map(json);
        BookAggregate.Dimensions dimensions = aggregate.getDimensions();

        assertThat(dimensions).isNotNull();
        assertThat(dimensions.getHeight()).isEqualTo("24.00 cm");
        assertThat(dimensions.getWidth()).isNull();
        assertThat(dimensions.getThickness()).isNull();
    }

    @Test
    void map_returnsNullWhenVolumeInfoMissing() {
        JsonNode invalid = objectMapper.createObjectNode();

        BookAggregate aggregate = mapper.map(invalid);

        assertThat(aggregate).isNull();
    }

    private JsonNode loadFixture(String path) {
        try (InputStream stream = getClass().getResourceAsStream(path)) {
            if (stream == null) {
                throw new IllegalStateException("Fixture not found: " + path);
            }
            return objectMapper.readTree(stream);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load fixture: " + path, e);
        }
    }
}
