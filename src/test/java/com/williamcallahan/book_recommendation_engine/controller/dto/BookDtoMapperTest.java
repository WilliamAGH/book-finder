package com.williamcallahan.book_recommendation_engine.controller.dto;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.Book.EditionInfo;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BookDtoMapperTest {

    @Test
    void toDto_mapsBasicFields() {
        Book book = new Book();
        book.setId("fixture-id");
        book.setTitle("Fixture Title");
        book.setDescription("Fixture Description");
        book.setAuthors(List.of("Author One"));
        book.setCategories(List.of("Fiction"));
        book.setPublishedDate(new Date(1726700000000L));
        book.setLanguage("en");
        book.setPageCount(352);
        book.setPublisher("Fixture Publisher");
        book.setS3ImagePath("covers/fixture.jpg");
        book.setExternalImageUrl("https://example.test/fixture.jpg");
        book.setCoverImageWidth(600);
        book.setCoverImageHeight(900);
        book.setIsCoverHighResolution(true);
        book.setQualifiers(Map.of("nytBestseller", Map.of("year", 2024)));
        book.setCachedRecommendationIds(List.of("rec-1", "rec-2"));

        CoverImages coverImages = new CoverImages("https://cdn.test/preferred.jpg", "https://cdn.test/fallback.jpg", CoverImageSource.GOOGLE_BOOKS);
        book.setCoverImages(coverImages);

        EditionInfo editionInfo = new EditionInfo("gb-123", "HARDCOVER", "Identifier", "1234567890", "9781234567890", new Date(1726000000000L), "https://cdn.test/hardcover.jpg");
        book.setOtherEditions(List.of(editionInfo));

        BookDto dto = BookDtoMapper.toDto(book);

        assertThat(dto.id()).isEqualTo("fixture-id");
        assertThat(dto.title()).isEqualTo("Fixture Title");
        assertThat(dto.publication().language()).isEqualTo("en");
        assertThat(dto.authors()).extracting(AuthorDto::name).containsExactly("Author One");
        assertThat(dto.categories()).containsExactly("Fiction");
        assertThat(dto.cover().preferredUrl()).isEqualTo("https://cdn.test/preferred.jpg");
        assertThat(dto.cover().source()).isEqualTo(ApplicationConstants.Provider.GOOGLE_BOOKS);
        assertThat(dto.tags()).hasSize(1);
        assertThat(dto.editions()).hasSize(1);
        assertThat(dto.recommendationIds()).containsExactly("rec-1", "rec-2");
    }

    @Test
    void toDto_stripsWrappingQuotesFromPublisher() {
        Book book = new Book();
        book.setId("id-sanitized");
        book.setTitle("Quoted Publisher Book");
        book.setAuthors(List.of("Author"));
        book.setPublisher("\"O'Reilly Media, Inc.\"");

        BookDto dto = BookDtoMapper.toDto(book);

        assertThat(dto.publication().publisher()).isEqualTo("O'Reilly Media, Inc.");
    }
}
