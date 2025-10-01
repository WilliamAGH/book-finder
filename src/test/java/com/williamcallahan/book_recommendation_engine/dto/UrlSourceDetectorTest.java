package com.williamcallahan.book_recommendation_engine.dto;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for cover source detection.
 *
 * <p>LegacyBookMapper hard-coded GOOGLE_BOOKS for many sources. The SSOT now lives in
 * {@link UrlSourceDetector}, so these tests verify the detector instead of deprecated mappers.</p>
 */
class UrlSourceDetectorTest {

    @Test
    void detectSource_returnsUndefinedForS3AndCdnUrls() {
        List<String> urls = List.of(
            "https://nyc3.digitaloceanspaces.com/mybucket/cover.jpg",
            "https://cdn.example.com/covers/book123.jpg",
            "https://mybucket.r2.dev/covers/book.jpg",
            "https://mybucket.s3.amazonaws.com/covers/book.jpg"
        );

        urls.forEach(url -> assertThat(UrlSourceDetector.detectSource(url))
            .as("Source for %s", url)
            .isEqualTo(CoverImageSource.UNDEFINED));
    }

    @Test
    void detectSource_identifiesOpenLibraryUrls() {
        BookCard card = new BookCard(
            "test-id",
            "test-slug",
            "Test Book",
            List.of("Test Author"),
            "https://covers.openlibrary.org/b/id/12345-L.jpg",
            4.5,
            100,
            Map.of()
        );

        assertThat(UrlSourceDetector.detectSource(card.coverUrl()))
            .isEqualTo(CoverImageSource.OPEN_LIBRARY);
    }

    @Test
    void detectSource_identifiesGoogleBooksUrls() {
        BookCard card = new BookCard(
            "test-id",
            "test-slug",
            "Test Book",
            List.of("Test Author"),
            "https://books.google.com/books/content?id=ABC123&printsec=frontcover",
            4.5,
            100,
            Map.of()
        );

        assertThat(UrlSourceDetector.detectSource(card.coverUrl()))
            .isEqualTo(CoverImageSource.GOOGLE_BOOKS);
    }

    @Test
    void detectSource_isCaseInsensitive() {
        BookCard card = new BookCard(
            "test-id",
            "test-slug",
            "Test Book",
            List.of("Test Author"),
            "HTTPS://COVERS.OPENLIBRARY.ORG/B/ID/12345-L.JPG",
            4.5,
            100,
            Map.of()
        );

        assertThat(UrlSourceDetector.detectSource(card.coverUrl()))
            .isEqualTo(CoverImageSource.OPEN_LIBRARY);
    }

    @Test
    void detectSource_defaultsToUndefinedForUnknown() {
        assertThat(UrlSourceDetector.detectSource("https://unknown-provider.com/cover.jpg"))
            .isEqualTo(CoverImageSource.UNDEFINED);
    }

    @Test
    void detectSource_handlesNullOrBlank() {
        assertThat(UrlSourceDetector.detectSource(null)).isEqualTo(CoverImageSource.UNDEFINED);
        assertThat(UrlSourceDetector.detectSource(" ")).isEqualTo(CoverImageSource.UNDEFINED);
    }

    @Test
    void detectSource_supportsOpenLibraryFromBookDetail() {
        BookDetail detail = new BookDetail(
            "id2",
            "slug2",
            "Title 2",
            "Description",
            "Publisher",
            null,
            "en",
            300,
            List.of("Author"),
            List.of("Category"),
            "https://covers.openlibrary.org/b/isbn/9780596520687-M.jpg",
            null,
            600,
            900,
            Boolean.TRUE,
            "OPEN_LIBRARY",
            4.0,
            50,
            null,
            null,
            "preview",
            "info",
            java.util.Collections.<String, Object>emptyMap(),
            java.util.Collections.<EditionSummary>emptyList()
        );

        assertThat(UrlSourceDetector.detectSource(detail.coverUrl()))
            .isEqualTo(CoverImageSource.OPEN_LIBRARY);
    }

    @Test
    void detectSource_supportsGoogleBooksApiUrlsFromListItem() {
        BookListItem item = new BookListItem(
            "test-id",
            "test-slug",
            "Test Book",
            "Description",
            List.of("Author"),
            List.of("Category"),
            "https://books.googleapis.com/books/content/images/frontcover/ABC?fife=w240-h345",
            4.5,
            100,
            Map.of()
        );

        assertThat(UrlSourceDetector.detectSource(item.coverUrl()))
            .isEqualTo(CoverImageSource.GOOGLE_BOOKS);
    }
}
