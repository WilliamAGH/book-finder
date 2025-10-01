package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.config.CacheComponentsConfig;
import com.williamcallahan.book_recommendation_engine.config.SitemapProperties;
import com.williamcallahan.book_recommendation_engine.repository.SitemapRepository;
import com.williamcallahan.book_recommendation_engine.repository.SitemapRepository.BookRow;
import com.williamcallahan.book_recommendation_engine.repository.SitemapRepository.DatasetFingerprint;
import com.williamcallahan.book_recommendation_engine.repository.SitemapRepository.PageMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cache.CacheManager;
import org.springframework.cache.support.SimpleCacheManager;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SitemapServiceTest {

    @Mock
    private SitemapRepository sitemapRepository;

    private SitemapProperties sitemapProperties;

    private SitemapService sitemapService;

    @BeforeEach
    void setUp() {
        sitemapProperties = new SitemapProperties();
        CacheComponentsConfig cacheConfig = new CacheComponentsConfig();
        CacheManager cacheManager = cacheConfig.sitemapCacheManager(sitemapProperties);
        if (cacheManager instanceof SimpleCacheManager simpleCacheManager) {
            simpleCacheManager.initializeCaches();
        }
        sitemapService = new SitemapService(sitemapRepository, sitemapProperties, cacheManager);
    }

    @Test
    void getBooksForXmlPage_usesCachingAndChronologicalOrder() {
        when(sitemapRepository.fetchBooksForXml(5000, 0)).thenReturn(List.of(
                new BookRow("1", "slug-a", "Alpha", Instant.parse("2024-01-01T00:00:00Z")),
                new BookRow("2", "slug-b", "Beta", Instant.parse("2024-01-02T00:00:00Z"))
        ));

        List<SitemapService.BookSitemapItem> first = sitemapService.getBooksForXmlPage(1);
        List<SitemapService.BookSitemapItem> second = sitemapService.getBooksForXmlPage(1);

        assertThat(first).hasSize(2);
        assertThat(first).isEqualTo(second);
        assertThat(first).isSortedAccordingTo((left, right) -> left.updatedAt().compareTo(right.updatedAt()));
        verify(sitemapRepository, times(1)).fetchBooksForXml(5000, 0);
    }

    @Test
    void getBookSitemapPageMetadata_cachesRepositoryResults() {
        when(sitemapRepository.fetchBookPageMetadata(5000)).thenReturn(List.of(
                new PageMetadata(1, Instant.parse("2024-02-01T00:00:00Z"))
        ));

        List<SitemapService.SitemapPageMetadata> metadata = sitemapService.getBookSitemapPageMetadata();
        assertThat(metadata).hasSize(1);

        List<SitemapService.SitemapPageMetadata> secondCall = sitemapService.getBookSitemapPageMetadata();
        assertThat(secondCall).isEqualTo(metadata);
        verify(sitemapRepository, times(1)).fetchBookPageMetadata(5000);
    }

    @Test
    void refreshSitemapCachesIfDatasetChanged_evictsCachesOnDelta() {
        when(sitemapRepository.countAllBooks()).thenReturn(2, 3);
        when(sitemapRepository.fetchBooksForXml(5000, 0)).thenReturn(List.of(
                new BookRow("1", "slug-a", "Alpha", Instant.parse("2024-01-01T00:00:00Z"))
        ));
        when(sitemapRepository.fetchBookFingerprint()).thenReturn(
                new DatasetFingerprint(1, Instant.parse("2024-03-01T00:00:00Z")),
                new DatasetFingerprint(1, Instant.parse("2024-03-01T00:00:00Z")),
                new DatasetFingerprint(2, Instant.parse("2024-03-02T00:00:00Z"))
        );
        when(sitemapRepository.fetchAuthorFingerprint()).thenReturn(
                new DatasetFingerprint(1, Instant.parse("2024-03-01T00:00:00Z")),
                new DatasetFingerprint(1, Instant.parse("2024-03-01T00:00:00Z")),
                new DatasetFingerprint(2, Instant.parse("2024-03-02T00:00:00Z"))
        );

        sitemapService.getBooksXmlPageCount();
        sitemapService.getBooksForXmlPage(1);
        verify(sitemapRepository, times(1)).countAllBooks();

        boolean changed = sitemapService.refreshSitemapCachesIfDatasetChanged();
        assertThat(changed).isTrue();

        sitemapService.getBooksXmlPageCount();
        verify(sitemapRepository, times(2)).countAllBooks();
    }

    @Test
    void refreshSitemapCachesIfDatasetChanged_noChangeReturnsFalse() {
        when(sitemapRepository.fetchBookFingerprint()).thenReturn(
                new DatasetFingerprint(1, Instant.parse("2024-03-01T00:00:00Z")),
                new DatasetFingerprint(1, Instant.parse("2024-03-01T00:00:00Z"))
        );
        when(sitemapRepository.fetchAuthorFingerprint()).thenReturn(
                new DatasetFingerprint(1, Instant.parse("2024-03-01T00:00:00Z")),
                new DatasetFingerprint(1, Instant.parse("2024-03-01T00:00:00Z"))
        );

        // First call initializes fingerprints
        sitemapService.currentBookFingerprint();
        sitemapService.currentAuthorFingerprint();

        // Second call should detect no changes
        boolean changed = sitemapService.refreshSitemapCachesIfDatasetChanged();
        assertThat(changed).isFalse();
    }

    @Test
    void normalizeBucket_handlesValidLetters() {
        assertThat(sitemapService.normalizeBucket("a")).isEqualTo("A");
        assertThat(sitemapService.normalizeBucket("Z")).isEqualTo("Z");
        assertThat(sitemapService.normalizeBucket("m")).isEqualTo("M");
    }

    @Test
    void normalizeBucket_handlesNumbers() {
        assertThat(sitemapService.normalizeBucket("0")).isEqualTo("0-9");
        assertThat(sitemapService.normalizeBucket("5")).isEqualTo("0-9");
        assertThat(sitemapService.normalizeBucket("9")).isEqualTo("0-9");
    }

    @Test
    void normalizeBucket_handlesSpecialCharacters() {
        assertThat(sitemapService.normalizeBucket("@")).isEqualTo("0-9");
        assertThat(sitemapService.normalizeBucket("#")).isEqualTo("0-9");
    }

    @Test
    void normalizeBucket_handlesEmptyOrNull() {
        assertThat(sitemapService.normalizeBucket(null)).isEqualTo("A");
        assertThat(sitemapService.normalizeBucket("")).isEqualTo("A");
        assertThat(sitemapService.normalizeBucket("  ")).isEqualTo("A");
    }

    @Test
    void getBooksXmlPageCount_calculatesCorrectPageCount() {
        when(sitemapRepository.countAllBooks()).thenReturn(12500);
        int pageCount = sitemapService.getBooksXmlPageCount();
        assertThat(pageCount).isEqualTo(3); // 12500 / 5000 = 2.5, rounds up to 3
    }

    @Test
    void getBooksXmlPageCount_handlesZeroBooks() {
        when(sitemapRepository.countAllBooks()).thenReturn(0);
        int pageCount = sitemapService.getBooksXmlPageCount();
        assertThat(pageCount).isEqualTo(0);
    }

    @Test
    void getBooksForXmlPage_handlesInvalidPageNumber() {
        when(sitemapRepository.fetchBooksForXml(5000, 0)).thenReturn(List.of(
                new BookRow("1", "slug-a", "Alpha", Instant.parse("2024-01-01T00:00:00Z"))
        ));

        // Negative page should be normalized to 1
        List<SitemapService.BookSitemapItem> result = sitemapService.getBooksForXmlPage(-1);
        assertThat(result).isNotNull();
    }

    @Test
    void clearSitemapCaches_clearsAllCaches() {
        when(sitemapRepository.countAllBooks()).thenReturn(5, 10);

        // First call populates cache
        sitemapService.getBooksXmlPageCount();
        verify(sitemapRepository, times(1)).countAllBooks();

        // Clear caches
        sitemapService.clearSitemapCaches();

        // Second call should hit repository again
        sitemapService.getBooksXmlPageCount();
        verify(sitemapRepository, times(2)).countAllBooks();
    }
}
