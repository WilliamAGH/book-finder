package com.williamcallahan.book_recommendation_engine.service.cache;

import com.williamcallahan.book_recommendation_engine.testutil.BookApiProxyFixtures;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookApiProxy;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksMockService;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.S3StorageService;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.util.SearchQueryUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class BookApiProxyCacheKeyTest {

    @TempDir
    Path tempDir;

    @Test
    void saveSearchUsesSearchQueryUtilsCacheKey() {
GoogleBooksService googleBooksService = mock(GoogleBooksService.class);
        S3StorageService s3StorageService = mock(S3StorageService.class);
        Optional<GoogleBooksMockService> mockService = Optional.empty();
        BookDataOrchestrator bookDataOrchestrator = mock(BookDataOrchestrator.class);

        BookApiProxy proxy = BookApiProxyFixtures.newProxy(
                googleBooksService,
                s3StorageService,
                mockService,
                bookDataOrchestrator,
                tempDir,
                true
        );

        String query = "C# in Depth";
        String lang = "en";
        List<Book> results = List.of(new Book());

        ReflectionTestUtils.invokeMethod(proxy, "saveSearchToLocalCache", query, lang, results);

        String expectedFilename = SearchQueryUtils.cacheKey(query, lang);
        Path expectedPath = tempDir.resolve("searches").resolve(expectedFilename);

        assertThat(Files.exists(expectedPath)).isTrue();
    }

    @Test
    void getSearchRespectsCacheKeyFormat() {
GoogleBooksService googleBooksService = mock(GoogleBooksService.class);
        S3StorageService s3StorageService = mock(S3StorageService.class);
        Optional<GoogleBooksMockService> mockService = Optional.empty();
        BookDataOrchestrator bookDataOrchestrator = mock(BookDataOrchestrator.class);

        BookApiProxy proxy = BookApiProxyFixtures.newProxy(
                googleBooksService,
                s3StorageService,
                mockService,
                bookDataOrchestrator,
                tempDir,
                true
        );

        String query = "Refactoring: Improving the Design of Existing Code";
        String lang = " ";
        List<Book> results = List.of(new Book());

        ReflectionTestUtils.invokeMethod(proxy, "saveSearchToLocalCache", query, lang, results);

        @SuppressWarnings("unchecked")
        List<Book> retrieved = (List<Book>) ReflectionTestUtils.invokeMethod(proxy, "getSearchFromLocalCache", query, lang);

        assertThat(retrieved).isNotNull();
        assertThat(retrieved).hasSize(1);

        String expectedFilename = SearchQueryUtils.cacheKey(query, lang);
        Path expectedPath = tempDir.resolve("searches").resolve(expectedFilename);
        assertThat(Files.exists(expectedPath)).isTrue();
    }
}
