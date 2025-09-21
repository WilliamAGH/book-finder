package com.williamcallahan.book_recommendation_engine.service.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksMockService;
import com.williamcallahan.book_recommendation_engine.util.SearchQueryUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.FileSystemResourceLoader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class GoogleBooksMockServiceCacheKeyTest {

    @TempDir
    Path tempDir;

    @Test
    void saveSearchResultsUsesSearchQueryUtilsCacheKey() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        GoogleBooksMockService mockService = new GoogleBooksMockService(mapper, new FileSystemResourceLoader());

        setField(mockService, "mockResponseDirectory", tempDir.toString());
        setField(mockService, "mockEnabled", true);

        List<Book> books = List.of(new Book());
        String query = "C# in Depth";

        GoogleBooksMockService.class.getDeclaredMethod("saveSearchResults", String.class, List.class)
            .invoke(mockService, query, books);

        String expectedFilename = SearchQueryUtils.cacheKey(query);
        Path expectedPath = tempDir.resolve("searches").resolve(expectedFilename);

        assertThat(Files.exists(expectedPath)).isTrue();
    }

    @Test
    void searchUsesSameCacheKeyForRetrieval() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        GoogleBooksMockService mockService = new GoogleBooksMockService(mapper, new FileSystemResourceLoader());

        setField(mockService, "mockResponseDirectory", tempDir.toString());
        setField(mockService, "mockEnabled", true);

        ObjectNode bookNode = mapper.createObjectNode();
        bookNode.put("id", "abc123");
        mockService.saveBookResponse("abc123", bookNode);

        List<Book> books = List.of(mapper.treeToValue(bookNode, Book.class));
        GoogleBooksMockService.class.getDeclaredMethod("saveSearchResults", String.class, List.class)
            .invoke(mockService, "Distributed Systems", books);

        boolean hasMockData = mockService.hasMockDataForSearch("Distributed Systems");
        assertThat(hasMockData).isTrue();

        String expectedFilename = SearchQueryUtils.cacheKey("Distributed Systems");
        Path expectedPath = tempDir.resolve("searches").resolve(expectedFilename);
        assertThat(Files.exists(expectedPath)).isTrue();
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        var field = GoogleBooksMockService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
