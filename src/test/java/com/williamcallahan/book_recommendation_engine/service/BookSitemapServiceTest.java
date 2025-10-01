package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.config.SitemapProperties;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.BookSitemapItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BookSitemapServiceTest {

    @Mock
    private SitemapService sitemapService;

    @Mock
    private BookDataOrchestrator bookDataOrchestrator;

    @Mock
    private S3StorageService s3StorageService;

    private SitemapProperties sitemapProperties;

    private BookSitemapService bookSitemapService;

    @BeforeEach
    void setUp() {
        sitemapProperties = new SitemapProperties();
        sitemapProperties.setS3AccumulatedIdsKey("sitemaps/books.json");
        sitemapProperties.setSchedulerCoverSampleSize(5);
        sitemapProperties.setSchedulerExternalHydrationSize(3);
        bookSitemapService = new BookSitemapService(
                sitemapService,
                sitemapProperties,
                new ObjectMapper(),
                bookDataOrchestrator,
                s3StorageService
        );
    }

    @Test
    void synchronizeSnapshot_uploadsBooksToS3() throws Exception {
        when(sitemapService.getBooksXmlPageCount()).thenReturn(1);
        BookSitemapItem item = new BookSitemapItem("book-1", "slug-1", "Title", Instant.parse("2024-01-01T00:00:00Z"));
        when(sitemapService.getBooksForXmlPage(1)).thenReturn(List.of(item));
        when(s3StorageService.uploadFileAsync(any(), any(), anyLong(), eq("application/json")))
                .thenReturn(CompletableFuture.completedFuture("https://example.com/sitemaps/books.json"));

        BookSitemapService.SnapshotSyncResult result = bookSitemapService.synchronizeSnapshot();

        assertThat(result.uploaded()).isTrue();
        assertThat(result.snapshot().books()).containsExactly(item);

        ArgumentCaptor<java.io.InputStream> streamCaptor = ArgumentCaptor.forClass(java.io.InputStream.class);
        ArgumentCaptor<Long> lengthCaptor = ArgumentCaptor.forClass(Long.class);
        verify(s3StorageService).uploadFileAsync(eq("sitemaps/books.json"), streamCaptor.capture(), lengthCaptor.capture(), eq("application/json"));
        byte[] bytes;
        try (java.io.InputStream captured = streamCaptor.getValue()) {
            bytes = captured.readAllBytes();
        }
        assertThat(lengthCaptor.getValue()).isEqualTo((long) bytes.length);
        String payload = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        assertThat(payload).contains("\"slug\":\"slug-1\"");

        JsonNode root = new ObjectMapper().readTree(payload);
        assertThat(root.size()).isEqualTo(3);
        assertThat(root.get("totalBooks").asInt()).isEqualTo(1);
        assertThat(root.get("generatedAt").asText()).isNotBlank();
        JsonNode booksNode = root.get("books");
        assertThat(booksNode).isNotNull();
        assertThat(booksNode.isArray()).isTrue();
        assertThat(booksNode.size()).isEqualTo(1);

        JsonNode first = booksNode.get(0);
        assertThat(first.get("id").asText()).isEqualTo("book-1");
        assertThat(first.get("slug").asText()).isEqualTo("slug-1");
        assertThat(first.get("title").asText()).isEqualTo("Title");
        assertThat(first.get("updatedAt").asText()).isEqualTo("2024-01-01T00:00:00Z");
        assertThat(collectFieldNames(first)).containsExactlyInAnyOrder("id", "slug", "title", "updatedAt");
    }

    private Set<String> collectFieldNames(JsonNode node) {
        Set<String> names = new HashSet<>();
        Iterator<String> iterator = node.fieldNames();
        while (iterator.hasNext()) {
            names.add(iterator.next());
        }
        return names;
    }
}
