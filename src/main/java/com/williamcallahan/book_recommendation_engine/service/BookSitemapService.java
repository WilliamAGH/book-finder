package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.config.SitemapProperties;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.BookSitemapItem;
import com.williamcallahan.book_recommendation_engine.service.S3StorageService;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Builds sitemap snapshots from Postgres and synchronises supporting artefacts (S3 JSON, cover probes, API warmups).
 */
@Service
public class BookSitemapService {

    private static final Logger logger = LoggerFactory.getLogger(BookSitemapService.class);

    private final SitemapService sitemapService;
    private final SitemapProperties sitemapProperties;
    private final ObjectMapper objectMapper;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final S3StorageService s3StorageService;

    @Autowired
    public BookSitemapService(SitemapService sitemapService,
                              SitemapProperties sitemapProperties,
                              ObjectMapper objectMapper,
                              @Autowired(required = false) BookDataOrchestrator bookDataOrchestrator,
                              @Autowired(required = false) S3StorageService s3StorageService) {
        this.sitemapService = sitemapService;
        this.sitemapProperties = sitemapProperties;
        this.objectMapper = objectMapper;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.s3StorageService = s3StorageService;
    }

    @PostConstruct
    void validateConfiguration() {
        if (s3StorageService != null) {
            String s3Key = sitemapProperties.getS3AccumulatedIdsKey();
            if (s3Key == null || s3Key.isBlank()) {
                throw new IllegalStateException("BookSitemapService requires sitemap.s3.accumulated-ids-key when S3 storage is enabled.");
            }
        }
    }

    public SnapshotSyncResult synchronizeSnapshot() {
        SitemapSnapshot snapshot = buildSnapshot();
        boolean uploaded = uploadSnapshot(snapshot);
        return new SnapshotSyncResult(snapshot, uploaded, sitemapProperties.getS3AccumulatedIdsKey());
    }

    public SitemapSnapshot buildSnapshot() {
        int pageCount = sitemapService.getBooksXmlPageCount();
        if (pageCount == 0) {
            logger.info("Sitemap snapshot build found no book pages to process.");
            return new SitemapSnapshot(Instant.now(), Collections.emptyList());
        }

        List<BookSitemapItem> aggregated = new ArrayList<>();
        for (int page = 1; page <= pageCount; page++) {
            List<BookSitemapItem> pageItems = sitemapService.getBooksForXmlPage(page);
            if (pageItems.isEmpty()) {
                continue;
            }
            aggregated.addAll(pageItems);
        }

        logger.info("Built sitemap snapshot with {} book entries across {} XML pages.", aggregated.size(), pageCount);
        return new SitemapSnapshot(Instant.now(), Collections.unmodifiableList(aggregated));
    }

    public boolean uploadSnapshot(SitemapSnapshot snapshot) {
        if (snapshot.books().isEmpty()) {
            logger.info("Skipping sitemap snapshot upload because no books were harvested.");
            return false;
        }
        if (s3StorageService == null) {
            logger.info("Skipping sitemap snapshot upload because S3 storage service is not configured.");
            return false;
        }
        String s3Key = sitemapProperties.getS3AccumulatedIdsKey();
        if (s3Key == null || s3Key.isBlank()) {
            logger.warn("Sitemap snapshot upload skipped – no S3 key configured (sitemap.s3.accumulated-ids-key).");
            return false;
        }

        try {
            String payload = buildSnapshotPayload(snapshot);
            s3StorageService.uploadGenericJsonAsync(s3Key, payload, true).join();
            logger.info("Uploaded sitemap snapshot ({} entries) to S3 key '{}'.", snapshot.books().size(), s3Key);
            return true;
        } catch (Exception e) {
            logger.error("Failed to upload sitemap snapshot to S3 key '{}': {}", s3Key, e.getMessage(), e);
            return false;
        }
    }

    public ExternalHydrationSummary hydrateExternally(List<BookSitemapItem> items, int limit) {
        if (items == null || items.isEmpty() || limit <= 0) {
            return new ExternalHydrationSummary(0, 0, 0, 0);
        }
        if (bookDataOrchestrator == null) {
            logger.debug("Skipping external hydration – BookDataOrchestrator not available.");
            return new ExternalHydrationSummary(limit, 0, 0, 0);
        }

        List<BookSitemapItem> slice = items.stream()
                .filter(item -> item.bookId() != null && !item.bookId().isBlank())
                .limit(limit)
                .collect(Collectors.toList());
        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();

        for (BookSitemapItem item : slice) {
            try {
                Mono<Book> mono = bookDataOrchestrator.getBookByIdTiered(item.bookId());
                boolean present = mono.timeout(Duration.ofSeconds(15))
                        .blockOptional(Duration.ofSeconds(20))
                        .isPresent();
                if (present) {
                    succeeded.incrementAndGet();
                } else {
                    failures.incrementAndGet();
                }
            } catch (Exception e) {
                failures.incrementAndGet();
                logger.debug("Hydration attempt failed for book {}: {}", item.bookId(), e.getMessage());
            }
        }

        return new ExternalHydrationSummary(limit, slice.size(), succeeded.get(), failures.get());
    }

    private String buildSnapshotPayload(SitemapSnapshot snapshot) throws JsonProcessingException {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("generatedAt", snapshot.generatedAt().toString());
        root.put("totalBooks", snapshot.books().size());
        ArrayNode items = root.putArray("books");
        snapshot.books().forEach(book -> {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("id", book.bookId());
            node.put("slug", book.slug());
            node.put("title", book.title());
            if (book.updatedAt() != null) {
                node.put("updatedAt", book.updatedAt().toString());
            }
            items.add(node);
        });
        return objectMapper.writeValueAsString(root);
    }

    public record SitemapSnapshot(Instant generatedAt, List<BookSitemapItem> books) {}

    public record SnapshotSyncResult(SitemapSnapshot snapshot, boolean uploaded, String s3Key) {}

    public record ExternalHydrationSummary(int requested, int attempted, int succeeded, int failed) {}
}
