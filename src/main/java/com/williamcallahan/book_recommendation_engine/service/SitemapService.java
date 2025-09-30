package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.config.SitemapProperties;
import com.williamcallahan.book_recommendation_engine.repository.SitemapRepository;
import com.williamcallahan.book_recommendation_engine.repository.SitemapRepository.AuthorRow;
import com.williamcallahan.book_recommendation_engine.repository.SitemapRepository.BookRow;
import com.williamcallahan.book_recommendation_engine.repository.SitemapRepository.DatasetFingerprint;
import com.williamcallahan.book_recommendation_engine.repository.SitemapRepository.PageMetadata;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Coordinates Postgres-backed sitemap data access for both HTML and XML rendering.
 */
@Service
public class SitemapService {

    public static final List<String> LETTER_BUCKETS;

    static {
        List<String> letters = IntStream.rangeClosed('A', 'Z')
                .mapToObj(c -> String.valueOf((char) c))
                .collect(Collectors.toCollection(ArrayList::new));
        letters.add("0-9");
        LETTER_BUCKETS = Collections.unmodifiableList(letters);
    }

    private final SitemapRepository sitemapRepository;
    private final SitemapProperties properties;
    private final Cache booksXmlPageCountCache;
    private final Cache booksXmlPageCache;
    private final Cache overviewCache;
    private final Cache bookBucketCountsCache;
    private final Cache authorBucketCountsCache;
    private final Cache authorListingDescriptorsCache;
    private final Cache authorXmlPageCountCache;
    private final Cache authorXmlPageCache;
    private final Cache bookPageMetadataCache;
    private final Cache authorPageMetadataCache;
    private final AtomicReference<DatasetFingerprint> bookFingerprintRef = new AtomicReference<>();
    private final AtomicReference<DatasetFingerprint> authorFingerprintRef = new AtomicReference<>();

    public SitemapService(SitemapRepository sitemapRepository,
                          SitemapProperties properties,
                          @Qualifier("sitemapCacheManager") CacheManager cacheManager) {
        this.sitemapRepository = sitemapRepository;
        this.properties = properties;
        this.booksXmlPageCountCache = requireCache(cacheManager, CacheNames.BOOK_XML_PAGE_COUNT);
        this.booksXmlPageCache = requireCache(cacheManager, CacheNames.BOOK_XML_PAGE);
        this.overviewCache = requireCache(cacheManager, CacheNames.BOOK_OVERVIEW);
        this.bookBucketCountsCache = requireCache(cacheManager, CacheNames.BOOK_BUCKET_COUNTS);
        this.authorBucketCountsCache = requireCache(cacheManager, CacheNames.AUTHOR_BUCKET_COUNTS);
        this.authorListingDescriptorsCache = requireCache(cacheManager, CacheNames.AUTHOR_LISTING_DESCRIPTORS);
        this.authorXmlPageCountCache = requireCache(cacheManager, CacheNames.AUTHOR_XML_PAGE_COUNT);
        this.authorXmlPageCache = requireCache(cacheManager, CacheNames.AUTHOR_XML_PAGE);
        this.bookPageMetadataCache = requireCache(cacheManager, CacheNames.BOOK_PAGE_METADATA);
        this.authorPageMetadataCache = requireCache(cacheManager, CacheNames.AUTHOR_PAGE_METADATA);
    }

    public SitemapOverview getOverview() {
        return cached(overviewCache, "overview", () ->
                new SitemapOverview(getBookLetterCounts(), getAuthorLetterCounts()));
    }

    public PagedResult<BookSitemapItem> getBooksByLetter(String letter, int page) {
        String bucket = normalizeBucket(letter);
        int safePage = PagingUtils.atLeast(page, 1);
        Map<String, Integer> counts = getBookLetterCounts();
        int totalItems = counts.getOrDefault(bucket, 0);
        if (totalItems == 0) {
            return new PagedResult<>(Collections.emptyList(), safePage, 0, 0);
        }
        int pageSize = properties.getHtmlPageSize();
        int totalPages = (int) Math.ceil((double) totalItems / pageSize);
        if (safePage > totalPages) {
            return new PagedResult<>(Collections.emptyList(), safePage, totalPages, totalItems);
        }
        int offset = (safePage - 1) * pageSize;
        List<BookSitemapItem> items = sitemapRepository.fetchBooksForBucket(bucket, pageSize, offset)
                .stream()
                .map(row -> new BookSitemapItem(row.bookId(), row.slug(), row.title(), row.updatedAt()))
                .toList();
        return new PagedResult<>(items, safePage, totalPages, totalItems);
    }

    public PagedResult<AuthorSection> getAuthorsByLetter(String letter, int page) {
        String bucket = normalizeBucket(letter);
        int safePage = PagingUtils.atLeast(page, 1);
        Map<String, Integer> counts = getAuthorLetterCounts();
        int totalItems = counts.getOrDefault(bucket, 0);
        if (totalItems == 0) {
            return new PagedResult<>(Collections.emptyList(), safePage, 0, 0);
        }
        int pageSize = properties.getHtmlPageSize();
        int totalPages = (int) Math.ceil((double) totalItems / pageSize);
        if (safePage > totalPages) {
            return new PagedResult<>(Collections.emptyList(), safePage, totalPages, totalItems);
        }
        int offset = (safePage - 1) * pageSize;
        List<AuthorRow> authorRows = sitemapRepository.fetchAuthorsForBucket(bucket, pageSize, offset);
        if (authorRows.isEmpty()) {
            return new PagedResult<>(Collections.emptyList(), safePage, totalPages, totalItems);
        }
        Set<String> authorIds = authorRows.stream().map(AuthorRow::id)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, List<BookRow>> booksByAuthor = sitemapRepository.fetchBooksForAuthors(authorIds);
        List<AuthorSection> sections = authorRows.stream()
                .map(row -> new AuthorSection(
                        row.id(),
                        row.name(),
                        row.updatedAt(),
                        booksByAuthor.getOrDefault(row.id(), List.of()).stream()
                                .map(bookRow -> new BookSitemapItem(bookRow.bookId(), bookRow.slug(), bookRow.title(), bookRow.updatedAt()))
                                .toList()
                ))
                .toList();
        return new PagedResult<>(sections, safePage, totalPages, totalItems);
    }

    public int getBooksXmlPageCount() {
        return cached(booksXmlPageCountCache, "pageCount", this::calculateBooksXmlPageCount);
    }

    public List<BookSitemapItem> getBooksForXmlPage(int page) {
        int safePage = PagingUtils.atLeast(page, 1);
        return cached(booksXmlPageCache, safePage, () -> loadBooksForXmlPage(safePage));
    }

    public List<AuthorListingDescriptor> listAuthorListingDescriptors() {
        return getAuthorListingDescriptors();
    }

    public int getAuthorXmlPageCount() {
        return cached(authorXmlPageCountCache, "pageCount", () -> {
            int totalListingPages = getAuthorListingDescriptors().size();
            if (totalListingPages == 0) {
                return 0;
            }
            int xmlPageSize = properties.getXmlPageSize();
            return (int) Math.ceil((double) totalListingPages / xmlPageSize);
        });
    }

    public List<AuthorListingXmlItem> getAuthorListingsForXmlPage(int page) {
        int safePage = PagingUtils.atLeast(page, 1);
        return cached(authorXmlPageCache, safePage, () -> loadAuthorListingsForXmlPage(safePage));
    }

    public List<SitemapPageMetadata> getBookSitemapPageMetadata() {
        return cached(bookPageMetadataCache, "metadata", this::loadBookPageMetadata);
    }

    public List<SitemapPageMetadata> getAuthorSitemapPageMetadata() {
        return cached(authorPageMetadataCache, "metadata", this::loadAuthorPageMetadata);
    }

    public DatasetFingerprint currentBookFingerprint() {
        return bookFingerprintRef.updateAndGet(existing -> existing != null ? existing : sitemapRepository.fetchBookFingerprint());
    }

    public DatasetFingerprint currentAuthorFingerprint() {
        return authorFingerprintRef.updateAndGet(existing -> existing != null ? existing : sitemapRepository.fetchAuthorFingerprint());
    }

    public boolean refreshSitemapCachesIfDatasetChanged() {
        // Fetch latest fingerprints from database
        DatasetFingerprint latestBook = sitemapRepository.fetchBookFingerprint();
        DatasetFingerprint latestAuthor = sitemapRepository.fetchAuthorFingerprint();
        
        // Thread-safe comparison and update using compareAndSet pattern
        DatasetFingerprint previousBook = bookFingerprintRef.get();
        DatasetFingerprint previousAuthor = authorFingerprintRef.get();
        
        boolean bookChanged = !Objects.equals(previousBook, latestBook);
        boolean authorChanged = !Objects.equals(previousAuthor, latestAuthor);
        boolean changed = bookChanged || authorChanged;
        
        if (changed) {
            // Update fingerprints atomically
            if (bookChanged) {
                bookFingerprintRef.compareAndSet(previousBook, latestBook);
            }
            if (authorChanged) {
                authorFingerprintRef.compareAndSet(previousAuthor, latestAuthor);
            }
            // Clear all sitemap caches when data changes
            clearSitemapCaches();
        }
        return changed;
    }

    public void clearSitemapCaches() {
        clearCache(booksXmlPageCountCache);
        clearCache(booksXmlPageCache);
        clearCache(overviewCache);
        clearCache(bookBucketCountsCache);
        clearCache(authorBucketCountsCache);
        clearCache(authorListingDescriptorsCache);
        clearCache(authorXmlPageCountCache);
        clearCache(authorXmlPageCache);
        clearCache(bookPageMetadataCache);
        clearCache(authorPageMetadataCache);
    }

    public String normalizeBucket(String letter) {
        if (!StringUtils.hasText(letter)) {
            return "A";
        }
        String normalized = letter.trim().toUpperCase(Locale.ROOT);
        if (LETTER_BUCKETS.contains(normalized)) {
            return normalized;
        }
        return normalized.matches("[A-Z]") ? normalized : "0-9";
    }

    private Map<String, Integer> getBookLetterCounts() {
        return cached(bookBucketCountsCache, "counts", this::loadBookLetterCounts);
    }

    private Map<String, Integer> getAuthorLetterCounts() {
        return cached(authorBucketCountsCache, "counts", this::loadAuthorLetterCounts);
    }

    private List<AuthorListingDescriptor> getAuthorListingDescriptors() {
        return cached(authorListingDescriptorsCache, "descriptors", this::loadAuthorListingDescriptors);
    }

    private int calculateBooksXmlPageCount() {
        int total = sitemapRepository.countAllBooks();
        int pageSize = properties.getXmlPageSize();
        return total == 0 ? 0 : (int) Math.ceil((double) total / pageSize);
    }

    private List<BookSitemapItem> loadBooksForXmlPage(int page) {
        int pageSize = properties.getXmlPageSize();
        int offset = (page - 1) * pageSize;
        return sitemapRepository.fetchBooksForXml(pageSize, offset)
                .stream()
                .map(row -> new BookSitemapItem(row.bookId(), row.slug(), row.title(), row.updatedAt()))
                .toList();
    }

    private List<AuthorListingXmlItem> loadAuthorListingsForXmlPage(int page) {
        List<AuthorListingDescriptor> descriptors = getAuthorListingDescriptors();
        if (descriptors.isEmpty()) {
            return List.of();
        }
        int xmlPageSize = properties.getXmlPageSize();
        int startIndex = (page - 1) * xmlPageSize;
        if (startIndex >= descriptors.size()) {
            return List.of();
        }
        int endIndex = Math.min(startIndex + xmlPageSize, descriptors.size());
        List<AuthorListingDescriptor> slice = descriptors.subList(startIndex, endIndex);
        List<AuthorListingXmlItem> results = new ArrayList<>(slice.size());
        for (AuthorListingDescriptor descriptor : slice) {
            PagedResult<AuthorSection> authorPage = getAuthorsByLetter(descriptor.bucket(), descriptor.page());
            Instant lastModified = authorPage.items().stream()
                    .flatMap(author -> {
                        List<Instant> instants = new ArrayList<>();
                        if (author.updatedAt() != null) {
                            instants.add(author.updatedAt());
                        }
                        if (author.books() != null) {
                            author.books().stream()
                                    .map(BookSitemapItem::updatedAt)
                                    .filter(Objects::nonNull)
                                    .forEach(instants::add);
                        }
                        return instants.stream();
                    })
                    .max(Instant::compareTo)
                    .orElseGet(() -> currentAuthorFingerprint().lastModified());
            results.add(new AuthorListingXmlItem(descriptor.bucket(), descriptor.page(), lastModified));
        }
        return List.copyOf(results);
    }

    private List<SitemapPageMetadata> loadBookPageMetadata() {
        int pageSize = properties.getXmlPageSize();
        List<PageMetadata> metadata = sitemapRepository.fetchBookPageMetadata(pageSize);
        if (metadata.isEmpty()) {
            return List.of();
        }
        return metadata.stream()
                .map(entry -> new SitemapPageMetadata(entry.pageNumber(), entry.lastModified()))
                .toList();
    }

    private List<SitemapPageMetadata> loadAuthorPageMetadata() {
        int totalPages = getAuthorXmlPageCount();
        if (totalPages == 0) {
            return List.of();
        }
        List<SitemapPageMetadata> results = new ArrayList<>(totalPages);
        for (int page = 1; page <= totalPages; page++) {
            Instant lastModified = getAuthorListingsForXmlPage(page).stream()
                    .map(AuthorListingXmlItem::lastModified)
                    .max(Instant::compareTo)
                    .orElseGet(() -> currentAuthorFingerprint().lastModified());
            results.add(new SitemapPageMetadata(page, lastModified));
        }
        return List.copyOf(results);
    }

    private Map<String, Integer> loadBookLetterCounts() {
        Map<String, Integer> raw = sitemapRepository.countBooksByBucket();
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String bucket : LETTER_BUCKETS) {
            counts.put(bucket, raw.getOrDefault(bucket, 0));
        }
        return Map.copyOf(counts);
    }

    private Map<String, Integer> loadAuthorLetterCounts() {
        Map<String, Integer> raw = sitemapRepository.countAuthorsByBucket();
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String bucket : LETTER_BUCKETS) {
            counts.put(bucket, raw.getOrDefault(bucket, 0));
        }
        return Map.copyOf(counts);
    }

    private List<AuthorListingDescriptor> loadAuthorListingDescriptors() {
        Map<String, Integer> counts = getAuthorLetterCounts();
        if (counts.isEmpty()) {
            return List.of();
        }
        int pageSize = properties.getHtmlPageSize();
        List<AuthorListingDescriptor> descriptors = new ArrayList<>();
        for (String bucket : LETTER_BUCKETS) {
            int total = counts.getOrDefault(bucket, 0);
            if (total == 0) {
                continue;
            }
            int totalPages = (int) Math.ceil((double) total / pageSize);
            for (int page = 1; page <= totalPages; page++) {
                descriptors.add(new AuthorListingDescriptor(bucket, page));
            }
        }
        return List.copyOf(descriptors);
    }

    private Cache requireCache(CacheManager cacheManager, String name) {
        Cache cache = cacheManager.getCache(name);
        if (cache == null) {
            throw new IllegalStateException("Missing cache configuration for " + name);
        }
        return cache;
    }

    private void clearCache(Cache cache) {
        cache.clear();
    }

    private <T> T cached(Cache cache, Object key, Supplier<T> loader) {
        return cache.get(key, () -> {
            T value = loader.get();
            if (value == null) {
                throw new IllegalStateException("Cache loader returned null for key " + key);
            }
            return value;
        });
    }

    private static final class CacheNames {
        private static final String BOOK_XML_PAGE_COUNT = "sitemapBookXmlPageCount";
        private static final String BOOK_XML_PAGE = "sitemapBookXmlPage";
        private static final String BOOK_OVERVIEW = "sitemapOverview";
        private static final String BOOK_BUCKET_COUNTS = "sitemapBookBucketCounts";
        private static final String AUTHOR_BUCKET_COUNTS = "sitemapAuthorBucketCounts";
        private static final String AUTHOR_LISTING_DESCRIPTORS = "sitemapAuthorListingDescriptors";
        private static final String AUTHOR_XML_PAGE_COUNT = "sitemapAuthorXmlPageCount";
        private static final String AUTHOR_XML_PAGE = "sitemapAuthorXmlPage";
        private static final String BOOK_PAGE_METADATA = "sitemapBookPageMetadata";
        private static final String AUTHOR_PAGE_METADATA = "sitemapAuthorPageMetadata";

        private CacheNames() {
        }
    }

    public record SitemapOverview(Map<String, Integer> bookLetterCounts,
                                  Map<String, Integer> authorLetterCounts) {}

    public record PagedResult<T>(List<T> items, int page, int totalPages, int totalItems) {}

    public record BookSitemapItem(String bookId, String slug, String title, Instant updatedAt) {}

    public record AuthorSection(String authorId, String authorName, Instant updatedAt, List<BookSitemapItem> books) {}

    public record AuthorListingDescriptor(String bucket, int page) {}

    public record AuthorListingXmlItem(String bucket, int page, Instant lastModified) {
        public String toPath() {
            return "/sitemap/authors/" + bucket + "/" + page;
        }
    }

    public record SitemapPageMetadata(int page, Instant lastModified) {}
}
