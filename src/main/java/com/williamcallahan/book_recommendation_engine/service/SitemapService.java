package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.config.SitemapProperties;
import com.williamcallahan.book_recommendation_engine.repository.SitemapRepository;
import com.williamcallahan.book_recommendation_engine.repository.SitemapRepository.AuthorRow;
import com.williamcallahan.book_recommendation_engine.repository.SitemapRepository.BookRow;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
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

    public SitemapService(SitemapRepository sitemapRepository, SitemapProperties properties) {
        this.sitemapRepository = sitemapRepository;
        this.properties = properties;
    }

    public SitemapOverview getOverview() {
        return new SitemapOverview(countBooksByBucket(), countAuthorsByBucket());
    }

    public PagedResult<BookSitemapItem> getBooksByLetter(String letter, int page) {
        String bucket = normalizeBucket(letter);
        int safePage = PagingUtils.atLeast(page, 1);
        int pageSize = properties.getHtmlPageSize();
        int totalItems = countBooksByBucket().getOrDefault(bucket, 0);
        if (totalItems == 0) {
            return new PagedResult<>(Collections.emptyList(), safePage, 0, 0);
        }
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
        int pageSize = properties.getHtmlPageSize();
        int totalItems = countAuthorsByBucket().getOrDefault(bucket, 0);
        if (totalItems == 0) {
            return new PagedResult<>(Collections.emptyList(), safePage, 0, 0);
        }
        int totalPages = (int) Math.ceil((double) totalItems / pageSize);
        if (safePage > totalPages) {
            return new PagedResult<>(Collections.emptyList(), safePage, totalPages, totalItems);
        }
        int offset = (safePage - 1) * pageSize;
        List<AuthorRow> authorRows = sitemapRepository.fetchAuthorsForBucket(bucket, pageSize, offset);
        if (authorRows.isEmpty()) {
            return new PagedResult<>(Collections.emptyList(), safePage, totalPages, totalItems);
        }
        Set<String> authorIds = authorRows.stream().map(AuthorRow::id).collect(Collectors.toCollection(LinkedHashSet::new));
        Map<String, List<BookRow>> booksByAuthor = sitemapRepository.fetchBooksForAuthors(authorIds);
        List<AuthorSection> sections = authorRows.stream()
                .map(row -> new AuthorSection(
                        row.id(),
                        row.name(),
                        row.updatedAt(),
                        booksByAuthor.getOrDefault(row.id(), Collections.emptyList())
                                .stream()
                                .map(bookRow -> new BookSitemapItem(bookRow.bookId(), bookRow.slug(), bookRow.title(), bookRow.updatedAt()))
                                .toList()
                ))
                .toList();
        return new PagedResult<>(sections, safePage, totalPages, totalItems);
    }

    public int getBooksXmlPageCount() {
        int total = sitemapRepository.countAllBooks();
        int pageSize = properties.getXmlPageSize();
        return total == 0 ? 0 : (int) Math.ceil((double) total / pageSize);
    }

    public List<BookSitemapItem> getBooksForXmlPage(int page) {
        int safePage = PagingUtils.atLeast(page, 1);
        int pageSize = properties.getXmlPageSize();
        int offset = (safePage - 1) * pageSize;
        return sitemapRepository.fetchBooksForXml(pageSize, offset)
                .stream()
                .map(row -> new BookSitemapItem(row.bookId(), row.slug(), row.title(), row.updatedAt()))
                .toList();
    }

    public List<AuthorListingDescriptor> listAuthorListingDescriptors() {
        Map<String, Integer> counts = countAuthorsByBucket();
        if (counts.isEmpty()) {
            return Collections.emptyList();
        }
        List<AuthorListingDescriptor> descriptors = new ArrayList<>();
        int pageSize = properties.getHtmlPageSize();
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
        return descriptors;
    }

    public int getAuthorXmlPageCount() {
        int totalListingPages = listAuthorListingDescriptors().size();
        if (totalListingPages == 0) {
            return 0;
        }
        int xmlPageSize = properties.getXmlPageSize();
        return (int) Math.ceil((double) totalListingPages / xmlPageSize);
    }

    public List<AuthorListingXmlItem> getAuthorListingsForXmlPage(int page) {
        List<AuthorListingDescriptor> descriptors = listAuthorListingDescriptors();
        if (descriptors.isEmpty()) {
            return Collections.emptyList();
        }
        int safePage = PagingUtils.atLeast(page, 1);
        int xmlPageSize = properties.getXmlPageSize();
        int startIndex = (safePage - 1) * xmlPageSize;
        if (startIndex >= descriptors.size()) {
            return Collections.emptyList();
        }
        int endIndex = Math.min(startIndex + xmlPageSize, descriptors.size());
        List<AuthorListingDescriptor> slice = descriptors.subList(startIndex, endIndex);
        List<AuthorListingXmlItem> results = new ArrayList<>(slice.size());
        for (AuthorListingDescriptor descriptor : slice) {
            PagedResult<AuthorSection> pageResult = getAuthorsByLetter(descriptor.bucket(), descriptor.page());
            Instant lastModified = pageResult.items().stream()
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
                    .orElseGet(Instant::now);
            results.add(new AuthorListingXmlItem(descriptor.bucket(), descriptor.page(), lastModified));
        }
        return results;
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

    private Map<String, Integer> countBooksByBucket() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        Map<String, Integer> raw = sitemapRepository.countBooksByBucket();
        for (String bucket : LETTER_BUCKETS) {
            counts.put(bucket, raw.getOrDefault(bucket, 0));
        }
        return counts;
    }

    private Map<String, Integer> countAuthorsByBucket() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        Map<String, Integer> raw = sitemapRepository.countAuthorsByBucket();
        for (String bucket : LETTER_BUCKETS) {
            counts.put(bucket, raw.getOrDefault(bucket, 0));
        }
        return counts;
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
}
