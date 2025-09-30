package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.config.SitemapProperties;
import com.williamcallahan.book_recommendation_engine.service.SitemapService;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.AuthorListingXmlItem;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.AuthorSection;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.BookSitemapItem;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.PagedResult;
import com.williamcallahan.book_recommendation_engine.service.SitemapService.SitemapOverview;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Controller responsible for generating the server-rendered sitemap page and XML sitemap feeds.
 */
@Controller
public class SitemapController {

    private static final DateTimeFormatter LAST_MODIFIED_FORMATTER = DateTimeFormatter.ISO_INSTANT;

    private final SitemapService sitemapService;
    private final SitemapProperties sitemapProperties;

    public SitemapController(SitemapService sitemapService, SitemapProperties sitemapProperties) {
        this.sitemapService = sitemapService;
        this.sitemapProperties = sitemapProperties;
    }

    @GetMapping("/sitemap")
    public String sitemapLanding(@RequestParam(name = "view", required = false) String view,
                                 @RequestParam(name = "letter", required = false) String letter,
                                 @RequestParam(name = "page", required = false, defaultValue = "1") int page) {
        String normalizedView = normalizeView(view);
        String bucket = sitemapService.normalizeBucket(letter);
        int safePage = PagingUtils.atLeast(page, 1);
        return "redirect:/sitemap/" + normalizedView + "/" + bucket + "/" + safePage;
    }

    @GetMapping("/sitemap/{view}/{letter}/{page}")
    public String sitemapDynamic(@PathVariable String view,
                                 @PathVariable String letter,
                                 @PathVariable int page,
                                 Model model) {
        String normalizedView = normalizeView(view);
        String bucket = sitemapService.normalizeBucket(letter);
        int safePage = PagingUtils.atLeast(page, 1);

        if (!normalizedView.equals(view) || !bucket.equals(letter) || safePage != page) {
            return "redirect:/sitemap/" + normalizedView + "/" + bucket + "/" + safePage;
        }

        return populateSitemapModel(normalizedView, bucket, safePage, model);
    }

    @GetMapping(value = "/sitemap.xml", produces = MediaType.APPLICATION_XML_VALUE)
    @ResponseBody
    public ResponseEntity<String> sitemapIndex() {
        int totalBookPages = sitemapService.getBooksXmlPageCount();
        int totalAuthorPages = sitemapService.getAuthorXmlPageCount();
        String baseUrl = sitemapProperties.getBaseUrl();

        Map<Integer, Instant> bookLastModified = sitemapService.getBookSitemapPageMetadata().stream()
                .collect(Collectors.toMap(SitemapService.SitemapPageMetadata::page, SitemapService.SitemapPageMetadata::lastModified));
        Map<Integer, Instant> authorLastModified = sitemapService.getAuthorSitemapPageMetadata().stream()
                .collect(Collectors.toMap(SitemapService.SitemapPageMetadata::page, SitemapService.SitemapPageMetadata::lastModified));

        Instant bookFallback = sitemapService.currentBookFingerprint().lastModified();
        Instant authorFallback = sitemapService.currentAuthorFingerprint().lastModified();

        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");
        for (int i = 1; i <= totalBookPages; i++) {
            Instant lastModified = bookLastModified.getOrDefault(i, bookFallback);
            xml.append("  <sitemap>\n");
            xml.append("    <loc>").append(escapeXml(baseUrl + "/sitemap-xml/books/" + i + ".xml")).append("</loc>\n");
            xml.append("    <lastmod>").append(LAST_MODIFIED_FORMATTER.format(lastModified)).append("</lastmod>\n");
            xml.append("  </sitemap>\n");
        }

        for (int i = 1; i <= totalAuthorPages; i++) {
            Instant lastModified = authorLastModified.getOrDefault(i, authorFallback);
            xml.append("  <sitemap>\n");
            xml.append("    <loc>").append(escapeXml(baseUrl + "/sitemap-xml/authors/" + i + ".xml")).append("</loc>\n");
            xml.append("    <lastmod>").append(LAST_MODIFIED_FORMATTER.format(lastModified)).append("</lastmod>\n");
            xml.append("  </sitemap>\n");
        }
        xml.append("</sitemapindex>");

        return ResponseEntity.ok(xml.toString());
    }

    @GetMapping(value = "/sitemap-xml/books/{page}.xml", produces = MediaType.APPLICATION_XML_VALUE)
    @ResponseBody
    public ResponseEntity<String> booksSitemap(@PathVariable("page") int page) {
        int totalPages = sitemapService.getBooksXmlPageCount();
        if (totalPages == 0 || page < 1 || page > totalPages) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
        List<BookSitemapItem> items = sitemapService.getBooksForXmlPage(page);
        String xml = buildBookUrlSet(items);
        return ResponseEntity.ok(xml);
    }

    @GetMapping(value = "/sitemap-xml/authors/{page}.xml", produces = MediaType.APPLICATION_XML_VALUE)
    @ResponseBody
    public ResponseEntity<String> authorsSitemap(@PathVariable("page") int page) {
        int totalPages = sitemapService.getAuthorXmlPageCount();
        if (totalPages == 0 || page < 1 || page > totalPages) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
        List<AuthorListingXmlItem> listings = sitemapService.getAuthorListingsForXmlPage(page);
        String xml = buildAuthorUrlSet(listings);
        return ResponseEntity.ok(xml);
    }

    private String buildBookUrlSet(List<BookSitemapItem> items) {
        String baseUrl = sitemapProperties.getBaseUrl();
        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");
        Instant fallback = sitemapService.currentBookFingerprint().lastModified();
        for (BookSitemapItem item : items) {
            if (item.slug() == null || item.slug().isBlank()) {
                continue; // Skip items without valid slugs
            }
            Instant lastModified = item.updatedAt() != null ? item.updatedAt() : fallback;
            xml.append("  <url>\n");
            // Slugs should already be URL-safe, but ensure proper encoding
            String safeSlug = validateAndEncodeSlug(item.slug());
            xml.append("    <loc>").append(escapeXml(baseUrl + "/book/" + safeSlug)).append("</loc>\n");
            xml.append("    <lastmod>").append(LAST_MODIFIED_FORMATTER.format(lastModified)).append("</lastmod>\n");
            xml.append("    <changefreq>weekly</changefreq>\n");
            xml.append("    <priority>0.8</priority>\n");
            xml.append("  </url>\n");
        }
        xml.append("</urlset>");
        return xml.toString();
    }

    private String buildAuthorUrlSet(List<AuthorListingXmlItem> items) {
        String baseUrl = sitemapProperties.getBaseUrl();
        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");
        Instant fallback = sitemapService.currentAuthorFingerprint().lastModified();
        for (AuthorListingXmlItem item : items) {
            xml.append("  <url>\n");
            xml.append("    <loc>").append(escapeXml(baseUrl + item.toPath())).append("</loc>\n");
            Instant lastModified = item.lastModified() != null ? item.lastModified() : fallback;
            xml.append("    <lastmod>").append(LAST_MODIFIED_FORMATTER.format(lastModified)).append("</lastmod>\n");
            xml.append("    <changefreq>daily</changefreq>\n");
            xml.append("    <priority>0.6</priority>\n");
            xml.append("  </url>\n");
        }
        xml.append("</urlset>");
        return xml.toString();
    }

    private String validateAndEncodeSlug(String slug) {
        if (slug == null || slug.isBlank()) {
            return "unknown";
        }
        // Slugs should already be URL-safe (lowercase, hyphens, alphanumeric)
        // But handle edge cases where encoding is needed
        try {
            // Only encode if it contains characters that need encoding
            if (slug.matches("[a-z0-9-]+")) {
                return slug; // Already safe
            }
            return URLEncoder.encode(slug, StandardCharsets.UTF_8);
        } catch (Exception e) {
            // Fallback to basic sanitization
            return slug.replaceAll("[^a-zA-Z0-9-]", "-");
        }
    }

    private String escapeXml(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("&", "&amp;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }

    private String normalizeView(String view) {
        if (ValidationUtils.isNullOrBlank(view)) {
            return "authors";
        }
        String candidate = view.trim().toLowerCase(Locale.ROOT);
        return "books".equals(candidate) ? "books" : "authors";
    }

    private String populateSitemapModel(String view,
                                        String bucket,
                                        int page,
                                        Model model) {
        SitemapOverview overview = sitemapService.getOverview();

        if ("books".equals(view)) {
            PagedResult<BookSitemapItem> books = sitemapService.getBooksByLetter(bucket, page);
            model.addAttribute("books", books.items());
            model.addAttribute("totalPages", books.totalPages());
            model.addAttribute("totalItems", books.totalItems());
        } else {
            PagedResult<AuthorSection> authors = sitemapService.getAuthorsByLetter(bucket, page);
            model.addAttribute("authors", authors.items());
            model.addAttribute("totalPages", authors.totalPages());
            model.addAttribute("totalItems", authors.totalItems());
        }

        model.addAttribute("viewType", view);
        model.addAttribute("activeLetter", bucket);
        model.addAttribute("letters", SitemapService.LETTER_BUCKETS);
        model.addAttribute("bookLetterCounts", overview.bookLetterCounts());
        model.addAttribute("authorLetterCounts", overview.authorLetterCounts());
        model.addAttribute("pageNumber", page);
        model.addAttribute("baseUrl", sitemapProperties.getBaseUrl());
        String canonicalPath = "/sitemap/" + view + "/" + bucket + "/" + page;
        String canonicalUrl = sitemapProperties.getBaseUrl() + canonicalPath;
        model.addAttribute("canonicalPath", canonicalPath);
        model.addAttribute("canonicalUrl", canonicalUrl);
        model.addAttribute("pageTitle", "Sitemap");
        model.addAttribute("pageDescription", "Browse all book and author pages indexed for search engines.");
        return "sitemap";
    }
}
