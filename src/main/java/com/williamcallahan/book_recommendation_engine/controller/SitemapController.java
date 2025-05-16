/**
 * Controller responsible for generating XML sitemaps
 *
 * @author William Callahan
 * 
 * Features:
 * - Generates sitemap index with links to sub-sitemaps
 * - Creates separate sitemaps for static and dynamic content
 * - Uses S3 storage for book ID persistence
 * - Formats XML according to sitemap protocol standards
 * - Sets appropriate change frequency and priority values
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.service.BookSitemapService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Mono;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Set;

@Controller
public class SitemapController {

    private static final Logger logger = LoggerFactory.getLogger(SitemapController.class); // Added logger
    // private final BookCacheService bookCacheService; // Replaced
    private final BookSitemapService bookSitemapService;
    private static final String BASE_URL = "https://findmybook.net";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_DATE;

    /**
     * Constructs sitemap controller with required services
     * 
     * @param bookSitemapService Service for retrieving book IDs for sitemap generation
     */
    @Autowired
    public SitemapController(BookSitemapService bookSitemapService) {
        this.bookSitemapService = bookSitemapService;
    }

    /**
     * Generates the main sitemap index file
     * - Contains links to more specific sitemaps
     * - Formatted according to sitemap protocol standards
     * - Uses current date for lastmod values
     * 
     * @return XML string containing sitemap index content
     */
    @GetMapping(value = "/sitemap.xml", produces = MediaType.APPLICATION_XML_VALUE)
    @ResponseBody
    public Mono<String> getSitemapIndex() {
        String currentDate = LocalDate.now().format(DATE_FORMATTER);

        StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");

        // Static pages sitemap
        xml.append("  <sitemap>\n");
        xml.append("    <loc>").append(BASE_URL).append("/sitemap_static.xml</loc>\n");
        xml.append("    <lastmod>").append(currentDate).append("</lastmod>\n");
        xml.append("  </sitemap>\n");

        // Books sitemap
        xml.append("  <sitemap>\n");
        xml.append("    <loc>").append(BASE_URL).append("/sitemap_books.xml</loc>\n");
        xml.append("    <lastmod>").append(currentDate).append("</lastmod>\n"); // This could also be the last modified date of the S3 JSON file
        xml.append("  </sitemap>\n");

        xml.append("</sitemapindex>");
        return Mono.just(xml.toString());
    }

    /**
     * Generates sitemap for static pages
     * - Includes homepage and search page
     * - Sets appropriate priority and change frequency
     * - Uses current date for lastmod values
     * 
     * @return XML string containing static pages sitemap
     */
    @GetMapping(value = "/sitemap_static.xml", produces = MediaType.APPLICATION_XML_VALUE)
    @ResponseBody
    public Mono<String> getStaticPagesSitemap() {
        String currentDate = LocalDate.now().format(DATE_FORMATTER);
        StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");

        // Home page
        xml.append("  <url>\n");
        xml.append("    <loc>").append(BASE_URL).append("/").append("</loc>\n");
        xml.append("    <lastmod>").append(currentDate).append("</lastmod>\n");
        xml.append("    <changefreq>daily</changefreq>\n");
        xml.append("    <priority>1.0</priority>\n");
        xml.append("  </url>\n");

        // Search page
        xml.append("  <url>\n");
        xml.append("    <loc>").append(BASE_URL).append("/search").append("</loc>\n");
        xml.append("    <lastmod>").append(currentDate).append("</lastmod>\n");
        xml.append("    <changefreq>weekly</changefreq>\n");
        xml.append("    <priority>0.8</priority>\n");
        xml.append("  </url>\n");

        xml.append("</urlset>");
        return Mono.just(xml.toString());
    }

    /**
     * Generates sitemap for book detail pages
     * - Includes all book IDs from accumulated cache
     * - Retrieves book IDs from S3 storage
     * - Sets appropriate priority and change frequency
     * - Uses current date for lastmod values
     * 
     * @return XML string containing book pages sitemap
     */
    @GetMapping(value = "/sitemap_books.xml", produces = MediaType.APPLICATION_XML_VALUE)
    @ResponseBody
    public Mono<String> getBooksSitemap() {
        String currentDate = LocalDate.now().format(DATE_FORMATTER);
        Set<String> bookIdsForSitemap = bookSitemapService.getAccumulatedBookIdsFromS3();

        if (bookIdsForSitemap == null || bookIdsForSitemap.isEmpty()) {
            // Return an empty urlset if no IDs are found or an error occurred upstream
            return Mono.just("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\"></urlset>");
        }
        
        // Building the XML reactively might be slightly better if the set is huge, 
        // but for typical sitemap sizes, Mono.just with a StringBuilder is fine.
        return Mono.fromSupplier(() -> {
            StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
            xml.append("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");

            for (String bookId : bookIdsForSitemap) {
                xml.append("  <url>\n");
                xml.append("    <loc>").append(BASE_URL).append("/book/").append(bookId).append("</loc>\n");
                xml.append("    <lastmod>").append(currentDate).append("</lastmod>\n"); // Ideally, use book's actual last modified date from S3 data if stored
                xml.append("    <changefreq>monthly</changefreq>\n");
                xml.append("    <priority>0.7</priority>\n");
                xml.append("  </url>\n");
            }
            xml.append("</urlset>");
            return xml.toString();
        });
    }

    /**
     * Manually triggers the update of accumulated book IDs in S3
     * This is the same process run by the hourly scheduler
     * 
     * @return ResponseEntity indicating success or failure
     */
    @PostMapping(value = "/admin/trigger-sitemap-update")
    @ResponseBody
    public Mono<ResponseEntity<String>> manualTriggerSitemapUpdate() {
        logger.info("Manual trigger: Updating accumulated book IDs in S3.");
        try {
            bookSitemapService.updateAccumulatedBookIdsInS3();
            logger.info("Manual trigger finished: Accumulated book ID update process completed.");
            return Mono.just(ResponseEntity.ok("Sitemap book ID update triggered successfully."));
        } catch (Exception e) {
            logger.error("Error during manual sitemap book ID update trigger:", e);
            return Mono.just(ResponseEntity.status(500).body("Error triggering sitemap update: " + e.getMessage()));
        }
    }
}
