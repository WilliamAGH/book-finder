package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Strongly typed configuration for sitemap generation.
 */
@Component
@ConfigurationProperties(prefix = "sitemap")
public class SitemapProperties {

    /**
     * Base URL used for sitemap links (defaults to production domain).
     */
    private String baseUrl = "https://findmybook.net";

    /**
     * Number of entries shown per HTML page (authors/books view).
     */
    private int htmlPageSize = 100;

    /**
     * Max number of URLs per XML sitemap file.
     */
    private int xmlPageSize = 5000;

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public int getHtmlPageSize() {
        return htmlPageSize;
    }

    public void setHtmlPageSize(int htmlPageSize) {
        this.htmlPageSize = htmlPageSize;
    }

    public int getXmlPageSize() {
        return xmlPageSize;
    }

    public void setXmlPageSize(int xmlPageSize) {
        this.xmlPageSize = xmlPageSize;
    }
}
