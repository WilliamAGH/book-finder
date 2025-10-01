package com.williamcallahan.book_recommendation_engine.config;

import jakarta.annotation.PostConstruct;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.Set;

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

    /**
     * Base time-to-live for sitemap caches.
     */
    private Duration cacheTtl = Duration.ofDays(7);

    /**
     * Additional jitter applied to sitemap cache expirations to avoid herd effects.
     */
    private Duration cacheJitter = Duration.ofHours(24);

    /**
     * Whether the consolidated sitemap scheduler runs.
     */
    private boolean schedulerEnabled = true;

    /**
     * Cron expression for sitemap refresh job.
     */
    private String schedulerCron = "0 15 * * * *";

    /**
     * Number of books to sample for cover warmup runs.
     */
    private int schedulerCoverSampleSize = 25;

    /**
     * Number of books to hydrate through external APIs per run.
     */
    private int schedulerExternalHydrationSize = 10;

    /**
     * Maximum random delay in seconds applied before scheduled refresh begins.
     */
    private int schedulerJitterSeconds = 0;

    /**
     * S3 key used to persist accumulated sitemap book identifiers.
     */
    private String s3AccumulatedIdsKey = "sitemaps/accumulated-book-ids.json";

    @PostConstruct
    void validate() {
        Set<Integer> allowedXmlSizes = Set.of(1000, 2500, 5000);
        Assert.isTrue(allowedXmlSizes.contains(xmlPageSize),
                "sitemap.xml-page-size must be one of 1000, 2500, or 5000");
        Assert.isTrue(!cacheTtl.isNegative(), "sitemap.cache-ttl must be non-negative");
        Assert.isTrue(!cacheJitter.isNegative(), "sitemap.cache-jitter must be non-negative");
        Assert.isTrue(schedulerJitterSeconds >= 0, "sitemap.scheduler-jitter-seconds must be non-negative");
    }

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

    public boolean isSchedulerEnabled() {
        return schedulerEnabled;
    }

    public void setSchedulerEnabled(boolean schedulerEnabled) {
        this.schedulerEnabled = schedulerEnabled;
    }

    public String getSchedulerCron() {
        return schedulerCron;
    }

    public void setSchedulerCron(String schedulerCron) {
        this.schedulerCron = schedulerCron;
    }

    public int getSchedulerCoverSampleSize() {
        return schedulerCoverSampleSize;
    }

    public void setSchedulerCoverSampleSize(int schedulerCoverSampleSize) {
        this.schedulerCoverSampleSize = schedulerCoverSampleSize;
    }

    public int getSchedulerExternalHydrationSize() {
        return schedulerExternalHydrationSize;
    }

    public void setSchedulerExternalHydrationSize(int schedulerExternalHydrationSize) {
        this.schedulerExternalHydrationSize = schedulerExternalHydrationSize;
    }

    public String getS3AccumulatedIdsKey() {
        return s3AccumulatedIdsKey;
    }

    public void setS3AccumulatedIdsKey(String s3AccumulatedIdsKey) {
        this.s3AccumulatedIdsKey = s3AccumulatedIdsKey;
    }

    public int getSchedulerJitterSeconds() {
        return schedulerJitterSeconds;
    }

    public void setSchedulerJitterSeconds(int schedulerJitterSeconds) {
        this.schedulerJitterSeconds = Math.max(0, schedulerJitterSeconds);
    }

    public Duration getCacheTtl() {
        return cacheTtl;
    }

    public void setCacheTtl(Duration cacheTtl) {
        this.cacheTtl = cacheTtl != null ? cacheTtl : Duration.ofDays(7);
    }

    public Duration getCacheJitter() {
        return cacheJitter;
    }

    public void setCacheJitter(Duration cacheJitter) {
        this.cacheJitter = cacheJitter != null ? cacheJitter : Duration.ofHours(24);
    }
}
