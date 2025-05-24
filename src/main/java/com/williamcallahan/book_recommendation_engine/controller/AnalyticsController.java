/**
 * Controller for serving analytics scripts to avoid tracking prevention blocking
 *
 * @author William Callahan
 *
 * Features:
 * - Self-hosts Clicky analytics script to reduce tracking prevention blocks
 * - Caches script content to minimize external requests
 * - Provides fallback when Clicky service is unavailable
 * - Supports conditional loading based on configuration
 */

package com.williamcallahan.book_recommendation_engine.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Controller
@ConditionalOnProperty(name = "app.clicky.enabled", havingValue = "true", matchIfMissing = true)
public class AnalyticsController {

    private static final Logger logger = LoggerFactory.getLogger(AnalyticsController.class);

    @Value("${app.clicky.site-id:101484793}")
    private String clickySiteId;

    @Value("${app.clicky.cache-duration-minutes:60}")
    private int cacheDurationMinutes;

    private final RestTemplate restTemplate;
    private final AsyncTaskExecutor mvcTaskExecutor;
    private final ConcurrentMap<String, CachedScript> scriptCache = new ConcurrentHashMap<>();

    public AnalyticsController(RestTemplate restTemplate,
                               @Qualifier("mvcTaskExecutor") AsyncTaskExecutor mvcTaskExecutor) {
        this.restTemplate = restTemplate;
        this.mvcTaskExecutor = mvcTaskExecutor;
    }

    /**
     * Serves the Clicky analytics script from our domain to avoid tracking prevention
     */
    @GetMapping(value = "/analytics/clicky/{siteId}.js", produces = "application/javascript")
    public CompletableFuture<ResponseEntity<String>> getClickyScript(@PathVariable String siteId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (!clickySiteId.equals(siteId)) {
                    logger.warn("Invalid Clicky site ID requested: {}", siteId);
                    return ResponseEntity.notFound().build();
                }
                String script = getCachedScript(siteId);
                if (script == null) {
                    logger.warn("Failed to load Clicky script for site ID: {}", siteId);
                    return ResponseEntity.notFound().build();
                }
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.valueOf("application/javascript"));
                headers.setCacheControl("public, max-age=" + (cacheDurationMinutes * 60));
                return ResponseEntity.ok()
                        .headers(headers)
                        .body(script);
            } catch (Exception e) {
                logger.error("Error serving Clicky script for site ID: {}", siteId, e);
                return ResponseEntity.internalServerError().build();
            }
        }, mvcTaskExecutor);
    }

    /**
     * Gets the script from cache or fetches it from Clicky if cache is expired
     */
    private String getCachedScript(String siteId) {
        CachedScript cached = scriptCache.get(siteId);
        if (cached != null && !cached.isExpired()) {
            return cached.getScript();
        }
        try {
            String url = "https://static.getclicky.com/" + siteId + ".js";
            logger.debug("Fetching Clicky script from: {}", url);
            String script = restTemplate.getForObject(url, String.class);
            if (script != null && !script.trim().isEmpty()) {
                scriptCache.put(siteId, new CachedScript(script, Duration.ofMinutes(cacheDurationMinutes)));
                logger.debug("Successfully cached Clicky script for site ID: {}", siteId);
                return script;
            }
        } catch (Exception e) {
            logger.error("Failed to fetch Clicky script from external source for site ID: {}", siteId, e);
        }
        if (cached != null) {
            logger.warn("Using expired cached Clicky script for site ID: {}", siteId);
            return cached.getScript();
        }
        return null;
    }

    /**
     * Cache entry for storing script content with expiration
     */
    private static class CachedScript {
        private final String script;
        private final long expirationTime;

        public CachedScript(String script, Duration cacheDuration) {
            this.script = script;
            this.expirationTime = System.currentTimeMillis() + cacheDuration.toMillis();
        }

        public String getScript() {
            return script;
        }

        public boolean isExpired() {
            return System.currentTimeMillis() > expirationTime;
        }
    }
}
