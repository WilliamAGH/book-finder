package com.williamcallahan.book_recommendation_engine.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Warms up critical caches on application startup to prevent first-request delays.
 * Also provides scheduled refresh to keep cache warm.
 */
@Service
@Slf4j
public class CacheWarmupService {

    private final NewYorkTimesService newYorkTimesService;

    public CacheWarmupService(NewYorkTimesService newYorkTimesService) {
        this.newYorkTimesService = newYorkTimesService;
    }

    /**
     * Warm cache when application is ready.
     * Runs asynchronously to not delay startup.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void warmupCachesOnStartup() {
        log.info("Starting cache warmup...");
        
        // Warm NYT bestsellers cache
        warmupBestsellersCache();
        
        log.info("Cache warmup completed");
    }

    /**
     * Refresh bestsellers cache every 15 minutes to keep it warm.
     */
    @Scheduled(fixedDelayString = "${app.cache.warmup.bestsellers.refresh-interval:900000}") // 15 minutes
    public void scheduledBestsellersRefresh() {
        log.debug("Refreshing bestsellers cache (scheduled)");
        warmupBestsellersCache();
    }

    private void warmupBestsellersCache() {
        try {
            // Pre-load the most common list
            newYorkTimesService.getCurrentBestSellers("hardcover-fiction", 8)
                .subscribe(
                    list -> log.info("Warmed bestsellers cache with {} books", list.size()),
                    error -> log.warn("Failed to warm bestsellers cache: {}", error.getMessage())
                );
        } catch (Exception e) {
            log.warn("Exception during bestsellers cache warmup: {}", e.getMessage());
        }
    }
}