package com.williamcallahan.book_recommendation_engine.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;

/**
 * Warms up critical caches on application startup to prevent first-request delays.
 * Also provides scheduled refresh to keep cache warm.
 */
@Service
@Slf4j
public class CacheWarmupService {

    private final NewYorkTimesService newYorkTimesService;
    private volatile Disposable bestsellersSubscription;

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
            // Dispose of previous subscription to prevent memory leak
            if (bestsellersSubscription != null && !bestsellersSubscription.isDisposed()) {
                bestsellersSubscription.dispose();
            }

            // Pre-load the most common list
            bestsellersSubscription = newYorkTimesService.getCurrentBestSellersCards("hardcover-fiction", 8)
                .subscribe(
                    list -> log.info("Warmed bestsellers cache with {} bestsellers", list.size()),
                    error -> log.warn("Failed to warm bestsellers cache: {}", error.getMessage())
                );
        } catch (Exception e) {
            log.warn("Exception during bestsellers cache warmup: {}", e.getMessage());
        }
    }
}
