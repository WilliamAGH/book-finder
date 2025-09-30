package com.williamcallahan.book_recommendation_engine.util;

import org.slf4j.Logger;

/**
 * Centralized console logging for external API calls during opportunistic backfill/supplementation.
 * 
 * These logs help debug the tiered search flow:
 * - Postgres (primary)
 * - Google Books authenticated (supplement)
 * - Google Books unauthenticated (fallback supplement)
 * - OpenLibrary (final fallback)
 */
public class ExternalApiLogger {
    
    private static final String PREFIX = "[EXTERNAL-API]";
    
    /**
     * Log an external API call attempt
     */
    public static void logApiCallAttempt(Logger log, String apiName, String operation, String query, boolean authenticated) {
        String authType = authenticated ? "AUTHENTICATED" : "UNAUTHENTICATED";
        String message = String.format("%s [%s] %s ATTEMPT: %s for query='%s'", 
            PREFIX, apiName, authType, operation, query);
        log.info(message);
        System.out.println(message);
    }
    
    /**
     * Log an external API call success
     */
    public static void logApiCallSuccess(Logger log, String apiName, String operation, String query, int resultCount) {
        String message = String.format("%s [%s] SUCCESS: %s returned %d result(s) for query='%s'", 
            PREFIX, apiName, operation, resultCount, query);
        log.info(message);
        System.out.println(message);
    }
    
    /**
     * Log an external API call failure
     */
    public static void logApiCallFailure(Logger log, String apiName, String operation, String query, String reason) {
        String message = String.format("%s [%s] FAILURE: %s failed for query='%s' - %s", 
            PREFIX, apiName, operation, query, reason);
        log.warn(message);
        System.err.println(message);
    }
    
    /**
     * Log circuit breaker blocking an API call
     */
    public static void logCircuitBreakerBlocked(Logger log, String apiName, String query) {
        String message = String.format("%s [%s] CIRCUIT-BREAKER-OPEN: Blocking authenticated call for query='%s' (unauthenticated fallback will be attempted)", 
            PREFIX, apiName, query);
        log.info(message);
        System.out.println(message);
    }
    
    /**
     * Log when external API fallback is disabled
     */
    public static void logFallbackDisabled(Logger log, String apiName, String query) {
        String message = String.format("%s [%s] DISABLED: External fallback disabled for query='%s'", 
            PREFIX, apiName, query);
        log.debug(message);
        System.out.println(message);
    }
    
    /**
     * Log the start of a tiered search operation
     */
    public static void logTieredSearchStart(Logger log,
                                            String query,
                                            int postgresResults,
                                            int desiredTotal,
                                            int externalNeeded) {
        String message = String.format(
            "%s [TIERED-SEARCH] START: query='%s', postgresResults=%d, desiredTotal=%d, needFromExternal=%d",
            PREFIX,
            query,
            postgresResults,
            desiredTotal,
            externalNeeded);
        log.info(message);
        System.out.println(message);
    }
    
    /**
     * Log the completion of a tiered search operation
     */
    public static void logTieredSearchComplete(Logger log, String query, int postgresResults, int externalResults, int totalResults) {
        String message = String.format("%s [TIERED-SEARCH] COMPLETE: query='%s', postgresResults=%d, externalResults=%d, totalResults=%d", 
            PREFIX, query, postgresResults, externalResults, totalResults);
        log.info(message);
        System.out.println(message);
    }
    
    /**
     * Log HTTP request details
     */
    public static void logHttpRequest(Logger log, String method, String url, boolean authenticated) {
        String authType = authenticated ? "AUTHENTICATED" : "UNAUTHENTICATED";
        String message = String.format("%s [HTTP] %s %s request to: %s", 
            PREFIX, authType, method, url);
        log.info(message);
        System.out.println(message);
    }
    
    /**
     * Log HTTP response details
     */
    public static void logHttpResponse(Logger log, int statusCode, String url, int bodySize) {
        String message = String.format("%s [HTTP] Response: status=%d, url=%s, bodySize=%d bytes", 
            PREFIX, statusCode, url, bodySize);
        log.info(message);
        System.out.println(message);
    }
    
    /**
     * Log stream processing
     */
    public static void logStreamProgress(Logger log, String operation, int currentCount, int targetCount) {
        String message = String.format("%s [STREAM] %s: %d/%d items processed", 
            PREFIX, operation, currentCount, targetCount);
        log.debug(message);
        System.out.println(message);
    }
    /**
     * Log start of a background hydration attempt for a specific context (search, recommendations, detail, etc.)
     */
    public static void logHydrationStart(Logger log, String context, String identifier, String correlationId) {
        String correlation = correlationId != null && !correlationId.isBlank()
            ? ", correlation='" + correlationId + "'"
            : "";
        String message = String.format("%s [HYDRATION] %s START: identifier='%s'%s",
            PREFIX,
            context,
            identifier,
            correlation);
        log.info(message);
        System.out.println(message);
    }

    /**
     * Log success of a hydration attempt, including the canonical identifier that was materialized.
     */
    public static void logHydrationSuccess(Logger log, String context, String identifier, String canonicalId, String tier) {
        String tierInfo = tier != null && !tier.isBlank()
            ? ", tier='" + tier + "'"
            : "";
        String message = String.format("%s [HYDRATION] %s SUCCESS: identifier='%s', canonicalId='%s'%s",
            PREFIX,
            context,
            identifier,
            canonicalId,
            tierInfo);
        log.info(message);
        System.out.println(message);
    }

    /**
     * Log failure of a hydration attempt.
     */
    public static void logHydrationFailure(Logger log, String context, String identifier, String reason) {
        String message = String.format("%s [HYDRATION] %s FAILURE: identifier='%s' - %s",
            PREFIX,
            context,
            identifier,
            reason);
        log.warn(message);
        System.err.println(message);
    }

    /**
     * Log persistence success (upsert) for a canonical book record.
     */
    public static void logPersistenceSuccess(Logger log, String context, String canonicalId, boolean created, String title) {
        String state = created ? "CREATED" : "UPDATED";
        String safeTitle = title != null ? title : "";
        String message = String.format("%s [PERSISTENCE] %s %s: canonicalId='%s', title='%s'",
            PREFIX,
            context,
            state,
            canonicalId,
            safeTitle);
        log.info(message);
        System.out.println(message);
    }

    /**
     * Log persistence failure for a canonical book record.
     */
    public static void logPersistenceFailure(Logger log, String context, String identifier, String reason) {
        String message = String.format("%s [PERSISTENCE] %s FAILURE: identifier='%s' - %s",
            PREFIX,
            context,
            identifier,
            reason);
        log.warn(message);
        System.err.println(message);
    }

}
