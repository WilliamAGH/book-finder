/**
 * Interface defining a standard caching strategy for external API data
 * - Provides a consistent approach to multi-level caching
 * - Enforces standard cache-first patterns across services
 * - Allows different implementations for different types of data
 * - Defines clear flow from fastest to slowest caches
 * - Helps minimize external API calls
 *
 * @author William Callahan
 * @param <K> The type of key used for cache lookups
 * @param <V> The type of value stored in the cache
 */

package com.williamcallahan.book_recommendation_engine.service;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture; // Added import
import reactor.core.publisher.Mono;
import java.util.Optional;

public interface CachingStrategy<K, V> {
    
    /**
     * Defines the order of caches to check when retrieving data
     */
    enum CacheLayer {
        MEMORY,     // Fastest, in-memory cache
        LOCAL_DISK, // Local filesystem cache (dev/test environments)
        SPRING,     // Spring CacheManager
        DATABASE,   // Persistent database cache
        S3,         // Remote S3 storage
        MOCK,       // Mock data for testing
        API         // External API (slowest, last resort)
    }

    /**
     * Retrieves a value from the cache or external source using the standard caching flow
     * 
     * @param key The key to look up
     * @return CompletionStage that will complete with the value wrapped in Optional if found, or empty Optional if not found
     */
    CompletionStage<Optional<V>> get(K key);
    
    /**
     * Retrieves a value from the cache or external source using reactive programming
     * 
     * @param key The key to look up
     * @return Mono that will emit the value if found, or empty if not found
     */
    Mono<V> getReactive(K key);
    
    /**
     * Puts a value into all appropriate cache layers
     * 
     * @param key The key to associate with the value
     * @param value The value to cache
     * @return CompletionStage that completes when the operation is done
     */
    CompletionStage<Void> put(K key, V value);
    
    /**
     * Puts a value into all appropriate cache layers using reactive programming
     * 
     * @param key The key to associate with the value
     * @param value The value to cache
     * @return Mono that completes when the operation is done
     */
    Mono<Void> putReactive(K key, V value);
    
    /**
     * Removes a value from all cache layers
     * 
     * @param key The key to remove
     * @return CompletionStage that completes when the operation is done
     */
    CompletionStage<Void> evict(K key);
    
    /**
     * Checks if a value exists in any cache layer
     * 
     * @param key The key to check
     * @return CompletableFuture<Boolean> true if the value exists in any cache layer, false otherwise
     */
    CompletableFuture<Boolean> existsAsync(K key);
    
    /**
     * Gets a value from cache only, without fetching from external source
     * 
     * @param key The key to look up
     * @return CompletableFuture<Optional<V>> containing the value if found in any cache, empty otherwise
     */
    CompletableFuture<Optional<V>> getFromCacheOnlyAsync(K key);
    
    /**
     * Clears all cache layers
     * 
     * @return CompletionStage that completes when the operation is done
     */
    CompletionStage<Void> clearAll();
}
