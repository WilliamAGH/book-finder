/**
 * Event fired to notify clients about search progress and status updates
 * Used for showing loading indicators and status messages in the UI
 *
 * @author William Callahan
 *
 * Features:
 * - Provides real-time search status updates
 * - Supports beautiful loading indicators in UI
 * - Tracks progress across multiple API sources
 * - Integrates with existing WebSocket infrastructure
 */
package com.williamcallahan.book_recommendation_engine.service.event;

public class SearchProgressEvent {
    
    public enum SearchStatus {
        STARTING,           // Search is beginning
        SEARCHING_CACHE,    // Checking cache layers
        SEARCHING_GOOGLE,   // Searching Google Books API
        SEARCHING_OPENLIBRARY, // Searching OpenLibrary API
        RATE_LIMITED,       // Hit rate limit, trying alternative
        DEDUPLICATING,      // Processing and deduplicating results
        COMPLETE,           // All searches finished
        ERROR               // Error occurred
    }
    
    private final String searchQuery;
    private final SearchStatus status;
    private final String message;
    private final String queryHash; // For WebSocket topic routing
    private final String source; // Which API source this status refers to
    
    /**
     * Constructs a SearchProgressEvent
     *
     * @param searchQuery The original search query
     * @param status Current search status
     * @param message Human-readable status message
     * @param queryHash Hash of the query for WebSocket routing
     * @param source The API source this status refers to (optional)
     */
    public SearchProgressEvent(String searchQuery, SearchStatus status, String message, 
                             String queryHash, String source) {
        this.searchQuery = searchQuery;
        this.status = status;
        this.message = message;
        this.queryHash = queryHash;
        this.source = source;
    }
    
    /**
     * Convenience constructor without source
     */
    public SearchProgressEvent(String searchQuery, SearchStatus status, String message, String queryHash) {
        this(searchQuery, status, message, queryHash, null);
    }
    
    public String getSearchQuery() {
        return searchQuery;
    }
    
    public SearchStatus getStatus() {
        return status;
    }
    
    public String getMessage() {
        return message;
    }
    
    public String getQueryHash() {
        return queryHash;
    }
    
    public String getSource() {
        return source;
    }
}
