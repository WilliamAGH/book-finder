/**
 * Event fired when new search results are available from background API calls
 * Used to notify WebSocket clients of additional search results as they arrive
 *
 * @author William Callahan
 *
 * Features:
 * - Carries incremental search results from background API calls
 * - Includes source information for result attribution
 * - Supports progressive result loading in UI
 * - Integrates with existing WebSocket notification infrastructure
 */
package com.williamcallahan.book_recommendation_engine.service.event;

import com.williamcallahan.book_recommendation_engine.model.Book;

import java.util.List;

public class SearchResultsUpdatedEvent {
    
    private final String searchQuery;
    private final List<Book> newResults;
    private final String source; // "GOOGLE_BOOKS" or "OPEN_LIBRARY"
    private final int totalResultsNow;
    private final String queryHash; // For WebSocket topic routing
    private final boolean isComplete; // Whether this source has finished searching
    
    /**
     * Constructs a SearchResultsUpdatedEvent
     *
     * @param searchQuery The original search query
     * @param newResults List of new books found
     * @param source The API source that provided these results
     * @param totalResultsNow Total number of results available now
     * @param queryHash Hash of the query for WebSocket routing
     * @param isComplete Whether this source has finished searching
     */
    public SearchResultsUpdatedEvent(String searchQuery, List<Book> newResults, String source, 
                                   int totalResultsNow, String queryHash, boolean isComplete) {
        this.searchQuery = searchQuery;
        this.newResults = newResults;
        this.source = source;
        this.totalResultsNow = totalResultsNow;
        this.queryHash = queryHash;
        this.isComplete = isComplete;
    }
    
    public String getSearchQuery() {
        return searchQuery;
    }
    
    public List<Book> getNewResults() {
        return newResults;
    }
    
    public String getSource() {
        return source;
    }
    
    public int getTotalResultsNow() {
        return totalResultsNow;
    }
    
    public String getQueryHash() {
        return queryHash;
    }
    
    public boolean isComplete() {
        return isComplete;
    }
}
