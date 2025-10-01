/**
 * Event fired when a book cover image is updated
 *
 * @author William Callahan
 *
 * Features:
 * - Carries information about updated book cover images
 * - Used for cross-service notifications
 * - Contains metadata for cache invalidation
 * - Includes image source tracking for attribution
 * - Supports WebSocket notifications to UI
 */
package com.williamcallahan.book_recommendation_engine.service.event;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;

/**
 * Event class representing a book cover image update
 * - Provides notification when cover images change
 * - Contains both identifier and image URL information
 * - Tracks image source for attribution
 * - Used to trigger cache updates and WebSocket notifications
 */
public class BookCoverUpdatedEvent {
    /**
     * ISBN or Google Book ID used by BookCoverCacheService
     * - Primary identifier for lookup in caching system
     * - Can be ISBN-10, ISBN-13, or Google Book ID 
     */
    private final String identifierKey;
    
    /**
     * URL to the updated cover image
     * - Points to new image location
     * - Used to update UIs and caches
     */
    private final String newCoverUrl;
    
    /**
     * The definitive Google Book ID for the book entity
     * - Primary key for Google Books API
     * - Used for consistent book identification
     */
    private final String googleBookId;
    
    /**
     * Source provider of the cover image
     * - Tracks origin of image for attribution
     * - Helps prioritize image sources
     */
    private final CoverImageSource source;

    /**
     * Constructor with basic required fields
     * - Creates event with undefined source
     * 
     * @param identifierKey Book identifier key (ISBN or ID)
     * @param newCoverUrl URL to the new cover image
     * @param googleBookId Google Books API identifier
     */
    public BookCoverUpdatedEvent(String identifierKey, String newCoverUrl, String googleBookId) {
        this.identifierKey = identifierKey;
        this.newCoverUrl = newCoverUrl;
        this.googleBookId = googleBookId;
        this.source = CoverImageSource.UNDEFINED; // Default if not provided
    }

    /**
     * Constructor with source information
     * - Creates event with specific image source
     * - Provides full image provenance
     * 
     * @param identifierKey Book identifier key (ISBN or ID)
     * @param newCoverUrl URL to the new cover image
     * @param googleBookId Google Books API identifier
     * @param source Source provider of the image
     */
    public BookCoverUpdatedEvent(String identifierKey, String newCoverUrl, String googleBookId, CoverImageSource source) {
        this.identifierKey = identifierKey;
        this.newCoverUrl = newCoverUrl;
        this.googleBookId = googleBookId;
        this.source = source != null ? source : CoverImageSource.UNDEFINED;
    }

    /**
     * Get the book identifier key
     * 
     * @return Book identifier key (ISBN or ID)
     */
    public String getIdentifierKey() {
        return identifierKey;
    }

    /**
     * Get the URL to the new cover image
     * 
     * @return URL to the new cover image
     */
    public String getNewCoverUrl() {
        return newCoverUrl;
    }

    /**
     * Get the Google Books API identifier
     * 
     * @return Google Books ID
     */
    public String getGoogleBookId() {
        return googleBookId;
    }

    /**
     * Get the source provider of the image
     * 
     * @return Cover image source provider
     */
    public CoverImageSource getSource() {
        return source;
    }

    /**
     * Generate string representation of event
     * - Used for logging and debugging
     * - Includes all event properties
     * 
     * @return String representation of the event
     */
    @Override
    public String toString() {
        return "BookCoverUpdatedEvent{" +
               "identifierKey='" + identifierKey + '\'' +
               ", newCoverUrl='" + newCoverUrl + '\'' +
               ", googleBookId='" + googleBookId + '\'' +
               ", source=" + (source != null ? source.name() : "null") +
               '}';
    }
}
