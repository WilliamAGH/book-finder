/**
 * Event model for asynchronous book data enrichment operations
 * Represents enrichment data updates for various book attributes
 * Used in reactive streams for non-blocking UI updates
 *
 * @author William Callahan
 *
 * Features:
 * - Generic event structure for multiple enrichment types
 * - Supports covers, editions, affiliate links, and similar books
 * - Designed for WebSocket/SSE streaming to frontend
 * - Lightweight payload model for efficient data transfer
 */

package com.williamcallahan.book_recommendation_engine.types;

/**
 * Enrichment event for book data updates
 */
public class EnrichmentEvent {
    /** Type of enrichment (e.g. "cover", "editions", "affiliateLinks", "similar") */
    private String type;
    
    /** Book ID this enrichment applies to */
    private String id;
    
    /** Enrichment data payload (varies by type) */
    private Object payload;

    /**
     * Default constructor for JSON deserialization
     */
    public EnrichmentEvent() {
    }

    /**
     * Creates enrichment event with specified attributes
     *
     * @param type event type identifier
     * @param id book ID this event relates to
     * @param payload enrichment data
     */
    public EnrichmentEvent(String type, String id, Object payload) {
        this.type = type;
        this.id = id;
        this.payload = payload;
    }

    /**
     * Gets the enrichment event type
     *
     * @return event type identifier
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the enrichment event type
     *
     * @param type event type identifier
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Gets the book ID this event relates to
     *
     * @return book identifier
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the book ID this event relates to
     *
     * @param id book identifier
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the enrichment data payload
     *
     * @return enrichment data (type-specific)
     */
    public Object getPayload() {
        return payload;
    }

    /**
     * Sets the enrichment data payload
     *
     * @param payload enrichment data (type-specific)
     */
    public void setPayload(Object payload) {
        this.payload = payload;
    }
}
