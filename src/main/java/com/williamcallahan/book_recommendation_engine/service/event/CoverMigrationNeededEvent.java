package com.williamcallahan.book_recommendation_engine.service.event;

import org.springframework.context.ApplicationEvent;

/**
 * Event published when a book is hydrated from Postgres with an external cover URL
 * but no S3-persisted cover, triggering background S3 migration.
 * 
 * This event is the critical piece for Bug #4 implementation:
 * - PostgresBookRepository.hydrateCover() publishes this event
 * - BookCoverManagementService listens and triggers background S3 upload
 * 
 * @author William Callahan
 */
public class CoverMigrationNeededEvent extends ApplicationEvent {
    
    private final String bookId;
    private final String externalImageUrl;
    private final String sourceHint;
    
    /**
     * Constructs event indicating a book needs cover migration to S3.
     * 
     * @param source The object publishing the event (typically PostgresBookRepository)
     * @param bookId Canonical UUID of the book
     * @param externalImageUrl External cover URL to migrate
     * @param sourceHint Source hint for the external URL (e.g., "GOOGLE_BOOKS")
     */
    public CoverMigrationNeededEvent(Object source, String bookId, String externalImageUrl, String sourceHint) {
        super(source);
        this.bookId = bookId;
        this.externalImageUrl = externalImageUrl;
        this.sourceHint = sourceHint;
    }
    
    public String getBookId() {
        return bookId;
    }
    
    public String getExternalImageUrl() {
        return externalImageUrl;
    }
    
    public String getSourceHint() {
        return sourceHint;
    }
    
    @Override
    public String toString() {
        return "CoverMigrationNeededEvent{" +
                "bookId='" + bookId + '\'' +
                ", externalImageUrl='" + externalImageUrl + '\'' +
                ", sourceHint='" + sourceHint + '\'' +
                '}';
    }
}
