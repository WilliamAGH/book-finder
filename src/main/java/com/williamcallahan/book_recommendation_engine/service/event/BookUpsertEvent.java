package com.williamcallahan.book_recommendation_engine.service.event;

/**
 * Event published when a book has been upserted into Postgres.
 * Used to notify clients on book detail pages that persistence completed.
 */
public class BookUpsertEvent {
    private final String bookId;
    private final String slug;
    private final String title;
    private final boolean isNew;
    private final String context;

    public BookUpsertEvent(String bookId, String slug, String title, boolean isNew, String context) {
        this.bookId = bookId;
        this.slug = slug;
        this.title = title;
        this.isNew = isNew;
        this.context = context;
    }

    public String getBookId() { return bookId; }
    public String getSlug() { return slug; }
    public String getTitle() { return title; }
    public boolean isNew() { return isNew; }
    public String getContext() { return context; }
}
