package com.williamcallahan.book_recommendation_engine.service.event;

public class BookCoverUpdatedEvent {
    private final String identifierKey; // ISBN or Google Book ID used by BookCoverCacheService
    private final String newCoverUrl;
    private final String googleBookId;  // The definitive Google Book ID for the book entity
    private final com.williamcallahan.book_recommendation_engine.types.CoverImageSource source; // Added source

    public BookCoverUpdatedEvent(String identifierKey, String newCoverUrl, String googleBookId) {
        this.identifierKey = identifierKey;
        this.newCoverUrl = newCoverUrl;
        this.googleBookId = googleBookId;
        this.source = com.williamcallahan.book_recommendation_engine.types.CoverImageSource.UNDEFINED; // Default if not provided
    }

    public BookCoverUpdatedEvent(String identifierKey, String newCoverUrl, String googleBookId, com.williamcallahan.book_recommendation_engine.types.CoverImageSource source) {
        this.identifierKey = identifierKey;
        this.newCoverUrl = newCoverUrl;
        this.googleBookId = googleBookId;
        this.source = source != null ? source : com.williamcallahan.book_recommendation_engine.types.CoverImageSource.UNDEFINED;
    }

    public String getIdentifierKey() {
        return identifierKey;
    }

    public String getNewCoverUrl() {
        return newCoverUrl;
    }

    public String getGoogleBookId() {
        return googleBookId;
    }

    public com.williamcallahan.book_recommendation_engine.types.CoverImageSource getSource() {
        return source;
    }

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
