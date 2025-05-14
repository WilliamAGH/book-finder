package com.williamcallahan.book_recommendation_engine.service.event;

public class BookCoverUpdatedEvent {
    private final String identifierKey; // ISBN or Google Book ID used by BookCoverCacheService
    private final String newCoverUrl;
    private final String googleBookId;  // The definitive Google Book ID for the book entity

    public BookCoverUpdatedEvent(String identifierKey, String newCoverUrl, String googleBookId) {
        this.identifierKey = identifierKey;
        this.newCoverUrl = newCoverUrl;
        this.googleBookId = googleBookId;
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

    @Override
    public String toString() {
        return "BookCoverUpdatedEvent{" +
               "identifierKey='" + identifierKey + '\'' +
               ", newCoverUrl='" + newCoverUrl + '\'' +
               ", googleBookId='" + googleBookId + '\'' +
               '}';
    }
}
