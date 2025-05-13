package com.williamcallahan.book_recommendation_engine.types;

/**
 * Enum representing available external cover image sources
 */
public enum CoverImageSource {
    ANY("Any source"),
    GOOGLE_BOOKS("Google Books"),
    OPEN_LIBRARY("Open Library"),
    LONGITOOD("Longitood");

    private final String displayName;

    CoverImageSource(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }
}
