package com.williamcallahan.book_recommendation_engine.types;

public enum ImageSourceName {
    GOOGLE_BOOKS("Google Books API"),
    OPEN_LIBRARY("OpenLibrary"),
    LONGITOOD("Longitood"),
    LOCAL_CACHE("LocalCache"),
    S3_CACHE("S3 Cache"),
    INTERNAL_PROCESSING("Internal Processing"),
    UNKNOWN("Unknown");

    private final String displayName;

    ImageSourceName(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }
} 