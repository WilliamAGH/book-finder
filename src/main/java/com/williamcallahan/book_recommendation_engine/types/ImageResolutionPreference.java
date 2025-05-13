package com.williamcallahan.book_recommendation_engine.types;

/**
 * Enum representing available image resolution preferences
 */
public enum ImageResolutionPreference {
    ANY("Any Resolution"),
    HIGH_ONLY("High Resolution Only"),
    HIGH_FIRST("High Resolution First");

    private final String displayName;

    ImageResolutionPreference(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }
}
