package com.williamcallahan.book_recommendation_engine.types;

public class CoverImages {
    private String preferredUrl;
    private String fallbackUrl;

    public CoverImages() {
    }

    public CoverImages(String preferredUrl, String fallbackUrl) {
        this.preferredUrl = preferredUrl;
        this.fallbackUrl = fallbackUrl;
    }

    public String getPreferredUrl() {
        return preferredUrl;
    }

    public void setPreferredUrl(String preferredUrl) {
        this.preferredUrl = preferredUrl;
    }

    public String getFallbackUrl() {
        return fallbackUrl;
    }

    public void setFallbackUrl(String fallbackUrl) {
        this.fallbackUrl = fallbackUrl;
    }
} 