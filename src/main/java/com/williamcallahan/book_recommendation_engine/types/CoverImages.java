package com.williamcallahan.book_recommendation_engine.types;

public class CoverImages {
    private String preferredUrl;
    private String fallbackUrl;
    private CoverImageSource source;

    public CoverImages() {
        this.source = CoverImageSource.UNDEFINED;
    }

    public CoverImages(String preferredUrl, String fallbackUrl) {
        this.preferredUrl = preferredUrl;
        this.fallbackUrl = fallbackUrl;
        this.source = CoverImageSource.UNDEFINED; // Default, should be set explicitly by services
    }

    public CoverImages(String preferredUrl, String fallbackUrl, CoverImageSource source) {
        this.preferredUrl = preferredUrl;
        this.fallbackUrl = fallbackUrl;
        this.source = source;
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

    public CoverImageSource getSource() {
        return source;
    }

    public void setSource(CoverImageSource source) {
        this.source = source;
    }
}
