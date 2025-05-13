package com.williamcallahan.book_recommendation_engine.service.image;

public class ImageDetails {
    private final String urlOrPath;
    private final int width;
    private final int height;
    private final boolean dimensionsKnown;

    public ImageDetails(String urlOrPath, int width, int height, boolean dimensionsKnown) {
        this.urlOrPath = urlOrPath;
        this.width = width;
        this.height = height;
        this.dimensionsKnown = dimensionsKnown;
    }

    public ImageDetails(String urlOrPath) {
        this(urlOrPath, 0, 0, false);
    }

    public String getUrlOrPath() {
        return urlOrPath;
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public boolean areDimensionsKnown() {
        return dimensionsKnown;
    }
}
