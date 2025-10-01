package com.williamcallahan.book_recommendation_engine.testutil;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;

/** Utility methods for constructing ImageDetails in tests. */
public final class ImageTestData {
    private ImageTestData() {}

    public static ImageDetails localCache(String cacheDirName, String fileName, int width, int height) {
        return new ImageDetails(
                "/" + cacheDirName + "/" + fileName,
                ApplicationConstants.Provider.GOOGLE_BOOKS,
                fileName,
                CoverImageSource.LOCAL_CACHE,
                ImageResolutionPreference.ORIGINAL,
                width,
                height
        );
    }

    public static ImageDetails s3Cache(String publicUrl, String keyOrPath, int width, int height) {
        return new ImageDetails(
                publicUrl,
                "S3_CACHE",
                keyOrPath,
                CoverImageSource.S3_CACHE,
                ImageResolutionPreference.ORIGINAL,
                width,
                height
        );
    }

    public static ImageDetails placeholder(String path) {
        return new ImageDetails(
                path,
                "SYSTEM_PLACEHOLDER",
                "placeholder",
                CoverImageSource.LOCAL_CACHE,
                ImageResolutionPreference.UNKNOWN
        );
    }
}
