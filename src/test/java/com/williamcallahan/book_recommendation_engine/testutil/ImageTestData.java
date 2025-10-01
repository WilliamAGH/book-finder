package com.williamcallahan.book_recommendation_engine.testutil;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;

/** Utility methods for constructing ImageDetails in tests. */
public final class ImageTestData {
    private ImageTestData() {}

    public static ImageDetails localCache(String cacheDirName, String fileName, int width, int height) {
        ImageDetails details = new ImageDetails(
                "/" + cacheDirName + "/" + fileName,
                ApplicationConstants.Provider.GOOGLE_BOOKS,
                fileName,
                CoverImageSource.GOOGLE_BOOKS,  // Use actual data source, not cache
                ImageResolutionPreference.ORIGINAL,
                width,
                height
        );
        details.setStorageLocation(ImageDetails.STORAGE_LOCAL);  // Track storage separately
        details.setStorageKey(fileName);
        return details;
    }

    public static ImageDetails s3Cache(String publicUrl, String keyOrPath, int width, int height) {
        ImageDetails details = new ImageDetails(
                publicUrl,
                "GOOGLE_BOOKS",  // Use actual data source
                keyOrPath,
                CoverImageSource.GOOGLE_BOOKS,  // Not S3_CACHE
                ImageResolutionPreference.ORIGINAL,
                width,
                height
        );
        details.setStorageLocation(ImageDetails.STORAGE_S3);  // Track storage separately
        details.setStorageKey(keyOrPath);
        return details;
    }

    public static ImageDetails placeholder(String path) {
        ImageDetails details = new ImageDetails(
                path,
                "SYSTEM_PLACEHOLDER",
                "placeholder",
                CoverImageSource.NONE,  // Use NONE, not LOCAL_CACHE
                ImageResolutionPreference.UNKNOWN
        );
        details.setStorageLocation(ImageDetails.STORAGE_LOCAL);  // Placeholders stored locally
        return details;
    }
}
