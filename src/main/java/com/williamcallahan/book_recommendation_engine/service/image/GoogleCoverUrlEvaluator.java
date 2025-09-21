package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.util.ImageCacheUtils;
import org.springframework.stereotype.Component;

/**
 * Helper for evaluating Google Books cover URLs.
 */
@Component
public class GoogleCoverUrlEvaluator {

    public boolean isAcceptableUrl(String url) {
        return ImageCacheUtils.isLikelyGoogleCoverUrl(url);
    }

    public boolean hasFrontCoverHint(String url) {
        return ImageCacheUtils.hasGoogleFrontCoverHint(url);
    }
}
