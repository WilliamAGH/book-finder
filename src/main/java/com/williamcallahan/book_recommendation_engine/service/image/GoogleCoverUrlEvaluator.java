package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.util.GoogleBooksUrlEnhancer;
import org.springframework.stereotype.Component;

/**
 * Helper for evaluating Google Books cover URLs.
 */
@Component
public class GoogleCoverUrlEvaluator {

    public boolean isAcceptableUrl(String url) {
        return GoogleBooksUrlEnhancer.isGoogleBooksUrl(url);
    }

    public boolean hasFrontCoverHint(String url) {
        return GoogleBooksUrlEnhancer.hasFrontCoverHint(url);
    }
}
