package com.williamcallahan.book_recommendation_engine.dto;

/**
 * Projection representing a recommended book card along with scoring context.
 */
public record RecommendationCard(BookCard card, Double score, String reason) {
}

