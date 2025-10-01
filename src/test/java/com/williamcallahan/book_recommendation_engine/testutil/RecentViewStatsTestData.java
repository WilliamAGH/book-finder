package com.williamcallahan.book_recommendation_engine.testutil;

import com.williamcallahan.book_recommendation_engine.service.RecentBookViewRepository;

import java.time.Instant;

/** Helpers to create RecentBookViewRepository.ViewStats instances for tests. */
public final class RecentViewStatsTestData {
    private RecentViewStatsTestData() {}

    public static RecentBookViewRepository.ViewStats viewStats(String bookId,
                                                               Instant lastViewed,
                                                               long views24h,
                                                               long views7d,
                                                               long views30d) {
        return new RecentBookViewRepository.ViewStats(bookId, lastViewed, views24h, views7d, views30d);
        
    }
}