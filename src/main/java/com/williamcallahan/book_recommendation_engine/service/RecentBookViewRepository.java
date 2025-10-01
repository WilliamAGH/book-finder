package com.williamcallahan.book_recommendation_engine.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;

/**
 * Repository abstraction for persisting and aggregating recent book view activity.
 *
 * <p>Stores a row for every view in {@code recent_book_views} and exposes aggregated
 * counters that power the homepage and other analytics surfaces.</p>
 */
@Service
@Slf4j
public class RecentBookViewRepository {


    private final JdbcTemplate jdbcTemplate;

    public RecentBookViewRepository(@Nullable JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Indicates whether persistence is available (i.e., {@link JdbcTemplate} is configured).
     */
    public boolean isEnabled() {
        return jdbcTemplate != null;
    }

    /**
     * Records a single book view asynchronously. Safe to invoke even when the repository is disabled.
     * Uses @Async to prevent blocking the main request thread.
     *
     * @param canonicalBookId Canonical UUID string for the book that was viewed
     * @param viewedAt         Timestamp for the view (defaults to {@link Instant#now()} when null)
     * @param source           Optional source label (e.g., "web", "api")
     */
    @Async
    public void recordView(String canonicalBookId, @Nullable Instant viewedAt, @Nullable String source) {
        if (!isEnabled() || ValidationUtils.isNullOrBlank(canonicalBookId)) {
            return;
        }

        Instant effectiveInstant = viewedAt != null ? viewedAt : Instant.now();

        try {
            jdbcTemplate.update(
                    "INSERT INTO recent_book_views (book_id, viewed_at, source) VALUES (?, ?, ?)",
                    ps -> {
                        ps.setString(1, canonicalBookId);
                        ps.setTimestamp(2, Timestamp.from(effectiveInstant));
                        if (ValidationUtils.isNullOrBlank(source)) {
                            ps.setNull(3, java.sql.Types.VARCHAR);
                        } else {
                            ps.setString(3, source);
                        }
                    }
            );
        } catch (Exception ex) {
            log.debug("Failed to record recent view for book {}: {}", canonicalBookId, ex.getMessage());
        }
    }

    /**
     * Fetches aggregate view statistics for a single book over standard windows.
     */
    public Optional<ViewStats> fetchStatsForBook(String canonicalBookId) {
        if (!isEnabled() || ValidationUtils.isNullOrBlank(canonicalBookId)) {
            return Optional.empty();
        }

        String sql = """
                SELECT book_id,
                       MAX(viewed_at) AS last_viewed_at,
                       COUNT(*) FILTER (WHERE viewed_at >= now() - INTERVAL '24 hours') AS views_24h,
                       COUNT(*) FILTER (WHERE viewed_at >= now() - INTERVAL '7 days') AS views_7d,
                       COUNT(*) FILTER (WHERE viewed_at >= now() - INTERVAL '30 days') AS views_30d
                FROM recent_book_views
                WHERE book_id = ?
                  AND viewed_at >= now() - INTERVAL '30 days'
                GROUP BY book_id
                """;

        try {
            return jdbcTemplate.query(sql, ps -> ps.setString(1, canonicalBookId), rs -> {
                if (!rs.next()) {
                    return Optional.<ViewStats>empty();
                }
                Timestamp timestamp = rs.getTimestamp("last_viewed_at");
                Instant lastViewed = timestamp != null ? timestamp.toInstant() : Instant.EPOCH;
                return Optional.of(new ViewStats(
                        rs.getString("book_id"),
                        lastViewed,
                        rs.getLong("views_24h"),
                        rs.getLong("views_7d"),
                        rs.getLong("views_30d")
                ));
            });
        } catch (Exception ex) {
            log.debug("Failed to fetch view stats for book {}: {}", canonicalBookId, ex.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Returns the most recently viewed books (unique by canonical ID) ordered by their last view timestamps.
     * Includes rolling counters for 24h/7d/30d windows.
     */
    public List<ViewStats> fetchMostRecentViews(int limit) {
        if (!isEnabled() || limit <= 0) {
            return Collections.emptyList();
        }

        String sql = """
                SELECT rv.book_id,
                       rv.last_viewed_at,
                       rv.views_24h,
                       rv.views_7d,
                       rv.views_30d
                FROM (
                    SELECT book_id,
                           MAX(viewed_at) AS last_viewed_at,
                           COUNT(*) FILTER (WHERE viewed_at >= now() - INTERVAL '24 hours') AS views_24h,
                           COUNT(*) FILTER (WHERE viewed_at >= now() - INTERVAL '7 days') AS views_7d,
                           COUNT(*) FILTER (WHERE viewed_at >= now() - INTERVAL '30 days') AS views_30d
                    FROM recent_book_views
                    WHERE viewed_at >= now() - INTERVAL '30 days'
                    GROUP BY book_id
                ) rv
                ORDER BY rv.last_viewed_at DESC
                LIMIT ?
                """;

        try {
            return jdbcTemplate.query(sql, ps -> ps.setInt(1, limit), (rs, rowNum) -> {
                Timestamp timestamp = rs.getTimestamp("last_viewed_at");
                Instant lastViewed = timestamp != null ? timestamp.toInstant() : Instant.EPOCH;
                return new ViewStats(
                        rs.getString("book_id"),
                        lastViewed,
                        rs.getLong("views_24h"),
                        rs.getLong("views_7d"),
                        rs.getLong("views_30d")
                );
            });
        } catch (Exception ex) {
            log.debug("Failed to fetch recent view aggregates: {}", ex.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Aggregated counters for a single book across different time windows.
     */
    public record ViewStats(String bookId,
                            Instant lastViewedAt,
                            long viewsLast24h,
                            long viewsLast7d,
                            long viewsLast30d) {
    }
}
