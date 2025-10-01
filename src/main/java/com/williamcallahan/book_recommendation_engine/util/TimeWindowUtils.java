package com.williamcallahan.book_recommendation_engine.util;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for time window calculations and time-based metrics.
 * Centralizes common time window patterns used in monitoring and scheduling.
 */
public final class TimeWindowUtils {

    private TimeWindowUtils() {
        // Utility class
    }

    /**
     * Checks if a given timestamp is within a time window from now.
     *
     * @param timestamp the timestamp to check
     * @param windowDuration the duration of the window
     * @return true if the timestamp is within the window
     */
    public static boolean isWithinWindow(Instant timestamp, Duration windowDuration) {
        if (timestamp == null || windowDuration == null) {
            return false;
        }
        Instant now = Instant.now();
        Instant windowStart = now.minus(windowDuration);
        return timestamp.isAfter(windowStart) && !timestamp.isAfter(now);
    }

    /**
     * Checks if a given timestamp is within the last N minutes.
     *
     * @param timestamp the timestamp to check
     * @param minutes the number of minutes
     * @return true if the timestamp is within the last N minutes
     */
    public static boolean isWithinLastMinutes(Instant timestamp, long minutes) {
        return isWithinWindow(timestamp, Duration.ofMinutes(minutes));
    }

    /**
     * Checks if a given timestamp is within the last N hours.
     *
     * @param timestamp the timestamp to check
     * @param hours the number of hours
     * @return true if the timestamp is within the last N hours
     */
    public static boolean isWithinLastHours(Instant timestamp, long hours) {
        return isWithinWindow(timestamp, Duration.ofHours(hours));
    }

    /**
     * Calculates the start of the current hour.
     *
     * @return the start of the current hour
     */
    public static Instant getCurrentHourStart() {
        return Instant.now().truncatedTo(ChronoUnit.HOURS);
    }

    /**
     * Calculates the start of the current day.
     *
     * @return the start of the current day
     */
    public static Instant getCurrentDayStart() {
        return Instant.now().truncatedTo(ChronoUnit.DAYS);
    }

    /**
     * Calculates the start of the current week.
     *
     * @return the start of the current week
     */
    public static Instant getCurrentWeekStart() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime weekStart = now.minusDays(now.getDayOfWeek().getValue() - 1)
            .truncatedTo(ChronoUnit.DAYS);
        return weekStart.atZone(ZoneId.systemDefault()).toInstant();
    }

    /**
     * Calculates the start of the current month.
     *
     * @return the start of the current month
     */
    public static Instant getCurrentMonthStart() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime monthStart = now.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
        return monthStart.atZone(ZoneId.systemDefault()).toInstant();
    }

    /**
     * Checks if two timestamps are in the same hour.
     *
     * @param timestamp1 the first timestamp
     * @param timestamp2 the second timestamp
     * @return true if both timestamps are in the same hour
     */
    public static boolean isSameHour(Instant timestamp1, Instant timestamp2) {
        if (timestamp1 == null || timestamp2 == null) {
            return false;
        }
        return timestamp1.truncatedTo(ChronoUnit.HOURS)
            .equals(timestamp2.truncatedTo(ChronoUnit.HOURS));
    }

    /**
     * Checks if two timestamps are on the same day.
     *
     * @param timestamp1 the first timestamp
     * @param timestamp2 the second timestamp
     * @return true if both timestamps are on the same day
     */
    public static boolean isSameDay(Instant timestamp1, Instant timestamp2) {
        if (timestamp1 == null || timestamp2 == null) {
            return false;
        }
        return timestamp1.truncatedTo(ChronoUnit.DAYS)
            .equals(timestamp2.truncatedTo(ChronoUnit.DAYS));
    }

    /**
     * Calculates the time elapsed since a given timestamp.
     *
     * @param timestamp the timestamp
     * @return the duration elapsed since the timestamp
     */
    public static Duration getElapsedTime(Instant timestamp) {
        if (timestamp == null) {
            return Duration.ZERO;
        }
        return Duration.between(timestamp, Instant.now());
    }

    /**
     * Calculates the time remaining until a given timestamp.
     *
     * @param timestamp the future timestamp
     * @return the duration until the timestamp, or ZERO if in the past
     */
    public static Duration getTimeUntil(Instant timestamp) {
        if (timestamp == null) {
            return Duration.ZERO;
        }
        Duration duration = Duration.between(Instant.now(), timestamp);
        return duration.isNegative() ? Duration.ZERO : duration;
    }

    /**
     * Creates a sliding window counter key based on current time.
     * Useful for creating time-bucketed metrics.
     *
     * @param prefix the key prefix
     * @param bucketSizeMinutes the size of each bucket in minutes
     * @return the time-bucketed key
     */
    public static String createTimeWindowKey(String prefix, int bucketSizeMinutes) {
        long currentMinute = Instant.now().toEpochMilli() / (60000L * bucketSizeMinutes);
        return prefix + "_" + currentMinute;
    }

    /**
     * Calculates the number of time windows between two timestamps.
     *
     * @param start the start timestamp
     * @param end the end timestamp
     * @param windowSize the size of each window
     * @return the number of complete windows
     */
    public static long countWindows(Instant start, Instant end, Duration windowSize) {
        if (start == null || end == null || windowSize == null || windowSize.isZero()) {
            return 0;
        }
        Duration duration = Duration.between(start, end);
        return duration.toMillis() / windowSize.toMillis();
    }

    /**
     * Checks if a time window boundary has been crossed between two timestamps.
     *
     * @param previous the previous timestamp
     * @param current the current timestamp
     * @param windowUnit the window unit (e.g., HOURS, DAYS)
     * @return true if a boundary was crossed
     */
    public static boolean hasWindowBoundaryCrossed(Instant previous, Instant current, ChronoUnit windowUnit) {
        if (previous == null || current == null) {
            return true;
        }
        Instant previousWindow = previous.truncatedTo(windowUnit);
        Instant currentWindow = current.truncatedTo(windowUnit);
        return !previousWindow.equals(currentWindow);
    }

    /**
     * Converts milliseconds to a human-readable duration string.
     *
     * @param millis the duration in milliseconds
     * @return human-readable string (e.g., "2h 15m 30s")
     */
    public static String formatDuration(long millis) {
        if (millis < 0) {
            return "0s";
        }

        long hours = TimeUnit.MILLISECONDS.toHours(millis);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(millis) % 60;
        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis) % 60;

        StringBuilder result = new StringBuilder();
        if (hours > 0) {
            result.append(hours).append("h ");
        }
        if (minutes > 0) {
            result.append(minutes).append("m ");
        }
        if (seconds > 0 || result.length() == 0) {
            result.append(seconds).append("s");
        }

        return result.toString().trim();
    }
}