package com.williamcallahan.book_recommendation_engine.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * Utility class for common date parsing patterns across the application.
 * Centralizes date parsing logic to eliminate duplication.
 */
public final class DateParsingUtils {

    // Common date formats used across the application
    private static final List<SimpleDateFormat> COMMON_DATE_FORMATS = List.of(
        new SimpleDateFormat("yyyy-MM-dd"),
        new SimpleDateFormat("yyyy-MM"),
        new SimpleDateFormat("yyyy"),
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"),
        new SimpleDateFormat("MMM dd, yyyy", Locale.ENGLISH),
        new SimpleDateFormat("MMMM dd, yyyy", Locale.ENGLISH),
        new SimpleDateFormat("dd/MM/yyyy"),
        new SimpleDateFormat("MM/dd/yyyy")
    );

    private static final DateTimeFormatter ISO_LOCAL_DATE = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter ISO_DATE_TIME = DateTimeFormatter.ISO_DATE_TIME;

    private DateParsingUtils() {
        // Utility class
    }

    /**
     * Attempts to parse a date string using multiple common formats.
     * Used for flexible date parsing when the format is unknown.
     *
     * @param dateString the date string to parse
     * @return parsed Date or null if parsing fails with all formats
     */
    public static Date parseFlexibleDate(String dateString) {
        if (dateString == null || dateString.trim().isEmpty()) {
            return null;
        }

        String trimmed = dateString.trim();

        // Try each format
        for (SimpleDateFormat format : COMMON_DATE_FORMATS) {
            try {
                synchronized (format) {
                    format.setLenient(false);
                    return format.parse(trimmed);
                }
            } catch (ParseException e) {
                // Continue to next format
            }
        }

        // Try as a year only (special case)
        if (trimmed.matches("\\d{4}")) {
            try {
                SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy");
                synchronized (yearFormat) {
                    yearFormat.setLenient(false);
                    return yearFormat.parse(trimmed);
                }
            } catch (Exception e) {
                // Continue
            }
        }

        return null;
    }

    /**
     * Parses an ISO date string (yyyy-MM-dd).
     *
     * @param dateString the ISO date string
     * @return parsed LocalDate or null if parsing fails
     */
    public static LocalDate parseIsoLocalDate(String dateString) {
        if (dateString == null || dateString.trim().isEmpty()) {
            return null;
        }
        try {
            return LocalDate.parse(dateString.trim(), ISO_LOCAL_DATE);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /**
     * Parses an ISO date-time string.
     *
     * @param dateTimeString the ISO date-time string
     * @return parsed LocalDateTime or null if parsing fails
     */
    public static LocalDateTime parseIsoDateTime(String dateTimeString) {
        if (dateTimeString == null || dateTimeString.trim().isEmpty()) {
            return null;
        }
        try {
            return LocalDateTime.parse(dateTimeString.trim(), ISO_DATE_TIME);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    /**
     * Converts a LocalDate to a Date.
     *
     * @param localDate the LocalDate to convert
     * @return Date or null if input is null
     */
    public static Date toDate(LocalDate localDate) {
        if (localDate == null) {
            return null;
        }
        return Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
    }

    /**
     * Converts a LocalDateTime to a Date.
     *
     * @param localDateTime the LocalDateTime to convert
     * @return Date or null if input is null
     */
    public static Date toDate(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    /**
     * Formats a Date to ISO date string (yyyy-MM-dd).
     *
     * @param date the date to format
     * @return formatted string or null if input is null
     */
    public static String formatIsoDate(Date date) {
        if (date == null) {
            return null;
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return format.format(date);
    }

    /**
     * Formats a LocalDate to ISO date string.
     *
     * @param localDate the date to format
     * @return formatted string or null if input is null
     */
    public static String formatIsoDate(LocalDate localDate) {
        if (localDate == null) {
            return null;
        }
        return localDate.format(ISO_LOCAL_DATE);
    }

    /**
     * Extracts just the year from a date string, handling various formats.
     *
     * @param dateString the date string
     * @return the year as string, or null if extraction fails
     */
    public static String extractYear(String dateString) {
        if (dateString == null || dateString.trim().isEmpty()) {
            return null;
        }

        // Try to parse as full date first
        Date date = parseFlexibleDate(dateString);
        if (date != null) {
            SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy");
            return yearFormat.format(date);
        }

        // Try to extract year from string directly (e.g., "2023-01-01" -> "2023")
        String trimmed = dateString.trim();
        if (trimmed.length() >= 4 && trimmed.matches("\\d{4}.*")) {
            return trimmed.substring(0, 4);
        }

        return null;
    }

    /**
     * Parses a date that might be in multiple formats (for NYT bestseller dates).
     * Handles ISO format and other common formats.
     *
     * @param dateString the date string
     * @return LocalDate or null if parsing fails
     */
    public static LocalDate parseBestsellerDate(String dateString) {
        if (dateString == null || dateString.trim().isEmpty()) {
            return null;
        }

        // First try ISO format
        LocalDate isoDate = parseIsoLocalDate(dateString);
        if (isoDate != null) {
            return isoDate;
        }

        // Try flexible parsing and convert
        Date flexDate = parseFlexibleDate(dateString);
        if (flexDate != null) {
            return flexDate.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
        }

        return null;
    }
}