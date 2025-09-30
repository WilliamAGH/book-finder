package com.williamcallahan.book_recommendation_engine.util;

import org.springframework.lang.Nullable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for book dimension strings using modern Java patterns.
 * <p>
 * Converts dimension strings like "24.00 cm" to numeric values.
 * Uses Java 21 records for immutable, zero-boilerplate data transfer.
 * 
 * @since 1.0.0
 */
public final class DimensionParser {
    
    // Compiled pattern (shared across invocations for performance)
    // Matches: digits with optional single decimal point, optional unit
    private static final Pattern DIMENSION_PATTERN = Pattern.compile(
        "(\\d+(?:\\.\\d+)?)\\s*(cm|mm|in|inches)?",
        Pattern.CASE_INSENSITIVE
    );
    
    private DimensionParser() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Immutable dimension data using Java 21 record (zero boilerplate).
     * <p>
     * Records automatically provide:
     * - Constructor
     * - Getters (height(), width(), thickness())
     * - equals(), hashCode(), toString()
     * - Immutability
     * 
     * @param height Height in centimeters (may be null)
     * @param width Width in centimeters (may be null)
     * @param thickness Thickness in centimeters (may be null)
     */
    public record ParsedDimensions(
        @Nullable Double height,
        @Nullable Double width,
        @Nullable Double thickness
    ) {
        /**
         * Checks if at least one dimension is present.
         * 
         * @return true if any dimension is non-null
         */
        public boolean hasAnyDimension() {
            return height != null || width != null || thickness != null;
        }
    }
    
    /**
     * Parses dimension string to numeric value in centimeters.
     * <p>
     * Supports multiple units with automatic conversion:
     * - cm (centimeters) - default, no conversion
     * - mm (millimeters) - divided by 10
     * - in/inches - multiplied by 2.54
     * 
     * Uses Java 21 pattern matching for clean null handling.
     * 
     * @param dimensionStr String like "24.00 cm", "24", "9.5 in"
     * @return Numeric value in centimeters, or null if unparseable
     * 
     * @example
     * <pre>
     * parseToCentimeters("24.00 cm") → 24.00
     * parseToCentimeters("240 mm")   → 24.00
     * parseToCentimeters("9.5 in")   → 24.13
     * parseToCentimeters("24")       → 24.00 (assumes cm)
     * parseToCentimeters(null)       → null
     * </pre>
     */
    @Nullable
    public static Double parseToCentimeters(@Nullable String dimensionStr) {
        // Modern Java: Clean null checks
        if (dimensionStr == null || dimensionStr.isBlank()) {
            return null;
        }
        
        return parseWithUnit(dimensionStr);
    }
    
    /**
     * Internal parser with unit conversion.
     */
    @Nullable
    private static Double parseWithUnit(String dimensionStr) {
        Matcher matcher = DIMENSION_PATTERN.matcher(dimensionStr.trim());
        if (!matcher.find()) {
            return null;
        }
        
        try {
            double value = Double.parseDouble(matcher.group(1));
            String unit = matcher.group(2);
            
            // Modern Java: Clean unit conversion
            if (unit == null || unit.equalsIgnoreCase("cm")) {
                return value;
            } else if (unit.equalsIgnoreCase("mm")) {
                return value / 10.0;
            } else if (unit.equalsIgnoreCase("in") || unit.equalsIgnoreCase("inches")) {
                return value * 2.54;
            } else {
                // Unknown unit, assume cm
                return value;
            }
            
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    /**
     * Parses all three dimensions at once using modern record.
     * <p>
     * Zero-boilerplate approach using Java 21 record for immutable data.
     * 
     * @param height Height string (e.g., "24.00 cm")
     * @param width Width string (e.g., "16.00 cm")
     * @param thickness Thickness string (e.g., "3.00 cm")
     * @return Immutable ParsedDimensions record
     */
    public static ParsedDimensions parseAll(
        @Nullable String height,
        @Nullable String width,
        @Nullable String thickness
    ) {
        return new ParsedDimensions(
            parseToCentimeters(height),
            parseToCentimeters(width),
            parseToCentimeters(thickness)
        );
    }
    
    /**
     * Validates dimension string format without parsing.
     * <p>
     * Useful for quick validation before expensive parsing.
     * 
     * @param dimensionStr Dimension string to validate
     * @return true if string matches expected format
     */
    public static boolean isValidFormat(@Nullable String dimensionStr) {
        if (dimensionStr == null || dimensionStr.isBlank()) {
            return false;
        }
        
        return DIMENSION_PATTERN.matcher(dimensionStr.trim()).find();
    }
}
