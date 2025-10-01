package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.util.DimensionParser.ParsedDimensions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DimensionParser using JUnit 5 parameterized tests.
 */
class DimensionParserTest {
    
    @ParameterizedTest
    @CsvSource({
        "24.00 cm,24.00",
        "24 cm,24.00",
        "24.5 cm,24.5",
        "24,24.00",  // No unit, assumes cm
        "240 mm,24.00",  // Millimeters to cm
        "9.45 in,24.003",  // Inches to cm (9.45 * 2.54)
        "9.45 inches,24.003"  // Long form inches
    })
    void shouldParseVariousFormats(String input, double expected) {
        Double result = DimensionParser.parseToCentimeters(input);
        assertNotNull(result);
        assertEquals(expected, result, 0.01, 
            "Failed to parse: " + input);
    }
    
    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"  ", "\t", "\n"})
    void shouldReturnNullForInvalidInput(String input) {
        assertNull(DimensionParser.parseToCentimeters(input));
    }
    
    @ParameterizedTest
    @ValueSource(strings = {
        "invalid",
        "twenty-four cm"  // Non-numeric
    })
    void shouldReturnNullForMalformedStrings(String input) {
        assertNull(DimensionParser.parseToCentimeters(input));
    }
    
    @Test
    void shouldExtractFirstValidNumber() {
        // Regex will match first valid number pattern it finds
        // "24.00.00 cm" → matches "24.00" (first valid decimal)
        Double result = DimensionParser.parseToCentimeters("24.00.00 cm");
        assertNotNull(result);
        assertEquals(24.0, result, 0.001);
        
        // "cm 24" → matches "24" (no unit found before it)
        result = DimensionParser.parseToCentimeters("cm 24");
        assertNotNull(result);
        assertEquals(24.0, result, 0.001);
    }
    
    @Test
    void shouldParseGoogleBooksFormat() {
        // Real example from Google Books API
        String googleFormat = "24.00 cm";
        Double result = DimensionParser.parseToCentimeters(googleFormat);
        
        assertNotNull(result);
        assertEquals(24.00, result, 0.001);
    }
    
    @Test
    void shouldHandleCaseInsensitiveUnits() {
        Double upperCase = DimensionParser.parseToCentimeters("24 CM");
        assertNotNull(upperCase);
        assertEquals(24.0, upperCase.doubleValue(), 0.001);

        Double mixedCase = DimensionParser.parseToCentimeters("24 Cm");
        assertNotNull(mixedCase);
        assertEquals(24.0, mixedCase.doubleValue(), 0.001);

        Double millimeters = DimensionParser.parseToCentimeters("240 MM");
        assertNotNull(millimeters);
        assertEquals(24.0, millimeters.doubleValue(), 0.001);

        Double inches = DimensionParser.parseToCentimeters("9.45 IN");
        assertNotNull(inches);
        assertEquals(24.003, inches.doubleValue(), 0.001);
    }
    
    @Test
    void shouldParseAllDimensions() {
        ParsedDimensions dimensions = DimensionParser.parseAll(
            "24.00 cm",
            "16.00 cm",
            "3.00 cm"
        );
        
        assertNotNull(dimensions);
        Double height = dimensions.height();
        Double width = dimensions.width();
        Double thickness = dimensions.thickness();
        assertNotNull(height);
        assertEquals(24.00, height.doubleValue(), 0.001);
        assertNotNull(width);
        assertEquals(16.00, width.doubleValue(), 0.001);
        assertNotNull(thickness);
        assertEquals(3.00, thickness.doubleValue(), 0.001);
        assertTrue(dimensions.hasAnyDimension());
    }
    
    @Test
    void shouldHandlePartialDimensions() {
        ParsedDimensions dimensions = DimensionParser.parseAll(
            "24.00 cm",
            null,
            "3.00 cm"
        );
        
        assertNotNull(dimensions);
        Double height = dimensions.height();
        Double thickness = dimensions.thickness();
        assertNotNull(height);
        assertEquals(24.00, height.doubleValue(), 0.001);
        assertNull(dimensions.width());
        assertNotNull(thickness);
        assertEquals(3.00, thickness.doubleValue(), 0.001);
        assertTrue(dimensions.hasAnyDimension());
    }
    
    @Test
    void shouldDetectNoDimensions() {
        ParsedDimensions dimensions = DimensionParser.parseAll(null, null, null);
        
        assertNotNull(dimensions);
        assertNull(dimensions.height());
        assertNull(dimensions.width());
        assertNull(dimensions.thickness());
        assertFalse(dimensions.hasAnyDimension());
    }
    
    @Test
    void shouldConvertMillimetersCorrectly() {
        // 240mm = 24cm
        Double result = DimensionParser.parseToCentimeters("240 mm");
        assertNotNull(result);
        assertEquals(24.0, result.doubleValue(), 0.001);
    }
    
    @Test
    void shouldConvertInchesCorrectly() {
        // 1 inch = 2.54 cm
        Double result = DimensionParser.parseToCentimeters("1 in");
        assertNotNull(result);
        assertEquals(2.54, result.doubleValue(), 0.001);
        
        // 10 inches = 25.4 cm
        result = DimensionParser.parseToCentimeters("10 inches");
        assertNotNull(result);
        assertEquals(25.4, result.doubleValue(), 0.001);
    }
    
    @ParameterizedTest
    @CsvSource({
        "24.00 cm,true",
        "24,true",
        "240 mm,true",
        "9.45 in,true",
        "invalid,false",
        "'',false"
    })
    void shouldValidateFormat(String input, boolean expected) {
        assertEquals(expected, DimensionParser.isValidFormat(input));
    }
    
    @Test
    void shouldHandleDecimalPrecision() {
        // Test various decimal precisions
        Double first = DimensionParser.parseToCentimeters("24.1 cm");
        Double second = DimensionParser.parseToCentimeters("24.12 cm");
        Double third = DimensionParser.parseToCentimeters("24.123 cm");
        assertNotNull(first);
        assertNotNull(second);
        assertNotNull(third);
        assertEquals(24.1, first.doubleValue(), 0.0001);
        assertEquals(24.12, second.doubleValue(), 0.0001);
        assertEquals(24.123, third.doubleValue(), 0.0001);
    }
    
    @Test
    void shouldTestRecordFeatures() {
        // Test that record provides expected methods
        ParsedDimensions d1 = new ParsedDimensions(24.0, 16.0, 3.0);
        ParsedDimensions d2 = new ParsedDimensions(24.0, 16.0, 3.0);
        ParsedDimensions d3 = new ParsedDimensions(25.0, 16.0, 3.0);
        
        // Automatic equals/hashCode from record
        assertEquals(d1, d2);
        assertNotEquals(d1, d3);
        assertEquals(d1.hashCode(), d2.hashCode());
        
        // Automatic toString
        assertNotNull(d1.toString());
        assertTrue(d1.toString().contains("24.0"));
    }
}
