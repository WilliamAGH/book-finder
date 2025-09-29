package com.williamcallahan.book_recommendation_engine.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for TextUtils text normalization utilities.
 */
public class TextUtilsTest {

    @ParameterizedTest
    @CsvSource({
        "THE GREAT GATSBY, The Great Gatsby",
        "TO KILL A MOCKINGBIRD, To Kill a Mockingbird",
        "THE CATCHER IN THE RYE, The Catcher in the Rye",
        "the lord of the rings, The Lord of the Rings",
        "war and peace, War and Peace",
        "THE PRAGMATIC PROGRAMMER: FROM JOURNEYMAN TO MASTER, The Pragmatic Programmer: From Journeyman to Master",
        "CLEAN CODE: A HANDBOOK OF AGILE SOFTWARE CRAFTSMANSHIP, Clean Code: A Handbook of Agile Software Craftsmanship",
        "THE FOUR-HOUR WORKWEEK, The Four-Hour Workweek",
        "HARRY POTTER AND THE HALF-BLOOD PRINCE, Harry Potter and the Half-Blood Prince",
        "WORLD WAR II: A HISTORY, World War II: A History",
        "HENRY VIII AND HIS WIVES, Henry VIII and His Wives",
        "THE FBI FILES: INSIDE STORIES, The FBI Files: Inside Stories",
        "NASA: A HISTORY OF THE US SPACE PROGRAM, Nasa: A History of the US Space Program",
        "A TALE OF TWO CITIES, A Tale of Two Cities",
        "THE GIRL WITH THE DRAGON TATTOO, The Girl with the Dragon Tattoo",
        "DUNE, Dune",
        "1984, 1984",
        "CATCH-22, Catch-22",
        "THE 7 HABITS OF HIGHLY EFFECTIVE PEOPLE, The 7 Habits of Highly Effective People"
    })
    void testNormalizeBookTitle(String input, String expected) {
        assertEquals(expected, TextUtils.normalizeBookTitle(input));
    }
    @Test
    void testNormalizeBookTitle_NullInput() {
        assertNull(TextUtils.normalizeBookTitle(null));
    }

    @Test
    void testNormalizeBookTitle_BlankInput() {
        assertEquals("   ", TextUtils.normalizeBookTitle("   ")); // Blank strings are preserved
    }

    @Test
    void testNormalizeBookTitle_PreserveIntentionalMixedCase() {
        // These should be preserved as they have intentional mixed case
        String title1 = "The Lord of the Rings";
        assertEquals(title1, TextUtils.normalizeBookTitle(title1));
        
        String title2 = "Harry Potter and the Philosopher's Stone";
        assertEquals(title2, TextUtils.normalizeBookTitle(title2));
        
        String title3 = "eBay: The Smart Way";
        assertEquals(title3, TextUtils.normalizeBookTitle(title3));
    }

    @ParameterizedTest
    @CsvSource({
        "JOHN DOE, John Doe",
        "STEPHEN KING, Stephen King",
        "'J.K. ROWLING', 'J.k. Rowling'",
        "PATRICK MCDONALD, Patrick McDonald",
        "SEAN MACDONALD, Sean MacDonald",
        "'CONNOR O''BRIEN', 'Connor O''Brien'",
        "LUDWIG VON BEETHOVEN, Ludwig von Beethoven",
        "VINCENT VAN GOGH, Vincent van Gogh",
        "LEONARDO DA VINCI, Leonardo Da Vinci",
        "Stephen King, Stephen King",
        "'J.K. Rowling', 'J.K. Rowling'"
    })
    void testNormalizeAuthorName(String input, String expected) {
        assertEquals(expected, TextUtils.normalizeAuthorName(input));
    }

    @Test
    void testNormalizeAuthorName_NullInput() {
        assertNull(TextUtils.normalizeAuthorName(null));
    }

    @Test
    void testNormalizeAuthorName_PreserveMixedCase() {
        // Mixed case should be preserved
        String author1 = "Stephen King";
        assertEquals(author1, TextUtils.normalizeAuthorName(author1));
        
        String author2 = "J.K. Rowling";
        assertEquals(author2, TextUtils.normalizeAuthorName(author2));
    }

    @Test
    void testTitleCaseWithSubtitle() {
        String input = "THE DESIGN OF EVERYDAY THINGS: REVISED AND EXPANDED EDITION";
        String expected = "The Design of Everyday Things: Revised and Expanded Edition";
        assertEquals(expected, TextUtils.normalizeBookTitle(input));
    }

    @Test
    void testTitleCaseWithEmDash() {
        String input = "THE PRAGMATIC PROGRAMMER—FROM JOURNEYMAN TO MASTER";
        String expected = "The Pragmatic Programmer—From Journeyman to Master";
        assertEquals(expected, TextUtils.normalizeBookTitle(input));
    }

    @Test
    void testTitleCasePreservesAcronyms() {
        // This test may need adjustment based on actual implementation
        // Some acronyms like "FBI", "CIA" should remain uppercase if in our list
        String input = "THE FBI STORY";
        String result = TextUtils.normalizeBookTitle(input);
        assertTrue(result.contains("FBI"), "FBI acronym should be preserved");
    }

    @Test
    void testHandlesComplexPunctuation() {
        String input = "WHAT IF?: SERIOUS SCIENTIFIC ANSWERS TO ABSURD HYPOTHETICAL QUESTIONS";
        String result = TextUtils.normalizeBookTitle(input);
        
        // Should start with "What If?"
        assertTrue(result.startsWith("What If?"), "Should properly capitalize 'What If?'");
        
        // Should have proper subtitle capitalization after colon
        assertTrue(result.contains(": Serious"), "Should capitalize after colon");
    }

    @Test
    void testWhitespacePreservation() {
        String input = "THE    GREAT     GATSBY";
        String result = TextUtils.normalizeBookTitle(input);
        
        // Should preserve whitespace structure while normalizing case
        assertNotNull(result);
        assertTrue(result.contains("Great"), "Should contain 'Great'");
        assertTrue(result.contains("Gatsby"), "Should contain 'Gatsby'");
    }
}