package com.williamcallahan.book_recommendation_engine.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IsbnUtilsTest {

    @Test
    void sanitizeRemovesWhitespaceAndHyphen() {
        assertThat(IsbnUtils.sanitize("978-1 4028-9462-6")).isEqualTo("9781402894626");
    }

    @Test
    void sanitizeUppercasesCheckDigitX() {
        assertThat(IsbnUtils.sanitize("0-306-40615-x")).isEqualTo("030640615X");
    }

    @Test
    void sanitizeReturnsNullWhenNoIsbnCharactersRemain() {
        assertThat(IsbnUtils.sanitize("abc")).isNull();
    }

    @Test
    void sanitizeReturnsNullForNullInput() {
        assertThat(IsbnUtils.sanitize(null)).isNull();
    }
}
