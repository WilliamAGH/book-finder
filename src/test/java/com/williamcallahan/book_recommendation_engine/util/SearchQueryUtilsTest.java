package com.williamcallahan.book_recommendation_engine.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SearchQueryUtilsTest {

    @Test
    @DisplayName("normalize trims whitespace and preserves non-empty input")
    void normalizeTrimsAndPreservesContent() {
        assertThat(SearchQueryUtils.normalize("  The Pragmatic Programmer  "))
            .isEqualTo("The Pragmatic Programmer");
    }

    @Test
    @DisplayName("normalize falls back to wildcard for null or blank input")
    void normalizeFallsBackToWildcard() {
        assertThat(SearchQueryUtils.normalize(null)).isEqualTo("*");
        assertThat(SearchQueryUtils.normalize("   ")).isEqualTo("*");
    }

    @Test
    @DisplayName("canonicalize lowercases and trims while preserving null")
    void canonicalizeLowercasesAndTrims() {
        assertThat(SearchQueryUtils.canonicalize("  Mixed Case Query  "))
            .isEqualTo("mixed case query");
        assertThat(SearchQueryUtils.canonicalize(null)).isNull();
    }

    @Test
    @DisplayName("cacheKey sanitizes unsafe characters and appends extension")
    void cacheKeySanitizesInput() {
        assertThat(SearchQueryUtils.cacheKey("C# in Depth", "en"))
            .isEqualTo("c__in_depth-en.json");
    }

    @Test
    @DisplayName("cacheKey uses 'any' when language is blank")
    void cacheKeyUsesAnyLanguageFallback() {
        assertThat(SearchQueryUtils.cacheKey("Distributed Systems", " "))
            .isEqualTo("distributed_systems-any.json");
    }
}
