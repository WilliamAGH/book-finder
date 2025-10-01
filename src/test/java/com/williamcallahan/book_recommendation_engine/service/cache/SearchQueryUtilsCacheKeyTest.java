package com.williamcallahan.book_recommendation_engine.service.cache;

import com.williamcallahan.book_recommendation_engine.util.SearchQueryUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SearchQueryUtilsCacheKeyTest {

    @Test
    void cacheKey_normalizesWhitespaceAndPunctuation() {
        String cacheKey = SearchQueryUtils.cacheKey("  C# In Depth  ");

        assertThat(cacheKey).isEqualTo("c__in_depth.json");
    }

    @Test
    void cacheKey_withLanguageIncludesLangSegment() {
        String cacheKey = SearchQueryUtils.cacheKey("Refactoring: Improving the Design of Existing Code", " en ");

        assertThat(cacheKey).isEqualTo("refactoring__improving_the_design_of_existing_code-en.json");
    }

    @Test
    void cacheKey_defaultsLanguageWhenMissing() {
        assertThat(SearchQueryUtils.cacheKey("Clean Code", null)).isEqualTo("clean_code-any.json");
        assertThat(SearchQueryUtils.cacheKey("Clean Code", " ")).isEqualTo("clean_code-any.json");
    }

    @Test
    void cacheKey_handlesNullQuery() {
        assertThat(SearchQueryUtils.cacheKey(null)).isEqualTo(".json");
        assertThat(SearchQueryUtils.cacheKey(null, "fr")).isEqualTo("-fr.json");
    }
}
