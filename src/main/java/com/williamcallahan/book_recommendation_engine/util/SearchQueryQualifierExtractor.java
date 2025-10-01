package com.williamcallahan.book_recommendation_engine.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts semantic qualifiers from search queries while preserving
 * the canonical logic previously hosted in {@link BookJsonParser}.
 * <p>
 * Centralising this behaviour keeps query enrichment reusable without
 * relying on deprecated legacy utilities.
 */
public final class SearchQueryQualifierExtractor {

    private SearchQueryQualifierExtractor() {
        // Utility class
    }

    /**
     * Parses a raw user query into a map of qualifier flags and metadata.
     *
     * @param query raw search terms supplied by the caller
     * @return mutable map of qualifiers; empty when no heuristics matched
     */
    public static Map<String, Object> extract(String query) {
        if (query == null || query.trim().isEmpty()) {
            return Map.of();
        }

        String normalizedQuery = SearchQueryUtils.canonicalize(query);
        if (normalizedQuery == null || normalizedQuery.isEmpty()) {
            return Map.of();
        }

        Map<String, Object> qualifiers = new LinkedHashMap<>();

        if (containsAny(normalizedQuery, "new york times bestseller", "nyt bestseller", "ny times bestseller")) {
            qualifiers.put("nytBestseller", true);
        }

        boolean mentionsAward = containsAny(normalizedQuery, "award winner", "prize winner", "pulitzer", "nobel");
        if (mentionsAward) {
            qualifiers.put("awardWinner", true);
            if (normalizedQuery.contains("pulitzer")) {
                qualifiers.put("pulitzerPrize", true);
            }
            if (normalizedQuery.contains("nobel")) {
                qualifiers.put("nobelPrize", true);
            }
        }

        if (containsAny(normalizedQuery, "best books", "top books", "must read")) {
            qualifiers.put("recommendedList", true);
        }

        List<String> queryTerms = new ArrayList<>(Arrays.asList(normalizedQuery.split("\\s+")));
        if (!queryTerms.isEmpty()) {
            qualifiers.put("queryTerms", queryTerms);
        }
        qualifiers.put("searchQuery", query.trim());

        return qualifiers;
    }

    private static boolean containsAny(String haystack, String... needles) {
        return Arrays.stream(needles).anyMatch(haystack::contains);
    }
}
