package com.williamcallahan.book_recommendation_engine.controller.dto;

import java.util.Map;

/** DTO representing a qualifier/tag assignment. */
public record TagDto(String key, Map<String, Object> attributes) {
}
