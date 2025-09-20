package com.williamcallahan.book_recommendation_engine.controller.dto;

/** DTO representing a normalized collection/list assignment. */
public record CollectionDto(String id,
                            String name,
                            String type,
                            Integer rank,
                            String source) {
}
