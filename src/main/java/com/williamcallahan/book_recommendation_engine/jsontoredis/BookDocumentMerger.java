package com.williamcallahan.book_recommendation_engine.jsontoredis;

import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Service for merging book documents during migration.
 * Extracted from JsonS3ToRedisService to reduce file size and improve testability.
 */
@SuppressWarnings({"unchecked"})
@Component
public class BookDocumentMerger {
    private static final Logger log = LoggerFactory.getLogger(BookDocumentMerger.class);
    
    /**
     * Create a unified document from CachedBook data
     */
    public Map<String, Object> createUnifiedDocument(String googleBookId, String googleBooksJson) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            // Parse JSON string to Map
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            Map<String, Object> bookData = mapper.readValue(googleBooksJson, new TypeReference<Map<String,Object>>() {});
            result.putAll(bookData);
            
            // Add standard fields
            result.put("id", googleBookId);
            result.put("googleBooksId", googleBookId);
            result.put("dataSource", "google_books");
            
            // Add metadata
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("lastUpdated", Instant.now().toString());
            metadata.put("migrationVersion", "2.0");
            metadata.put("hasGoogleBooksData", true);
            result.put("_metadata", metadata);
            
            // Build searchable text from book data
            if (!result.containsKey("searchableText")) {
                StringBuilder searchableText = new StringBuilder();
                // Add title
                if (result.containsKey("title")) {
                    searchableText.append(result.get("title")).append(" ");
                }
                // Add authors
                if (result.containsKey("authors")) {
                    searchableText.append(result.get("authors")).append(" ");
                }
                // Add description
                if (result.containsKey("description")) {
                    searchableText.append(result.get("description")).append(" ");
                }
                // Add categories
                if (result.containsKey("categories")) {
                    searchableText.append(result.get("categories")).append(" ");
                }
                result.put("searchableText", searchableText.toString().trim());
            }
            
            return result;
            
        } catch (Exception e) {
            log.error("Failed to create unified document for book {}: {}", googleBookId, e.getMessage());
            throw new RuntimeException("Document creation failed", e);
        }
    }
    
    /**
     * Merge unified documents intelligently
     */
    public Map<String, Object> mergeUnifiedDocuments(Map<String, Object> existing, Map<String, Object> incoming) {
        Map<String, Object> merged = new HashMap<>(existing);
        
        for (Map.Entry<String, Object> entry : incoming.entrySet()) {
            String key = entry.getKey();
            Object newValue = entry.getValue();
            
            if (newValue != null) {
                switch (key) {
                    case "_metadata":
                        // Merge metadata
                        Map<String, Object> existingMeta = (Map<String, Object>) merged.getOrDefault("_metadata", new HashMap<>());
                        Map<String, Object> newMeta = (Map<String, Object>) newValue;
                        Map<String, Object> mergedMeta = new HashMap<>(existingMeta);
                        mergedMeta.putAll(newMeta);
                        merged.put(key, mergedMeta);
                        break;
                        
                    case "searchableText":
                        // Combine searchable text
                        String existingText = (String) merged.getOrDefault("searchableText", "");
                        String newText = (String) newValue;
                        if (!existingText.contains(newText)) {
                            merged.put(key, existingText + " " + newText);
                        }
                        break;
                        
                    default:
                        // For all other fields, newer data takes precedence
                        merged.put(key, newValue);
                        break;
                }
            }
        }
        
        // Update metadata timestamps
        Map<String, Object> metadata = (Map<String, Object>) merged.computeIfAbsent("_metadata", k -> new HashMap<>());
        metadata.put("lastUpdated", Instant.now().toString());
        metadata.put("mergedAt", Instant.now().toString());
        
        return merged;
    }
    
    /**
     * Merge CachedBook objects
     */
    public CachedBook mergeCachedBooks(CachedBook existing, CachedBook incoming) {
        // If no existing book, return incoming
        if (existing == null) {
            return incoming;
        }
        
        // If no incoming book, return existing
        if (incoming == null) {
            return existing;
        }
        
        // Start with existing book data - create a copy
        CachedBook merged = new CachedBook();
        
        // Copy all fields from existing
        merged.setId(existing.getId());
        merged.setGoogleBooksId(existing.getGoogleBooksId());
        merged.setTitle(existing.getTitle());
        merged.setAuthors(existing.getAuthors());
        merged.setDescription(existing.getDescription());
        merged.setCoverImageUrl(existing.getCoverImageUrl());
        merged.setIsbn10(existing.getIsbn10());
        merged.setIsbn13(existing.getIsbn13());
        merged.setPublishedDate(existing.getPublishedDate());
        merged.setCategories(existing.getCategories());
        merged.setAverageRating(existing.getAverageRating());
        merged.setRatingsCount(existing.getRatingsCount());
        merged.setPageCount(existing.getPageCount());
        merged.setLanguage(existing.getLanguage());
        merged.setPublisher(existing.getPublisher());
        merged.setInfoLink(existing.getInfoLink());
        merged.setPreviewLink(existing.getPreviewLink());
        merged.setPurchaseLink(existing.getPurchaseLink());
        merged.setRawData(existing.getRawData());
        merged.setEmbedding(existing.getEmbedding());
        merged.setCreatedAt(existing.getCreatedAt());
        merged.setLastAccessed(existing.getLastAccessed());
        merged.setAccessCount(existing.getAccessCount());
        merged.setCachedRecommendationIds(existing.getCachedRecommendationIds());
        merged.setQualifiers(existing.getQualifiers());
        merged.setOtherEditions(existing.getOtherEditions());
        merged.setS3Key(existing.getS3Key());
        merged.setLastUpdated(existing.getLastUpdated());
        merged.setSlug(existing.getSlug());
        
        // Update with non-null values from incoming
        if (incoming.getTitle() != null) merged.setTitle(incoming.getTitle());
        if (incoming.getAuthors() != null && !incoming.getAuthors().isEmpty()) {
            merged.setAuthors(incoming.getAuthors());
        }
        if (incoming.getDescription() != null) merged.setDescription(incoming.getDescription());
        if (incoming.getCategories() != null && !incoming.getCategories().isEmpty()) {
            merged.setCategories(incoming.getCategories());
        }
        if (incoming.getPublisher() != null) merged.setPublisher(incoming.getPublisher());
        if (incoming.getPublishedDate() != null) merged.setPublishedDate(incoming.getPublishedDate());
        if (incoming.getPageCount() != null) merged.setPageCount(incoming.getPageCount());
        if (incoming.getLanguage() != null) merged.setLanguage(incoming.getLanguage());
        if (incoming.getAverageRating() != null) merged.setAverageRating(incoming.getAverageRating());
        if (incoming.getRatingsCount() != null) merged.setRatingsCount(incoming.getRatingsCount());
        
        // Merge identifiers
        if (incoming.getIsbn10() != null) merged.setIsbn10(incoming.getIsbn10());
        if (incoming.getIsbn13() != null) merged.setIsbn13(incoming.getIsbn13());
        
        // Merge image links with priority: incoming > existing
        if (incoming.getCoverImageUrl() != null) merged.setCoverImageUrl(incoming.getCoverImageUrl());
        
        // Update metadata
        merged.setLastUpdated(Instant.now());
        
        return merged;
    }
}