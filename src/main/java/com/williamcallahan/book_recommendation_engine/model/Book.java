/**
 * Core book entity model containing all book metadata and cover image information
 *
 * Features:
 * - Represents books fetched from external sources like Google Books API
 * - Stores comprehensive book details including bibliographic data
 * - Tracks cover image metadata including resolution information
 * - Contains edition information for related formats of the same book
 */
package com.williamcallahan.book_recommendation_engine.model;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Book {

    @EqualsAndHashCode.Include
    private String id;
    private String slug;
    private String title;
    private List<String> authors;
    private String description;
    private String s3ImagePath;
    private String externalImageUrl;
    private String isbn10;
    private String isbn13;
    private Date publishedDate;
    private List<String> categories;
    private List<CollectionAssignment> collections = new ArrayList<>();
    private Double averageRating;
    private Integer ratingsCount;
    private String rawRatingsData;
    private Boolean hasRatings;
    private Integer pageCount;
    private String language;
    private String publisher;
    private String infoLink;
    private String previewLink;
    private String purchaseLink;
    private Double listPrice;
    private String currencyCode;
    private String webReaderLink;
    private Boolean pdfAvailable;
    private Boolean epubAvailable;
    private Integer coverImageWidth;
    private Integer coverImageHeight;
    private Boolean isCoverHighResolution;
    private Double heightCm;
    private Double widthCm;
    private Double thicknessCm;
    private Double weightGrams;
    private CoverImages coverImages;
    private Integer editionNumber;
    private String editionGroupKey;
    private List<EditionInfo> otherEditions;
    private String asin;
    private Map<String, Object> qualifiers;
    private List<String> cachedRecommendationIds;
    private transient String rawJsonResponse;

    public Book() {
        this.otherEditions = new ArrayList<>();
        this.qualifiers = new HashMap<>();
        this.cachedRecommendationIds = new ArrayList<>();
    }

    public Book(String id,
                String title,
                List<String> authors,
                String description,
                String s3ImagePath,
                String externalImageUrl) {
        this.id = id;
        this.title = title;
        this.authors = authors;
        this.description = description;
        this.s3ImagePath = s3ImagePath;
        this.externalImageUrl = externalImageUrl;
        this.otherEditions = new ArrayList<>();
        this.qualifiers = new HashMap<>();
        this.cachedRecommendationIds = new ArrayList<>();
    }

    public void setAuthors(List<String> authors) {
        if (authors == null) {
            this.authors = new ArrayList<>();
            return;
        }
        this.authors = authors.stream()
            .filter(Objects::nonNull)
            .map(String::trim)
            .filter(author -> !author.isEmpty())
            .collect(Collectors.toList());
    }

    public List<CollectionAssignment> getCollections() {
        if (collections == null || collections.isEmpty()) {
            return List.of();
        }
        return List.copyOf(collections);
    }

    public void setCollections(List<CollectionAssignment> collections) {
        this.collections = (collections == null || collections.isEmpty())
            ? new ArrayList<>()
            : new ArrayList<>(collections);
    }

    public void addCollection(CollectionAssignment assignment) {
        if (assignment == null) {
            return;
        }
        if (this.collections == null) {
            this.collections = new ArrayList<>();
        }
        this.collections.add(assignment);
    }

    public String getCoverImageUrl() {
        return (s3ImagePath != null && !s3ImagePath.isEmpty()) ? s3ImagePath : externalImageUrl;
    }

    public void setCoverImageUrl(String coverImageUrl) {
        // Don't overwrite existing values with null
        if (coverImageUrl == null) {
            return;
        }

        // Check if it's an S3 URL (amazonaws.com or other S3-compatible services)
        if (coverImageUrl.contains("s3.amazonaws.com") || coverImageUrl.contains(".digitaloceanspaces.com")) {
            this.s3ImagePath = coverImageUrl;
        } else if (coverImageUrl.startsWith("http://") || coverImageUrl.startsWith("https://")) {
            // External URL (Google Books, Open Library, etc.)
            this.externalImageUrl = coverImageUrl;
        } else {
            // Relative path or S3 key (no protocol)
            this.s3ImagePath = coverImageUrl;
        }
    }

    public String getImageUrl() {
        return externalImageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.externalImageUrl = imageUrl;
    }

    public void setQualifiers(Map<String, Object> qualifiers) {
        this.qualifiers = qualifiers != null ? qualifiers : new HashMap<>();
    }

    public void addQualifier(String key, Object value) {
        if (key == null) {
            return;
        }
        if (this.qualifiers == null) {
            this.qualifiers = new HashMap<>();
        }
        this.qualifiers.put(key, value);
    }

    public boolean hasQualifier(String key) {
        return this.qualifiers != null && this.qualifiers.containsKey(key);
    }

    public void setCachedRecommendationIds(List<String> cachedRecommendationIds) {
        this.cachedRecommendationIds = cachedRecommendationIds != null ? new ArrayList<>(cachedRecommendationIds) : new ArrayList<>();
    }

    public void addRecommendationIds(List<String> newRecommendationIds) {
        if (newRecommendationIds == null || newRecommendationIds.isEmpty()) {
            return;
        }
        if (this.cachedRecommendationIds == null) {
            this.cachedRecommendationIds = new ArrayList<>();
        }
        for (String recommendationId : newRecommendationIds) {
            if (recommendationId != null && !recommendationId.isEmpty() && !this.cachedRecommendationIds.contains(recommendationId)) {
                this.cachedRecommendationIds.add(recommendationId);
            }
        }
    }

    @Override
    public String toString() {
        return "Book{" +
            "id='" + id + '\'' +
            ", title='" + title + '\'' +
            ", authors=" + authors +
            ", otherEditionsCount=" + (otherEditions != null ? otherEditions.size() : 0) +
            '}';
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CollectionAssignment {
        private String collectionId;
        private String name;
        private String collectionType;
        private Integer rank;
        private String source;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EditionInfo {
        private String googleBooksId;
        private String type;
        private String identifier;
        private String editionIsbn10;
        private String editionIsbn13;
        private Date publishedDate;
        private String coverImageUrl;
    }
}
