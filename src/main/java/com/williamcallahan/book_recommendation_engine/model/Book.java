package com.williamcallahan.book_recommendation_engine.model;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.ArrayList;

public class Book {
    private String id;
    private String title;
    private List<String> authors;
    private String description;
    private String coverImageUrl;
    private String imageUrl;
    private String isbn10;
    private String isbn13;
    private Date publishedDate;
    private List<String> categories;
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

    private Integer coverImageWidth;
    private Integer coverImageHeight;
    private Boolean isCoverHighResolution;

    private List<EditionInfo> otherEditions;

    public static class EditionInfo {
        private String googleBooksId;
        private String type;
        private String identifier;
        private Date publishedDate;
        private String coverImageUrl;

        public EditionInfo() {}

        public EditionInfo(String googleBooksId, String type, String identifier, Date publishedDate, String coverImageUrl) {
            this.googleBooksId = googleBooksId;
            this.type = type;
            this.identifier = identifier;
            this.publishedDate = publishedDate;
            this.coverImageUrl = coverImageUrl;
        }

        public String getGoogleBooksId() {
            return googleBooksId;
        }

        public void setGoogleBooksId(String googleBooksId) {
            this.googleBooksId = googleBooksId;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getIdentifier() {
            return identifier;
        }

        public void setIdentifier(String identifier) {
            this.identifier = identifier;
        }

        public Date getPublishedDate() {
            return publishedDate;
        }

        public void setPublishedDate(Date publishedDate) {
            this.publishedDate = publishedDate;
        }

        public String getCoverImageUrl() {
            return coverImageUrl;
        }

        public void setCoverImageUrl(String coverImageUrl) {
            this.coverImageUrl = coverImageUrl;
        }
    }

    public Book() {
        this.otherEditions = new ArrayList<>();
    }

    public Book(String id, String title, List<String> authors, String description, String coverImageUrl, String imageUrl) {
        this.id = id;
        this.title = title;
        this.authors = authors;
        this.description = description;
        this.coverImageUrl = coverImageUrl;
        this.imageUrl = imageUrl;
        this.otherEditions = new ArrayList<>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<String> getAuthors() {
        return authors;
    }

    public void setAuthors(List<String> authors) {
        this.authors = authors;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCoverImageUrl() {
        return coverImageUrl;
    }

    public void setCoverImageUrl(String coverImageUrl) {
        this.coverImageUrl = coverImageUrl;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public String getIsbn10() {
        return isbn10;
    }

    public void setIsbn10(String isbn10) {
        this.isbn10 = isbn10;
    }

    public String getIsbn13() {
        return isbn13;
    }

    public void setIsbn13(String isbn13) {
        this.isbn13 = isbn13;
    }

    public Date getPublishedDate() {
        return publishedDate;
    }

    public void setPublishedDate(Date publishedDate) {
        this.publishedDate = publishedDate;
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    public Double getAverageRating() {
        return averageRating;
    }

    public void setAverageRating(Double averageRating) {
        this.averageRating = averageRating;
    }

    public Integer getRatingsCount() {
        return ratingsCount;
    }

    public void setRatingsCount(Integer ratingsCount) {
        this.ratingsCount = ratingsCount;
    }

    public String getRawRatingsData() {
        return rawRatingsData;
    }

    public void setRawRatingsData(String rawRatingsData) {
        this.rawRatingsData = rawRatingsData;
    }

    public Boolean getHasRatings() {
        return hasRatings;
    }

    public void setHasRatings(Boolean hasRatings) {
        this.hasRatings = hasRatings;
    }

    public Integer getPageCount() {
        return pageCount;
    }

    public void setPageCount(Integer pageCount) {
        this.pageCount = pageCount;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getInfoLink() {
        return infoLink;
    }

    public void setInfoLink(String infoLink) {
        this.infoLink = infoLink;
    }

    public String getPreviewLink() {
        return previewLink;
    }

    public void setPreviewLink(String previewLink) {
        this.previewLink = previewLink;
    }

    public String getPurchaseLink() {
        return purchaseLink;
    }

    public void setPurchaseLink(String purchaseLink) {
        this.purchaseLink = purchaseLink;
    }

    public Double getListPrice() {
        return listPrice;
    }

    public void setListPrice(Double listPrice) {
        this.listPrice = listPrice;
    }

    public String getCurrencyCode() {
        return currencyCode;
    }

    public void setCurrencyCode(String currencyCode) {
        this.currencyCode = currencyCode;
    }

    public String getWebReaderLink() {
        return webReaderLink;
    }

    public void setWebReaderLink(String webReaderLink) {
        this.webReaderLink = webReaderLink;
    }

    public List<EditionInfo> getOtherEditions() {
        return otherEditions;
    }

    public void setOtherEditions(List<EditionInfo> otherEditions) {
        this.otherEditions = otherEditions;
    }

    public Integer getCoverImageWidth() {
        return coverImageWidth;
    }

    public void setCoverImageWidth(Integer coverImageWidth) {
        this.coverImageWidth = coverImageWidth;
    }

    public Integer getCoverImageHeight() {
        return coverImageHeight;
    }

    public void setCoverImageHeight(Integer coverImageHeight) {
        this.coverImageHeight = coverImageHeight;
    }

    public Boolean getIsCoverHighResolution() {
        return isCoverHighResolution;
    }

    public void setIsCoverHighResolution(Boolean isCoverHighResolution) {
        this.isCoverHighResolution = isCoverHighResolution;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Book book = (Book) o;
        return Objects.equals(id, book.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
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
}
