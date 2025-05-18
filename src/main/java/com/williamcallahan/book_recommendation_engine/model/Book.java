/**
 * Core book entity model containing all book metadata and cover image information
 * - Represents books fetched from external sources like Google Books API
 * - Stores comprehensive book details including bibliographic data
 * - Tracks cover image metadata including resolution information
 * - Contains edition information for related formats of the same book
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.model;

import com.williamcallahan.book_recommendation_engine.types.CoverImages;

import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
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
    
    private Boolean pdfAvailable;
    private Boolean epubAvailable;

    private Integer coverImageWidth;
    private Integer coverImageHeight;
    private Boolean isCoverHighResolution;

    private CoverImages coverImages;

    private List<EditionInfo> otherEditions;
    
    // Map to store special qualifiers such as "new york times bestseller" or other search-specific attributes
    private Map<String, Object> qualifiers;

    // Transient field to store the raw JSON response from Google Books API for provenance logging
    private transient String rawJsonResponse;

    /**
     * Nested class representing alternate editions of a book
     * - Contains edition-specific metadata like ISBN and publication date
     * - Stores unique identifiers for different book formats
     * - Tracks cover image URLs specific to each edition
     */
    public static class EditionInfo {
        private String googleBooksId;
        private String type;
        private String identifier;
        private String editionIsbn10;
        private String editionIsbn13;
        private Date publishedDate;
        private String coverImageUrl;

        /**
         * Default constructor for EditionInfo
         */
        public EditionInfo() {}

        /**
         * Full constructor for EditionInfo
         * 
         * @param googleBooksId Google Books identifier for this edition
         * @param type Type of edition or identifier (ISBN_10, ISBN_13, etc)
         * @param identifier Actual identifier value
         * @param editionIsbn10 Specific ISBN-10 for this edition
         * @param editionIsbn13 Specific ISBN-13 for this edition
         * @param publishedDate Publication date for this edition
         * @param coverImageUrl Cover image URL specific to this edition
         */
        public EditionInfo(String googleBooksId, String type, String identifier, String editionIsbn10, String editionIsbn13, Date publishedDate, String coverImageUrl) {
            this.googleBooksId = googleBooksId;
            this.type = type;
            this.identifier = identifier;
            this.editionIsbn10 = editionIsbn10;
            this.editionIsbn13 = editionIsbn13;
            this.publishedDate = publishedDate;
            this.coverImageUrl = coverImageUrl;
        }

        /**
         * Get Google Books identifier for this edition
         * 
         * @return Google Books identifier
         */
        public String getGoogleBooksId() {
            return googleBooksId;
        }

        /**
         * Set Google Books identifier for this edition
         * 
         * @param googleBooksId Google Books identifier
         */
        public void setGoogleBooksId(String googleBooksId) {
            this.googleBooksId = googleBooksId;
        }

        /**
         * Get edition type or identifier type
         * - Examples: ISBN_10, ISBN_13, ISSN, etc
         * 
         * @return Edition type
         */
        public String getType() {
            return type;
        }

        /**
         * Set edition type or identifier type
         * - Examples: ISBN_10, ISBN_13, ISSN, etc
         * 
         * @param type Edition type
         */
        public void setType(String type) {
            this.type = type;
        }

        /**
         * Get identifier value for this edition
         * - Actual ISBN, ISSN, or other identifier number
         * 
         * @return Edition identifier
         */
        public String getIdentifier() {
            return identifier;
        }

        /**
         * Set identifier value for this edition
         * - Actual ISBN, ISSN, or other identifier number
         * 
         * @param identifier Edition identifier
         */
        public void setIdentifier(String identifier) {
            this.identifier = identifier;
        }

        /**
         * Get specific ISBN-10 for this edition
         * 
         * @return Specific ISBN-10
         */
        public String getEditionIsbn10() {
            return editionIsbn10;
        }

        /**
         * Set specific ISBN-10 for this edition
         * 
         * @param editionIsbn10 Specific ISBN-10
         */
        public void setEditionIsbn10(String editionIsbn10) {
            this.editionIsbn10 = editionIsbn10;
        }

        /**
         * Get specific ISBN-13 for this edition
         * 
         * @return Specific ISBN-13
         */
        public String getEditionIsbn13() {
            return editionIsbn13;
        }

        /**
         * Set specific ISBN-13 for this edition
         * 
         * @param editionIsbn13 Specific ISBN-13
         */
        public void setEditionIsbn13(String editionIsbn13) {
            this.editionIsbn13 = editionIsbn13;
        }

        /**
         * Get edition-specific publication date
         * - Different formats may have different publication dates
         * 
         * @return Edition publication date
         */
        public Date getPublishedDate() {
            return publishedDate;
        }

        /**
         * Set edition-specific publication date
         * - Different formats may have different publication dates
         * 
         * @param publishedDate Edition publication date
         */
        public void setPublishedDate(Date publishedDate) {
            this.publishedDate = publishedDate;
        }

        /**
         * Get edition-specific cover image URL
         * - Different editions may have different cover images
         * 
         * @return Edition cover image URL
         */
        public String getCoverImageUrl() {
            return coverImageUrl;
        }

        /**
         * Set edition-specific cover image URL
         * - Different editions may have different cover images
         * 
         * @param coverImageUrl Edition cover image URL
         */
        public void setCoverImageUrl(String coverImageUrl) {
            this.coverImageUrl = coverImageUrl;
        }
    }

    /**
     * Default constructor
     * - Initializes empty otherEditions collection
     */
    public Book() {
        this.otherEditions = new ArrayList<>();
        this.qualifiers = new HashMap<>();
    }

    /**
     * Full constructor with essential book properties
     * 
     * @param id Unique identifier for the book
     * @param title Book title
     * @param authors List of book authors
     * @param description Book description or summary
     * @param coverImageUrl URL to book cover image
     * @param imageUrl URL to alternative book image
     */
    public Book(String id, String title, List<String> authors, String description, String coverImageUrl, String imageUrl) {
        this.id = id;
        this.title = title;
        this.authors = authors;
        this.description = description;
        this.coverImageUrl = coverImageUrl;
        this.imageUrl = imageUrl;
        this.otherEditions = new ArrayList<>();
        this.qualifiers = new HashMap<>();
    }

    /**
     * Get the book's unique identifier
     * 
     * @return Unique book identifier
     */
    public String getId() {
        return id;
    }

    /**
     * Set the book's unique identifier
     * 
     * @param id Unique book identifier
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Get the book's title
     * 
     * @return Book title
     */
    public String getTitle() {
        return title;
    }

    /**
     * Set the book's title
     * 
     * @param title Book title
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Get the list of book authors
     * 
     * @return List of author names
     */
    public List<String> getAuthors() {
        return authors;
    }

    /**
     * Set the list of book authors
     * 
     * @param authors List of author names
     */
    public void setAuthors(List<String> authors) {
        this.authors = authors;
    }

    /**
     * Get the book description or summary
     * 
     * @return Book description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Set the book description or summary
     * 
     * @param description Book description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Get the URL to the book cover image
     * 
     * @return URL to book cover image
     */
    public String getCoverImageUrl() {
        return coverImageUrl;
    }

    /**
     * Set the URL to the book cover image
     * 
     * @param coverImageUrl URL to book cover image
     */
    public void setCoverImageUrl(String coverImageUrl) {
        this.coverImageUrl = coverImageUrl;
    }

    /**
     * Get alternative image URL for the book
     * 
     * @return Alternative image URL
     */
    public String getImageUrl() {
        return imageUrl;
    }

    /**
     * Set the alternative image URL for the book
     * 
     * @param imageUrl Alternative image URL
     */
    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    /**
     * Get ISBN-10 identifier
     * 
     * @return ISBN-10 value
     */
    public String getIsbn10() {
        return isbn10;
    }

    /**
     * Set ISBN-10 identifier
     * 
     * @param isbn10 ISBN-10 value
     */
    public void setIsbn10(String isbn10) {
        this.isbn10 = isbn10;
    }

    /**
     * Get ISBN-13 identifier
     * 
     * @return ISBN-13 value
     */
    public String getIsbn13() {
        return isbn13;
    }

    /**
     * Set ISBN-13 identifier
     * 
     * @param isbn13 ISBN-13 value
     */
    public void setIsbn13(String isbn13) {
        this.isbn13 = isbn13;
    }

    /**
     * Get book publication date
     * 
     * @return Publication date
     */
    public Date getPublishedDate() {
        return publishedDate;
    }

    /**
     * Set book publication date
     * 
     * @param publishedDate Publication date
     */
    public void setPublishedDate(Date publishedDate) {
        this.publishedDate = publishedDate;
    }

    /**
     * Get book categories or genres
     * 
     * @return List of book categories
     */
    public List<String> getCategories() {
        return categories;
    }

    /**
     * Set book categories or genres
     * 
     * @param categories List of book categories
     */
    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    /**
     * Get average book rating
     * 
     * @return Average rating value
     */
    public Double getAverageRating() {
        return averageRating;
    }

    /**
     * Set average book rating
     * 
     * @param averageRating Average rating value
     */
    public void setAverageRating(Double averageRating) {
        this.averageRating = averageRating;
    }

    /**
     * Get number of ratings for the book
     * 
     * @return Number of ratings
     */
    public Integer getRatingsCount() {
        return ratingsCount;
    }

    /**
     * Set number of ratings for the book
     * 
     * @param ratingsCount Number of ratings
     */
    public void setRatingsCount(Integer ratingsCount) {
        this.ratingsCount = ratingsCount;
    }

    /**
     * Get raw ratings data in JSON format
     * 
     * @return Raw ratings data string
     */
    public String getRawRatingsData() {
        return rawRatingsData;
    }

    /**
     * Set the raw ratings data for this book
     * 
     * @param rawRatingsData Raw ratings data string
     */
    public void setRawRatingsData(String rawRatingsData) {
        this.rawRatingsData = rawRatingsData;
    }

    /**
     * Check if book has ratings
     * 
     * @return True if book has ratings, false otherwise
     */
    public Boolean getHasRatings() {
        return hasRatings;
    }

    /**
     * Set whether book has ratings
     * 
     * @param hasRatings True if book has ratings, false otherwise
     */
    public void setHasRatings(Boolean hasRatings) {
        this.hasRatings = hasRatings;
    }

    /**
     * Get total number of pages in the book
     * 
     * @return Number of pages
     */
    public Integer getPageCount() {
        return pageCount;
    }

    /**
     * Set total number of pages in the book
     * 
     * @param pageCount Number of pages
     */
    public void setPageCount(Integer pageCount) {
        this.pageCount = pageCount;
    }

    /**
     * Get book language code
     * 
     * @return Language code (e.g., "en" for English)
     */
    public String getLanguage() {
        return language;
    }

    /**
     * Set book language code
     * 
     * @param language Language code (e.g., "en" for English)
     */
    public void setLanguage(String language) {
        this.language = language;
    }

    /**
     * Get book publisher name
     * 
     * @return Publisher name
     */
    public String getPublisher() {
        return publisher;
    }

    /**
     * Set book publisher name
     * 
     * @param publisher Publisher name
     */
    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    /**
     * Get link to more information about the book
     * 
     * @return Information link URL
     */
    public String getInfoLink() {
        return infoLink;
    }

    /**
     * Set link to more information about the book
     * 
     * @param infoLink Information link URL
     */
    public void setInfoLink(String infoLink) {
        this.infoLink = infoLink;
    }

    /**
     * Get link to preview the book
     * 
     * @return Preview link URL
     */
    public String getPreviewLink() {
        return previewLink;
    }

    /**
     * Set link to preview the book
     * 
     * @param previewLink Preview link URL
     */
    public void setPreviewLink(String previewLink) {
        this.previewLink = previewLink;
    }

    /**
     * Get link to purchase the book
     * 
     * @return Purchase link URL
     */
    public String getPurchaseLink() {
        return purchaseLink;
    }

    /**
     * Set link to purchase the book
     * 
     * @param purchaseLink Purchase link URL
     */
    public void setPurchaseLink(String purchaseLink) {
        this.purchaseLink = purchaseLink;
    }

    /**
     * Get book list price
     * 
     * @return List price value
     */
    public Double getListPrice() {
        return listPrice;
    }

    /**
     * Set book list price
     * 
     * @param listPrice List price value
     */
    public void setListPrice(Double listPrice) {
        this.listPrice = listPrice;
    }

    /**
     * Get currency code for list price
     * 
     * @return Currency code (e.g., "USD")
     */
    public String getCurrencyCode() {
        return currencyCode;
    }

    /**
     * Set currency code for list price
     * 
     * @param currencyCode Currency code (e.g., "USD")
     */
    public void setCurrencyCode(String currencyCode) {
        this.currencyCode = currencyCode;
    }

    /**
     * Get link to web reader for the book
     * 
     * @return Web reader link URL
     */
    public String getWebReaderLink() {
        return webReaderLink;
    }

    /**
     * Set link to web reader for the book
     * 
     * @param webReaderLink Web reader link URL
     */
    public void setWebReaderLink(String webReaderLink) {
        this.webReaderLink = webReaderLink;
    }
    
    /**
     * Check if PDF format is available for this book
     * 
     * @return True if PDF is available, false otherwise
     */
    public Boolean getPdfAvailable() {
        return pdfAvailable;
    }
    
    /**
     * Set PDF availability for this book
     * 
     * @param pdfAvailable True if PDF is available, false otherwise
     */
    public void setPdfAvailable(Boolean pdfAvailable) {
        this.pdfAvailable = pdfAvailable;
    }
    
    /**
     * Check if EPUB format is available for this book
     * 
     * @return True if EPUB is available, false otherwise
     */
    public Boolean getEpubAvailable() {
        return epubAvailable;
    }
    
    /**
     * Set EPUB availability for this book
     * 
     * @param epubAvailable True if EPUB is available, false otherwise
     */
    public void setEpubAvailable(Boolean epubAvailable) {
        this.epubAvailable = epubAvailable;
    }

    /**
     * Get list of alternate editions for this book
     * 
     * @return List of edition information
     */
    public List<EditionInfo> getOtherEditions() {
        return otherEditions;
    }

    /**
     * Set list of alternate editions for this book
     * 
     * @param otherEditions List of edition information
     */
    public void setOtherEditions(List<EditionInfo> otherEditions) {
        this.otherEditions = otherEditions;
    }

    /**
     * Get cover image width in pixels
     * 
     * @return Cover image width
     */
    public Integer getCoverImageWidth() {
        return coverImageWidth;
    }

    /**
     * Set cover image width in pixels
     * 
     * @param coverImageWidth Cover image width
     */
    public void setCoverImageWidth(Integer coverImageWidth) {
        this.coverImageWidth = coverImageWidth;
    }

    /**
     * Get cover image height in pixels
     * 
     * @return Cover image height
     */
    public Integer getCoverImageHeight() {
        return coverImageHeight;
    }

    /**
     * Set cover image height in pixels
     * 
     * @param coverImageHeight Cover image height
     */
    public void setCoverImageHeight(Integer coverImageHeight) {
        this.coverImageHeight = coverImageHeight;
    }

    /**
     * Check if book cover is high resolution
     * 
     * @return True if cover is high resolution, false otherwise
     */
    public Boolean getIsCoverHighResolution() {
        return isCoverHighResolution;
    }

    /**
     * Set high resolution flag for book cover
     * 
     * @param isCoverHighResolution True if cover is high resolution, false otherwise
     */
    public void setIsCoverHighResolution(Boolean isCoverHighResolution) {
        this.isCoverHighResolution = isCoverHighResolution;
    }

    /**
     * Get cover images container with various resolution URLs
     * 
     * @return Cover images container
     */
    public CoverImages getCoverImages() {
        return coverImages;
    }

    /**
     * Set cover images container with various resolution URLs
     * 
     * @param coverImages Cover images container
     */
    public void setCoverImages(CoverImages coverImages) {
        this.coverImages = coverImages;
    }

    /**
     * Get raw JSON response from external API
     * - Used for debugging and logging purposes
     * - Contains original data from API response
     * 
     * @return Raw JSON response string
     */
    public String getRawJsonResponse() {
        return rawJsonResponse;
    }

    /**
     * Set raw JSON response from external API
     * - Used for provenance tracking of book data
     * - Preserves original data from API response
     * 
     * @param rawJsonResponse Raw JSON response string
     */
    public void setRawJsonResponse(String rawJsonResponse) {
        this.rawJsonResponse = rawJsonResponse;
    }

    /**
     * Get map of special qualifiers for this book 
     * (e.g., "new york times bestseller", "award winner", search terms that matched)
     * 
     * @return Map of qualifier name to qualifier data
     */
    public Map<String, Object> getQualifiers() {
        return qualifiers;
    }

    /**
     * Set map of special qualifiers for this book
     * 
     * @param qualifiers Map of qualifier name to qualifier data
     */
    public void setQualifiers(Map<String, Object> qualifiers) {
        this.qualifiers = qualifiers;
    }
    
    /**
     * Add a single qualifier to this book
     * 
     * @param key Qualifier name/type
     * @param value Qualifier data/value
     */
    public void addQualifier(String key, Object value) {
        if (this.qualifiers == null) {
            this.qualifiers = new HashMap<>();
        }
        this.qualifiers.put(key, value);
    }
    
    /**
     * Check if this book has a specific qualifier
     * 
     * @param key Qualifier name/type to check
     * @return true if the book has this qualifier
     */
    public boolean hasQualifier(String key) {
        return this.qualifiers != null && this.qualifiers.containsKey(key);
    }

    /**
     * Equality check based on book ID
     * - Two books are considered equal if they have the same ID
     * - Other properties are not considered in equality check
     * 
     * @param o Object to compare with
     * @return True if the books have the same ID, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Book book = (Book) o;
        return Objects.equals(id, book.id);
    }

    /**
     * Generate hash code based on book ID
     * 
     * @return Hash code value
     */
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    /**
     * String representation of book
     * - Includes ID, title, authors and other edition count
     * - Used for logging and debugging
     * 
     * @return String representation of book
     */
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
