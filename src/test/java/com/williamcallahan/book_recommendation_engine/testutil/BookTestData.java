package com.williamcallahan.book_recommendation_engine.testutil;

import com.williamcallahan.book_recommendation_engine.model.Book;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Fluent builder utilities for constructing Book test instances.
 */
public final class BookTestData {

    private BookTestData() {}

    public static BookBuilder aBook() {
        return new BookBuilder();
    }

    public static Book minimalBook(String id) {
        return aBook().id(id).title("Title " + id).build();
    }

    public static class BookBuilder {
        private String id = "test-book-id";
        private String title = "Test Book";
        private String description = "";
        private String language = "en";
        private List<String> authors = new ArrayList<>(List.of("Test Author"));
        private List<String> categories = new ArrayList<>();
        private String s3ImagePath = null;
        private Date publishedDate = Date.from(Instant.parse("2020-01-01T00:00:00Z"));
        private String isbn13 = "9781234567890";
        private String externalImageUrl = "https://example.com/test-book-cover.jpg";

        public BookBuilder id(String id) { this.id = id; return this; }
        public BookBuilder title(String title) { this.title = title; return this; }
        public BookBuilder description(String description) { this.description = description; return this; }
        public BookBuilder language(String language) { this.language = language; return this; }
        public BookBuilder authors(List<String> authors) { this.authors = new ArrayList<>(authors); return this; }
        public BookBuilder categories(List<String> categories) { this.categories = new ArrayList<>(categories); return this; }
        public BookBuilder s3ImagePath(String path) { this.s3ImagePath = path; return this; }
        public BookBuilder publishedDate(Date date) { this.publishedDate = date; return this; }
        public BookBuilder isbn13(String isbn13) { this.isbn13 = isbn13; return this; }
        public BookBuilder coverImageUrl(String url) { this.externalImageUrl = url; return this; }
        public BookBuilder externalImageUrl(String url) { this.externalImageUrl = url; return this; }

        public Book build() {
            Book b = new Book();
            b.setId(id);
            b.setTitle(title);
            b.setDescription(description);
            b.setLanguage(language);
            b.setAuthors(authors);
            b.setCategories(categories);
            b.setS3ImagePath(s3ImagePath);
            b.setPublishedDate(publishedDate);
            b.setIsbn13(isbn13);
            if (externalImageUrl != null) {
                b.setExternalImageUrl(externalImageUrl);
            }
            return b;
        }
    }
}
