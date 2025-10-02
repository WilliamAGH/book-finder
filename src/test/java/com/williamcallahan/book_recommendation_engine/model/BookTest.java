package com.williamcallahan.book_recommendation_engine.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BookTest {

    @Test
    void setCoverImageUrlAssignsS3ForAmazonHost() {
        Book book = new Book();

        String s3Url = "https://s3.amazonaws.com/example-bucket/covers/book.jpg";
        book.setCoverImageUrl(s3Url);

        assertThat(book.getS3ImagePath()).isEqualTo(s3Url);
        assertThat(book.getExternalImageUrl()).isNull();
    }

    @Test
    void setCoverImageUrlAssignsS3ForSpacesHost() {
        Book book = new Book();

        String spacesUrl = "https://nyc3.digitaloceanspaces.com/example/covers/book.jpg";
        book.setCoverImageUrl(spacesUrl);

        assertThat(book.getS3ImagePath()).isEqualTo(spacesUrl);
        assertThat(book.getExternalImageUrl()).isNull();
    }

    @Test
    void setCoverImageUrlAssignsExternalForHttpAndHttps() {
        Book book = new Book();

        String httpUrl = "http://images.example.com/cover.jpg";
        book.setCoverImageUrl(httpUrl);

        assertThat(book.getExternalImageUrl()).isEqualTo(httpUrl);
        assertThat(book.getS3ImagePath()).isNull();

        String httpsUrl = "https://images.example.com/cover.jpg";
        book.setCoverImageUrl(httpsUrl);

        assertThat(book.getExternalImageUrl()).isEqualTo(httpsUrl);
        assertThat(book.getS3ImagePath()).isNull();
    }

    @Test
    void setCoverImageUrlIgnoresSpoofedQueryParams() {
        Book book = new Book();

        String spoofed = "https://images.example.com/cover.jpg?redirect=https://s3.amazonaws.com/evil";
        book.setCoverImageUrl(spoofed);

        assertThat(book.getExternalImageUrl()).isEqualTo(spoofed);
        assertThat(book.getS3ImagePath()).isNull();
    }
}
