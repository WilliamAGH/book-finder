package com.williamcallahan.book_recommendation_engine.dto;

import com.williamcallahan.book_recommendation_engine.dto.EditionSummary;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.util.BookDomainMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DtoToBookMapperCoverSourceTest {

    @Test
    @DisplayName("toBook(BookCard) detects Google Books cover source")
    void toBookFromCard_detectsGoogleBooksSource() {
        BookCard card = new BookCard(
            "card-id",
            "card-slug",
            "Card Title",
            List.of("Author"),
            "https://books.google.com/books/content?id=ABC123&printsec=frontcover",
            4.2,
            120,
            Map.of()
        );

        Book book = BookDomainMapper.fromCard(card);

        assertThat(book).isNotNull();
        assertThat(book.getCoverImages()).isNotNull();
        assertThat(book.getCoverImages().getSource()).isEqualTo(CoverImageSource.GOOGLE_BOOKS);
    }

    @Test
    @DisplayName("toBook(BookDetail) detects Open Library cover source")
    void toBookFromDetail_detectsOpenLibrarySource() {
        BookDetail detail = new BookDetail(
            "detail-id",
            "detail-slug",
            "Detail Title",
            "Description",
            "Publisher",
            null,
            "en",
            350,
            List.of("Author"),
            List.of("Category"),
            "https://covers.openlibrary.org/b/id/12345-L.jpg",
            null,
            600,
            900,
            Boolean.TRUE,
            "OPEN_LIBRARY",
            4.5,
            340,
            "isbn10",
            "isbn13",
            "preview",
            "info",
            Collections.<String, Object>emptyMap(),
            Collections.<EditionSummary>emptyList()
        );

        Book book = BookDomainMapper.fromDetail(detail);

        assertThat(book).isNotNull();
        assertThat(book.getCoverImages()).isNotNull();
        assertThat(book.getCoverImages().getSource()).isEqualTo(CoverImageSource.OPEN_LIBRARY);
    }

    @Test
    @DisplayName("toBook(BookListItem) leaves unknown sources as UNDEFINED")
    void toBookFromListItem_defaultsToUndefinedForUnknownSources() {
        BookListItem item = new BookListItem(
            "item-id",
            "item-slug",
            "Item Title",
            "Description",
            List.of("Author"),
            List.of("Category"),
            "https://cdn.example.com/covers/item-id.jpg",
            3.9,
            80,
            Map.of()
        );

        Book book = BookDomainMapper.fromListItem(item);

        assertThat(book).isNotNull();
        assertThat(book.getCoverImages()).isNotNull();
        assertThat(book.getCoverImages().getSource()).isEqualTo(CoverImageSource.UNDEFINED);
    }
}
