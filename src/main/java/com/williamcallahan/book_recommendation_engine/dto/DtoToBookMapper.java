package com.williamcallahan.book_recommendation_engine.dto;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.util.BookDomainMapper;

import java.util.List;

/**
 * TEMPORARY BRIDGE: Maps DTOs back to Book entities for backward compatibility.
 *
 * <p>This wrapper now delegates to {@link com.williamcallahan.book_recommendation_engine.util.BookDomainMapper}
 * so new code can
 * consume a non-deprecated adapter while existing callers continue using this
 * class until it is removed.</p>
 *
 * @deprecated Use the DTOs directly and rely on {@code BookQueryRepository}
 * projections such as {@link BookCard} and {@link BookDetail}.
 */
@Deprecated(since = "2025-10-01", forRemoval = true)
public class DtoToBookMapper {

    private DtoToBookMapper() {
        // Utility class
    }

    /**
     * Map BookCard DTO to Book entity (temporary bridge).
     */
    public static Book toBook(BookCard card) {
        return BookDomainMapper.fromCard(card);
    }

    /**
     * Map multiple BookCards to Books (temporary bridge).
     */
    public static List<Book> toBooks(List<BookCard> cards) {
        return BookDomainMapper.fromCards(cards);
    }

    /**
     * Map BookDetail DTO to Book entity (temporary bridge).
     */
    public static Book toBook(BookDetail detail) {
        return BookDomainMapper.fromDetail(detail);
    }

    /**
     * Map BookListItem DTO to Book entity (temporary bridge).
     */
    public static Book toBook(BookListItem item) {
        return BookDomainMapper.fromListItem(item);
    }

    /**
     * Map BookListItem DTOs to Book entities.
     */
    public static List<Book> toBooksFromListItems(List<BookListItem> items) {
        return BookDomainMapper.fromListItems(items);
    }
}
