package com.williamcallahan.book_recommendation_engine.util;

import com.williamcallahan.book_recommendation_engine.dto.BookAggregate;
import com.williamcallahan.book_recommendation_engine.model.Book;

/**
 * @deprecated Persist {@link com.williamcallahan.book_recommendation_engine.dto.BookAggregate}
 * using {@link com.williamcallahan.book_recommendation_engine.service.BookUpsertService}
 * and consume canonical DTO projections via
 * {@link com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository}.
 */
@Deprecated(since = "2025-10-01", forRemoval = true)
public final class BookAggregateToBookConverter {

    private BookAggregateToBookConverter() {
        // Utility class
    }

    public static Book convert(BookAggregate aggregate) {
        return BookDomainMapper.fromAggregate(aggregate);
    }
}
