package com.williamcallahan.book_recommendation_engine.types;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.image.ImageDetails;
import reactor.core.publisher.Mono;

public interface ExternalCoverService {
    /**
     * Fetches a cover for a book from the specific external service.
     *
     * @param book The book to fetch a cover for
     * @return A Mono emitting ImageDetails if a cover is found, or an empty Mono otherwise.
     */
    Mono<ImageDetails> fetchCover(Book book);
}
