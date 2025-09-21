package com.williamcallahan.book_recommendation_engine.testutil;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import org.mockito.ArgumentMatcher;

/** Common Mockito argument matchers used in tests. */
public final class EventMatchers {
    private EventMatchers() {}

    public static ArgumentMatcher<BookCoverUpdatedEvent> bookCoverUpdated(String identifierKey,
                                                                          String expectedUrl,
                                                                          CoverImageSource expectedSource) {
        return event -> event != null
                && identifierKey.equals(event.getIdentifierKey())
                && expectedUrl.equals(event.getNewCoverUrl())
                && expectedSource == event.getSource();
    }
}