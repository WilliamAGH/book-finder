package com.williamcallahan.book_recommendation_engine.testutil;

import com.williamcallahan.book_recommendation_engine.model.Book;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** StepVerifier shortcuts for common patterns in list-based Monos. */
public final class ReactorAssertions {
    private ReactorAssertions() {}

    /** Verify the Mono emits a single list whose first element has the given id, then completes. */
    public static void verifyListHasSingleId(Mono<List<Book>> mono, String id) {
        StepVerifier.create(mono)
                .expectNextMatches(list -> list.size() == 1 && id.equals(list.get(0).getId()))
                .verifyComplete();
    }

    /** Verify the Mono emits an empty list and completes. */
    public static void verifyEmptyList(Mono<List<Book>> mono) {
        StepVerifier.create(mono)
                .expectNextMatches(List::isEmpty)
                .verifyComplete();
    }

    /** Verify a Mono completes successfully (useful for Mono<Void>). */
    public static void verifyCompletes(Mono<?> mono) {
        StepVerifier.create(mono).verifyComplete();
    }

    /** Verify using an arbitrary predicate on the emitted list. */
    public static void verifyListMatches(Mono<List<Book>> mono, Predicate<List<Book>> predicate) {
        StepVerifier.create(mono)
                .expectNextMatches(list -> {
                    assertTrue(predicate.test(list));
                    return true;
                })
                .verifyComplete();
    }
}