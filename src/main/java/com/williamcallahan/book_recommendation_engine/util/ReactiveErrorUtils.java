package com.williamcallahan.book_recommendation_engine.util;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

@Slf4j
public final class ReactiveErrorUtils {
    private ReactiveErrorUtils() {
    }

    public static <T> Function<Throwable, Mono<T>> logAndReturnEmpty(String context) {
        return error -> {
            log.error("{}: {}", context, error.getMessage());
            return Mono.empty();
        };
    }

    public static <T> Function<Throwable, Mono<List<T>>> logAndReturnEmptyList(String context) {
        return error -> {
            log.error("{}: {}", context, error.getMessage());
            return Mono.just(Collections.emptyList());
        };
    }

    public static <T> Function<Throwable, Flux<T>> logAndReturnEmptyFlux(String context) {
        return error -> {
            log.error("{}: {}", context, error.getMessage());
            return Flux.empty();
        };
    }

    public static <T> Function<Throwable, Mono<T>> logAndReturnDefault(String context, T defaultValue) {
        return error -> {
            log.error("{}: {}", context, error.getMessage());
            return Mono.just(defaultValue);
        };
    }

    public static Function<Throwable, Mono<String>> logAndReturnEmptyString(String context) {
        return error -> {
            log.error("{}: {}", context, error.getMessage());
            return Mono.just("");
        };
    }

    public static Function<Throwable, Mono<Boolean>> logAndReturnFalse(String context) {
        return error -> {
            log.error("{}: {}", context, error.getMessage());
            return Mono.just(false);
        };
    }

    public static <T> Mono<T> logErrorAndContinue(Throwable error, String context) {
        log.error("{}: {}", context, error.getMessage());
        return Mono.empty();
    }

    public static void logError(Throwable error, String context) {
        log.error("{}: {}", context, error.getMessage(), error);
    }

    public static void logErrorWithDetails(Throwable error, String context, Object... details) {
        log.error("{}: {} - Details: {}", context, error.getMessage(), details, error);
    }

    /**
     * Collect a Flux to a list safely, returning empty list on error.
     */
    public static <T> Mono<List<T>> collectSafely(Flux<T> flux) {
        return flux.collectList()
                   .onErrorReturn(Collections.emptyList());
    }

    /**
     * Collect a Flux to a list safely with error logging.
     */
    public static <T> Mono<List<T>> collectSafely(Flux<T> flux, String context) {
        return flux.collectList()
                   .onErrorResume(e -> {
                       log.error("{}: {}", context, e.getMessage());
                       return Mono.just(Collections.emptyList());
                   });
    }

    /**
     * Take a limited number of items and collect to list.
     */
    public static <T> Mono<List<T>> limitAndCollect(Flux<T> flux, int limit) {
        return flux.take(limit)
                   .collectList()
                   .defaultIfEmpty(Collections.emptyList());
    }

    /**
     * Take a limited number of items, collect to list, with error handling.
     */
    public static <T> Mono<List<T>> limitAndCollectSafely(Flux<T> flux, int limit, String context) {
        return flux.take(limit)
                   .collectList()
                   .defaultIfEmpty(Collections.emptyList())
                   .onErrorResume(e -> {
                       log.error("{}: {}", context, e.getMessage());
                       return Mono.just(Collections.emptyList());
                   });
    }

    /**
     * Convert a Mono that might be empty to a default value.
     */
    public static <T> Mono<T> withDefault(Mono<T> mono, T defaultValue) {
        return mono.defaultIfEmpty(defaultValue);
    }

    /**
     * Convert a Mono that might error to a default value.
     */
    public static <T> Mono<T> withDefaultOnError(Mono<T> mono, T defaultValue, String context) {
        return mono.onErrorResume(e -> {
            log.error("{}: {}", context, e.getMessage());
            return Mono.just(defaultValue);
        });
    }
}