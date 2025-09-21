package com.williamcallahan.book_recommendation_engine.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.Function;

/**
 * Utility class for common reactive controller patterns.
 * Reduces boilerplate in controller classes handling Mono responses.
 */
@Slf4j
public final class ReactiveControllerUtils {

    private ReactiveControllerUtils() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    /**
     * Build a standard response: 200 OK with data, or 404 Not Found if empty.
     */
    public static <T> Mono<ResponseEntity<T>> buildResponse(Mono<T> data) {
        return data.map(ResponseEntity::ok)
                   .switchIfEmpty(Mono.just(ResponseEntity.notFound().build()));
    }

    /**
     * Build a response with custom empty response status.
     */
    public static <T> Mono<ResponseEntity<T>> buildResponse(Mono<T> data, HttpStatus emptyStatus) {
        return data.map(ResponseEntity::ok)
                   .switchIfEmpty(Mono.just(ResponseEntity.status(emptyStatus).build()));
    }

    /**
     * Build a response with error handling, logging errors and returning 500.
     */
    public static <T> Mono<ResponseEntity<T>> withErrorHandling(Mono<T> data, String context) {
        return buildResponse(data)
            .onErrorResume(e -> {
                log.error("{}: {}", context, e.getMessage(), e);
                return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build());
            });
    }

    /**
     * Build a response with custom error handling.
     */
    public static <T> Mono<ResponseEntity<T>> withErrorHandling(Mono<T> data,
                                                                String context,
                                                                Function<Throwable, ResponseEntity<T>> errorHandler) {
        return buildResponse(data)
            .onErrorResume(e -> {
                log.error("{}: {}", context, e.getMessage(), e);
                return Mono.just(errorHandler.apply(e));
            });
    }

    /**
     * Build an error response with a message body.
     */
    public static Mono<ResponseEntity<Map<String, String>>> errorResponse(HttpStatus status, String message) {
        return Mono.just(ResponseEntity
            .status(status)
            .body(Map.of("error", message)));
    }

    /**
     * Build a bad request response with error message.
     */
    public static Mono<ResponseEntity<Map<String, String>>> badRequest(String message) {
        return errorResponse(HttpStatus.BAD_REQUEST, message);
    }

    /**
     * Build an internal server error response with error message.
     */
    public static Mono<ResponseEntity<Map<String, String>>> internalServerError(String message) {
        return errorResponse(HttpStatus.INTERNAL_SERVER_ERROR, message);
    }
}