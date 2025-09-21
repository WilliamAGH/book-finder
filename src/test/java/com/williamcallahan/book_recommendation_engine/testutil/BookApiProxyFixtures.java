package com.williamcallahan.book_recommendation_engine.testutil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.service.BookApiProxy;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksMockService;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.S3StorageService;

import java.nio.file.Path;
import java.util.Optional;

/** Factory helpers to build BookApiProxy for tests with sensible defaults. */
public final class BookApiProxyFixtures {
    private BookApiProxyFixtures() {}

    public static BookApiProxy newProxy(GoogleBooksService googleBooksService,
                                        S3StorageService s3StorageService,
                                        Optional<GoogleBooksMockService> mockService,
                                        BookDataOrchestrator bookDataOrchestrator,
                                        Path tempDir,
                                        boolean enableGoogleFallback) {
        return new BookApiProxy(
                googleBooksService,
                s3StorageService,
                new ObjectMapper(),
                mockService,
                enableGoogleFallback,
                tempDir.toString(),
                false,
                false,
                false,
                bookDataOrchestrator
        );
    }
}