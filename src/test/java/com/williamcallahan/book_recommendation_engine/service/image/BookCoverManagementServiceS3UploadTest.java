/**
 * Test suite for the S3 upload functionality in the BookCoverManagementService
 * 
 * This test class focuses on validating:
 * - Successful S3 image uploads from local cache
 * - Error handling during S3 upload failures
 * - Fallback mechanisms to local cache when S3 is unavailable
 * - Placeholder image handling when no image source is available
 * 
 * Tests use mocked dependencies to isolate the S3 upload functionality and
 * verify the correct event publishing and cache updates in various scenarios
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.EnvironmentService;
import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.util.ImageCacheUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import org.mockito.ArgumentMatcher;

import java.util.Comparator;

/**
 * Unit tests for S3 upload functionality in BookCoverManagementService
 */
public class BookCoverManagementServiceS3UploadTest {
    
    private static final Logger logger = LoggerFactory.getLogger(BookCoverManagementServiceS3UploadTest.class);

    private final String LOCAL_PLACEHOLDER_PATH = "/images/placeholder-book-cover.svg";
    
    /**
     * Tests the scenario where a locally cached image is successfully uploaded to S3
     * 
     * @throws Exception if test execution fails
     * 
     * @implNote This test verifies:
     * - Correct parameters are passed to the S3 upload service
     * - Cache is updated with S3 image details after successful upload
     * - BookCoverUpdatedEvent is published with the S3 URL
     * - The complete reactive flow works as expected
     */
    @Test
    public void testSuccessfulS3Upload() throws Exception {
        // ARRANGE
        byte[] testImageBytes = new byte[] {1, 2, 3, 4, 5}; // Mock image data
        
        S3BookCoverService s3Service = mock(S3BookCoverService.class);
        LocalDiskCoverCacheService diskService = mock(LocalDiskCoverCacheService.class);
        CoverSourceFetchingService sourceFetchingService = mock(CoverSourceFetchingService.class);
        CoverCacheManager cacheManager = mock(CoverCacheManager.class);
        ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        EnvironmentService environmentService = mock(EnvironmentService.class);

        BookCoverManagementService bookCoverManagementService = new BookCoverManagementService(
            cacheManager, sourceFetchingService, s3Service, diskService, eventPublisher, environmentService
        );
        
        Book testBook = createTestBook();
        String identifierKey = ImageCacheUtils.getIdentifierKey(testBook);

        when(environmentService.isBookCoverDebugMode()).thenReturn(true);
        when(diskService.getLocalPlaceholderPath()).thenReturn(LOCAL_PLACEHOLDER_PATH);

        Path tempDir = Files.createTempDirectory("cover-cache-success");
        try {
            String cacheDirName = tempDir.getFileName().toString();
            when(diskService.getCacheDirName()).thenReturn(cacheDirName);
            when(diskService.getCacheDirString()).thenReturn(tempDir.toString());

            Path localFile = tempDir.resolve("test-image.jpg");
            Files.write(localFile, testImageBytes);

            ImageDetails localCacheImageDetails = new ImageDetails(
                "/" + cacheDirName + "/test-image.jpg",
                "GOOGLE_BOOKS",
                "test-image.jpg",
                CoverImageSource.LOCAL_CACHE,
                ImageResolutionPreference.ORIGINAL,
                500, 700
            );

            when(sourceFetchingService.getBestCoverImageUrlAsync(
                    eq(testBook), anyString(), any(ImageProvenanceData.class)))
                .thenReturn(CompletableFuture.completedFuture(localCacheImageDetails));

            ImageDetails s3UploadedImageDetails = new ImageDetails(
                "https://cdn.example.com/books/test-image.jpg",
                "S3_CACHE",
                "books/test-image.jpg",
                CoverImageSource.S3_CACHE,
                ImageResolutionPreference.ORIGINAL,
                500, 700
            );

            when(s3Service.uploadProcessedCoverToS3Async(
                    any(byte[].class),
                    eq(".jpg"),
                    isNull(),
                    eq(500),
                    eq(700),
                    eq(testBook.getId()),
                    eq("GOOGLE_BOOKS"),
                    any(ImageProvenanceData.class)))
                .thenReturn(Mono.just(s3UploadedImageDetails));

            logger.info("Testing S3 upload functionality by calling BookCoverManagementService.processCoverInBackground");
            bookCoverManagementService.processCoverInBackground(testBook, "http://example.com/provisional.jpg");

            verify(s3Service, timeout(1000)).uploadProcessedCoverToS3Async(
                eq(testImageBytes),
                eq(".jpg"),
                isNull(),
                eq(500),
                eq(700),
                eq(testBook.getId()),
                eq("GOOGLE_BOOKS"),
                any(ImageProvenanceData.class)
            );

            verify(cacheManager, timeout(1000)).putFinalImageDetails(eq(identifierKey), eq(s3UploadedImageDetails));

            verify(eventPublisher, timeout(1000)).publishEvent(argThat(new ArgumentMatcher<BookCoverUpdatedEvent>() {
                @Override
                public boolean matches(BookCoverUpdatedEvent event) {
                    return event.getIdentifierKey().equals(identifierKey) &&
                           event.getNewCoverUrl().equals(s3UploadedImageDetails.getUrlOrPath()) &&
                           event.getSource() == CoverImageSource.S3_CACHE;
                }
            }));
        } finally {
            deleteDirectoryRecursively(tempDir);
        }
    }
    
    /**
     * Tests the scenario where S3 upload fails and we fall back to local cached image
     * 
     * @throws Exception if test execution fails
     * 
     * @implNote This test verifies:
     * - S3 upload error is handled properly
     * - System falls back to local cached image on S3 failure
     * - Cache is updated with local image details after S3 failure
     * - BookCoverUpdatedEvent is published with the local cache URL
     * - Error handling doesn't propagate exceptions to the caller
     */
    @Test
    public void testS3UploadFailure() throws Exception {
        // ARRANGE
        byte[] testImageBytes = new byte[] {1, 2, 3, 4, 5}; // Mock image data
        
        S3BookCoverService s3Service = mock(S3BookCoverService.class);
        LocalDiskCoverCacheService diskService = mock(LocalDiskCoverCacheService.class);
        CoverSourceFetchingService sourceFetchingService = mock(CoverSourceFetchingService.class); // Added declaration
        CoverCacheManager cacheManager = mock(CoverCacheManager.class);
        ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        EnvironmentService environmentService = mock(EnvironmentService.class);

        BookCoverManagementService bookCoverManagementService = new BookCoverManagementService(
            cacheManager, sourceFetchingService, s3Service, diskService, eventPublisher, environmentService
        );
        
        Book testBook = createTestBook();
        String identifierKey = ImageCacheUtils.getIdentifierKey(testBook);

        when(environmentService.isBookCoverDebugMode()).thenReturn(true);
        when(diskService.getLocalPlaceholderPath()).thenReturn(LOCAL_PLACEHOLDER_PATH);

        Path tempDir = Files.createTempDirectory("cover-cache-failure");
        try {
            String cacheDirName = tempDir.getFileName().toString();
            when(diskService.getCacheDirName()).thenReturn(cacheDirName);
            when(diskService.getCacheDirString()).thenReturn(tempDir.toString());

            Path localFile = tempDir.resolve("test-image.jpg");
            Files.write(localFile, testImageBytes);

            ImageDetails localCacheImageDetails = new ImageDetails(
                "/" + cacheDirName + "/test-image.jpg",
                "GOOGLE_BOOKS",
                "test-image.jpg",
                CoverImageSource.LOCAL_CACHE,
                ImageResolutionPreference.ORIGINAL,
                500, 700
            );

            when(sourceFetchingService.getBestCoverImageUrlAsync(
                    eq(testBook), anyString(), any(ImageProvenanceData.class)))
                .thenReturn(CompletableFuture.completedFuture(localCacheImageDetails));

            when(s3Service.uploadProcessedCoverToS3Async(
                    any(byte[].class),
                    anyString(),
                    isNull(),
                    anyInt(),
                    anyInt(),
                    eq(testBook.getId()),
                    eq("GOOGLE_BOOKS"),
                    any(ImageProvenanceData.class)))
                .thenReturn(Mono.error(new RuntimeException("S3 upload failed")));

            bookCoverManagementService.processCoverInBackground(testBook, "http://example.com/provisional.jpg");

            verify(s3Service, timeout(1000)).uploadProcessedCoverToS3Async(
                eq(testImageBytes),
                anyString(),
                isNull(),
                anyInt(),
                anyInt(),
                eq(testBook.getId()),
                eq("GOOGLE_BOOKS"),
                any(ImageProvenanceData.class)
            );

            verify(cacheManager, timeout(1000)).putFinalImageDetails(eq(identifierKey), eq(localCacheImageDetails));
            verify(eventPublisher, timeout(1000)).publishEvent(argThat(new ArgumentMatcher<BookCoverUpdatedEvent>() {
                @Override
                public boolean matches(BookCoverUpdatedEvent event) {
                    return event.getIdentifierKey().equals(identifierKey) &&
                           event.getNewCoverUrl().equals(localCacheImageDetails.getUrlOrPath()) &&
                           event.getSource() == localCacheImageDetails.getCoverImageSource();
                }
            }));

        } finally {
            deleteDirectoryRecursively(tempDir);
        }
    }
    
    /**
     * Tests the scenario where placeholder is used when no image is available
     * 
     * @throws Exception if test execution fails
     * 
     * @implNote This test verifies:
     * - System correctly handles the case when no image is available
     * - Placeholder image is correctly created and used as fallback
     * - Cache is updated with placeholder image details
     * - BookCoverUpdatedEvent is published with the placeholder URL
     * - The complete fallback mechanism functions properly
     */
    @Test
    public void testPlaceholderFallback() throws Exception {
        // ARRANGE
        S3BookCoverService s3Service = mock(S3BookCoverService.class); // Needed for BookCoverManagementService constructor
        LocalDiskCoverCacheService diskService = mock(LocalDiskCoverCacheService.class);
        CoverSourceFetchingService sourceFetchingService = mock(CoverSourceFetchingService.class);
        CoverCacheManager cacheManager = mock(CoverCacheManager.class);
        ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        EnvironmentService environmentService = mock(EnvironmentService.class);

        BookCoverManagementService bookCoverManagementService = new BookCoverManagementService(
            cacheManager, sourceFetchingService, s3Service, diskService, eventPublisher, environmentService
        );
        
        Book testBook = createTestBook();
        String identifierKey = ImageCacheUtils.getIdentifierKey(testBook);
        
        when(environmentService.isBookCoverDebugMode()).thenReturn(true);
        when(diskService.getLocalPlaceholderPath()).thenReturn(LOCAL_PLACEHOLDER_PATH);
        // No need to mock getCacheDirName or getCacheDirString if not directly used by this test's new flow
            
        // Configure source fetching to return null (or placeholder-like details)
        when(sourceFetchingService.getBestCoverImageUrlAsync(
                eq(testBook), anyString(), any(ImageProvenanceData.class)))
            .thenReturn(CompletableFuture.completedFuture(null)); // Simulate no image found
        
        ImageDetails placeholderDetails = new ImageDetails(
            LOCAL_PLACEHOLDER_PATH,
            "SYSTEM_PLACEHOLDER",
            "placeholder", // filename part
            CoverImageSource.LOCAL_CACHE, // Source for placeholder
            ImageResolutionPreference.UNKNOWN // Resolution for placeholder
        );
        
        // This mock is crucial: BookCoverManagementService calls this when getBestCoverImageUrlAsync yields no good image
        when(diskService.createPlaceholderImageDetails(
                eq(testBook.getId()), eq("background-fetch-failed")))
            .thenReturn(placeholderDetails);
        
        // ACT
        bookCoverManagementService.processCoverInBackground(testBook, "http://some.url/that-will-fail.jpg"); // Provisional hint
        
        // ASSERT
        // Verify that createPlaceholderImageDetails was called because fetching failed
        // Use Mockito timeout to wait for async operations
        verify(diskService, timeout(1000)).createPlaceholderImageDetails(eq(testBook.getId()), eq("background-fetch-failed"));
        
        // Verify cache is updated with placeholder details
        verify(cacheManager, timeout(1000)).putFinalImageDetails(eq(identifierKey), eq(placeholderDetails));
        
        // Verify event is published with placeholder details
        verify(eventPublisher, timeout(1000)).publishEvent(argThat(new ArgumentMatcher<BookCoverUpdatedEvent>() {
            @Override
            public boolean matches(BookCoverUpdatedEvent event) {
                return event.getIdentifierKey().equals(identifierKey) &&
                       event.getNewCoverUrl().equals(placeholderDetails.getUrlOrPath()) && // Corrected method name
                       event.getSource() == placeholderDetails.getCoverImageSource();
            }
        }));
    }

    private void deleteDirectoryRecursively(Path directory) {
        if (directory == null) {
            return;
        }
        try {
            Files.walk(directory)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ignored) {
                        // Best effort cleanup for temp test artifacts
                    }
                });
        } catch (IOException ignored) {
            // No-op for test cleanup failures
        }
    }
    
    /**
     * Creates a test book with predefined attributes for testing
     * 
     * @return A Book instance with test data
     * 
     * @implNote Creates a book with:
     * - ID: "test-book-id"
     * - Title: "Test Book"
     * - Single author: "Test Author"
     * - ISBN-13: "9781234567890"
     * - Cover URL: "https://example.com/test-book-cover.jpg"
     */
    private Book createTestBook() {
        Book book = new Book();
        book.setId("test-book-id");
        book.setTitle("Test Book");
        book.setAuthors(java.util.Collections.singletonList("Test Author"));
        book.setIsbn13("9781234567890");
        book.setCoverImageUrl("https://example.com/test-book-cover.jpg");
        return book;
    }
}
