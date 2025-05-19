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
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.ImageDetails;
import com.williamcallahan.book_recommendation_engine.types.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import org.mockito.ArgumentMatcher;

/**
 * Unit tests for S3 upload functionality in BookCoverManagementService
 */
public class BookCoverManagementServiceS3UploadTest {
    
    private static final Logger logger = LoggerFactory.getLogger(BookCoverManagementServiceS3UploadTest.class);

    // Path strings used in tests
    private final String LOCAL_PLACEHOLDER_PATH = "/images/placeholder-book-cover.svg";
    private final String CACHE_DIR_NAME = "book-covers";
    private final String CACHE_DIR_PATH = "/tmp/book-covers";
    
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
        
        // Set up test book
        Book testBook = createTestBook();
        String identifierKey = "test-book-id"; // Assuming createTestBook sets ID to "test-book-id"
        
        // Configure mock behaviors
        when(environmentService.isBookCoverDebugMode()).thenReturn(true);
        when(diskService.getLocalPlaceholderPath()).thenReturn(LOCAL_PLACEHOLDER_PATH);
        when(diskService.getCacheDirName()).thenReturn(CACHE_DIR_NAME);
        when(diskService.getCacheDirString()).thenReturn(CACHE_DIR_PATH);
        
        // Local cache hit that will be uploaded to S3
        ImageDetails localCacheImageDetails = new ImageDetails(
            CACHE_DIR_PATH + "/test-image.jpg", // Simulate a local file path
            "GOOGLE_BOOKS",
            "test-image.jpg",
            CoverImageSource.LOCAL_CACHE,
            ImageResolutionPreference.ORIGINAL,
            500, 700
        );
        
        when(sourceFetchingService.getBestCoverImageUrlAsync(
                eq(testBook), anyString(), any(ImageProvenanceData.class)))
            .thenReturn(CompletableFuture.completedFuture(localCacheImageDetails));
        
        // Configure S3 upload success result
        ImageDetails s3UploadedImageDetails = new ImageDetails(
            "https://cdn.example.com/books/test-image.jpg",
            "S3_CACHE", // Source name for S3
            "books/test-image.jpg", // Key for S3
            CoverImageSource.S3_CACHE,
            ImageResolutionPreference.ORIGINAL,
            500, 700
        );
        
        when(s3Service.uploadProcessedCoverToS3Async(
                any(byte[].class),
                eq(".jpg"),
                isNull(), // Assuming no CDN prefix for this direct upload call
                eq(500),
                eq(700),
                eq(testBook.getId()),
                eq("GOOGLE_BOOKS"), // Source name from localCacheImageDetails
                any(ImageProvenanceData.class)))
            .thenReturn(Mono.just(s3UploadedImageDetails));

        // Mock file reading for the S3 upload part within BookCoverManagementService
        // This requires knowing the path construction logic or mocking Files.readAllBytes
        // For simplicity, let's assume the testImageBytes are correctly read if the path matches.
        // If BookCoverManagementService uses diskService.getCacheDirString() + filename:
        Path mockLocalImagePath = Paths.get(CACHE_DIR_PATH, "test-image.jpg");
        try (var mockedFiles = mockStatic(java.nio.file.Files.class)) {
            mockedFiles.when(() -> java.nio.file.Files.exists(mockLocalImagePath)).thenReturn(true);
            mockedFiles.when(() -> java.nio.file.Files.readAllBytes(mockLocalImagePath)).thenReturn(testImageBytes);

            // ACT
            logger.info("Testing S3 upload functionality by calling BookCoverManagementService.processCoverInBackground");
            bookCoverManagementService.processCoverInBackground(testBook, "http://example.com/provisional.jpg");
        }
        
        // Give a moment for the @Async S3 result to be processed
        Thread.sleep(100); // Increased sleep time slightly for async operations
        
        // ASSERT
        // Verify S3 upload was attempted with correct parameters
        verify(s3Service).uploadProcessedCoverToS3Async(
            eq(testImageBytes),
            eq(".jpg"),
            isNull(),
            eq(500),
            eq(700),
            eq(testBook.getId()),
            eq("GOOGLE_BOOKS"),
            any(ImageProvenanceData.class)
        );
        
        // Verify cache is updated with S3 details
        verify(cacheManager).putFinalImageDetails(eq(identifierKey), eq(s3UploadedImageDetails));
        
        // Verify event is published with S3 details
        verify(eventPublisher).publishEvent(argThat(new ArgumentMatcher<BookCoverUpdatedEvent>() {
            @Override
            public boolean matches(BookCoverUpdatedEvent event) {
                return event.getIdentifierKey().equals(identifierKey) &&
                       event.getNewCoverUrl().equals(s3UploadedImageDetails.getUrlOrPath()) && // Corrected method name
                       event.getSource() == CoverImageSource.S3_CACHE;
            }
        }));
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
        
        // Set up test book
        Book testBook = createTestBook();
        String identifierKey = "test-book-id";
        
        // Configure mock behaviors
        when(environmentService.isBookCoverDebugMode()).thenReturn(true);
        when(diskService.getLocalPlaceholderPath()).thenReturn(LOCAL_PLACEHOLDER_PATH);
        when(diskService.getCacheDirName()).thenReturn(CACHE_DIR_NAME);
        when(diskService.getCacheDirString()).thenReturn(CACHE_DIR_PATH);
        
        // Local cache hit that will attempt S3 upload
        ImageDetails localCacheImageDetails = new ImageDetails(
            CACHE_DIR_PATH + "/test-image.jpg", // Simulate a local file path
            "GOOGLE_BOOKS",
            "test-image.jpg",
            CoverImageSource.LOCAL_CACHE,
            ImageResolutionPreference.ORIGINAL,
            500, 700
        );
        
        when(sourceFetchingService.getBestCoverImageUrlAsync(
                eq(testBook), anyString(), any(ImageProvenanceData.class)))
            .thenReturn(CompletableFuture.completedFuture(localCacheImageDetails));
            
        // Configure S3 upload to fail
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

        Path mockLocalImagePath = Paths.get(CACHE_DIR_PATH, "test-image.jpg");
        try (var mockedFiles = mockStatic(java.nio.file.Files.class)) {
            mockedFiles.when(() -> java.nio.file.Files.exists(mockLocalImagePath)).thenReturn(true);
            mockedFiles.when(() -> java.nio.file.Files.readAllBytes(mockLocalImagePath)).thenReturn(testImageBytes);
        
            // ACT
            bookCoverManagementService.processCoverInBackground(testBook, "http://example.com/provisional.jpg");
        }
            
        // Give time for the @Async callbacks
        Thread.sleep(100); // Increased sleep time
            
        // ASSERT
        // Verify S3 upload was attempted
        verify(s3Service).uploadProcessedCoverToS3Async(
            eq(testImageBytes),
            anyString(),
            isNull(),
            anyInt(),
            anyInt(),
            eq(testBook.getId()),
            eq("GOOGLE_BOOKS"),
            any(ImageProvenanceData.class)
        );
            
        // Verify fallback to local cache details in cacheManager and eventPublisher
        verify(cacheManager).putFinalImageDetails(eq(identifierKey), eq(localCacheImageDetails));
        verify(eventPublisher).publishEvent(argThat(new ArgumentMatcher<BookCoverUpdatedEvent>() {
            @Override
            public boolean matches(BookCoverUpdatedEvent event) {
                return event.getIdentifierKey().equals(identifierKey) &&
                       event.getNewCoverUrl().equals(localCacheImageDetails.getUrlOrPath()) && // Corrected method name
                       event.getSource() == localCacheImageDetails.getCoverImageSource();
            }
        }));
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
        String identifierKey = "test-book-id";
        
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
        
        Thread.sleep(100); // Allow async processing
        
        // ASSERT
        // Verify that createPlaceholderImageDetails was called because fetching failed
        verify(diskService).createPlaceholderImageDetails(eq(testBook.getId()), eq("background-fetch-failed"));
        
        // Verify cache is updated with placeholder details
        verify(cacheManager).putFinalImageDetails(eq(identifierKey), eq(placeholderDetails));
        
        // Verify event is published with placeholder details
        verify(eventPublisher).publishEvent(argThat(new ArgumentMatcher<BookCoverUpdatedEvent>() {
            @Override
            public boolean matches(BookCoverUpdatedEvent event) {
                return event.getIdentifierKey().equals(identifierKey) &&
                       event.getNewCoverUrl().equals(placeholderDetails.getUrlOrPath()) && // Corrected method name
                       event.getSource() == placeholderDetails.getCoverImageSource();
            }
        }));
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
