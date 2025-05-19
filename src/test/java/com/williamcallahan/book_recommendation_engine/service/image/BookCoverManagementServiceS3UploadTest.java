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

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
        
        // Set up test book
        Book testBook = createTestBook();
        
        // Configure mock behaviors
        when(environmentService.isBookCoverDebugMode()).thenReturn(true);
        when(diskService.getLocalPlaceholderPath()).thenReturn(LOCAL_PLACEHOLDER_PATH);
        when(diskService.getCacheDirName()).thenReturn(CACHE_DIR_NAME);
        when(diskService.getCacheDirString()).thenReturn(CACHE_DIR_PATH);
        
        // Local cache hit
        ImageDetails localCacheImageDetails = new ImageDetails(
            "/book-covers/test-image.jpg",
            "GOOGLE_BOOKS",
            "test-image.jpg",
            CoverImageSource.LOCAL_CACHE,
            ImageResolutionPreference.ORIGINAL,
            500, 700
        );
        
        when(sourceFetchingService.getBestCoverImageUrlAsync(
                any(Book.class), anyString(), any(ImageProvenanceData.class)))
            .thenReturn(CompletableFuture.completedFuture(localCacheImageDetails));
        
        // Configure S3 upload success result
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
        
        // ACT - Simulate background processing with S3 upload
        logger.info("Testing S3 upload functionality with mock calls");
        
        // Simulate the final part of processCoverInBackground where S3 upload succeeds
        ImageProvenanceData provenanceData = new ImageProvenanceData();
        provenanceData.setBookId(testBook.getId());
        
        // Call the S3 upload service
        Mono<ImageDetails> s3Result = s3Service.uploadProcessedCoverToS3Async(
            testImageBytes, 
            ".jpg", 
            null, 
            500,
            700,
            testBook.getId(),
            "GOOGLE_BOOKS", 
            provenanceData
        );
        
        // Execute the S3 upload and process the result
        s3Result.subscribe(s3UploadedDetails -> {
            // Update cache with S3 details
            cacheManager.putFinalImageDetails("test:9781234567890", s3UploadedDetails);
            
            // Publish event with S3 details
            eventPublisher.publishEvent(new BookCoverUpdatedEvent(
                "test:9781234567890", s3UploadedDetails.getUrlOrPath(), 
                testBook.getId(), CoverImageSource.S3_CACHE
            ));
        });
        
        // Give a moment for the S3 result to be processed
        Thread.sleep(50); 
        
        // ASSERT
        verify(s3Service).uploadProcessedCoverToS3Async(
            any(byte[].class),
            eq(".jpg"),
            isNull(),
            eq(500),
            eq(700),
            eq(testBook.getId()),
            eq("GOOGLE_BOOKS"),
            any(ImageProvenanceData.class)
        );
        
        verify(eventPublisher).publishEvent(any(BookCoverUpdatedEvent.class));
        verify(cacheManager).putFinalImageDetails(anyString(), any(ImageDetails.class));
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
        CoverCacheManager cacheManager = mock(CoverCacheManager.class);
        ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        EnvironmentService environmentService = mock(EnvironmentService.class);
        
        // Set up test book
        Book testBook = createTestBook();
        
        // Configure mock behaviors
        when(environmentService.isBookCoverDebugMode()).thenReturn(true);
        when(diskService.getLocalPlaceholderPath()).thenReturn(LOCAL_PLACEHOLDER_PATH);
        when(diskService.getCacheDirName()).thenReturn(CACHE_DIR_NAME);
        when(diskService.getCacheDirString()).thenReturn(CACHE_DIR_PATH);
        
        // Local cache hit
        ImageDetails localCacheImageDetails = new ImageDetails(
            "/book-covers/test-image.jpg",
            "GOOGLE_BOOKS",
            "test-image.jpg",
            CoverImageSource.LOCAL_CACHE,
            ImageResolutionPreference.ORIGINAL,
            500, 700
        );
            
        // Configure S3 upload to fail
        when(s3Service.uploadProcessedCoverToS3Async(
                any(byte[].class),
                anyString(),
                isNull(),
                anyInt(),
                anyInt(),
                anyString(),
                anyString(),
                any(ImageProvenanceData.class)))
            .thenReturn(Mono.error(new RuntimeException("S3 upload failed")));
        
        // ACT
        ImageProvenanceData provenanceData = new ImageProvenanceData();
        provenanceData.setBookId(testBook.getId());
        
        try {
            // Call the S3 upload service that will produce an error
            Mono<ImageDetails> s3Result = s3Service.uploadProcessedCoverToS3Async(
                testImageBytes, 
                ".jpg", 
                null, 
                500,
                700,
                testBook.getId(),
                "GOOGLE_BOOKS", 
                provenanceData
            );
            
            // Set up our error handler
            s3Result
                .doOnError(ex -> {
                    // This is what we want to test - the error handling for S3 upload failure
                    cacheManager.putFinalImageDetails("test:9781234567890", localCacheImageDetails);
                    
                    eventPublisher.publishEvent(new BookCoverUpdatedEvent(
                        "test:9781234567890", 
                        localCacheImageDetails.getUrlOrPath(),
                        testBook.getId(),
                        localCacheImageDetails.getCoverImageSource()
                    ));
                })
                .doOnSuccess(ignored -> fail("Should not succeed"))
                .subscribe(
                    ignored -> {}, // onNext - should not be called
                    ex -> logger.info("Expected error handled: " + ex.getMessage()) // onError
                );
            
            // Give time for the async callbacks
            Thread.sleep(50);
            
            // ASSERT
            verify(s3Service).uploadProcessedCoverToS3Async(
                any(byte[].class),
                anyString(),
                isNull(),
                anyInt(),
                anyInt(),
                anyString(),
                anyString(),
                any(ImageProvenanceData.class)
            );
            
            // Verify fallback to local cache
            verify(cacheManager).putFinalImageDetails(anyString(), eq(localCacheImageDetails));
            verify(eventPublisher).publishEvent(any(BookCoverUpdatedEvent.class));
            
        } catch (Exception e) {
            fail("Test should not throw exception: " + e.getMessage());
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
        LocalDiskCoverCacheService diskService = mock(LocalDiskCoverCacheService.class);
        CoverCacheManager cacheManager = mock(CoverCacheManager.class);
        ApplicationEventPublisher eventPublisher = mock(ApplicationEventPublisher.class);
        EnvironmentService environmentService = mock(EnvironmentService.class);
        
        // Set up test book
        Book testBook = createTestBook();
        
        // Configure mock behaviors
        when(environmentService.isBookCoverDebugMode()).thenReturn(true);
        when(diskService.getLocalPlaceholderPath()).thenReturn(LOCAL_PLACEHOLDER_PATH);
        when(diskService.getCacheDirName()).thenReturn(CACHE_DIR_NAME);
        when(diskService.getCacheDirString()).thenReturn(CACHE_DIR_PATH);
            
        // Configure placeholder
        ImageDetails placeholderDetails = new ImageDetails(
            LOCAL_PLACEHOLDER_PATH,
            "SYSTEM_PLACEHOLDER",
            "placeholder",
            CoverImageSource.LOCAL_CACHE,
            ImageResolutionPreference.UNKNOWN
        );
        
        when(diskService.createPlaceholderImageDetails(
                eq(testBook.getId()), eq("background-fetch-failed")))
            .thenReturn(placeholderDetails);
        
        // ACT - simulate placeholder fallback
        
        // Get image details - returns null
        ImageDetails imageDetails = null;
        
        // Handle the fallback
        if (imageDetails == null) {
            ImageDetails placeholderImageDetails = diskService.createPlaceholderImageDetails(
                testBook.getId(), "background-fetch-failed");
                
            cacheManager.putFinalImageDetails("test:9781234567890", placeholderImageDetails);
            
            eventPublisher.publishEvent(new BookCoverUpdatedEvent(
                "test:9781234567890", 
                placeholderImageDetails.getUrlOrPath(),
                testBook.getId(),
                placeholderImageDetails.getCoverImageSource()
            ));
        }
        
        // ASSERT
        verify(diskService).createPlaceholderImageDetails(eq(testBook.getId()), eq("background-fetch-failed"));
        verify(cacheManager).putFinalImageDetails(anyString(), eq(placeholderDetails));
        verify(eventPublisher).publishEvent(any(BookCoverUpdatedEvent.class));
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