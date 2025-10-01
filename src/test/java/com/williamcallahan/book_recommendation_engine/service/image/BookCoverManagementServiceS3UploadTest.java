/**
 * Test suite for the S3 upload functionality in the BookCoverManagementService
 *
 * This test focuses on validating:
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
import com.williamcallahan.book_recommendation_engine.testutil.BookTestData;
import com.williamcallahan.book_recommendation_engine.testutil.EventMatchers;
import com.williamcallahan.book_recommendation_engine.testutil.ImageTestData;
import com.williamcallahan.book_recommendation_engine.testutil.TestFiles;
import com.williamcallahan.book_recommendation_engine.service.EnvironmentService;
import com.williamcallahan.book_recommendation_engine.service.DuplicateBookService;
import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.util.cover.CoverIdentifierResolver;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import org.mockito.ArgumentMatcher;


/**
 * Unit tests for S3 upload functionality in BookCoverManagementService
 */
public class BookCoverManagementServiceS3UploadTest {
    
    private static final Logger logger = LoggerFactory.getLogger(BookCoverManagementServiceS3UploadTest.class);

    private final String LOCAL_PLACEHOLDER_PATH = ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH;
    
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
        DuplicateBookService duplicateBookService = mock(DuplicateBookService.class);
        CoverPersistenceService coverPersistenceService = mock(CoverPersistenceService.class);

        BookCoverManagementService bookCoverManagementService = new BookCoverManagementService(
            cacheManager,
            sourceFetchingService,
            s3Service,
            diskService,
            eventPublisher,
            environmentService,
            duplicateBookService,
            coverPersistenceService
        );
        
UUID bookUuid = UUID.fromString("11111111-1111-4111-8111-111111111111");
Book testBook = BookTestData.aBook()
    .id(bookUuid.toString())
    .title("Test Book")
    .authors(java.util.Collections.singletonList("Test Author"))
    .isbn13("9781234567890")
    .coverImageUrl("https://example.com/test-book-cover.jpg")
    .build();
        String identifierKey = CoverIdentifierResolver.resolve(testBook);

        when(environmentService.isBookCoverDebugMode()).thenReturn(true);
        when(diskService.getLocalPlaceholderPath()).thenReturn(LOCAL_PLACEHOLDER_PATH);

Path tempDir = TestFiles.createTempDir("cover-cache-success");
        try {
            String cacheDirName = tempDir.getFileName().toString();
            when(diskService.getCacheDirName()).thenReturn(cacheDirName);
            when(diskService.getCacheDirString()).thenReturn(tempDir.toString());

TestFiles.writeBytes(tempDir, "test-image.jpg", testImageBytes);

ImageDetails localCacheImageDetails = ImageTestData.localCache(cacheDirName, "test-image.jpg", 500, 700);

            when(sourceFetchingService.getBestCoverImageUrlAsync(
                    eq(testBook), anyString(), any(ImageProvenanceData.class)))
                .thenReturn(CompletableFuture.completedFuture(localCacheImageDetails));

ImageDetails s3UploadedImageDetails = ImageTestData.s3Cache(
                "https://cdn.example.com/books/test-image.jpg",
                "books/test-image.jpg",
                500,
                700
            );

            when(s3Service.uploadProcessedCoverToS3Async(
                    any(byte[].class),
                    eq(".jpg"),
                    isNull(),
                    eq(500),
                    eq(700),
                    eq(testBook.getId()),
                    eq(ApplicationConstants.Provider.GOOGLE_BOOKS),
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
                eq(ApplicationConstants.Provider.GOOGLE_BOOKS),
                any(ImageProvenanceData.class)
            );

            verify(cacheManager, timeout(1000)).putFinalImageDetails(eq(identifierKey), eq(s3UploadedImageDetails));

            verify(eventPublisher, timeout(1000)).publishEvent(argThat(EventMatchers.bookCoverUpdated(identifierKey, s3UploadedImageDetails.getUrlOrPath(), CoverImageSource.GOOGLE_BOOKS)));

            verify(coverPersistenceService, timeout(1000)).updateAfterS3Upload(
                eq(bookUuid),
                eq(s3UploadedImageDetails.getSourceSystemId()),
                eq(s3UploadedImageDetails.getUrlOrPath()),
                eq(s3UploadedImageDetails.getWidth()),
                eq(s3UploadedImageDetails.getHeight()),
                eq(CoverImageSource.GOOGLE_BOOKS)
            );
        } finally {
TestFiles.deleteRecursive(tempDir);
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
        DuplicateBookService duplicateBookService = mock(DuplicateBookService.class);

        CoverPersistenceService coverPersistenceService = mock(CoverPersistenceService.class);

        BookCoverManagementService bookCoverManagementService = new BookCoverManagementService(
            cacheManager,
            sourceFetchingService,
            s3Service,
            diskService,
            eventPublisher,
            environmentService,
            duplicateBookService,
            coverPersistenceService
        );
        
Book testBook = BookTestData.aBook().id("test-book-id").build();
        String identifierKey = CoverIdentifierResolver.resolve(testBook);

        when(environmentService.isBookCoverDebugMode()).thenReturn(true);
        when(diskService.getLocalPlaceholderPath()).thenReturn(LOCAL_PLACEHOLDER_PATH);

Path tempDir = TestFiles.createTempDir("cover-cache-failure");
        try {
            String cacheDirName = tempDir.getFileName().toString();
            when(diskService.getCacheDirName()).thenReturn(cacheDirName);
            when(diskService.getCacheDirString()).thenReturn(tempDir.toString());

TestFiles.writeBytes(tempDir, "test-image.jpg", testImageBytes);

ImageDetails localCacheImageDetails = ImageTestData.localCache(cacheDirName, "test-image.jpg", 500, 700);

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
                    eq(ApplicationConstants.Provider.GOOGLE_BOOKS),
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
                eq(ApplicationConstants.Provider.GOOGLE_BOOKS),
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
TestFiles.deleteRecursive(tempDir);
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
        DuplicateBookService duplicateBookService = mock(DuplicateBookService.class);

        CoverPersistenceService coverPersistenceService = mock(CoverPersistenceService.class);

        BookCoverManagementService bookCoverManagementService = new BookCoverManagementService(
            cacheManager,
            sourceFetchingService,
            s3Service,
            diskService,
            eventPublisher,
            environmentService,
            duplicateBookService,
            coverPersistenceService
        );
        
Book testBook = BookTestData.aBook().id("test-book-id").build();
        String identifierKey = CoverIdentifierResolver.resolve(testBook);
        
        when(environmentService.isBookCoverDebugMode()).thenReturn(true);
        when(diskService.getLocalPlaceholderPath()).thenReturn(LOCAL_PLACEHOLDER_PATH);
        // No need to mock getCacheDirName or getCacheDirString if not directly used by this test's new flow
            
        // Configure source fetching to return null (or placeholder-like details)
        when(sourceFetchingService.getBestCoverImageUrlAsync(
                eq(testBook), anyString(), any(ImageProvenanceData.class)))
            .thenReturn(CompletableFuture.completedFuture(null)); // Simulate no image found
        
ImageDetails placeholderDetails = ImageTestData.placeholder(LOCAL_PLACEHOLDER_PATH);
        
        // This mock is crucial: BookCoverManagementService calls this when getBestCoverImageUrlAsync yields no good image
        when(diskService.placeholderImageDetails(
                eq(testBook.getId()), eq("background-fetch-failed")))
            .thenReturn(placeholderDetails);
        
        // ACT
        bookCoverManagementService.processCoverInBackground(testBook, "http://some.url/that-will-fail.jpg"); // Provisional hint
        
        // ASSERT
        // Verify that placeholderImageDetails was called because fetching failed
        // Use Mockito timeout to wait for async operations
        verify(diskService, timeout(1000)).placeholderImageDetails(eq(testBook.getId()), eq("background-fetch-failed"));
        
        // Verify cache is updated with placeholder details
        verify(cacheManager, timeout(1000)).putFinalImageDetails(eq(identifierKey), eq(placeholderDetails));
        
// Verify event is published with placeholder details
verify(eventPublisher, timeout(1000)).publishEvent(argThat(EventMatchers.bookCoverUpdated(identifierKey, placeholderDetails.getUrlOrPath(), placeholderDetails.getCoverImageSource())));
    }

}
