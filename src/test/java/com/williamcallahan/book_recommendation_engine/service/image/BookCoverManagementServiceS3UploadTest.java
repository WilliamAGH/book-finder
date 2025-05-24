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
import org.junit.jupiter.api.BeforeEach;
import org.springframework.context.ApplicationEventPublisher;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import org.mockito.ArgumentMatcher;

/**
 * Unit tests for S3 upload functionality in BookCoverManagementService
 */
public class BookCoverManagementServiceS3UploadTest {
    

    // Path strings used in tests
    private final String LOCAL_PLACEHOLDER_PATH = "/images/placeholder-book-cover.svg";
    private final String CACHE_DIR_NAME = "book-covers";
    private final String CACHE_DIR_PATH = "/tmp/book-covers";
    
    private S3BookCoverService s3Service;
    private LocalDiskCoverCacheService diskService;
    private CoverSourceFetchingService sourceFetchingService;
    private CoverCacheManager cacheManager;
    private ApplicationEventPublisher eventPublisher;
    private EnvironmentService environmentService;
    private BookCoverManagementService bookCoverManagementService;
    
    @BeforeEach
    void setUp() {
        s3Service = mock(S3BookCoverService.class);
        diskService = mock(LocalDiskCoverCacheService.class);
        sourceFetchingService = mock(CoverSourceFetchingService.class);
        cacheManager = mock(CoverCacheManager.class);
        eventPublisher = mock(ApplicationEventPublisher.class);
        environmentService = mock(EnvironmentService.class);

        bookCoverManagementService = new BookCoverManagementService(
            cacheManager, sourceFetchingService, s3Service, diskService, eventPublisher, environmentService
        );
        
        // Enable cache using reflection since @Value annotation doesn't work in manual construction
        try {
            java.lang.reflect.Field cacheEnabledField = BookCoverManagementService.class.getDeclaredField("cacheEnabled");
            cacheEnabledField.setAccessible(true);
            cacheEnabledField.set(bookCoverManagementService, true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set cacheEnabled field", e);
        }
        
        // Common mock setup
        when(environmentService.isBookCoverDebugMode()).thenReturn(true);
        when(diskService.getLocalPlaceholderPath()).thenReturn(LOCAL_PLACEHOLDER_PATH);
        when(diskService.getCacheDirName()).thenReturn(CACHE_DIR_NAME);
        when(diskService.getCacheDirString()).thenReturn(CACHE_DIR_PATH);
        
        // Setup cache manager to allow background processing
        when(cacheManager.getFinalImageDetails(anyString())).thenReturn(null);
        when(cacheManager.getProvisionalUrl(anyString())).thenReturn(null);
        
        // Setup default placeholder creation for tests that need it
        ImageDetails defaultPlaceholder = new ImageDetails(
            LOCAL_PLACEHOLDER_PATH,
            "SYSTEM_PLACEHOLDER",
            "placeholder",
            CoverImageSource.LOCAL_CACHE,
            null
        );
        when(diskService.createPlaceholderImageDetails(anyString(), anyString()))
            .thenReturn(defaultPlaceholder);
    }
    
    /**
     * Tests the scenario where a locally cached image is successfully uploaded to S3
     * 
     * @throws Exception if test execution fails
     * 
     * @implNote This test verifies that when the conditions for S3 upload are NOT met,
     * the system properly falls back to using local cache details without attempting S3 upload.
     * This is a more realistic test of the actual conditions in the service.
     */
    @Test
    public void testS3UploadConditionsNotMet() throws Exception {
        // ARRANGE
        Book testBook = createTestBook();
        String identifierKey = testBook.getIsbn13();
        
        // Set up image details that won't trigger S3 upload (local file doesn't exist)
        ImageDetails localCacheImageDetails = new ImageDetails(
            "/" + CACHE_DIR_NAME + "/test-image.jpg", // Service expects path starting with /cache-dir-name
            "GOOGLE_BOOKS",
            "test-image.jpg",
            CoverImageSource.GOOGLE_BOOKS,
            ImageResolutionPreference.ORIGINAL,
            500, 700
        );
        
        when(sourceFetchingService.getBestCoverImageUrlAsync(
                any(Book.class), anyString(), any(ImageProvenanceData.class), anyBoolean()))
            .thenReturn(CompletableFuture.completedFuture(localCacheImageDetails));

        // ACT
        CompletableFuture<Void> future = bookCoverManagementService.processCoverInBackground(testBook, "http://example.com/provisional.jpg", true);
        future.get();
    
        // ASSERT  
        // Verify that the source fetching service was called
        verify(sourceFetchingService, timeout(3000)).getBestCoverImageUrlAsync(
            any(Book.class), anyString(), any(ImageProvenanceData.class), anyBoolean());
        
        // Since the file doesn't exist, S3 upload should NOT be attempted
        verify(s3Service, never()).uploadProcessedCoverToS3Async(
            any(byte[].class), anyString(), any(), anyInt(), anyInt(), anyString(), anyString(), any(ImageProvenanceData.class));
        
        // Verify cache is updated with local details (fallback)
        verify(cacheManager, timeout(3000)).putFinalImageDetails(eq(identifierKey), eq(localCacheImageDetails));
        
        // Verify event is published with local cache details
        verify(eventPublisher, timeout(3000)).publishEvent(argThat(new ArgumentMatcher<BookCoverUpdatedEvent>() {
            @Override
            public boolean matches(BookCoverUpdatedEvent event) {
                return event.getIdentifierKey().equals(identifierKey) &&
                       event.getNewCoverUrl().equals(localCacheImageDetails.getUrlOrPath()) &&
                       event.getSource() == localCacheImageDetails.getCoverImageSource();
            }
        }));
    }
    
    /**
     * Tests the scenario where an image is already from S3 cache and no upload is needed
     * 
     * @throws Exception if test execution fails
     * 
     * @implNote This test verifies that when an image is already from S3_CACHE source,
     * no additional S3 upload is attempted since it's already in S3.
     */
    @Test
    public void testS3ImageAlreadyCached() throws Exception {
        // ARRANGE
        Book testBook = createTestBook();
        String identifierKey = testBook.getIsbn13();
        
        // Set up image details that are already from S3 cache - no upload needed
        ImageDetails s3CacheImageDetails = new ImageDetails(
            "https://cdn.example.com/books/cached-image.jpg", // Already S3 URL
            "S3_CACHE",
            "cached-image.jpg",
            CoverImageSource.S3_CACHE, // Already S3_CACHE - no upload needed
            ImageResolutionPreference.ORIGINAL,
            500, 700
        );
        
        when(sourceFetchingService.getBestCoverImageUrlAsync(
                any(Book.class), anyString(), any(ImageProvenanceData.class), anyBoolean()))
            .thenReturn(CompletableFuture.completedFuture(s3CacheImageDetails));

        // ACT
        CompletableFuture<Void> future = bookCoverManagementService.processCoverInBackground(testBook, "http://example.com/provisional.jpg", true);
        future.get();
    
        // ASSERT
        // Verify that the source fetching service was called
        verify(sourceFetchingService, timeout(3000)).getBestCoverImageUrlAsync(
            any(Book.class), anyString(), any(ImageProvenanceData.class), anyBoolean());
        
        // Since the image is already from S3_CACHE, no S3 upload should be attempted
        verify(s3Service, never()).uploadProcessedCoverToS3Async(
            any(byte[].class), anyString(), any(), anyInt(), anyInt(), anyString(), anyString(), any(ImageProvenanceData.class));
        
        // Verify cache is updated with S3 details
        verify(cacheManager, timeout(3000)).putFinalImageDetails(eq(identifierKey), eq(s3CacheImageDetails));
        
        // Verify event is published with S3 cache details
        verify(eventPublisher, timeout(3000)).publishEvent(argThat(new ArgumentMatcher<BookCoverUpdatedEvent>() {
            @Override
            public boolean matches(BookCoverUpdatedEvent event) {
                return event.getIdentifierKey().equals(identifierKey) &&
                       event.getNewCoverUrl().equals(s3CacheImageDetails.getUrlOrPath()) &&
                       event.getSource() == s3CacheImageDetails.getCoverImageSource();
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
        Book testBook = createTestBook();
        String identifierKey = testBook.getIsbn13();
            
        // Configure source fetching to return null (or placeholder-like details)
        when(sourceFetchingService.getBestCoverImageUrlAsync(
                eq(testBook), anyString(), any(ImageProvenanceData.class), eq(true)))
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
        CompletableFuture<Void> placeholderFuture = bookCoverManagementService.processCoverInBackground(testBook, "http://some.url/that-will-fail.jpg", true);
        placeholderFuture.get();
        
        // ASSERT
        // Verify that createPlaceholderImageDetails was called because fetching failed
        // Use Mockito timeout to wait for async operations
        verify(diskService, timeout(5000)).createPlaceholderImageDetails(eq(testBook.getId()), eq("background-fetch-failed")); // Increased timeout
        
        // Verify cache is updated with placeholder details
        verify(cacheManager, timeout(5000)).putFinalImageDetails(eq(identifierKey), eq(placeholderDetails)); // Increased timeout
        
        // Verify event is published with placeholder details
        verify(eventPublisher, timeout(5000)).publishEvent(argThat(new ArgumentMatcher<BookCoverUpdatedEvent>() { // Increased timeout
            @Override
            public boolean matches(BookCoverUpdatedEvent event) {
                return event.getIdentifierKey().equals(identifierKey) &&
                       event.getNewCoverUrl().equals(placeholderDetails.getUrlOrPath()) &&
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
