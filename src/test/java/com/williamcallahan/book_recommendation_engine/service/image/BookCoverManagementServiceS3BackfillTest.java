package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.service.DuplicateBookService;
import com.williamcallahan.book_recommendation_engine.service.EnvironmentService;
import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for Bug #4: Background S3 persistence (backfill) of external cover URLs.
 * 
 * Tests the backfill logic that automatically uploads external cover URLs to S3
 * when books are hydrated from Postgres with external covers but no S3 paths.
 * 
 * @author William Callahan
 */
@ExtendWith(MockitoExtension.class)
class BookCoverManagementServiceS3BackfillTest {

    @Mock
    private CoverCacheManager coverCacheManager;
    
    @Mock
    private CoverSourceFetchingService coverSourceFetchingService;
    
    @Mock
    private S3BookCoverService s3BookCoverService;
    
    @Mock
    private LocalDiskCoverCacheService localDiskCoverCacheService;
    
    @Mock
    private ApplicationEventPublisher eventPublisher;
    
    @Mock
    private EnvironmentService environmentService;
    
    @Mock
    private DuplicateBookService duplicateBookService;
    
    @Mock
    private CoverPersistenceService coverPersistenceService;
    
    private BookCoverManagementService service;
    
    private static final String PLACEHOLDER_PATH = ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH;
    private static final String TEST_BOOK_ID = "test-book-123";
    private static final String TEST_ISBN = "9780134685991";
    private static final String EXTERNAL_GOOGLE_URL = "https://books.google.com/books/content?id=test&printsec=frontcover&img=1";
    private static final String EXTERNAL_OPENLIBRARY_URL = "https://covers.openlibrary.org/b/id/12345-L.jpg";
    private static final String S3_CDN_URL = "https://cdn.example.com/covers/test-book-123-large-google-books.jpg";
    
    @BeforeEach
    void setUp() {
        service = new BookCoverManagementService(
            coverCacheManager,
            coverSourceFetchingService,
            s3BookCoverService,
            localDiskCoverCacheService,
            eventPublisher,
            environmentService,
            duplicateBookService,
            coverPersistenceService
        );
        
        // Default mock behaviors
        lenient().when(localDiskCoverCacheService.getLocalPlaceholderPath()).thenReturn(PLACEHOLDER_PATH);
        lenient().when(s3BookCoverService.isS3Enabled()).thenReturn(true);
    }
    
    @Test
    void testShouldMigrateExternalCoverToS3_WithGoogleBooksUrl() {
        // Given: Book with Google Books external URL and no S3 path
        Book book = createBookWithExternalCover(EXTERNAL_GOOGLE_URL, null);
        when(s3BookCoverService.uploadCoverToS3Async(EXTERNAL_GOOGLE_URL, TEST_BOOK_ID, "google-books"))
            .thenReturn(Mono.empty());

        // When: Check backfill eligibility via background processing
        // Then: Should trigger backfill (verified by calling processCoverInBackground)
        service.processCoverInBackground(book, null);

        // Verify S3 upload was attempted (async, so verify the call was made)
        verify(s3BookCoverService, timeout(1000).times(1))
            .uploadCoverToS3Async(eq(EXTERNAL_GOOGLE_URL), eq(TEST_BOOK_ID), eq("google-books"));
    }
    
    @Test
    void testShouldMigrateExternalCoverToS3_WithOpenLibraryUrl() {
        // Given: Book with Open Library external URL and no S3 path
        Book book = createBookWithExternalCover(EXTERNAL_OPENLIBRARY_URL, null);
        
        ImageDetails s3Details = new ImageDetails(
            S3_CDN_URL, "S3_UPLOAD", "covers/test-book-123-large-open-library.jpg",
            CoverImageSource.OPEN_LIBRARY, ImageResolutionPreference.ORIGINAL, 600, 900
        );
        s3Details.setStorageLocation(ImageDetails.STORAGE_S3);
        
        when(s3BookCoverService.uploadCoverToS3Async(anyString(), anyString(), anyString()))
            .thenReturn(Mono.just(s3Details));
        
        // When: Process in background
        service.processCoverInBackground(book, null);
        
        // Then: Should upload to S3 with open-library source
        verify(s3BookCoverService, timeout(1000).times(1))
            .uploadCoverToS3Async(eq(EXTERNAL_OPENLIBRARY_URL), eq(TEST_BOOK_ID), eq("open-library"));
    }
    
    @Test
    void testShouldNotMigrate_WhenS3PathAlreadyExists() {
        // Given: Book with external URL AND existing S3 path
        Book book = createBookWithExternalCover(EXTERNAL_GOOGLE_URL, S3_CDN_URL);
        
        // When: Process in background
        service.processCoverInBackground(book, null);
        
        // Then: Should NOT attempt S3 upload (already has S3 path)
        verify(s3BookCoverService, never()).uploadCoverToS3Async(anyString(), anyString(), anyString());
    }
    
    @Test
    void testShouldNotMigrate_WhenNoExternalUrl() {
        // Given: Book with no external URL
        Book book = createBookWithExternalCover(null, null);
        
        // When: Process in background
        service.processCoverInBackground(book, null);
        
        // Then: Should NOT attempt S3 upload
        verify(s3BookCoverService, never()).uploadCoverToS3Async(anyString(), anyString(), anyString());
    }
    
    @Test
    void testShouldNotMigrate_WhenExternalUrlIsPlaceholder() {
        // Given: Book with placeholder as external URL
        Book book = createBookWithExternalCover(PLACEHOLDER_PATH, null);
        
        // When: Process in background
        service.processCoverInBackground(book, null);
        
        // Then: Should NOT attempt S3 upload
        verify(s3BookCoverService, never()).uploadCoverToS3Async(anyString(), anyString(), anyString());
    }
    
    @Test
    void testShouldNotMigrate_WhenS3Disabled() {
        // Given: S3 is disabled
        when(s3BookCoverService.isS3Enabled()).thenReturn(false);
        Book book = createBookWithExternalCover(EXTERNAL_GOOGLE_URL, null);
        
        // When: Process in background
        service.processCoverInBackground(book, null);
        
        // Then: Should NOT attempt S3 upload
        verify(s3BookCoverService, never()).uploadCoverToS3Async(anyString(), anyString(), anyString());
    }
    
    @Test
    void testMigrationSuccess_UpdatesCacheAndPublishesEvent() {
        // Given: Book with external URL needing migration/backfill
        Book book = createBookWithExternalCover(EXTERNAL_GOOGLE_URL, null);
        
        ImageDetails s3Details = new ImageDetails(
            S3_CDN_URL, "S3_UPLOAD", "covers/test-book-123-large-google-books.jpg",
            CoverImageSource.GOOGLE_BOOKS, ImageResolutionPreference.ORIGINAL, 600, 900
        );
        s3Details.setStorageLocation(ImageDetails.STORAGE_S3);
        
        when(s3BookCoverService.uploadCoverToS3Async(anyString(), anyString(), anyString()))
            .thenReturn(Mono.just(s3Details));
        
        // When: Process in background
        service.processCoverInBackground(book, null);
        
        // Then: Should update cache with S3 details
        verify(coverCacheManager, timeout(2000).times(1))
            .putFinalImageDetails(eq(TEST_ISBN), eq(s3Details));
        
        // And: Should publish event with S3 source
        ArgumentCaptor<BookCoverUpdatedEvent> eventCaptor = ArgumentCaptor.forClass(BookCoverUpdatedEvent.class);
        verify(eventPublisher, timeout(2000).times(1)).publishEvent(eventCaptor.capture());
        
        BookCoverUpdatedEvent event = eventCaptor.getValue();
        assertEquals(TEST_ISBN, event.getIdentifierKey());
        assertEquals(S3_CDN_URL, event.getNewCoverUrl());
        assertEquals(TEST_BOOK_ID, event.getGoogleBookId());
        assertEquals(CoverImageSource.GOOGLE_BOOKS, event.getSource());
    }
    
    @Test
    void testMigrationFailure_FallsBackToExternalUrl() {
        // Given: Book with external URL, but S3 upload fails
        Book book = createBookWithExternalCover(EXTERNAL_GOOGLE_URL, null);
        
        ImageDetails failedDetails = new ImageDetails(
            EXTERNAL_GOOGLE_URL, "google-books", "upload-failed",
            CoverImageSource.GOOGLE_BOOKS, ImageResolutionPreference.ORIGINAL
        );
        
        when(s3BookCoverService.uploadCoverToS3Async(anyString(), anyString(), anyString()))
            .thenReturn(Mono.just(failedDetails));
        
        // When: Process in background
        service.processCoverInBackground(book, null);
        
        // Then: Should fall back to external URL in cache
        verify(coverCacheManager, timeout(2000).times(1))
            .putFinalImageDetails(eq(TEST_ISBN), argThat(details ->
                details.getUrlOrPath().equals(EXTERNAL_GOOGLE_URL) &&
                details.getCoverImageSource() == CoverImageSource.GOOGLE_BOOKS
            ));
        
        // And: Should publish event with external URL
        ArgumentCaptor<BookCoverUpdatedEvent> eventCaptor = ArgumentCaptor.forClass(BookCoverUpdatedEvent.class);
        verify(eventPublisher, timeout(2000).times(1)).publishEvent(eventCaptor.capture());
        
        BookCoverUpdatedEvent event = eventCaptor.getValue();
        assertEquals(EXTERNAL_GOOGLE_URL, event.getNewCoverUrl());
        assertEquals(CoverImageSource.GOOGLE_BOOKS, event.getSource());
    }
    
    @Test
    void testMigrationException_FallsBackGracefully() {
        // Given: Book with external URL, but S3 upload throws exception
        Book book = createBookWithExternalCover(EXTERNAL_GOOGLE_URL, null);
        
        when(s3BookCoverService.uploadCoverToS3Async(anyString(), anyString(), anyString()))
            .thenReturn(Mono.error(new RuntimeException("S3 connection failed")));
        
        // When: Process in background
        service.processCoverInBackground(book, null);
        
        // Then: Should fall back to external URL without crashing
        verify(coverCacheManager, timeout(2000).times(1))
            .putFinalImageDetails(eq(TEST_ISBN), argThat(details ->
                details.getUrlOrPath().equals(EXTERNAL_GOOGLE_URL)
            ));
        
        // And: Should still publish event
        verify(eventPublisher, timeout(2000).times(1)).publishEvent(any(BookCoverUpdatedEvent.class));
    }
    
    @Test
    void testIdempotency_S3ServiceChecksExistence() {
        // Given: Book with external URL
        Book book = createBookWithExternalCover(EXTERNAL_GOOGLE_URL, null);
        
        ImageDetails s3Details = new ImageDetails(
            S3_CDN_URL, "S3_CACHE", "covers/test-book-123-large-google-books.jpg",
            CoverImageSource.GOOGLE_BOOKS, ImageResolutionPreference.ORIGINAL, 600, 900
        );
        s3Details.setStorageLocation(ImageDetails.STORAGE_S3);
        
        // S3BookCoverService has built-in existence checks that return existing details
        when(s3BookCoverService.uploadCoverToS3Async(anyString(), anyString(), anyString()))
            .thenReturn(Mono.just(s3Details));
        
        // When: Process same book multiple times
        service.processCoverInBackground(book, null);
        service.processCoverInBackground(book, null);
        
        // Then: S3BookCoverService is called, but its internal logic prevents duplicate uploads
        // (This test verifies the integration point; actual idempotency is in S3BookCoverService)
        verify(s3BookCoverService, timeout(2000).atLeast(1))
            .uploadCoverToS3Async(eq(EXTERNAL_GOOGLE_URL), eq(TEST_BOOK_ID), eq("google-books"));
    }
    
    @Test
    void testSourceInference_GoogleBooks() {
        // Given: Book with Google Books URL
        Book book = createBookWithExternalCover("https://books.google.com/books/content?id=abc", null);
        
        ImageDetails s3Details = new ImageDetails(S3_CDN_URL, "S3_UPLOAD", "key",
            CoverImageSource.GOOGLE_BOOKS, ImageResolutionPreference.ORIGINAL);
        s3Details.setStorageLocation(ImageDetails.STORAGE_S3);
        when(s3BookCoverService.uploadCoverToS3Async(anyString(), anyString(), anyString()))
            .thenReturn(Mono.just(s3Details));
        
        // When: Process in background
        service.processCoverInBackground(book, null);
        
        // Then: Should use "google-books" as source string
        verify(s3BookCoverService, timeout(1000).times(1))
            .uploadCoverToS3Async(anyString(), anyString(), eq("google-books"));
    }
    
    @Test
    void testSourceInference_OpenLibrary() {
        // Given: Book with Open Library URL
        Book book = createBookWithExternalCover("https://covers.openlibrary.org/b/id/123-L.jpg", null);
        
        ImageDetails s3Details = new ImageDetails(S3_CDN_URL, "S3_UPLOAD", "key",
            CoverImageSource.OPEN_LIBRARY, ImageResolutionPreference.ORIGINAL);
        s3Details.setStorageLocation(ImageDetails.STORAGE_S3);
        when(s3BookCoverService.uploadCoverToS3Async(anyString(), anyString(), anyString()))
            .thenReturn(Mono.just(s3Details));
        
        // When: Process in background
        service.processCoverInBackground(book, null);
        
        // Then: Should use "open-library" as source string
        verify(s3BookCoverService, timeout(1000).times(1))
            .uploadCoverToS3Async(anyString(), anyString(), eq("open-library"));
    }
    
    @Test
    void testSourceInference_UnknownUrl() {
        // Given: Book with unknown external URL
        Book book = createBookWithExternalCover("https://example.com/cover.jpg", null);
        
        ImageDetails s3Details = new ImageDetails(S3_CDN_URL, "S3_UPLOAD", "key",
            CoverImageSource.UNDEFINED, ImageResolutionPreference.ORIGINAL);
        s3Details.setStorageLocation(ImageDetails.STORAGE_S3);
        when(s3BookCoverService.uploadCoverToS3Async(anyString(), anyString(), anyString()))
            .thenReturn(Mono.just(s3Details));
        
        // When: Process in background
        service.processCoverInBackground(book, null);
        
        // Then: Should use "unknown" as source string
        verify(s3BookCoverService, timeout(1000).times(1))
            .uploadCoverToS3Async(anyString(), anyString(), eq("unknown"));
    }
    
    // Helper methods
    
    private Book createBookWithExternalCover(String externalUrl, String s3Path) {
        Book book = new Book();
        book.setId(TEST_BOOK_ID);
        book.setIsbn13(TEST_ISBN);
        book.setTitle("Test Book for Bug #4");
        book.setExternalImageUrl(externalUrl);
        book.setS3ImagePath(s3Path);
        return book;
    }
}



