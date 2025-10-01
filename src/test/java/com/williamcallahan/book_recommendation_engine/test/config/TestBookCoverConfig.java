/**
 * Test configuration for book cover management components
 * - Creates mock implementations of all services used by BookCoverManagementService
 * - Configures predictable test behavior for S3, cover sources, and caching
 * - Prevents real external service calls during testing
 * - Provides simulated responses for cover image requests
 * - Enables reliable unit and integration testing in isolation
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.test.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.williamcallahan.book_recommendation_engine.model.Book;
// CachedBookRepository removed with Redis; no longer needed
import com.williamcallahan.book_recommendation_engine.service.EnvironmentService;
import com.williamcallahan.book_recommendation_engine.service.PostgresBookRepository;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.event.BookCoverUpdatedEvent;
import com.williamcallahan.book_recommendation_engine.service.image.CoverCacheManager;
import com.williamcallahan.book_recommendation_engine.service.image.CoverSourceFetchingService;
import com.williamcallahan.book_recommendation_engine.service.image.ImageProcessingService;
import com.williamcallahan.book_recommendation_engine.service.image.LocalDiskCoverCacheService;
import com.williamcallahan.book_recommendation_engine.service.image.OpenLibraryServiceImpl;
import com.williamcallahan.book_recommendation_engine.service.image.S3BookCoverService;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.model.image.ImageAttemptStatus;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.model.image.ImageSourceName;
import com.williamcallahan.book_recommendation_engine.service.image.LongitoodService;
import com.williamcallahan.book_recommendation_engine.model.image.ProcessedImage;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * Test configuration for book cover management tests
 * Only active in test environment to ensure predictable test behavior
 */
@Configuration
@Profile("test")
public class TestBookCoverConfig {
    private static final Logger logger = LoggerFactory.getLogger(TestBookCoverConfig.class);

    /**
     * Provides a mock S3Client for the test environment
     * - Never makes real AWS calls
     * - Allows S3BookCoverService to be constructed properly in tests
     * 
     * @return Mock S3Client for testing
     */
    @Bean
    @Primary
    public S3Client testS3Client() {
        S3Client mockS3Client = Mockito.mock(S3Client.class);
        logger.info("Mock S3Client configured for testing");
        return mockS3Client;
    }

    /**
     * Provides a mock S3BookCoverService that never makes real AWS calls
     * - Returns preconfigured responses for test book IDs
     * - Tracks all method invocations for verification in tests
     * - Simulates S3 fetch and upload operations without external dependencies
     * 
     * @return Mock S3BookCoverService for testing
     */
    @Bean
    @Primary
    public S3BookCoverService testS3BookCoverService(WebClient.Builder webClientBuilder, 
                                                     ImageProcessingService imageProcessingService,
                                                     EnvironmentService environmentService,
                                                     S3Client s3Client) {
        S3BookCoverService mockS3BookCoverService = Mockito.mock(S3BookCoverService.class);
        
        // Mock fetchCover method to return empty or test-specific results
        Mockito.when(mockS3BookCoverService.fetchCover(Mockito.any(Book.class)))
               .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        
        // Configure special test book ID
        Mockito.when(mockS3BookCoverService.fetchCover(Mockito.argThat(book -> 
                "testbook123".equals(book.getId()) || "Hn41AgAAQBAJ".equals(book.getId()))))
               .thenAnswer(invocation -> {
                   ImageDetails details = new ImageDetails(
                       "https://test-cdn.example.com/images/book-covers/testbook123-lg-google-books.jpg", 
                       "GOOGLE_BOOKS", 
                       "images/book-covers/testbook123-lg-google-books.jpg", 
                       CoverImageSource.GOOGLE_BOOKS,  // Actual data source
                       ImageResolutionPreference.ORIGINAL,
                       300, 450
                   );
                   details.setStorageLocation(ImageDetails.STORAGE_S3);  // Stored in S3
                   details.setStorageKey("images/book-covers/testbook123-lg-google-books.jpg");
                   return CompletableFuture.completedFuture(Optional.of(details));
               });
               
        // Mock uploadProcessedCoverToS3Async method
        Mockito.when(mockS3BookCoverService.uploadProcessedCoverToS3Async(
                Mockito.any(byte[].class), 
                Mockito.anyString(), 
                Mockito.anyString(), 
                Mockito.anyInt(), 
                Mockito.anyInt(), 
                Mockito.anyString(), 
                Mockito.anyString(), 
                Mockito.any(ImageProvenanceData.class)))
               .thenAnswer(invocation -> {
                   ImageDetails details = new ImageDetails(
                       "https://test-cdn.example.com/images/book-covers/mock-upload.jpg",
                       "GOOGLE_BOOKS",  // Use actual data source
                       "images/book-covers/mock-upload.jpg",
                       CoverImageSource.GOOGLE_BOOKS,  // Not S3_CACHE
                       ImageResolutionPreference.ORIGINAL,
                       300, 450
                   );
                   details.setStorageLocation(ImageDetails.STORAGE_S3);  // Uploaded to S3
                   details.setStorageKey("images/book-covers/mock-upload.jpg");
                   return Mono.just(details);
               });
        
        // Mock isS3Enabled method
        Mockito.when(mockS3BookCoverService.isS3Enabled()).thenReturn(true);
        
        logger.info("Mock S3BookCoverService configured for testing");
        return mockS3BookCoverService;
    }
    
    /**
     * Provides a mock LocalDiskCoverCacheService that works without file system access
     * - Returns consistent paths for placeholders
     * - Simulates image caching without writing files
     * - Provides test-specific responses for cache lookups
     * 
     * @return Mock LocalDiskCoverCacheService for testing
     */
    @Bean
    @Primary
    public LocalDiskCoverCacheService testLocalDiskCoverCacheService() {
        BiFunction<String, String, ImageDetails> placeholderFactory = (bookIdForLog, reasonSuffix) -> {
            String cleanSuffix = reasonSuffix != null ? reasonSuffix.replaceAll("[^a-zA-Z0-9-]", "_") : "unknown";
            String placeholderPath = ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH;
            ImageDetails details = new ImageDetails(
                placeholderPath,
                "SYSTEM_PLACEHOLDER",
                "placeholder-" + cleanSuffix + "-" + bookIdForLog,
                CoverImageSource.NONE,
                ImageResolutionPreference.UNKNOWN
            );
            details.setStorageLocation(ImageDetails.STORAGE_LOCAL);
            details.setStorageKey(placeholderPath);
            return details;
        };

        LocalDiskCoverCacheService mockDiskCache = Mockito.mock(
            LocalDiskCoverCacheService.class,
            invocation -> {
                String methodName = invocation.getMethod().getName();
                switch (methodName) {
                    case "cacheRemoteImageAsync": {
                        String imageUrl = invocation.getArgument(0, String.class);
                        String bookIdForLog = invocation.getArgument(1, String.class);
                        ImageProvenanceData provenanceData = invocation.getArgument(2, ImageProvenanceData.class);
                        String sourceNameString = invocation.getArgument(3, String.class);

                        if (provenanceData != null) {
                            ImageSourceName sourceName = ImageSourceName.valueOf(sourceNameString.toUpperCase().replace('-', '_'));
                            ImageProvenanceData.AttemptedSourceInfo attemptInfo =
                                new ImageProvenanceData.AttemptedSourceInfo(sourceName, imageUrl, ImageAttemptStatus.SUCCESS);
                            if (provenanceData.getAttemptedImageSources() == null) {
                                provenanceData.setAttemptedImageSources(new java.util.ArrayList<>());
                            }
                            provenanceData.getAttemptedImageSources().add(attemptInfo);
                        }

                        if (imageUrl.contains("testbook123") || imageUrl.contains("Hn41AgAAQBAJ")) {
                            ImageDetails details = new ImageDetails(
                                "/book-covers/testbook-" + sourceNameString + ".jpg",
                                sourceNameString,
                                "testbook-" + sourceNameString + ".jpg",
                                CoverImageSource.GOOGLE_BOOKS,
                                ImageResolutionPreference.ORIGINAL,
                                300,
                                450
                            );
                            details.setStorageLocation(ImageDetails.STORAGE_LOCAL);
                            details.setStorageKey("testbook-" + sourceNameString + ".jpg");
                            return CompletableFuture.completedFuture(details);
                        }

                        if (imageUrl.contains("large-image")) {
                            ImageDetails details = new ImageDetails(
                                "/book-covers/large-" + bookIdForLog + ".jpg",
                                sourceNameString,
                                "large-" + bookIdForLog + ".jpg",
                                CoverImageSource.GOOGLE_BOOKS,
                                ImageResolutionPreference.ORIGINAL,
                                800,
                                1200
                            );
                            details.setStorageLocation(ImageDetails.STORAGE_LOCAL);
                            details.setStorageKey("large-" + bookIdForLog + ".jpg");
                            return CompletableFuture.completedFuture(details);
                        }

                        if (imageUrl.contains("small-image")) {
                            ImageDetails details = new ImageDetails(
                                "/book-covers/small-" + bookIdForLog + ".jpg",
                                sourceNameString,
                                "small-" + bookIdForLog + ".jpg",
                                CoverImageSource.GOOGLE_BOOKS,
                                ImageResolutionPreference.ORIGINAL,
                                120,
                                180
                            );
                            details.setStorageLocation(ImageDetails.STORAGE_LOCAL);
                            details.setStorageKey("small-" + bookIdForLog + ".jpg");
                            return CompletableFuture.completedFuture(details);
                        }

                        if (imageUrl.contains("books.google.com/books/content")) {
                            ImageDetails details = new ImageDetails(
                                imageUrl,
                                sourceNameString,
                                "google-content-" + bookIdForLog + ".jpg",
                                CoverImageSource.GOOGLE_BOOKS,
                                ImageResolutionPreference.ORIGINAL,
                                600,
                                900
                            );
                            details.setStorageLocation(ImageDetails.STORAGE_LOCAL);
                            details.setStorageKey("google-content-" + bookIdForLog + ".jpg");
                            return CompletableFuture.completedFuture(details);
                        }

                        if (imageUrl.contains("s3.amazonaws.com")) {
                            ImageDetails details = new ImageDetails(
                                "https://s3.amazonaws.com/mock/testbook-s3.jpg",
                                sourceNameString,
                                "s3-testbook.jpg",
                                CoverImageSource.MOCK,
                                ImageResolutionPreference.HIGH_ONLY,
                                600,
                                900
                            );
                            details.setStorageLocation(ImageDetails.STORAGE_S3);
                            details.setStorageKey("images/book-covers/mock-upload.jpg");
                            return CompletableFuture.completedFuture(details);
                        }

                        CoverImageSource source = CoverImageSource.GOOGLE_BOOKS;
                        if (imageUrl.contains("openlibrary.org")) {
                            source = CoverImageSource.OPEN_LIBRARY;
                        } else if (imageUrl.contains("longitood.com")) {
                            source = CoverImageSource.LONGITOOD;
                        }

                        ImageDetails details = new ImageDetails(
                            "/book-covers/mock-" + bookIdForLog + ".jpg",
                            sourceNameString,
                            "mock-" + bookIdForLog + ".jpg",
                            source,
                            ImageResolutionPreference.ORIGINAL,
                            300,
                            450
                        );
                        details.setStorageLocation(ImageDetails.STORAGE_LOCAL);
                        details.setStorageKey("mock-" + bookIdForLog + ".jpg");
                        return CompletableFuture.completedFuture(details);
                    }
                    case "placeholderImageDetails": {
                        String bookIdForLog = invocation.getArgument(0, String.class);
                        String reasonSuffix = invocation.getArgument(1, String.class);
                        return placeholderFactory.apply(bookIdForLog, reasonSuffix);
                    }
                    case "buildPlaceholderImageDetails": {
                        String bookIdForLog = invocation.getArgument(0, String.class);
                        String reasonSuffix = invocation.getArgument(1, String.class);
                        return placeholderFactory.apply(bookIdForLog, reasonSuffix);
                    }
                    default:
                        return Mockito.RETURNS_DEFAULTS.answer(invocation);
                }
            }
        );

        Mockito.when(mockDiskCache.getLocalPlaceholderPath()).thenReturn(ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH);
        Mockito.when(mockDiskCache.getCacheDirName()).thenReturn("book-covers");
        Mockito.when(mockDiskCache.getCacheDirString()).thenReturn("/tmp/book-covers");

        logger.info("Mock LocalDiskCoverCacheService configured for testing");
        return mockDiskCache;
    }

    /**
     * Provides a mock CoverCacheManager with in-memory caches for testing
     * - Uses real Caffeine caches but smaller size for testing
     * - Allows tests to verify cache operations function correctly
     * 
     * @return CoverCacheManager for testing
     */
    @Bean
    @Primary
    public CoverCacheManager testCoverCacheManager() {
        // Create a real CoverCacheManager but with smaller cache sizes for testing
        return new CoverCacheManager() {
            private final Cache<String, ImageDetails> identifierToFinalImageDetailsCache = 
                Caffeine.newBuilder()
                    .maximumSize(100)
                    .expireAfterAccess(1, TimeUnit.HOURS)
                    .build();
                    
            private final Cache<String, String> identifierToProvisionalUrlCache = 
                Caffeine.newBuilder()
                    .maximumSize(100)
                    .expireAfterWrite(30, TimeUnit.MINUTES)
                    .build();
                    
            // Override methods if needed for testing
            @Override
            public ImageDetails getFinalImageDetails(String identifierKey) {
                return identifierToFinalImageDetailsCache.getIfPresent(identifierKey);
            }
            
            @Override
            public void putFinalImageDetails(String identifierKey, ImageDetails imageDetails) {
                identifierToFinalImageDetailsCache.put(identifierKey, imageDetails);
            }
            
            @Override
            public String getProvisionalUrl(String identifierKey) {
                return identifierToProvisionalUrlCache.getIfPresent(identifierKey);
            }
            
            @Override
            public void putProvisionalUrl(String identifierKey, String provisionalUrl) {
                identifierToProvisionalUrlCache.put(identifierKey, provisionalUrl);
            }
            
            @Override
            public void invalidateProvisionalUrl(String identifierKey) {
                identifierToProvisionalUrlCache.invalidate(identifierKey);
            }
        };
    }
    
    /**
     * Provides a mock ImageProcessingService for testing
     * - Processes test images without actual image manipulation
     * - Returns predictable dimensions and data
     * 
     * @return Mock ImageProcessingService for testing
     */
    @Bean
    @Primary
    public ImageProcessingService testImageProcessingService() {
        ImageProcessingService mockImageProcessor = Mockito.mock(ImageProcessingService.class);
        
        // Mock the processImageForS3 method
        Mockito.when(mockImageProcessor.processImageForS3(Mockito.any(byte[].class), Mockito.anyString()))
            .thenAnswer(invocation -> {
                byte[] dummyBytes = new byte[1024]; // Simulate processed image bytes
                return CompletableFuture.completedFuture(
                    ProcessedImage.success(dummyBytes, ".jpg", "image/jpeg", 300, 450)
                );
            });
            
        // Add special case for image rejection due to dominant white
        Mockito.when(mockImageProcessor.processImageForS3(
                Mockito.argThat(bytes -> bytes != null && bytes.length > 0 && bytes[0] == (byte)0xFF), 
                Mockito.anyString()))
            .thenReturn(CompletableFuture.completedFuture(
                ProcessedImage.failure("LikelyNotACover_DominantColor")));
        
        logger.info("Mock ImageProcessingService configured for testing");
        return mockImageProcessor;
    }
    
    /**
     * Provides a mock ApplicationEventPublisher for testing
     * - Captures published events for verification
     * - Allows tests to check event publication behavior
     * 
     * @return Mock ApplicationEventPublisher for testing
     */
    @Bean
    @Primary
    public ApplicationEventPublisher testApplicationEventPublisher() {
        ApplicationEventPublisher mockPublisher = Mockito.mock(ApplicationEventPublisher.class);
        logger.info("Mock ApplicationEventPublisher configured for testing");
        return mockPublisher;
    }
    
    /**
     * Provides a mock EnvironmentService for testing
     * - Controls feature flags and environment settings
     * - Enables consistent testing behavior
     * 
     * @return Mock EnvironmentService for testing
     */
    @Bean
    @Primary
    public EnvironmentService testEnvironmentService() {
        EnvironmentService mockEnvironmentService = Mockito.mock(EnvironmentService.class);
        
        // Set default behavior
        Mockito.when(mockEnvironmentService.isBookCoverDebugMode()).thenReturn(true);
        Mockito.when(mockEnvironmentService.isDevelopmentMode()).thenReturn(true);
        
        logger.info("Mock EnvironmentService configured for testing");
        return mockEnvironmentService;
    }
    
    /**
     * Provides a mock CoverSourceFetchingService for testing
     * - Coordinates fetching of cover images from multiple sources
     * - Returns predictable results for testing
     * 
     * @return Mock CoverSourceFetchingService for testing
     */
    @Bean
    @Primary
    public CoverSourceFetchingService testCoverSourceFetchingService() {
        CoverSourceFetchingService mockService = Mockito.mock(CoverSourceFetchingService.class);
        
        // Default behavior to return a placeholder image details
        Mockito.when(mockService.getBestCoverImageUrlAsync(Mockito.any(Book.class), Mockito.anyString(), Mockito.any(ImageProvenanceData.class)))
            .thenAnswer(invocation -> {
                Book book = invocation.getArgument(0);
                String bookId = book != null ? book.getId() : "unknown-id";
                
                ImageDetails result = new ImageDetails(
                    "/book-covers/mock-cover-" + bookId + ".jpg",
                    "TEST_SOURCE",
                    "mock-cover-" + bookId + ".jpg",
                    CoverImageSource.GOOGLE_BOOKS,  // Use actual data source, not cache
                    ImageResolutionPreference.ORIGINAL,
                    300, 450
                );
                result.setStorageLocation(ImageDetails.STORAGE_LOCAL);  // Cached locally
                result.setStorageKey("mock-cover-" + bookId + ".jpg");
                
                return CompletableFuture.completedFuture(result);
            });
            
        // Special case for test books
        Mockito.when(mockService.getBestCoverImageUrlAsync(
                Mockito.argThat(book -> book != null && ("testbook123".equals(book.getId()) || "Hn41AgAAQBAJ".equals(book.getId()))),
                Mockito.anyString(),
                Mockito.any(ImageProvenanceData.class)))
            .thenAnswer(invocation -> {
                Book book = invocation.getArgument(0);
                String bookId = book.getId();
                
                ImageDetails result = new ImageDetails(
                    "/book-covers/high-quality-" + bookId + ".jpg",
                    ApplicationConstants.Provider.GOOGLE_BOOKS,
                    "high-quality-" + bookId + ".jpg",
                    CoverImageSource.GOOGLE_BOOKS,
                    ImageResolutionPreference.HIGH_ONLY,
                    800, 1200
                );
                
                return CompletableFuture.completedFuture(result);
            });
        
        logger.info("Mock CoverSourceFetchingService configured for testing");
        return mockService;
    }
    
    /**
     * Provides a mock LongitoodService for testing
     * - Simulates external Longitood API
     * - Returns predictable images for testing
     * 
     * @return Mock LongitoodService for testing
     */
    @Bean
    @Primary
    public LongitoodService testLongitoodService() {
        LongitoodService mockService = Mockito.mock(LongitoodService.class);
        
        // Default behavior to return empty
        Mockito.when(mockService.fetchCover(Mockito.any(Book.class)))
            .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        
        // Special case for test books
        Mockito.when(mockService.fetchCover(Mockito.argThat(book ->
                book != null && "testbook123".equals(book.getId()))))
            .thenReturn(CompletableFuture.completedFuture(Optional.of(
                new ImageDetails(
                    "https://covers.longitood.com/testbook123.jpg",
                    "LONGITOOD",
                    "testbook123",
                    CoverImageSource.LONGITOOD,
                    ImageResolutionPreference.ORIGINAL,
                    400, 600
                )
            )));
        Mockito.when(mockService.fetchAndCacheCover(Mockito.any(Book.class), Mockito.anyString(), Mockito.any()))
            .thenReturn(CompletableFuture.completedFuture(new ImageDetails(
                "https://covers.longitood.com/mock.jpg",
                "LONGITOOD",
                "mock-longitood",
                CoverImageSource.LONGITOOD,
                ImageResolutionPreference.ORIGINAL,
                400,
                600
            )));
        
        logger.info("Mock LongitoodService configured for testing");
        return mockService;
    }
    
    /**
     * Provides a mock OpenLibraryServiceImpl for testing
     * - Simulates OpenLibrary cover image API
     * - Returns predictable responses for tests
     * 
     * @return Mock OpenLibraryServiceImpl for testing
     */
    @Bean
    @Primary
    public OpenLibraryServiceImpl testOpenLibraryService() {
        OpenLibraryServiceImpl mockService = Mockito.mock(OpenLibraryServiceImpl.class);
        
        // Default behavior to return empty
        Mockito.when(mockService.fetchOpenLibraryCoverDetails(Mockito.anyString(), Mockito.anyString()))
            .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        Mockito.when(mockService.fetchAndCacheCover(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any()))
            .thenReturn(CompletableFuture.completedFuture(new ImageDetails(
                "https://covers.openlibrary.org/b/isbn/mock-L.jpg",
                "OPEN_LIBRARY",
                "mock-isbn",
                CoverImageSource.OPEN_LIBRARY,
                ImageResolutionPreference.MEDIUM,
                400,
                600
            )));
        
        // Special case for ISBN lookups
        Mockito.when(mockService.fetchOpenLibraryCoverDetails(
                Mockito.eq("9781234567890"), Mockito.anyString()))
            .thenReturn(CompletableFuture.completedFuture(Optional.of(
                new ImageDetails(
                    "https://covers.openlibrary.org/b/isbn/9781234567890-L.jpg",
                    "OPEN_LIBRARY",
                    "9781234567890",
                    CoverImageSource.OPEN_LIBRARY,
                    ImageResolutionPreference.ORIGINAL
                )
            )));
        
        logger.info("Mock OpenLibraryServiceImpl configured for testing");
        return mockService;
    }
    
    /**
     * Provides a mock GoogleBooksService for testing
     * - Simulates Google Books API responses
     * - Returns test-specific book data
     * 
     * @return Mock GoogleBooksService for testing
     */
    @Bean
    @Primary
    public GoogleBooksService testGoogleBooksService() {
        GoogleBooksService mockService = Mockito.mock(GoogleBooksService.class, Mockito.RETURNS_DEFAULTS);
        logger.info("Mock GoogleBooksService configured for testing");
        return mockService;
    }
    
    // CachedBookRepository removed; no bean required

    /**
     * Provides a mock PostgresBookRepository for testing
     * - Prevents actual database queries during testing
     * - Returns empty results by default
     * 
     * @return Mock PostgresBookRepository for testing
     */
    @Bean
    @Primary
    public PostgresBookRepository testPostgresBookRepository() {
        PostgresBookRepository mockRepository = Mockito.mock(PostgresBookRepository.class);
        
        logger.info("Mock PostgresBookRepository configured for testing");
        return mockRepository;
    }
    
    /**
     * Utility method to create an ArgumentCaptor for BookCoverUpdatedEvent
     * Makes it easy for tests to verify events being published
     * 
     * @return ArgumentCaptor for BookCoverUpdatedEvent
     */
    public static ArgumentCaptor<BookCoverUpdatedEvent> bookCoverEventCaptor() {
        return ArgumentCaptor.forClass(BookCoverUpdatedEvent.class);
    }
}
