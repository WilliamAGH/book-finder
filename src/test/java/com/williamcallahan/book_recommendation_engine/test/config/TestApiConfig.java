/**
 * Test configuration that prevents real API calls during tests
 * - Creates test-specific mock services
 * - Configures caching optimized for test environment
 * - Forces all external API calls to use mock data
 * - Loads test data from classpath resources
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.test.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.service.S3StorageService;
import com.williamcallahan.book_recommendation_engine.types.S3FetchResult;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.types.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.types.LongitoodService;
import com.williamcallahan.book_recommendation_engine.types.ImageDetails;
import org.mockito.Mockito;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import jakarta.annotation.PostConstruct;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import java.nio.charset.StandardCharsets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// S3Client imports
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

// LocalDiskCoverCacheService and related imports
import com.williamcallahan.book_recommendation_engine.service.image.LocalDiskCoverCacheService;
import com.williamcallahan.book_recommendation_engine.types.ImageProvenanceData;

/**
 * Test-specific configuration that prevents real API calls
 * Only active in test environment to ensure predictable test behavior
 */
@Configuration
@Profile("test")
public class TestApiConfig {
    private static final Logger logger = LoggerFactory.getLogger(TestApiConfig.class);
    
    private final ObjectMapper objectMapper;
    
    @Autowired
    public TestApiConfig(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }
    
    /**
     * Creates an S3 storage service that never makes real AWS calls
     * - Returns mock data for specific test book IDs
     * - Returns empty for anything else
     * - Records all attempted S3 operations for verification
     * 
     * @return Mock S3StorageService for testing
     */
    @Bean
    @Primary
    @Qualifier("testS3StorageService")
    public S3StorageService testS3StorageService() {
        return new TestS3StorageService(objectMapper);
    }
    
    /**
     * Test-specific mock implementation of the S3StorageService
     * Prevents real S3 calls during testing
     */
    private static class TestS3StorageService extends S3StorageService {
        private final ObjectMapper objectMapper;
        private final List<String> fetchRequests = new ArrayList<>();
        private final List<String> uploadRequests = new ArrayList<>();
        
        public TestS3StorageService(ObjectMapper objectMapper) {
            super(null, "test-bucket", "https://test-cdn.example.com/", "https://test.example.com/");
            this.objectMapper = objectMapper;
        }
        
        @Override
        public CompletableFuture<S3FetchResult<String>> fetchJsonAsync(String volumeId) {
            fetchRequests.add(volumeId);
            logger.debug("Test S3 fetch for volume: {}", volumeId);
            
            // Return mock data for specific test IDs
            if ("testbook123".equals(volumeId) || "Hn41AgAAQBAJ".equals(volumeId)) {
                try {
                    Resource mockResource = new ClassPathResource("mock-responses/books/" + volumeId + ".json");
                    if (mockResource.exists()) {
                        String json = new String(mockResource.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                        return CompletableFuture.completedFuture(S3FetchResult.success(json));
                    } else {
                        // Create a minimal fake response
                        ObjectNode fakeBook = objectMapper.createObjectNode();
                        fakeBook.put("id", volumeId);
                        fakeBook.put("kind", "books#volume");
                        
                        ObjectNode volumeInfo = objectMapper.createObjectNode();
                        volumeInfo.put("title", "Test Book " + volumeId);
                        
                        ArrayNode authors = objectMapper.createArrayNode();
                        authors.add("Test Author");
                        volumeInfo.set("authors", authors);
                        
                        volumeInfo.put("description", "This is a test book for unit tests");
                        fakeBook.set("volumeInfo", volumeInfo);
                        
                        return CompletableFuture.completedFuture(S3FetchResult.success(fakeBook.toString()));
                    }
                } catch (IOException e) {
                    logger.warn("Error creating mock S3 response: {}", e.getMessage());
                     // In case of error, simulate a service error or not found, depending on desired test behavior
                    return CompletableFuture.completedFuture(S3FetchResult.serviceError("Mock data loading error: " + e.getMessage()));
                }
            }
            
            // Return not found for anything else
            return CompletableFuture.completedFuture(S3FetchResult.notFound());
        }
        
        @Override
        public CompletableFuture<Void> uploadJsonAsync(String volumeId, String jsonContent) {
            uploadRequests.add(volumeId);
            logger.debug("Test S3 upload for volume: {}", volumeId);
            return CompletableFuture.completedFuture(null);
        }
    }
    
    /**
     * Initializer for test environment to ensure predictable test behavior
     * Logs activation of test profile and mock services
     */
    @PostConstruct
    public void testEnvironmentInitializer() {
        logger.info("=============================================================");
        logger.info("Test profile active - Using mock services for all external calls");
        logger.info("This prevents real API calls to Google Books and other services");
        logger.info("Tests will use pre-defined mock data from classpath resources");
        logger.info("=============================================================");
    }

    @Bean
    @Primary
    public LongitoodService testLongitoodService() {
        LongitoodService mockLongitoodService = Mockito.mock(LongitoodService.class);
        // Default behavior: return an empty optional for any book
        Mockito.when(mockLongitoodService.fetchCover(Mockito.any(Book.class)))
               .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        
        logger.info("Mock LongitoodService configured to return empty Optional by default.");
        return mockLongitoodService;
    }

    @Bean
    @Primary
    public S3Client testS3Client() {
        S3Client mockS3Client = Mockito.mock(S3Client.class);

        // Default behavior for headObject: simulate object not found
        Mockito.when(mockS3Client.headObject(Mockito.any(HeadObjectRequest.class)))
               .thenThrow(NoSuchKeyException.builder().message("Mock S3: Object not found for test").build());

        // Default behavior for putObject: simulate successful upload
        Mockito.when(mockS3Client.putObject(Mockito.any(PutObjectRequest.class), Mockito.any(RequestBody.class)))
               .thenReturn(PutObjectResponse.builder().build());
        
        // Add other specific S3 client behaviors if needed for particular tests

        logger.info("Mock S3Client configured.");
        return mockS3Client;
    }

    @Bean
    @Primary
    public LocalDiskCoverCacheService testLocalDiskCoverCacheService() {
        LocalDiskCoverCacheService mockDiskCache = Mockito.mock(LocalDiskCoverCacheService.class);

        // Stub getLocalPlaceholderPath
        String placeholderPath = "/images/mock-placeholder.svg";
        Mockito.when(mockDiskCache.getLocalPlaceholderPath()).thenReturn(placeholderPath);

        // Stub downloadAndStoreImageLocallyAsync to return a placeholder
        Mockito.when(mockDiskCache.downloadAndStoreImageLocallyAsync(
            Mockito.anyString(), Mockito.anyString(), 
            Mockito.any(ImageProvenanceData.class), Mockito.anyString()))
            .thenAnswer(invocation -> {
                String bookIdForLog = invocation.getArgument(1);
                ImageDetails placeholderDetails = new ImageDetails(
                    placeholderPath, 
                    "MOCK_DISK_CACHE", 
                    "mock-placeholder-" + bookIdForLog, 
                    CoverImageSource.NONE,
                    ImageResolutionPreference.ANY,
                    0, 0 // Placeholder dimensions
                );
                return CompletableFuture.completedFuture(placeholderDetails);
            });

        // Stub createPlaceholderImageDetails to return a valid placeholder
        Mockito.when(mockDiskCache.createPlaceholderImageDetails(Mockito.anyString(), Mockito.anyString()))
            .thenAnswer(invocation -> {
                String bookIdForLogArg = invocation.getArgument(0);
                String reasonSuffixArg = invocation.getArgument(1);
                // Using the same placeholderPath as defined above for consistency
                return new ImageDetails(
                    placeholderPath,
                    "MOCK_SYSTEM_PLACEHOLDER",
                    "mock-placeholder-" + reasonSuffixArg + "-" + bookIdForLogArg,
                    CoverImageSource.LOCAL_CACHE, // Corrected to LOCAL_CACHE
                    ImageResolutionPreference.UNKNOWN,
                    0,0
                );
            });
        
        // Stub other public methods if necessary for tests, e.g., methods to check if an image exists in cache
        // Mockito.when(mockDiskCache.getCachedImagePath(Mockito.anyString())).thenReturn(Optional.empty());

        logger.info("Mock LocalDiskCoverCacheService configured.");
        return mockDiskCache;
    }
}
