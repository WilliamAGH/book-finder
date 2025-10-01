package com.williamcallahan.book_recommendation_engine.service.image;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.GoogleApiFetcher;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.mapper.GoogleBooksMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class CoverSourceFetchingServiceTest {

    private LocalDiskCoverCacheService localDiskCoverCacheService;
    private S3BookCoverService s3BookCoverService;
    private OpenLibraryServiceImpl openLibraryService;
    private LongitoodService longitoodService;
    private GoogleBooksService googleBooksService;
    private GoogleApiFetcher googleApiFetcher;
    private CoverCacheManager coverCacheManager;
    private BookDataOrchestrator bookDataOrchestrator;
    private ImageSelectionService imageSelectionService;
    private ImageProvenanceHandler imageProvenanceHandler;
    private GoogleBooksMapper googleBooksMapper;

    private CoverSourceFetchingService service;

    @BeforeEach
    void setUp() {
        localDiskCoverCacheService = mock(LocalDiskCoverCacheService.class);
        s3BookCoverService = mock(S3BookCoverService.class);
        given(s3BookCoverService.uploadCoverToS3Async(anyString(), anyString(), anyString(), any(ImageProvenanceData.class)))
            .willAnswer(invocation -> {
                String imageUrl = invocation.getArgument(0, String.class);
                String sourceName = invocation.getArgument(2, String.class);
                ImageDetails uploaded = new ImageDetails(
                    imageUrl,
                    sourceName != null ? sourceName : "S3",
                    "mock-upload",
                    CoverImageSource.MOCK,
                    null,
                    600,
                    900
                );
                uploaded.setStorageLocation(ImageDetails.STORAGE_S3);
                uploaded.setStorageKey("mock-upload");
                return Mono.just(uploaded);
            });
        openLibraryService = mock(OpenLibraryServiceImpl.class);
        longitoodService = mock(LongitoodService.class);
        googleBooksService = mock(GoogleBooksService.class);
        googleApiFetcher = mock(GoogleApiFetcher.class);
        coverCacheManager = mock(CoverCacheManager.class);
        bookDataOrchestrator = mock(BookDataOrchestrator.class);

        given(localDiskCoverCacheService.getLocalPlaceholderPath()).willReturn("/placeholder");
        given(localDiskCoverCacheService.getCacheDirName()).willReturn("covers");
        given(localDiskCoverCacheService.placeholderImageDetails(anyString(), anyString()))
            .willAnswer(invocation -> {
                String bookIdForLog = invocation.getArgument(0, String.class);
                String reasonSuffix = invocation.getArgument(1, String.class);
                ImageDetails details = new ImageDetails(
                    "/placeholder",
                    "PLACEHOLDER",
                    reasonSuffix + "-" + bookIdForLog,
                    CoverImageSource.NONE,
                    null,
                    0,
                    0
                );
                details.setStorageLocation(ImageDetails.STORAGE_LOCAL);
                return details;
            });

        given(s3BookCoverService.fetchCover(any(Book.class))).willReturn(CompletableFuture.completedFuture(Optional.empty()));
        given(openLibraryService.fetchOpenLibraryCoverDetails(any(), any()))
            .willReturn(CompletableFuture.completedFuture(Optional.empty()));
        given(openLibraryService.fetchAndCacheCover(anyString(), anyString(), anyString(), any()))
            .willAnswer(invocation -> CompletableFuture.completedFuture(
                localDiskCoverCacheService.placeholderImageDetails("open-library-test", "ol-mock")));
        given(longitoodService.fetchCover(any(Book.class)))
            .willReturn(CompletableFuture.completedFuture(Optional.empty()));
        given(longitoodService.fetchAndCacheCover(any(Book.class), anyString(), any()))
            .willAnswer(invocation -> CompletableFuture.completedFuture(
                localDiskCoverCacheService.placeholderImageDetails("longitood-test", "longitood-mock")));
        given(coverCacheManager.isKnownBadImageUrl(any())).willReturn(false);

        imageSelectionService = mock(ImageSelectionService.class);
        imageProvenanceHandler = mock(ImageProvenanceHandler.class, invocation -> null);
        googleBooksMapper = new GoogleBooksMapper();

        given(imageSelectionService.selectBest(anyList(), anyString(), anyString()))
            .willAnswer(invocation -> {
                List<ImageDetails> candidates = invocation.getArgument(0);
                if (candidates == null || candidates.isEmpty()) {
                    return new ImageSelectionService.SelectionResult(null, "no-candidates");
                }
                return new ImageSelectionService.SelectionResult(candidates.get(0), null);
            });

        service = new CoverSourceFetchingService(
            localDiskCoverCacheService,
            s3BookCoverService,
            openLibraryService,
            longitoodService,
            googleBooksService,
            googleApiFetcher,
            coverCacheManager,
            bookDataOrchestrator,
            imageSelectionService,
            imageProvenanceHandler,
            googleBooksMapper
        );
    }

    @Test
    void tieredLookupReturnsOrchestratorCoverWithoutGoogle() {
        Book orchestrated = new Book();
        orchestrated.setId("postgres-book");
        orchestrated.setS3ImagePath("https://cdn.example.com/covers/postgres-book.jpg");
        orchestrated.setCoverImageWidth(600);
        orchestrated.setCoverImageHeight(900);
        CoverImages coverImages = new CoverImages();
        coverImages.setPreferredUrl("https://cdn.example.com/covers/postgres-book.jpg");
        coverImages.setFallbackUrl("https://cdn.example.com/covers/postgres-book-fallback.jpg");
        orchestrated.setCoverImages(coverImages);

        given(bookDataOrchestrator.fetchCanonicalBookReactive("postgres-book")).willReturn(Mono.just(orchestrated));

        ImageProvenanceData provenance = new ImageProvenanceData();
        Book requestBook = new Book();
        requestBook.setId("postgres-book");

        ImageDetails result = service.getBestCoverImageUrlAsync(requestBook, null, provenance)
            .toCompletableFuture()
            .join();

        assertThat(result.getUrlOrPath()).isEqualTo("https://cdn.example.com/covers/postgres-book.jpg");
        verify(bookDataOrchestrator).fetchCanonicalBookReactive("postgres-book");
        verify(s3BookCoverService).fetchCover(any(Book.class));
        verifyNoInteractions(googleApiFetcher);
    }

    @Test
    void tieredLookupFallsBackToGoogleWhenOrchestratorEmpty() {
        given(bookDataOrchestrator.fetchCanonicalBookReactive("vol-123")).willReturn(Mono.empty());

        JsonNode googleJson;
        try {
            googleJson = new ObjectMapper().readTree("{" +
                "\"id\":\"vol-123\"," +
                "\"volumeInfo\":{\"title\":\"Test\"," +
                "\"imageLinks\":{\"thumbnail\":\"https://books.google.com/books/content?id=vol-123&printsec=frontcover&img=1&zoom=1\"}}}" );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        given(googleApiFetcher.fetchVolumeByIdAuthenticated("vol-123")).willReturn(Mono.just(googleJson));
        given(googleApiFetcher.fetchVolumeByIdUnauthenticated("vol-123")).willReturn(Mono.empty());
        ImageProvenanceData provenance = new ImageProvenanceData();
        Book requestBook = new Book();
        requestBook.setId("vol-123");

        ImageDetails result = service.getBestCoverImageUrlAsync(requestBook, null, provenance)
            .toCompletableFuture()
            .join();

        assertThat(result.getUrlOrPath())
            .contains("books.google.com/books/content");
        verify(bookDataOrchestrator, times(2)).fetchCanonicalBookReactive("vol-123");
        verify(googleApiFetcher).fetchVolumeByIdAuthenticated("vol-123");
    }
}
