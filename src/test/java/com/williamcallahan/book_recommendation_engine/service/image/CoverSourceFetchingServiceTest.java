package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.model.image.ImageDetails;
import com.williamcallahan.book_recommendation_engine.model.image.ImageProvenanceData;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class CoverSourceFetchingServiceTest {

    private LocalDiskCoverCacheService localDiskCoverCacheService;
    private S3BookCoverService s3BookCoverService;
    private OpenLibraryServiceImpl openLibraryService;
    private LongitoodService longitoodService;
    private GoogleBooksService googleBooksService;
    private CoverCacheManager coverCacheManager;
    private BookDataOrchestrator bookDataOrchestrator;

    private CoverSourceFetchingService service;

    @BeforeEach
    void setUp() {
        localDiskCoverCacheService = mock(LocalDiskCoverCacheService.class);
        s3BookCoverService = mock(S3BookCoverService.class);
        openLibraryService = mock(OpenLibraryServiceImpl.class);
        longitoodService = mock(LongitoodService.class);
        googleBooksService = mock(GoogleBooksService.class);
        coverCacheManager = mock(CoverCacheManager.class);
        bookDataOrchestrator = mock(BookDataOrchestrator.class);

        given(localDiskCoverCacheService.getLocalPlaceholderPath()).willReturn("/placeholder");
        given(localDiskCoverCacheService.getCacheDirName()).willReturn("covers");
        given(localDiskCoverCacheService.createPlaceholderImageDetails(any(), any())).willAnswer(invocation -> {
            ImageDetails placeholder = new ImageDetails("/placeholder", "LOCAL", null, null, null);
            placeholder.setWidth(0);
            placeholder.setHeight(0);
            return placeholder;
        });

        given(s3BookCoverService.fetchCover(any(Book.class))).willReturn(CompletableFuture.completedFuture(Optional.empty()));
        given(openLibraryService.fetchOpenLibraryCoverDetails(any(), any()))
            .willReturn(CompletableFuture.completedFuture(Optional.empty()));
        given(longitoodService.fetchCover(any(Book.class)))
            .willReturn(CompletableFuture.completedFuture(Optional.empty()));
        given(coverCacheManager.isKnownBadImageUrl(any())).willReturn(false);

        service = new CoverSourceFetchingService(
            localDiskCoverCacheService,
            s3BookCoverService,
            openLibraryService,
            longitoodService,
            googleBooksService,
            coverCacheManager,
            bookDataOrchestrator
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

        given(bookDataOrchestrator.getBookByIdTiered("postgres-book")).willReturn(Mono.just(orchestrated));

        ImageProvenanceData provenance = new ImageProvenanceData();
        Book requestBook = new Book();
        requestBook.setId("postgres-book");

        ImageDetails result = service.getBestCoverImageUrlAsync(requestBook, null, provenance)
            .toCompletableFuture()
            .join();

        assertThat(result.getUrlOrPath()).isEqualTo("https://cdn.example.com/covers/postgres-book.jpg");
        verify(bookDataOrchestrator).getBookByIdTiered("postgres-book");
        verify(s3BookCoverService).fetchCover(any(Book.class));
        verify(googleBooksService, never()).getBookById(any());
        verify(googleBooksService, never()).searchBooksByISBN(any());
    }

    @Test
    void tieredLookupFallsBackToGoogleWhenOrchestratorEmpty() {
        given(bookDataOrchestrator.getBookByIdTiered("vol-123")).willReturn(Mono.empty());

        Book googleBook = new Book();
        googleBook.setId("vol-123");
        googleBook.setExternalImageUrl("https://books.googleusercontent.com/cover.jpg");
        googleBook.setRawJsonResponse("{}");

        given(googleBooksService.getBookById("vol-123")).willReturn(CompletableFuture.completedFuture(googleBook));
        given(localDiskCoverCacheService.downloadAndStoreImageLocallyAsync(argThat((String url) -> url.startsWith("https://books.googleusercontent.com/cover.jpg")), any(), any(), any()))
            .willReturn(CompletableFuture.completedFuture(new ImageDetails(
                "https://books.googleusercontent.com/cover.jpg",
                "GOOGLE",
                "vol-123",
                null,
                null,
                600,
                900
            )));

        ImageProvenanceData provenance = new ImageProvenanceData();
        Book requestBook = new Book();
        requestBook.setId("vol-123");

        ImageDetails result = service.getBestCoverImageUrlAsync(requestBook, null, provenance)
            .toCompletableFuture()
            .join();

        assertThat(result.getUrlOrPath()).isEqualTo("https://books.googleusercontent.com/cover.jpg");
        verify(bookDataOrchestrator).getBookByIdTiered("vol-123");
        verify(googleBooksService).getBookById("vol-123");

        ArgumentCaptor<ImageProvenanceData> provenanceCaptor = ArgumentCaptor.forClass(ImageProvenanceData.class);
        verify(localDiskCoverCacheService).downloadAndStoreImageLocallyAsync(argThat((String url) -> url.startsWith("https://books.googleusercontent.com/cover.jpg")), eq("vol-123"), provenanceCaptor.capture(), eq("GoogleBooksAPI-VolumeID"));
        assertThat(provenanceCaptor.getValue().getGoogleBooksApiResponse()).isEqualTo("{}");
    }
}
