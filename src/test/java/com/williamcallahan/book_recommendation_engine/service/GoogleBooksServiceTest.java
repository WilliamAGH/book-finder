package com.williamcallahan.book_recommendation_engine.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

@ExtendWith(MockitoExtension.class)
class GoogleBooksServiceTest {

    @Mock
    private RestTemplate restTemplate;

    private final String apiKey = "TEST_API_KEY";

    @InjectMocks
    private GoogleBooksService googleBooksService;

    @BeforeEach
    void setUp() {
        // Reset mocks if necessary
        // Mockito.reset(restTemplate);
    }

    @Test
    void searchBooks_returnsBooks_whenApiResponseHasItems() {
        // Arrange
        String query = "java";
        GoogleBooksApiResponse apiResponse = new GoogleBooksApiResponse();
        GoogleBooksVolume volume = new GoogleBooksVolume();
        volume.setId("vol1");
        VolumeInfo info = new VolumeInfo();
        info.setTitle("Effective Java");
        info.setAuthors(List.of("Joshua Bloch"));
        volume.setVolumeInfo(info);
        apiResponse.setItems(List.of(volume));

        when(restTemplate.getForObject(anyString(), eq(GoogleBooksApiResponse.class)))
            .thenReturn(apiResponse);

        // Act
        var result = googleBooksService.searchBooks(query);

        // Assert
        assertNotNull(result, "Result should not be null");
        assertEquals(1, result.size(), "Should return one book");
        var book = result.get(0);
        assertEquals("vol1", book.getId());
        assertEquals("Effective Java", book.getTitle());
        assertEquals(List.of("Joshua Bloch"), book.getAuthors());
    }

    @Test
    void searchBooks_returnsEmptyList_whenApiResponseHasNoItems() {
        // Arrange
        GoogleBooksApiResponse apiResponse = new GoogleBooksApiResponse();
        apiResponse.setItems(null);
        when(restTemplate.getForObject(anyString(), eq(GoogleBooksApiResponse.class)))
            .thenReturn(apiResponse);

        // Act
        var result = googleBooksService.searchBooks("anything");

        // Assert
        assertNotNull(result, "Result should not be null even if API returns null items");
        assertTrue(result.isEmpty(), "Result should be empty when API items is null");
    }

    @Test
    void searchBooks_throwsIllegalArgumentException_whenQueryIsNull() {
        assertThrows(IllegalArgumentException.class,
            () -> googleBooksService.searchBooks(null));
    }

    @Test
    void searchBooks_throwsIllegalArgumentException_whenQueryIsBlank() {
        assertThrows(IllegalArgumentException.class,
            () -> googleBooksService.searchBooks("  "));
    }

    @Test
    void searchBooks_throwsRuntimeException_whenHttpClientFails() {
        when(restTemplate.getForObject(anyString(), eq(GoogleBooksApiResponse.class)))
            .thenThrow(new RestClientException("Simulated network error"));

        assertThrows(RuntimeException.class,
            () -> googleBooksService.searchBooks("error"));
    }

    @Test
    void getBook_returnsBook_whenApiResponseIsValid() {
        // Arrange
        Volume apiVolume = new Volume();
        apiVolume.setId("bk123");
        VolumeInfo info = new VolumeInfo();
        info.setTitle("Clean Code");
        info.setAuthors(List.of("Robert C. Martin"));
        apiVolume.setVolumeInfo(info);

        when(restTemplate.getForObject(anyString(), eq(Volume.class)))
            .thenReturn(apiVolume);

        // Act
        var book = googleBooksService.getBook("bk123");

        // Assert
        assertNotNull(book, "Book should not be null for valid response");
        assertEquals("bk123", book.getId());
        assertEquals("Clean Code", book.getTitle());
        assertEquals(List.of("Robert C. Martin"), book.getAuthors());
    }

    @Test
    void getBook_throwsIllegalArgumentException_whenIdIsNull() {
        assertThrows(IllegalArgumentException.class,
            () -> googleBooksService.getBook(null));
    }

    @Test
    void getBook_throwsIllegalArgumentException_whenIdIsBlank() {
        assertThrows(IllegalArgumentException.class,
            () -> googleBooksService.getBook("   "));
    }

    @Test
    void getBook_throwsRuntimeException_whenBookNotFound() {
        when(restTemplate.getForObject(anyString(), eq(Volume.class)))
            .thenThrow(new HttpClientErrorException(HttpStatus.NOT_FOUND, "Not Found"));

        assertThrows(RuntimeException.class,
            () -> googleBooksService.getBook("unknown-id"));
    }
}