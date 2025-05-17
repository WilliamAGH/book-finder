package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BookRecommendationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(HomeController.class)
class HomeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private BookRecommendationService recommendationService;

    @Test
    void shouldReturnHomeViewWithRecommendationsWhenServiceSucceeds() throws Exception {
        // Arrange
        var books = List.of(new Book("Effective Java", "Joshua Bloch"));
        when(recommendationService.getRecommendations()).thenReturn(books);

        // Act & Assert
        mockMvc.perform(get("/"))
            .andExpect(status().isOk())
            .andExpect(view().name("home"))
            .andExpect(model().attributeExists("recommendations"))
            .andExpect(model().attribute("recommendations", hasSize(1)))
            .andExpect(model().attribute("recommendations", contains(books.get(0))));
    }

    @Test
    void shouldShowEmptyRecommendationsWhenServiceReturnsEmptyList() throws Exception {
        // Arrange
        when(recommendationService.getRecommendations()).thenReturn(emptyList());

        // Act & Assert
        mockMvc.perform(get("/"))
            .andExpect(status().isOk())
            .andExpect(view().name("home"))
            .andExpect(model().attribute("recommendations", hasSize(0)));
    }

    @Test
    void shouldReturnServerErrorWhenServiceThrowsException() throws Exception {
        // Arrange
        when(recommendationService.getRecommendations())
            .thenThrow(new RuntimeException("simulated failure"));

        // Act & Assert
        mockMvc.perform(get("/"))
            .andExpect(status().is5xxServerError());
    }
}