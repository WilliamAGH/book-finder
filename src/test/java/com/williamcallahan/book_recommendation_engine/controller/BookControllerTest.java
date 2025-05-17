package com.williamcallahan.book_recommendation_engine.controller;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.AfterEach;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Optional;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.hamcrest.Matchers.*;
import com.williamcallahan.book_recommendation_engine.controller.BookController;
import com.williamcallahan.book_recommendation_engine.service.BookService;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.exception.ResourceNotFoundException;

@WebMvcTest(BookController.class)
class BookControllerTest {
  @Autowired
  private MockMvc mockMvc;

  @Autowired
  private ObjectMapper objectMapper;

  @MockBean
  private BookService bookService;

  @AfterEach
  void tearDown() {
    reset(bookService);
  }

  @Test
  @DisplayName("GET /books - empty list returns 200 and []")
  void getAllBooks_emptyList_returnsEmptyArray() throws Exception {
    when(bookService.getAllBooks()).thenReturn(List.of());
    mockMvc.perform(get("/books"))
      .andExpect(status().isOk())
      .andExpect(content().contentType(MediaType.APPLICATION_JSON))
      .andExpect(jsonPath("$", hasSize(0)));
  }

  @Test
  @DisplayName("GET /books - non-empty list returns 200 and array of books")
  void getAllBooks_nonEmptyList_returnsArray() throws Exception {
    Book book = new Book(1L, "Effective Java", "Joshua Bloch");
    when(bookService.getAllBooks()).thenReturn(List.of(book));
    mockMvc.perform(get("/books"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$", hasSize(1)))
      .andExpect(jsonPath("$[0].id").value(1))
      .andExpect(jsonPath("$[0].title").value("Effective Java"))
      .andExpect(jsonPath("$[0].author").value("Joshua Bloch"));
  }

  @Test
  @DisplayName("GET /books/{id} - existing id returns 200 and book JSON")
  void getBookById_found_returnsBook() throws Exception {
    Book book = new Book(1L, "Domain-Driven Design", "Eric Evans");
    when(bookService.getBookById(1L)).thenReturn(Optional.of(book));
    mockMvc.perform(get("/books/1"))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.id").value(1))
      .andExpect(jsonPath("$.title").value("Domain-Driven Design"))
      .andExpect(jsonPath("$.author").value("Eric Evans"));
  }

  @Test
  @DisplayName("GET /books/{id} - non-existent id returns 404")
  void getBookById_notFound_returns404() throws Exception {
    when(bookService.getBookById(99L)).thenReturn(Optional.empty());
    mockMvc.perform(get("/books/99"))
      .andExpect(status().isNotFound());
  }

  @Test
  @DisplayName("POST /books - valid input returns 201 and created book")
  void createBook_validInput_returnsCreated() throws Exception {
    Book input = new Book(null, "Clean Code", "Robert C. Martin");
    Book saved = new Book(1L, "Clean Code", "Robert C. Martin");
    when(bookService.createBook(any(Book.class))).thenReturn(saved);

    mockMvc.perform(post("/books")
        .contentType(MediaType.APPLICATION_JSON)
        .content(objectMapper.writeValueAsString(input)))
      .andExpect(status().isCreated())
      .andExpect(header().string("Location", containsString("/books/1")))
      .andExpect(jsonPath("$.id").value(1))
      .andExpect(jsonPath("$.title").value("Clean Code"))
      .andExpect(jsonPath("$.author").value("Robert C. Martin"));
  }

  @Test
  @DisplayName("POST /books - invalid input returns 400")
  void createBook_invalidInput_returnsBadRequest() throws Exception {
    Book invalid = new Book(null, "", "Author");
    mockMvc.perform(post("/books")
        .contentType(MediaType.APPLICATION_JSON)
        .content(objectMapper.writeValueAsString(invalid)))
      .andExpect(status().isBadRequest());
  }

  @Test
  @DisplayName("PUT /books/{id} - existing id returns 200 and updated book")
  void updateBook_found_returnsUpdated() throws Exception {
    Book update = new Book(null, "Refactoring", "Martin Fowler");
    Book updated = new Book(1L, "Refactoring", "Martin Fowler");
    when(bookService.updateBook(eq(1L), any(Book.class))).thenReturn(Optional.of(updated));

    mockMvc.perform(put("/books/1")
        .contentType(MediaType.APPLICATION_JSON)
        .content(objectMapper.writeValueAsString(update)))
      .andExpect(status().isOk())
      .andExpect(jsonPath("$.title").value("Refactoring"))
      .andExpect(jsonPath("$.author").value("Martin Fowler"));
  }

  @Test
  @DisplayName("PUT /books/{id} - non-existent id returns 404")
  void updateBook_notFound_returns404() throws Exception {
    when(bookService.updateBook(eq(99L), any(Book.class))).thenReturn(Optional.empty());
    mockMvc.perform(put("/books/99")
        .contentType(MediaType.APPLICATION_JSON)
        .content(objectMapper.writeValueAsString(new Book())))
      .andExpect(status().isNotFound());
  }

  @Test
  @DisplayName("DELETE /books/{id} - existing id returns 204")
  void deleteBook_found_returnsNoContent() throws Exception {
    doNothing().when(bookService).deleteBook(1L);
    mockMvc.perform(delete("/books/1"))
      .andExpect(status().isNoContent());
  }

  @Test
  @DisplayName("DELETE /books/{id} - non-existent id returns 404")
  void deleteBook_notFound_returns404() throws Exception {
    doThrow(new ResourceNotFoundException("Book not found"))
      .when(bookService).deleteBook(99L);

    mockMvc.perform(delete("/books/99"))
      .andExpect(status().isNotFound());
  }
}