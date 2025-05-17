package com.williamcallahan.book_recommendation_engine.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookRepository;

@ExtendWith(MockitoExtension.class)
public class DuplicateBookServiceTest {

    @Mock
    private BookRepository bookRepository;

    @InjectMocks
    private DuplicateBookService duplicateBookService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testFindDuplicateBooks_returnsEmptyList_whenRepositoryIsEmpty() {
        when(bookRepository.findAll()).thenReturn(Collections.emptyList());

        List<Book> duplicates = duplicateBookService.findDuplicateBooks();

        assertNotNull(duplicates, "Duplicate list should not be null");
        assertTrue(duplicates.isEmpty(), "Expected no duplicates for empty repository");
    }

    @Test
    void testFindDuplicateBooks_returnsEmptyList_whenOnlyOneBook() {
        Book single = new Book("ISBN-1", "Single Book");
        when(bookRepository.findAll()).thenReturn(Collections.singletonList(single));

        List<Book> duplicates = duplicateBookService.findDuplicateBooks();

        assertNotNull(duplicates);
        assertTrue(duplicates.isEmpty(), "Expected no duplicates when only one book exists");
    }

    @Test
    void testFindDuplicateBooks_returnsPair_whenTwoBooksShareIsbn() {
        Book b1 = new Book("ISBN-2", "Duplicate Title");
        Book b2 = new Book("ISBN-2", "Duplicate Title");
        when(bookRepository.findAll()).thenReturn(Arrays.asList(b1, b2));

        List<Book> duplicates = duplicateBookService.findDuplicateBooks();

        assertEquals(2, duplicates.size(), "Should return both duplicate entries");
        assertTrue(duplicates.containsAll(Arrays.asList(b1, b2)));
    }

    @Test
    void testFindDuplicateBooks_returnsAll_whenThreeBooksShareIsbn() {
        Book b1 = new Book("ISBN-3", "Triplicate Title");
        Book b2 = new Book("ISBN-3", "Triplicate Title");
        Book b3 = new Book("ISBN-3", "Triplicate Title");
        when(bookRepository.findAll()).thenReturn(Arrays.asList(b1, b2, b3));

        List<Book> duplicates = duplicateBookService.findDuplicateBooks();

        assertEquals(3, duplicates.size(), "Should return all three entries with the same ISBN");
        assertTrue(duplicates.containsAll(Arrays.asList(b1, b2, b3)));
    }

    @Test
    void testFindDuplicateBooks_returnsAllGroups_whenMultipleGroupsExist() {
        Book a1 = new Book("ISBN-A", "Title A");
        Book a2 = new Book("ISBN-A", "Title A");
        Book b1 = new Book("ISBN-B", "Title B");
        Book b2 = new Book("ISBN-B", "Title B");
        Book unique = new Book("ISBN-C", "Unique Title");
        when(bookRepository.findAll()).thenReturn(Arrays.asList(a1, a2, b1, b2, unique));

        List<Book> duplicates = duplicateBookService.findDuplicateBooks();

        assertEquals(4, duplicates.size(), "Should return four entries from two duplicate groups");
        assertTrue(duplicates.containsAll(Arrays.asList(a1, a2, b1, b2)));
    }

    @Test
    void testFindDuplicateBooks_throwsRuntimeException_whenRepositoryFails() {
        when(bookRepository.findAll()).thenThrow(new RuntimeException("DB failure"));

        RuntimeException ex = assertThrows(RuntimeException.class, () ->
            duplicateBookService.findDuplicateBooks()
        );
        assertEquals("DB failure", ex.getMessage());
    }
}