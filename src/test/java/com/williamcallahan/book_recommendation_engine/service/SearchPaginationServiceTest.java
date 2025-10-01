package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SearchPaginationServiceTest {

    @Mock
    private TieredBookSearchService tieredBookSearchService;

    @Mock
    private BookDataOrchestrator bookDataOrchestrator;

    @Mock
    private BookSearchService bookSearchService;

    @Mock
    private BookQueryRepository bookQueryRepository;

    private SearchPaginationService service;

    @BeforeEach
    void setUp() {
        service = new SearchPaginationService(
            tieredBookSearchService,
            bookDataOrchestrator,
            bookSearchService,
            bookQueryRepository
        );
    }

    @Test
    @DisplayName("search() deduplicates results, preserves Postgres ordering, sorts external items, and schedules persistence")
    void searchDeduplicatesAndPersistsExternal() {
        Book postgresOne = buildBook("uuid-1", "Postgres One", true);
        Book postgresTwo = buildBook("uuid-2", "Postgres Two", true);
        Book externalAlpha = buildBook("ext-alpha", "Alpha Title", false);
        Book externalZeta = buildBook("ext-zeta", "Zeta Title", false);
        Book externalDuplicate = buildBook("ext-zeta", "Zeta Title", false);

        when(tieredBookSearchService.streamSearch(eq("java"), isNull(), eq(24), eq("newest"), eq(false)))
            .thenReturn(Flux.fromIterable(List.of(
                postgresOne,
                externalZeta,
                postgresTwo,
                externalAlpha,
                externalDuplicate
            )));
        doNothing().when(bookDataOrchestrator).persistBooksAsync(any(), eq("SEARCH"));

        SearchPaginationService.SearchRequest request = new SearchPaginationService.SearchRequest("java", 0, 12, "newest");
        SearchPaginationService.SearchPage page = service.search(request).block();

        assertThat(page).isNotNull();
        assertThat(page.pageItems())
            .extracting(Book::getId)
            .containsExactly("uuid-1", "uuid-2", "ext-alpha", "ext-zeta");
        assertThat(page.totalUnique()).isEqualTo(4);
        assertThat(page.hasMore()).isFalse();
        assertThat(page.prefetchedCount()).isZero();
        assertThat(page.nextStartIndex()).isZero();

        ArgumentCaptor<List<Book>> persistCaptor = ArgumentCaptor.forClass(List.class);
        verify(bookDataOrchestrator).persistBooksAsync(persistCaptor.capture(), eq("SEARCH"));
        assertThat(persistCaptor.getValue())
            .extracting(Book::getId)
            .containsExactlyInAnyOrder("ext-alpha", "ext-zeta");
    }

    private Book buildBook(String id, String title, boolean inPostgres) {
        Book book = new Book();
        book.setId(id);
        book.setTitle(title);
        book.setInPostgres(inPostgres);
        if (inPostgres) {
            book.setRetrievedFrom("POSTGRES");
        } else {
            book.setRetrievedFrom("EXTERNAL_API");
        }
        return book;
    }
}
