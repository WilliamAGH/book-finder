package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.dto.BookListItem;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
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

    @Test
    @DisplayName("search() slices with start offsets and computes prefetch metadata")
    void searchRespectsOffsetsAndPrefetch() {
        List<Book> dataset = new ArrayList<>();
        IntStream.range(0, 14)
            .forEach(i -> dataset.add(buildBook(String.format("pg-%02d", i), String.format("Postgres %02d", i), true)));
        IntStream.range(0, 15)
            .forEach(i -> dataset.add(buildBook(String.format("ext-%02d", i), String.format("External %02d", i), false)));

        when(tieredBookSearchService.streamSearch(eq("java"), isNull(), eq(36), eq("newest"), eq(false)))
            .thenAnswer(invocation -> Flux.fromIterable(dataset));

        SearchPaginationService.SearchRequest request = new SearchPaginationService.SearchRequest("java", 12, 12, "newest");
        SearchPaginationService.SearchPage page = service.search(request).block();

        assertThat(page).isNotNull();
        assertThat(page.pageItems())
            .extracting(Book::getId)
            .containsExactly(
                "pg-12",
                "pg-13",
                "ext-00",
                "ext-01",
                "ext-02",
                "ext-03",
                "ext-04",
                "ext-05",
                "ext-06",
                "ext-07",
                "ext-08",
                "ext-09"
            );
        assertThat(page.hasMore()).isTrue();
        assertThat(page.prefetchedCount()).isEqualTo(5);
        assertThat(page.nextStartIndex()).isEqualTo(24);
    }

    @Test
    @DisplayName("search() falls back to Postgres results when tiered service unavailable")
    void searchPostgresOnlyHonoursHighStartIndexes() {
        SearchPaginationService postgresOnlyService = new SearchPaginationService(
            null,
            bookDataOrchestrator,
            bookSearchService,
            bookQueryRepository
        );

        UUID idOne = UUID.randomUUID();
        UUID idTwo = UUID.randomUUID();
        UUID idThree = UUID.randomUUID();

        List<BookSearchService.SearchResult> results = List.of(
            new BookSearchService.SearchResult(idOne, 0.91, "TSVECTOR"),
            new BookSearchService.SearchResult(idTwo, 0.87, "TSVECTOR"),
            new BookSearchService.SearchResult(idThree, 0.72, "TSVECTOR")
        );

        when(bookSearchService.searchBooks(eq("miss"), eq(34))).thenReturn(results);
        when(bookQueryRepository.fetchBookListItems(anyList())).thenReturn(List.of(
            buildListItem(idOne, "First"),
            buildListItem(idTwo, "Second"),
            buildListItem(idThree, "Third")
        ));

        SearchPaginationService.SearchRequest request = new SearchPaginationService.SearchRequest("miss", 10, 12, "newest");
        SearchPaginationService.SearchPage page = postgresOnlyService.search(request).block();

        assertThat(page).isNotNull();
        assertThat(page.pageItems()).isEmpty();
        assertThat(page.uniqueResults()).hasSize(3);
        assertThat(page.hasMore()).isFalse();
        assertThat(page.prefetchedCount()).isZero();
        assertThat(page.nextStartIndex()).isEqualTo(10);
        verify(bookDataOrchestrator, never()).persistBooksAsync(any(), anyString());
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

    private BookListItem buildListItem(UUID id, String title) {
        Map<String, Object> tags = new HashMap<>();
        tags.put("nytBestseller", Map.of("rank", 1));
        return new BookListItem(
            id.toString(),
            "slug-" + id,
            title,
            title + " description",
            List.of("Fixture Author"),
            List.of("Fixture Category"),
            "https://example.test/" + id + ".jpg",
            4.5,
            100,
            tags
        );
    }
}
