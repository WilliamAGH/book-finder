/**
 * Test suite for BookCacheFacadeService
 * This class tests the caching and retrieval of books using the BookCacheFacadeService
 * 
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import com.williamcallahan.book_recommendation_engine.service.cache.BookReactiveCacheService;
import com.williamcallahan.book_recommendation_engine.service.cache.BookSyncCacheService;
import com.williamcallahan.book_recommendation_engine.service.similarity.BookSimilarityService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BookCacheFacadeServiceTest {

    @Mock
    private BookSyncCacheService bookSyncCacheService;
    @Mock
    private BookReactiveCacheService bookReactiveCacheService;
    @Mock
    private BookSimilarityService bookSimilarityService;
    @Mock
    private CacheManager cacheManager;
    @Mock
    private RedisCacheService redisCacheService;
    @Mock
    private CachedBookRepository cachedBookRepository; // Optional

    @Mock
    private Cache booksSpringCache; // Mock for the "books" Spring Cache itself

    @InjectMocks
    private BookCacheFacadeService bookCacheFacadeService;

    private Book testBook;
    private final String testBookId = "123";

    @BeforeEach
    void setUp() {
        testBook = createTestBook(testBookId, "Effective Java", "Joshua Bloch");
        // Setup for cacheEnabled field in the facade
        ReflectionTestUtils.setField(bookCacheFacadeService, "cacheEnabled", true);
        // Mock the CacheManager to return our mocked "books" cache
        when(cacheManager.getCache("books")).thenReturn(booksSpringCache);
    }

    private Book createTestBook(String id, String title, String author) {
        Book book = new Book();
        book.setId(id);
        book.setTitle(title);
        if (author != null) {
            book.setAuthors(List.of(author));
        } else {
            book.setAuthors(List.of("Unknown Author"));
        }
        book.setDescription("Test description for " + title);
        return book;
    }

    @Test
    @DisplayName("getBookById (facade) delegates to BookSyncCacheService when Spring Cache misses")
    void getBookById_facadeDelegatesToSyncService_onSpringCacheMiss() {
        when(booksSpringCache.get(testBookId, Book.class)).thenReturn(null); // Spring cache miss
        when(bookSyncCacheService.getBookById(testBookId)).thenReturn(testBook);

        Book result = bookCacheFacadeService.getBookById(testBookId);

        assertNotNull(result);
        assertEquals(testBookId, result.getId());
        verify(bookSyncCacheService).getBookById(testBookId);
    }
    
    @Test
    @DisplayName("getCachedBook returns book from Spring Cache")
    void getCachedBook_returnsFromSpringCache() {
        when(booksSpringCache.get(testBookId, Book.class)).thenReturn(testBook);
        Optional<Book> result = bookCacheFacadeService.getCachedBook(testBookId);
        assertTrue(result.isPresent());
        assertEquals(testBookId, result.get().getId());
        verify(booksSpringCache).get(testBookId, Book.class);
        verifyNoInteractions(redisCacheService, cachedBookRepository);
    }

    @Test
    @DisplayName("getCachedBook returns book from Redis if Spring Cache misses")
    void getCachedBook_returnsFromRedis_ifSpringMiss() {
        when(booksSpringCache.get(testBookId, Book.class)).thenReturn(null);
        when(redisCacheService.getBookById(testBookId)).thenReturn(Optional.of(testBook));
        Optional<Book> result = bookCacheFacadeService.getCachedBook(testBookId);
        assertTrue(result.isPresent());
        assertEquals(testBookId, result.get().getId());
        verify(redisCacheService).getBookById(testBookId);
        verifyNoInteractions(cachedBookRepository);
    }
    
    @Test
    @DisplayName("getCachedBook returns book from DB if Spring Cache and Redis miss")
    void getCachedBook_returnsFromDb_ifSpringAndRedisMiss() {
        ReflectionTestUtils.setField(bookCacheFacadeService, "cacheEnabled", true);
        when(booksSpringCache.get(testBookId, Book.class)).thenReturn(null);
        when(redisCacheService.getBookById(testBookId)).thenReturn(Optional.empty());
        
        CachedBook cachedDbBook = new CachedBook(); // Assume conversion
        cachedDbBook.setId(testBookId); // Use String ID
        cachedDbBook.setGoogleBooksId(testBookId);
        cachedDbBook.setTitle(testBook.getTitle());

        when(cachedBookRepository.findByGoogleBooksId(testBookId)).thenReturn(Optional.of(cachedDbBook));
        
        Optional<Book> result = bookCacheFacadeService.getCachedBook(testBookId);
        assertTrue(result.isPresent());
        assertEquals(testBookId, result.get().getId());
        verify(cachedBookRepository).findByGoogleBooksId(testBookId);
    }


    @Test
    @DisplayName("getCachedBook returns empty Optional when book not in any cache")
    void getCachedBook_ShouldReturnEmpty_WhenAbsentInAllCaches() {
        when(booksSpringCache.get(testBookId, Book.class)).thenReturn(null);
        when(redisCacheService.getBookById(testBookId)).thenReturn(Optional.empty());
        when(cachedBookRepository.findByGoogleBooksId(testBookId)).thenReturn(Optional.empty());
        
        Optional<Book> actual = bookCacheFacadeService.getCachedBook(testBookId);
        
        assertFalse(actual.isPresent());
    }

    @Test
    @DisplayName("cacheBook delegates to sync and reactive services")
    void cacheBook_delegatesToSubServices() {
        when(bookReactiveCacheService.cacheBookReactive(testBook)).thenReturn(Mono.empty());
        
        bookCacheFacadeService.cacheBook(testBook);
        
        verify(booksSpringCache).put(testBookId, testBook);
        verify(bookReactiveCacheService).cacheBookReactive(testBook);
    }

    @Test
    @DisplayName("evictBook delegates to sync, redis, and db services")
    void evictBook_delegatesToSubServices() {
        bookCacheFacadeService.evictBook(testBookId);
        
        verify(booksSpringCache).evictIfPresent(testBookId);
        verify(redisCacheService).evictBook(testBookId);
        verify(cachedBookRepository).findByGoogleBooksId(testBookId); // and then deleteById if present
    }

    @Test
    @DisplayName("clearAll delegates to sync, spring cache, and db repo")
    void clearAll_delegatesToSubServices() {
        bookCacheFacadeService.clearAll();
        
        verify(booksSpringCache).clear();
        verify(cachedBookRepository).deleteAll();
        verifyNoInteractions(redisCacheService); // Redis clearAll is not called by facade's clearAll
    }
    
    @Test
    @DisplayName("updateBook calls evictBook and cacheBook")
    void updateBook_callsEvictAndCache() {
        // Use spy to verify method calls on the same instance
        BookCacheFacadeService spyFacade = spy(bookCacheFacadeService);
        doNothing().when(spyFacade).evictBook(testBookId);
        doNothing().when(spyFacade).cacheBook(testBook);

        spyFacade.updateBook(testBook);

        verify(spyFacade).evictBook(testBookId);
        verify(spyFacade).cacheBook(testBook);
    }

    @Test
    @DisplayName("removeBook calls evictBook")
    void removeBook_callsEvictBook() {
        BookCacheFacadeService spyFacade = spy(bookCacheFacadeService);
        doNothing().when(spyFacade).evictBook(testBookId);
        
        spyFacade.removeBook(testBookId);
        
        verify(spyFacade).evictBook(testBookId);
    }

    @Test
    @DisplayName("isBookInCache checks relevant caches")
    void isBookInCache_checksRelevantLayers() {
        when(booksSpringCache.get(testBookId)).thenReturn(null);
        when(redisCacheService.getBookById(testBookId)).thenReturn(Optional.empty());
        when(cachedBookRepository.findByGoogleBooksId(testBookId)).thenReturn(Optional.empty());
        assertFalse(bookCacheFacadeService.isBookInCache(testBookId));

        when(booksSpringCache.get(testBookId)).thenReturn(new Cache.ValueWrapper() {
            @Override public Object get() { return testBook; }
        });
        assertTrue(bookCacheFacadeService.isBookInCache(testBookId));
        when(booksSpringCache.get(testBookId)).thenReturn(null); // reset

        when(redisCacheService.getBookById(testBookId)).thenReturn(Optional.of(testBook));
        assertTrue(bookCacheFacadeService.isBookInCache(testBookId));
        when(redisCacheService.getBookById(testBookId)).thenReturn(Optional.empty());// reset
        
        CachedBook cachedDbBook = new CachedBook();
        when(cachedBookRepository.findByGoogleBooksId(testBookId)).thenReturn(Optional.of(cachedDbBook));
        assertTrue(bookCacheFacadeService.isBookInCache(testBookId));
    }
    
    @Test
    @DisplayName("getAllCachedBookIds delegates to repository when cache enabled")
    void getAllCachedBookIds_delegatesToRepository_whenCacheEnabled() {
        Set<String> expectedIds = Set.of("id1", "id2");
        when(cachedBookRepository.findAllDistinctGoogleBooksIds()).thenReturn(expectedIds);
        
        Set<String> actualIds = bookCacheFacadeService.getAllCachedBookIds();
        
        assertEquals(expectedIds, actualIds);
        verify(cachedBookRepository).findAllDistinctGoogleBooksIds();
    }

    @Test
    @DisplayName("getAllCachedBookIds returns empty set when repository is null")
    void getAllCachedBookIds_returnsEmptySet_whenRepositoryNull() {
        ReflectionTestUtils.setField(bookCacheFacadeService, "cachedBookRepository", null);
        ReflectionTestUtils.setField(bookCacheFacadeService, "cacheEnabled", false); // Also ensure cacheEnabled reflects this
        
        Set<String> actualIds = bookCacheFacadeService.getAllCachedBookIds();
        
        assertTrue(actualIds.isEmpty());
    }
}
