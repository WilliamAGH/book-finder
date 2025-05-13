package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service for tracking and managing recently viewed books.
 */
@Service
public class RecentlyViewedService {

    private final GoogleBooksService googleBooksService;
    
    // In-memory storage for recently viewed books (thread-safe implementation)
    private final LinkedList<Book> recentlyViewedBooks = new LinkedList<>();
    private static final int MAX_RECENT_BOOKS = 10;

    @Autowired
    public RecentlyViewedService(GoogleBooksService googleBooksService) {
        this.googleBooksService = googleBooksService;
    }

    /**
     * Add a book to the recently viewed list.
     * 
     * @param book the book to add
     */
    public synchronized void addToRecentlyViewed(Book book) {
        // Remove the book if it already exists to avoid duplicates
        recentlyViewedBooks.removeIf(b -> b.getId().equals(book.getId()));
        
        // Add the book to the beginning of the list
        recentlyViewedBooks.addFirst(book);
        
        // Trim the list if it exceeds the maximum size
        while (recentlyViewedBooks.size() > MAX_RECENT_BOOKS) {
            recentlyViewedBooks.removeLast();
        }
    }
    
    /**
     * Get the list of recently viewed books.
     * 
     * @return a copy of the recently viewed books list
     */
    public synchronized List<Book> getRecentlyViewedBooks() {
        if (recentlyViewedBooks.isEmpty()) {
            // If no books have been viewed yet, provide some default books
            try {
                List<Book> defaultBooks = googleBooksService.searchBooksAsyncReactive("java programming")
                                                            .blockOptional()
                                                            .orElse(Collections.emptyList());
                // Limit the number of default books after fetching
                defaultBooks = defaultBooks.stream()
                        .filter(book -> isValidCoverImage(book.getCoverImageUrl()))
                        .sorted((b1, b2) -> {
                            if(b1.getPublishedDate() == null && b2.getPublishedDate() == null) return 0;
                            if(b1.getPublishedDate() == null) return 1;
                            if(b2.getPublishedDate() == null) return -1;
                            return b2.getPublishedDate().compareTo(b1.getPublishedDate());
                        })
                        .limit(MAX_RECENT_BOOKS)
                        .collect(Collectors.toList());
                return defaultBooks;
            } catch (Exception e) {
                return Collections.emptyList();
            }
        }
        return new ArrayList<>(recentlyViewedBooks);
    }
    
    /**
     * Check if a book cover image is valid (not a placeholder)
     * 
     * @param imageUrl the image URL to check
     * @return true if the image is a valid cover, false otherwise
     */
    private boolean isValidCoverImage(String imageUrl) {
        if (imageUrl == null) {
            return false;
        }
        
        // Check if it's our placeholder image
        if (imageUrl.contains("placeholder-book-cover.svg")) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Clear the recently viewed books list.
     */
    public synchronized void clearRecentlyViewedBooks() {
        recentlyViewedBooks.clear();
    }
}