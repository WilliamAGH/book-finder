package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class RecommendationService {
    private static final Logger logger = LoggerFactory.getLogger(RecommendationService.class);
    private static final int MAX_SEARCH_RESULTS = 40;
    private static final int DEFAULT_RECOMMENDATION_COUNT = 6;
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "the", "and", "for", "with", "are", "was", "from", "that", "this", "but", "not",
            "you", "your", "get", "will", "all", "any", "uses", "using", "learn", "what",
            "which", "its", "into", "then", "also"
    ));

    private final GoogleBooksService googleBooksService;

    @Autowired
    public RecommendationService(GoogleBooksService googleBooksService) {
        this.googleBooksService = googleBooksService;
    }

    /**
     * Get similar books recommendations based on the provided book ID
     * Uses a multi-faceted approach to find similar books
     * 
     * @param bookId The ID of the book to find recommendations for
     * @param count The number of recommendations to return (default 6)
     * @return List of recommended books
     */
    public List<Book> getSimilarBooks(String bookId, int count) {
        if (count <= 0) {
            count = DEFAULT_RECOMMENDATION_COUNT;
        }
        
        Book sourceBook = googleBooksService.getBookById(bookId).blockOptional().orElse(null);
        if (sourceBook == null) {
            logger.warn("Cannot get recommendations - source book with ID {} not found", bookId);
            return Collections.emptyList();
        }
        
        // Store results with similarity scores
        Map<String, ScoredBook> recommendationMap = new HashMap<>();
        
        // Step 1: Find books by the same author(s)
        findBooksByAuthors(sourceBook, recommendationMap);
        
        // Step 2: Find books in the same categories
        findBooksByCategories(sourceBook, recommendationMap);

        // Step 3: Find books by title/description keyword similarity
        findBooksByText(sourceBook, recommendationMap);
        
        // Step 3: Combine and sort by similarity score
        List<Book> recommendations = recommendationMap.values().stream()
                .sorted(Comparator.comparing(ScoredBook::getScore).reversed())
                .map(ScoredBook::getBook)
                .filter(book -> !book.getId().equals(bookId)) // Exclude the source book
                .limit(count)
                .collect(Collectors.toList());
        
        logger.info("Generated {} recommendations for book ID: {}", recommendations.size(), bookId);
        
        return recommendations;
    }
    
    /**
     * Find books by the same authors as the source book
     */
    private void findBooksByAuthors(Book sourceBook, Map<String, ScoredBook> recommendationMap) {
        if (sourceBook.getAuthors() == null || sourceBook.getAuthors().isEmpty()) {
            return;
        }
        
        for (String author : sourceBook.getAuthors()) {
            try {
                List<Book> authorBooks = googleBooksService.searchBooksByAuthor(author).blockOptional().orElse(Collections.emptyList());
                for (Book book : authorBooks) {
                    // Same author is a strong indicator of similarity
                    addOrUpdateRecommendation(recommendationMap, book, 4.0);
                }
            } catch (Exception e) {
                logger.warn("Error finding books by author {}: {}", author, e.getMessage());
            }
        }
    }
    
    /**
     * Find books with the same categories as the source book
     */
    private void findBooksByCategories(Book sourceBook, Map<String, ScoredBook> recommendationMap) {
        if (sourceBook.getCategories() == null || sourceBook.getCategories().isEmpty()) {
            return;
        }
        
        // Join categories with OR for broader results, but limited to primary categories
        List<String> mainCategories = sourceBook.getCategories().stream()
                .map(category -> category.split("\\s*/\\s*")[0]) // Get primary category before any '/'
                .distinct()
                .limit(3) // Limit to top 3 categories to avoid too broad searches
                .collect(Collectors.toList());
        
        if (mainCategories.isEmpty()) {
            return;
        }
        
        try {
            String categoryQueryString = "subject:" + String.join(" OR subject:", mainCategories);
            List<Book> categoryBooks = googleBooksService.searchBooksAsyncReactive(categoryQueryString)
                                                            .blockOptional()
                                                            .orElse(Collections.emptyList())
                                                            .stream()
                                                            .limit(MAX_SEARCH_RESULTS)
                                                            .collect(Collectors.toList());
            
            for (Book book : categoryBooks) {
                // Calculate category overlap score
                double categoryScore = calculateCategoryOverlapScore(sourceBook, book);
                addOrUpdateRecommendation(recommendationMap, book, categoryScore);
            }
        } catch (Exception e) {
            logger.warn("Error finding books by categories: {}", e.getMessage());
        }
    }
    
    /**
     * Calculate a score based on category overlap between two books
     */
    private double calculateCategoryOverlapScore(Book sourceBook, Book candidateBook) {
        if (sourceBook.getCategories() == null || candidateBook.getCategories() == null ||
            sourceBook.getCategories().isEmpty() || candidateBook.getCategories().isEmpty()) {
            return 0.5; // Some basic score if we can't calculate
        }
        
        Set<String> sourceCategories = normalizeCategories(sourceBook.getCategories());
        Set<String> candidateCategories = normalizeCategories(candidateBook.getCategories());
        
        // Find intersecting categories
        Set<String> intersection = new HashSet<>(sourceCategories);
        intersection.retainAll(candidateCategories);
        
        // More overlapping categories = higher score
        double overlapRatio = (double) intersection.size() / 
                Math.max(1, Math.min(sourceCategories.size(), candidateCategories.size()));
        
        // Scale to range 1.0 - 3.0
        return 1.0 + (overlapRatio * 2.0);
    }
    
    /**
     * Normalize categories for better matching
     */
    private Set<String> normalizeCategories(List<String> categories) {
        Set<String> normalized = new HashSet<>();
        for (String category : categories) {
            // Split compound categories and add each part
            for (String part : category.split("\\s*/\\s*")) {
                normalized.add(part.toLowerCase().trim());
            }
        }
        return normalized;
    }
    
    /**
     * Add or update a book in the recommendation map
     */
    private void addOrUpdateRecommendation(Map<String, ScoredBook> recommendationMap, Book book, double score) {
        ScoredBook existing = recommendationMap.get(book.getId());
        if (existing != null) {
            // We've seen this book before, increase its score
            existing.setScore(existing.getScore() + score);
        } else {
            // New recommendation
            recommendationMap.put(book.getId(), new ScoredBook(book, score));
        }
    }
    
    /**
     * Find books by title/description keyword similarity
     */
    private void findBooksByText(Book sourceBook, Map<String, ScoredBook> recommendationMap) {
        if ((sourceBook.getTitle() == null || sourceBook.getTitle().isEmpty())
                && (sourceBook.getDescription() == null || sourceBook.getDescription().isEmpty())) {
            return;
        }
        String combinedText = (sourceBook.getTitle() + " " + sourceBook.getDescription()).toLowerCase();
        String[] tokens = combinedText.split("[^a-z0-9]+");
        Set<String> keywords = new LinkedHashSet<>();
        for (String token : tokens) {
            if (token.length() > 2 && !STOP_WORDS.contains(token)) {
                keywords.add(token);
                if (keywords.size() >= 10) {
                    break;
                }
            }
        }
        if (keywords.isEmpty()) {
            return;
        }
        String query = String.join(" ", keywords);
        try {
            List<Book> textBooks = googleBooksService.searchBooksAsyncReactive(query)
                                                        .blockOptional()
                                                        .orElse(Collections.emptyList())
                                                        .stream()
                                                        .limit(MAX_SEARCH_RESULTS)
                                                        .collect(Collectors.toList());
            for (Book book : textBooks) {
                String candidateText = ((book.getTitle() != null ? book.getTitle() : "") + " "
                        + (book.getDescription() != null ? book.getDescription() : "")).toLowerCase();
                int matchCount = 0;
                for (String kw : keywords) {
                    if (candidateText.contains(kw)) {
                        matchCount++;
                    }
                }
                if (matchCount > 0) {
                    double score = 2.0 * matchCount;
                    addOrUpdateRecommendation(recommendationMap, book, score);
                }
            }
        } catch (Exception e) {
            logger.warn("Error finding books by text keywords: {}", e.getMessage());
        }
    }

    /**
     * Helper class to track books with their similarity scores
     */
    private static class ScoredBook {
        private final Book book;
        private double score;
        
        public ScoredBook(Book book, double score) {
            this.book = book;
            this.score = score;
        }
        
        public Book getBook() {
            return book;
        }
        
        public double getScore() {
            return score;
        }
        
        public void setScore(double score) {
            this.score = score;
        }
    }
}
