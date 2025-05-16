package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Service for generating book recommendations based on various similarity criteria
 *
 * @author William Callahan
 *
 * - Uses multi-faceted approach combining author, category, and text-based matching
 * - Implements scoring algorithm to rank recommendations by relevance
 * - Supports language-aware filtering to match source book language
 * - Provides reactive API for non-blocking recommendation generation
 * - Handles category normalization for better cross-book matching
 * - Implements keyword extraction with stop word filtering
 */
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

    private final GoogleBooksService googleBooksService; // Retain for other searches if needed, or remove if all book fetching goes via BookCacheService
    private final BookCacheService bookCacheService;

    /**
     * Constructs the RecommendationService with required dependencies
     * 
     * @param googleBooksService Service for searching books in Google Books API
     * @param bookCacheService Service for retrieving cached book information
     * 
     * @implNote Uses both GoogleBooksService for searches and BookCacheService for book details
     */
    @Autowired
    public RecommendationService(GoogleBooksService googleBooksService, BookCacheService bookCacheService) {
        this.googleBooksService = googleBooksService;
        this.bookCacheService = bookCacheService;
    }

    /**
     * Generates recommendations for books similar to the specified book
     * 
     * @param bookId The Google Books ID to find recommendations for
     * @param finalCount The number of recommendations to return (defaults to 6 if ≤ 0)
     * @return Mono emitting list of recommended books in descending order of relevance
     * 
     * @implNote Combines three recommendation strategies (author, category, text matching)
     * Scores and ranks results to provide the most relevant recommendations
     * Filters by language to match the source book when language information is available
     */
    public Mono<List<Book>> getSimilarBooks(String bookId, int finalCount) {
        final int effectiveCount = (finalCount <= 0) ? DEFAULT_RECOMMENDATION_COUNT : finalCount;

        // Use BookCacheService to get the source book
        return bookCacheService.getBookByIdReactive(bookId) 
            .flatMap(sourceBook -> {
                if (sourceBook == null) {
                    logger.warn("Cannot get recommendations - source book with ID {} not found via BookCacheService", bookId);
                    return Mono.just(Collections.<Book>emptyList());
                }

                Flux<ScoredBook> authorsFlux = findBooksByAuthorsReactive(sourceBook);
                Flux<ScoredBook> categoriesFlux = findBooksByCategoriesReactive(sourceBook);
                Flux<ScoredBook> textFlux = findBooksByTextReactive(sourceBook);

                return Flux.merge(authorsFlux, categoriesFlux, textFlux)
                    .collect(Collectors.toMap(
                        scoredBook -> scoredBook.getBook().getId(), // Key: book ID
                        scoredBook -> scoredBook,                  // Value: ScoredBook itself
                        (sb1, sb2) -> {                             // Merge function for duplicates
                            sb1.setScore(sb1.getScore() + sb2.getScore()); // Accumulate scores
                            return sb1;
                        },
                        HashMap::new // Supplier for the map
                    ))
                    .map(recommendationMap -> {
                        String sourceLang = sourceBook.getLanguage(); // Get language of the source book
                        boolean filterByLanguage = sourceLang != null && !sourceLang.isEmpty();

                        List<Book> recommendations = recommendationMap.values().stream()
                            .sorted(Comparator.comparing(ScoredBook::getScore).reversed())
                            .map(ScoredBook::getBook)
                            .filter(book -> !book.getId().equals(sourceBook.getId())) // Exclude the source book
                            .filter(recommendedBook -> { // Add language filter
                                if (!filterByLanguage) {
                                    return true; // Don't filter if source language is unknown
                                }
                                String recommendedLang = recommendedBook.getLanguage();
                                // Use Objects.equals for null-safe comparison
                                return Objects.equals(sourceLang, recommendedLang);
                            })
                            .limit(effectiveCount)
                            .collect(Collectors.toList());
                        logger.info("Generated {} recommendations for book ID: {} (Language filter active: {})", recommendations.size(), sourceBook.getId(), filterByLanguage);
                        return recommendations;
                    });
            })
            .switchIfEmpty(Mono.<List<Book>>defer(() -> {
                logger.warn("Source book with ID {} not found, returning empty recommendations.", bookId);
                return Mono.just(Collections.<Book>emptyList());
            }));
    }
    
    /**
     * Finds books by the same authors as the source book
     * 
     * @param sourceBook The source book to find author matches for
     * @return Flux emitting scored books by the same authors
     * 
     * @implNote Assigns high score (4.0) to author matches as they are strong indicators
     * Returns empty flux if source book has no authors
     */
    private Flux<ScoredBook> findBooksByAuthorsReactive(Book sourceBook) {
        if (sourceBook.getAuthors() == null || sourceBook.getAuthors().isEmpty()) {
            return Flux.empty();
        }
        String langCode = sourceBook.getLanguage(); // Get language from source book
        
        return Flux.fromIterable(sourceBook.getAuthors())
            .flatMap(author -> googleBooksService.searchBooksByAuthor(author, langCode) // Pass langCode
                .flatMapMany(Flux::fromIterable)
                .map(book -> new ScoredBook(book, 4.0)) // Same author is a strong indicator
                .onErrorResume(e -> {
                    logger.warn("Error finding books by author {}: {}", author, e.getMessage());
                    return Flux.empty();
                })
            );
    }
    
    /**
     * Finds books in the same categories as the source book
     * 
     * @param sourceBook The source book to find category matches for
     * @return Flux emitting scored books in matching categories
     * 
     * @implNote Extracts main categories and builds optimized category search query
     * Score varies based on category overlap calculation
     */
    private Flux<ScoredBook> findBooksByCategoriesReactive(Book sourceBook) {
        if (sourceBook.getCategories() == null || sourceBook.getCategories().isEmpty()) {
            return Flux.empty();
        }

        List<String> mainCategories = sourceBook.getCategories().stream()
            .map(category -> category.split("\\s*/\\s*")[0])
            .distinct()
            .limit(3)
            .collect(Collectors.toList());

        if (mainCategories.isEmpty()) {
            return Flux.empty();
        }

        String categoryQueryString = "subject:" + String.join(" OR subject:", mainCategories);
        String langCode = sourceBook.getLanguage(); // Get language from source book
        
        return googleBooksService.searchBooksAsyncReactive(categoryQueryString, langCode) // Pass langCode
            .flatMapMany(Flux::fromIterable)
            .take(MAX_SEARCH_RESULTS) // Limit results after fetching the list from Mono<List<Book>>
            .map(book -> {
                double categoryScore = calculateCategoryOverlapScore(sourceBook, book);
                return new ScoredBook(book, categoryScore);
            })
            .onErrorResume(e -> {
                logger.warn("Error finding books by categories '{}': {}", categoryQueryString, e.getMessage());
                return Flux.empty();
            });
    }
    
    /**
     * Calculates similarity score based on category overlap between books
     * 
     * @param sourceBook The source book for comparison
     * @param candidateBook The candidate book being evaluated
     * @return Score between 1.0 and 3.0 reflecting category similarity
     * 
     * @implNote Uses normalized categories for more accurate matching
     * Score is proportional to the percentage of overlapping categories
     */
    private double calculateCategoryOverlapScore(Book sourceBook, Book candidateBook) {
        if (sourceBook.getCategories() == null || candidateBook.getCategories() == null ||
            sourceBook.getCategories().isEmpty() || candidateBook.getCategories().isEmpty()) {
            return 0.5; // Some basic score if it can't calculate
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
     * Normalizes book categories for consistent comparison
     * 
     * @param categories List of categories to normalize
     * @return Set of normalized category strings
     * 
     * @implNote Splits compound categories on slashes
     * Converts to lowercase and trims whitespace
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
     * Finds books with similar keywords in title and description
     * 
     * @param sourceBook The source book to find keyword matches for
     * @return Flux emitting scored books with matching keywords
     * 
     * @implNote Extracts significant keywords from title and description
     * Filters out common stop words and short tokens
     * Score based on quantity of matching keywords
     */
    private Flux<ScoredBook> findBooksByTextReactive(Book sourceBook) {
        if ((sourceBook.getTitle() == null || sourceBook.getTitle().isEmpty()) &&
            (sourceBook.getDescription() == null || sourceBook.getDescription().isEmpty())) {
            return Flux.empty();
        }

        String combinedText = (sourceBook.getTitle() + " " + sourceBook.getDescription()).toLowerCase();
        String[] tokens = combinedText.split("[^a-z0-9]+");
        Set<String> keywords = new LinkedHashSet<>();
        for (String token : tokens) {
            if (token.length() > 2 && !STOP_WORDS.contains(token)) {
                keywords.add(token);
                if (keywords.size() >= 10) break;
            }
        }

        if (keywords.isEmpty()) {
            return Flux.empty();
        }

        String query = String.join(" ", keywords);
        String langCode = sourceBook.getLanguage(); // Get language from source book

        return googleBooksService.searchBooksAsyncReactive(query, langCode) // Pass langCode
            .flatMapMany(Flux::fromIterable)
            .take(MAX_SEARCH_RESULTS)
            .flatMap(book -> {
                String candidateText = ((book.getTitle() != null ? book.getTitle() : "") + " " +
                                      (book.getDescription() != null ? book.getDescription() : "")).toLowerCase();
                int matchCount = 0;
                for (String kw : keywords) {
                    if (candidateText.contains(kw)) {
                        matchCount++;
                    }
                }
                if (matchCount > 0) {
                    double score = 2.0 * matchCount;
                    return Mono.just(new ScoredBook(book, score));
                }
                return Mono.empty();
            })
            .onErrorResume(e -> {
                logger.warn("Error finding books by text keywords '{}': {}", query, e.getMessage());
                return Flux.empty();
            });
    }

    /**
     * Helper class to track books with their calculated similarity scores
     * 
     * Encapsulates:  
     * - Book object
     * - Similarity score that can be accumulated from multiple sources
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
