/**
 * Service for generating book recommendations based on various similarity criteria
 * This class is responsible for generating book recommendations using a combination of strategies:
 * 
 * - Uses multi-faceted approach combining author, category, and text-based matching
 * - Implements scoring algorithm to rank recommendations by relevance
 * - Supports language-aware filtering to match source book language
 * - Provides reactive API for non-blocking recommendation generation
 * - Handles category normalization for better cross-book matching
 * - Implements keyword extraction with stop word filtering
 * - Caches recommendations for improved performance and reduced API usage
 * - Updates recommendation IDs in source book for persistent recommendation history
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.Locale;
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

    /**
     * Constructs the RecommendationService with required dependencies
     *
     * @param googleBooksService Service for searching books in Google Books API
     *
     * @implNote Uses GoogleBooksService for searches and book details
     */
    public RecommendationService(GoogleBooksService googleBooksService) {
        this.googleBooksService = googleBooksService;
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

        return Mono.fromFuture(googleBooksService.getBookById(bookId).toCompletableFuture())
            .flatMap(sourceBook -> {
                if (sourceBook == null) {
                    logger.warn("Cannot get recommendations - source book with ID {} not found.", bookId);
                    return Mono.just(Collections.<Book>emptyList());
                }

                // Tier 1: Try to use cached recommendation IDs from the sourceBook object
                List<String> cachedIds = sourceBook.getCachedRecommendationIds();
                if (cachedIds != null && !cachedIds.isEmpty()) {
                    logger.info("Found {} cached recommendation IDs for book {}.", cachedIds.size(), bookId);
                    // Fetch full Book objects for these IDs
                    // Shuffle to provide variety if we have more cached IDs than needed
                    List<String> idsToFetch = new ArrayList<>(cachedIds);
                    Collections.shuffle(idsToFetch);
                    if (idsToFetch.size() > effectiveCount * 2) { // Fetch a bit more to allow for filtering
                        idsToFetch = idsToFetch.subList(0, effectiveCount * 2);
                    }

                    return Flux.fromIterable(idsToFetch)
                        .flatMap(recId -> Mono.fromFuture(googleBooksService.getBookById(recId).toCompletableFuture())
                            .filter(Objects::nonNull) // Ensure book exists in cache
                            .filter(recBook -> !recBook.getId().equals(sourceBook.getId())) // Exclude source book
                            .filter(recBook -> { // Language filter
                                String sourceLang = sourceBook.getLanguage();
                                boolean filterByLanguage = sourceLang != null && !sourceLang.isEmpty();
                                if (!filterByLanguage) return true;
                                return Objects.equals(sourceLang, recBook.getLanguage());
                            })
                        )
                        .collectList()
                        .flatMap(cachedBooks -> {
                            if (cachedBooks.size() >= effectiveCount) {
                                logger.info("Serving {} recommendations for book {} from S3/cached IDs.", Math.min(cachedBooks.size(), effectiveCount), bookId);
                                return Mono.just(cachedBooks.stream().limit(effectiveCount).collect(Collectors.toList()));
                            } else {
                                logger.info("Cached recommendations for book {} are insufficient ({} found, {} needed). Fetching from API.", bookId, cachedBooks.size(), effectiveCount);
                                return fetchRecommendationsFromApiAndUpdateCache(sourceBook, effectiveCount);
                            }
                        });
                } else {
                    logger.info("No cached recommendation IDs for book {}. Fetching from API.", bookId);
                    return fetchRecommendationsFromApiAndUpdateCache(sourceBook, effectiveCount);
                }
            })
            .switchIfEmpty(Mono.just(Collections.emptyList())); // If the whole chain above is empty, provide an empty list.
    }

    /**
     * Fetches book recommendations using the Google Books API and updates the cache
     * This is a fallback method used when cached recommendations are unavailable or insufficient
     * 
     * @param sourceBook The book to find recommendations for
     * @param effectiveCount The desired number of recommendations to return
     * @return Mono emitting a list of recommended books
     * 
     * @implNote Uses a multi-strategy approach:
     * 1. Retrieves books by same authors (via findBooksByAuthorsReactive)
     * 2. Retrieves books in similar categories (via findBooksByCategoriesReactive)
     * 3. Retrieves books with matching keywords (via findBooksByTextReactive)
     * 4. Merges results, with duplicates having their scores combined
     * 5. Filters by language and excludes the source book itself
     * 6. Updates the source book's cached recommendation IDs for future use
     */
    private Mono<List<Book>> fetchRecommendationsFromApiAndUpdateCache(Book sourceBook, int effectiveCount) {
        Flux<ScoredBook> authorsFlux = findBooksByAuthorsReactive(sourceBook);
        Flux<ScoredBook> categoriesFlux = findBooksByCategoriesReactive(sourceBook);
        Flux<ScoredBook> textFlux = findBooksByTextReactive(sourceBook);

        return Flux.merge(authorsFlux, categoriesFlux, textFlux)
            .collect(Collectors.toMap(
                scoredBook -> scoredBook.getBook().getId(),
                scoredBook -> scoredBook,
                (sb1, sb2) -> {
                    sb1.setScore(sb1.getScore() + sb2.getScore());
                    return sb1;
                },
                HashMap::new
            ))
            .flatMap(recommendationMap -> {
                String sourceLang = sourceBook.getLanguage();
                boolean filterByLanguage = sourceLang != null && !sourceLang.isEmpty();

                List<Book> recommendations = recommendationMap.values().stream()
                    .sorted(Comparator.comparing(ScoredBook::getScore).reversed())
                    .map(ScoredBook::getBook)
                    .filter(book -> !book.getId().equals(sourceBook.getId()))
                    .filter(recommendedBook -> {
                        if (!filterByLanguage) return true;
                        String recommendedLang = recommendedBook.getLanguage();
                        return Objects.equals(sourceLang, recommendedLang);
                    })
                    .collect(Collectors.toList()); // Collect all potential recommendations before limiting

                if (!recommendations.isEmpty()) {
                    List<String> newRecommendationIds = recommendations.stream()
                        .map(Book::getId)
                        .distinct()
                        .collect(Collectors.toList());
                    
                    sourceBook.addRecommendationIds(newRecommendationIds);
                    
                    // Proactively cache each of the newly found recommended books
                    Mono<Void> cacheIndividualRecommendedBooksMono = Flux.fromIterable(recommendations)
                        .flatMap(recommendedBook -> {
                            // Ensure rawJsonResponse is set if it's available and BookCacheFacadeService uses it
                            // This might already be handled by BookJsonParser
                            // Skip caching - Redis removed
                            return Mono.just(recommendedBook);
                        })
                        .then();
                    
                    // Save the updated sourceBook with new recommendation IDs, after caching individual books
                    return cacheIndividualRecommendedBooksMono
                        // Skip caching - Redis removed
                        .then(Mono.fromRunnable(() -> logger.info("Updated cachedRecommendationIds for book {} with {} new IDs and cached {} individual recommended books.", sourceBook.getId(), newRecommendationIds.size(), recommendations.size())))
                        .thenReturn(recommendations.stream().limit(effectiveCount).collect(Collectors.toList())) // Return limited list after saving
                        .doOnSuccess(finalList -> logger.info("Fetched {} total potential recommendations for book ID {} from API, updated cache. Returning {} recommendations.", recommendations.size(), sourceBook.getId(), finalList.size()))
                        .onErrorResume(e -> {
                            logger.error("Error saving updated source book {} to cache after fetching recommendations: {}", sourceBook.getId(), e.getMessage());
                            // Still return the recommendations even if caching fails for this update
                            return Mono.just(recommendations.stream().limit(effectiveCount).collect(Collectors.toList()));
                        });
                } else {
                     logger.info("No recommendations generated from API for book ID: {}", sourceBook.getId());
                    return Mono.just(Collections.emptyList());
                }
            });
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
                normalized.add(part.toLowerCase(Locale.ROOT).trim());
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

        String combinedText = (sourceBook.getTitle() + " " + sourceBook.getDescription()).toLowerCase(Locale.ROOT);
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
                                      (book.getDescription() != null ? book.getDescription() : "")).toLowerCase(Locale.ROOT);
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
     * - Book object with all its metadata
     * - Similarity score that accumulates across multiple recommendation strategies
     * - Used for ranking recommendations by relevance
     * - Allows scores to be combined when a book is found by multiple strategies
     */
    private static class ScoredBook {
        private final Book book;
        private double score;
        
        /**
         * Constructs a new ScoredBook with the given book and initial score
         * 
         * @param book The book to be scored
         * @param score The initial similarity score
         */
        public ScoredBook(Book book, double score) {
            this.book = book;
            this.score = score;
        }
        
        /**
         * Gets the book object
         * 
         * @return The book
         */
        public Book getBook() {
            return book;
        }
        
        /**
         * Gets the current similarity score
         * 
         * @return The similarity score
         */
        public double getScore() {
            return score;
        }
        
        /**
         * Updates the similarity score (used when combining scores from multiple sources)
         * 
         * @param score The new score to set
         */
        public void setScore(double score) {
            this.score = score;
        }
    }
}
