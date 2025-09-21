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
import com.williamcallahan.book_recommendation_engine.service.BookRecommendationPersistenceService.RecommendationRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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

    private static final String REASON_AUTHOR = "AUTHOR";
    private static final String REASON_CATEGORY = "CATEGORY";
    private static final String REASON_TEXT = "TEXT";

    private final GoogleBooksService googleBooksService;
    private final BookDataOrchestrator bookDataOrchestrator;
    private final BookRecommendationPersistenceService recommendationPersistenceService;
    private final boolean googleFallbackEnabled;

    /**
     * Constructs the RecommendationService with required dependencies
     *
     * @param googleBooksService Service for searching books in Google Books API
     *
     * @implNote Uses GoogleBooksService for searches and book details
     */
    public RecommendationService(GoogleBooksService googleBooksService,
                                 BookDataOrchestrator bookDataOrchestrator,
                                 BookRecommendationPersistenceService recommendationPersistenceService,
                                 @Value("${app.features.google-fallback.enabled:false}") boolean googleFallbackEnabled) {
        this.googleBooksService = googleBooksService;
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.recommendationPersistenceService = recommendationPersistenceService;
        this.googleFallbackEnabled = googleFallbackEnabled;
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

        return fetchCanonicalBook(bookId)
                .flatMap(sourceBook -> fetchCachedRecommendations(sourceBook, effectiveCount)
                        .flatMap(cached -> {
                            if (!cached.isEmpty()) {
                                logger.info("Serving {} cached Postgres recommendations for book {}.", cached.size(), bookId);
                                return Mono.just(cached);
                            }
                            logger.info("No cached Postgres recommendations for book {}. Falling back to API pipeline.", bookId);
                            return fetchRecommendationsFromApiAndUpdateCache(sourceBook, effectiveCount);
                        }))
                .switchIfEmpty(fetchLegacyRecommendations(bookId, effectiveCount))
                .onErrorResume(ex -> {
                    logger.error("Failed to assemble recommendations for {}: {}", bookId, ex.getMessage(), ex);
                    return Mono.just(Collections.<Book>emptyList());
                });
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
    private Mono<List<Book>> fetchLegacyRecommendations(String bookId, int effectiveCount) {
        return fetchSourceBookTiered(bookId)
                .flatMap(sourceBook -> fetchRecommendationsFromApiAndUpdateCache(sourceBook, effectiveCount))
                .switchIfEmpty(Mono.just(Collections.<Book>emptyList()));
    }

    private Mono<Book> fetchCanonicalBook(String identifier) {
        if (identifier == null || identifier.isBlank() || bookDataOrchestrator == null) {
            return Mono.empty();
        }
        Mono<Book> byId = Mono.defer(() -> {
            Mono<Book> lookup = bookDataOrchestrator.getBookByIdTiered(identifier);
            return lookup == null ? Mono.empty() : lookup;
        });
        Mono<Book> bySlug = Mono.defer(() -> {
            Mono<Book> lookup = bookDataOrchestrator.getBookBySlugTiered(identifier);
            return lookup == null ? Mono.empty() : lookup;
        });
        return byId.switchIfEmpty(bySlug);
    }

    private Mono<Book> fetchCanonicalBookSafe(String identifier) {
        return fetchCanonicalBook(identifier)
                .doOnError(ex -> logger.debug("Postgres lookup failed for recommendation {}: {}", identifier, ex.getMessage()))
                .onErrorResume(ex -> Mono.empty());
    }

    private Mono<List<Book>> fetchCachedRecommendations(Book sourceBook, int limit) {
        if (sourceBook == null) {
            return Mono.just(Collections.<Book>emptyList());
        }
        List<String> cachedIds = sourceBook.getCachedRecommendationIds();
        if (cachedIds == null || cachedIds.isEmpty()) {
            return Mono.just(Collections.<Book>emptyList());
        }

        List<String> idsToFetch = new ArrayList<>(cachedIds);
        Collections.shuffle(idsToFetch);

        return Flux.fromIterable(idsToFetch)
                .flatMapSequential(this::fetchCanonicalBookSafe, 4, 8)
                .filter(Objects::nonNull)
                .filter(recommended -> sourceBook.getId() == null || !sourceBook.getId().equals(recommended.getId()))
                .distinct(Book::getId)
                .take(limit)
                .collectList()
                .doOnNext(results -> logger.debug("Hydrated {} cached recommendations for {}", results.size(), sourceBook.getId()));
    }

    private Mono<List<Book>> fetchRecommendationsFromApiAndUpdateCache(Book sourceBook, int effectiveCount) {
        Flux<ScoredBook> authorsFlux = findBooksByAuthorsReactive(sourceBook);
        Flux<ScoredBook> categoriesFlux = findBooksByCategoriesReactive(sourceBook);
        Flux<ScoredBook> textFlux = findBooksByTextReactive(sourceBook);

        return Flux.merge(authorsFlux, categoriesFlux, textFlux)
            .collect(Collectors.toMap(
                scoredBook -> scoredBook.getBook().getId(),
                scoredBook -> scoredBook,
                (sb1, sb2) -> {
                    sb1.mergeWith(sb2);
                    return sb1;
                },
                HashMap::new
            ))
            .flatMap(recommendationMap -> {
                String sourceLang = sourceBook.getLanguage();
                boolean filterByLanguage = sourceLang != null && !sourceLang.isEmpty();

                List<ScoredBook> orderedCandidates = recommendationMap.values().stream()
                    .filter(scored -> isEligibleRecommendation(sourceBook, scored.getBook(), filterByLanguage, sourceLang))
                    .sorted(Comparator.comparing(ScoredBook::getScore).reversed())
                    .collect(Collectors.toList());

                if (orderedCandidates.isEmpty()) {
                    logger.info("No recommendations generated from API for book ID: {}", sourceBook.getId());
                    return Mono.just(Collections.<Book>emptyList());
                }

                List<Book> orderedBooks = orderedCandidates.stream()
                    .map(ScoredBook::getBook)
                    .collect(Collectors.toList());

                List<Book> limitedRecommendations = orderedBooks.stream()
                    .limit(effectiveCount)
                    .collect(Collectors.toList());

                List<String> newRecommendationIds = orderedBooks.stream()
                    .map(Book::getId)
                    .filter(Objects::nonNull)
                    .distinct()
                    .collect(Collectors.toList());

                sourceBook.addRecommendationIds(newRecommendationIds);

                Mono<Void> cacheIndividualRecommendedBooksMono = Flux.fromIterable(orderedBooks)
                    .flatMap(recommendedBook -> Mono.just(recommendedBook))
                    .then();

                Set<String> limitedIds = limitedRecommendations.stream()
                    .map(Book::getId)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toCollection(LinkedHashSet::new));

                Mono<Void> persistenceMono = recommendationPersistenceService != null
                    ? recommendationPersistenceService
                        .persistPipelineRecommendations(sourceBook, buildPersistenceRecords(orderedCandidates, limitedIds))
                        .onErrorResume(ex -> {
                            logger.warn("Failed to persist recommendations for {}: {}", sourceBook.getId(), ex.getMessage());
                            return Mono.empty();
                        })
                    : Mono.empty();

                return Mono.when(cacheIndividualRecommendedBooksMono, persistenceMono)
                    .then(Mono.fromRunnable(() -> logger.info("Updated cachedRecommendationIds for book {} with {} new IDs and cached {} individual recommended books.",
                            sourceBook.getId(), newRecommendationIds.size(), orderedBooks.size())))
                    .thenReturn(limitedRecommendations)
                    .doOnSuccess(finalList -> logger.info("Fetched {} total potential recommendations for book ID {} from API, updated cache. Returning {} recommendations.", orderedBooks.size(), sourceBook.getId(), finalList.size()))
                    .onErrorResume(e -> {
                        logger.error("Error completing recommendation pipeline for book {}: {}", sourceBook.getId(), e.getMessage());
                        return Mono.just(limitedRecommendations);
                    });
            });
    }

    private List<RecommendationRecord> buildPersistenceRecords(List<ScoredBook> orderedCandidates, Set<String> limitedIds) {
        if (recommendationPersistenceService == null || limitedIds.isEmpty()) {
            return List.of();
        }

        return orderedCandidates.stream()
            .filter(scored -> {
                Book candidate = scored.getBook();
                return candidate != null && candidate.getId() != null && limitedIds.contains(candidate.getId());
            })
            .map(scored -> new RecommendationRecord(
                scored.getBook(),
                scored.getScore(),
                new ArrayList<>(scored.getReasons())))
            .collect(Collectors.toList());
    }

    private boolean isEligibleRecommendation(Book sourceBook, Book candidate, boolean filterByLanguage, String sourceLang) {
        if (candidate == null) {
            return false;
        }
        String candidateId = candidate.getId();
        if (candidateId == null || candidateId.isBlank()) {
            return false;
        }
        String sourceId = sourceBook != null ? sourceBook.getId() : null;
        if (sourceId != null && sourceId.equals(candidateId)) {
            return false;
        }
        if (!filterByLanguage) {
            return true;
        }
        return Objects.equals(sourceLang, candidate.getLanguage());
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
            .flatMap(author -> searchBooksTiered("inauthor:" + author, langCode, MAX_SEARCH_RESULTS)
                .flatMapMany(Flux::fromIterable)
                .map(book -> new ScoredBook(book, 4.0, REASON_AUTHOR))
                .onErrorResume(e -> {
                    logger.warn("Error finding books by author {}: {}", author, e.getMessage());
                    return Flux.empty();
                }));
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
        
        return searchBooksTiered(categoryQueryString, langCode, MAX_SEARCH_RESULTS)
            .flatMapMany(Flux::fromIterable)
            .take(MAX_SEARCH_RESULTS)
            .map(book -> {
                double categoryScore = calculateCategoryOverlapScore(sourceBook, book);
                return new ScoredBook(book, categoryScore, REASON_CATEGORY);
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
                PagingUtils.atLeast(Math.min(sourceCategories.size(), candidateCategories.size()), 1);
        
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

        return searchBooksTiered(query, langCode, MAX_SEARCH_RESULTS)
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
                    return Mono.just(new ScoredBook(book, score, REASON_TEXT));
                }
                return Mono.empty();
            })
            .onErrorResume(e -> {
                logger.warn("Error finding books by text keywords '{}': {}", query, e.getMessage());
                return Flux.empty();
            });
    }

    private Mono<Book> fetchSourceBookTiered(String bookId) {
        Mono<Book> postgresMono = bookDataOrchestrator != null
                ? bookDataOrchestrator.getBookByIdTiered(bookId)
                : Mono.empty();

        return postgresMono
                .switchIfEmpty(Mono.fromFuture(googleBooksService.getBookById(bookId).toCompletableFuture())
                    .flatMap(book -> book == null ? Mono.empty() : Mono.just(book)))
                .doOnNext(book -> logger.debug("Source book resolved for {} via tiered lookup.", bookId));
    }

    private Mono<List<Book>> searchBooksTiered(String query, String langCode, int limit) {
        Mono<List<Book>> primary = Mono.defer(() -> {
            if (bookDataOrchestrator == null) {
                return Mono.just(Collections.emptyList());
            }
            return bookDataOrchestrator.searchBooksTiered(query, langCode, limit, null)
                    .defaultIfEmpty(Collections.emptyList())
                    .onErrorResume(ex -> {
                        logger.debug("Postgres search failed for query '{}' (lang {}): {}", query, langCode, ex.getMessage());
                        return Mono.just(Collections.emptyList());
                    });
        });

        return primary.flatMap(results -> {
            List<Book> trimmed = trimSearchResults(results, limit);
            if (!trimmed.isEmpty()) {
                logger.debug("Returning {} results from Postgres tier for query '{}'", trimmed.size(), query);
                return Mono.just(trimmed);
            }
            if (!googleFallbackEnabled) {
                logger.debug("Google fallback disabled for recommendation query '{}'. Returning empty list.", query);
                return Mono.just(Collections.emptyList());
            }
            return googleBooksService.searchBooksAsyncReactive(query, langCode, limit, null)
                    .defaultIfEmpty(Collections.emptyList())
                    .map(list -> trimSearchResults(list, limit))
                    .doOnError(ex -> logger.warn("Google Books search failed for query '{}' (lang {}): {}", query, langCode, ex.getMessage()))
                    .onErrorResume(ex -> Mono.just(Collections.emptyList()));
        });
    }

    private List<Book> trimSearchResults(List<Book> books, int limit) {
        if (books == null || books.isEmpty()) {
            return Collections.emptyList();
        }
        return books.stream()
                .filter(Objects::nonNull)
                .filter(book -> book.getId() != null && !book.getId().isBlank())
                .limit(limit)
                .collect(Collectors.toList());
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
        private final LinkedHashSet<String> reasons = new LinkedHashSet<>();

        public ScoredBook(Book book, double score, String reason) {
            this.book = book;
            this.score = score;
            if (reason != null && !reason.isBlank()) {
                this.reasons.add(reason);
            }
        }

        public Book getBook() {
            return book;
        }

        public double getScore() {
            return score;
        }

        public Set<String> getReasons() {
            return Collections.unmodifiableSet(reasons);
        }

        public void mergeWith(ScoredBook other) {
            this.score += other.score;
            this.reasons.addAll(other.reasons);
        }
    }
}
