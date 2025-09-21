/**
 * Controller for handling user-facing web pages in the Book Finder
 *
 * @author William Callahan
 *
 * Features:
 * - Renders home page with recently viewed and recommended books
 * - Manages search page and search result display
 * - Handles book detail pages with metadata and similar book recommendations
 * - Processes ISBN lookups and redirects to canonical book URLs
 * - Integrates with caching services for optimal performance
 * - Applies SEO optimizations including metadata and keyword generation
 * - Manages cover image resolution and source preferences
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.DuplicateBookService;
import com.williamcallahan.book_recommendation_engine.service.EnvironmentService;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.NewYorkTimesService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
import com.williamcallahan.book_recommendation_engine.service.image.BookCoverManagementService;
import com.williamcallahan.book_recommendation_engine.service.image.LocalDiskCoverCacheService;
import com.williamcallahan.book_recommendation_engine.util.IsbnUtils;
import com.williamcallahan.book_recommendation_engine.util.SeoUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.Map;
import java.util.HashMap;
import java.util.Locale;

@Controller
public class HomeController {

    private static final Logger logger = LoggerFactory.getLogger(HomeController.class);
    private final BookDataOrchestrator bookDataOrchestrator;
    private final GoogleBooksService googleBooksService;
    private final RecentlyViewedService recentlyViewedService;
    private final RecommendationService recommendationService;
    private final BookCoverManagementService bookCoverManagementService;
    private final EnvironmentService environmentService;
    private final DuplicateBookService duplicateBookService;
    private final LocalDiskCoverCacheService localDiskCoverCacheService;
    private final NewYorkTimesService newYorkTimesService;
    private final boolean googleFallbackEnabled;
    private final boolean isYearFilteringEnabled;

    private static final int MAX_RECENT_BOOKS = 8;
    private static final int MAX_BESTSELLERS = 8;
    
    private static final List<String> EXPLORE_QUERIES = Arrays.asList(
            "Classic literature",
            "Modern thrillers",
            "Space opera adventures",
            "Historical fiction bestsellers",
            "Award-winning science fiction",
            "Inspiring biographies",
            "Mind-bending philosophy",
            "Beginner's cookbooks",
            "Epic fantasy sagas",
            "Cyberpunk futures",
            "Cozy mysteries",
            "Environmental science",
            "Artificial intelligence ethics",
            "World mythology",
            "Travel memoirs"
    );
    private static final Random RANDOM = new Random();

    // Regex patterns for ISBN validation
    private static final Pattern ISBN10_PATTERN = Pattern.compile("^[0-9]{9}[0-9X]$");
    private static final Pattern ISBN13_PATTERN = Pattern.compile("^[0-9]{13}$");
    private static final Pattern ISBN_ANY_PATTERN = Pattern.compile("^[0-9]{9}[0-9X]$|^[0-9]{13}$");

    @Value("${app.default-cover-preference:GOOGLE_BOOKS}")
    private String defaultCoverPreference;

    @Value("${app.seo.max-description-length:170}")
    private int maxDescriptionLength;

    // Affiliate IDs from properties
    @Value("${affiliate.barnesandnoble.publisher-id:#{null}}")
    private String barnesNobleCjPublisherId;

    @Value("${affiliate.barnesandnoble.website-id:#{null}}")
    private String barnesNobleCjWebsiteId;

    @Value("${affiliate.bookshop.affiliate-id:#{null}}")
    private String bookshopAffiliateId;

    @Value("${affiliate.amazon.associate-tag:#{null}}")
    private String amazonAssociateTag;

    /**
     * Constructs the HomeController with required services
     * 
     * @param bookDataOrchestrator Service for orchestrating book data retrieval
     * @param googleBooksService Service for accessing Google Books API
     * @param recentlyViewedService Service for tracking user book view history
     * @param recommendationService Service for generating book recommendations
     * @param bookCoverManagementService Service for retrieving and caching book cover images
     * @param environmentService Service providing environment configuration information
     * @param duplicateBookService Service for handling duplicate book editions
     */
    public HomeController(BookDataOrchestrator bookDataOrchestrator,
                          GoogleBooksService googleBooksService,
                          RecentlyViewedService recentlyViewedService,
                          RecommendationService recommendationService,
                          BookCoverManagementService bookCoverManagementService,
                          EnvironmentService environmentService,
                          DuplicateBookService duplicateBookService,
                          LocalDiskCoverCacheService localDiskCoverCacheService,
                          @Value("${app.feature.year-filtering.enabled:false}") boolean isYearFilteringEnabled,
                          @Value("${app.features.google-fallback.enabled:false}") boolean googleFallbackEnabled,
                          NewYorkTimesService newYorkTimesService) {
        this.bookDataOrchestrator = bookDataOrchestrator;
        this.googleBooksService = googleBooksService;
        this.recentlyViewedService = recentlyViewedService;
        this.recommendationService = recommendationService;
        this.bookCoverManagementService = bookCoverManagementService;
        this.environmentService = environmentService;
        this.duplicateBookService = duplicateBookService;
        this.localDiskCoverCacheService = localDiskCoverCacheService;
        this.isYearFilteringEnabled = isYearFilteringEnabled;
        this.googleFallbackEnabled = googleFallbackEnabled;
        this.newYorkTimesService = newYorkTimesService;
    }

    /**
     * Handles requests to the home page
     * - Displays recently viewed books for returning users
     * - Populates with default recommendations for new users
     * - Optimizes cover images for display through caching service
     *
     * @param model The model for the view
     * @return Mono containing the name of the template to render
     */
    @GetMapping("/")
    public Mono<String> home(Model model) {
        model.addAttribute("isDevelopmentMode", environmentService.isDevelopmentMode());
        model.addAttribute("currentEnv", environmentService.getCurrentEnvironmentMode());
        model.addAttribute("activeTab", "home");
        model.addAttribute("title", "Home");
        model.addAttribute("description", "Discover your next favorite book with our recommendation engine. Explore recently viewed books and new arrivals.");
        model.addAttribute("canonicalUrl", "https://findmybook.net/"); 
        model.addAttribute("ogImage", "https://findmybook.net/images/default-social-image.png"); // Default OG image
        model.addAttribute("keywords", "book recommendations, find books, book suggestions, reading, literature, home");

        // Fetch Current Bestsellers from New York Times (via S3/Cache)
        Mono<List<Book>> bestsellersMono = newYorkTimesService.getCurrentBestSellers("hardcover-fiction", MAX_BESTSELLERS)
            .flatMap(this::processBooksCovers)
            .map(bookList -> bookList.stream()
                .filter(this::isActualCover)
                .collect(Collectors.toList())
            )
            .map(this::deduplicateBooksById)
            .map(bookList -> {
                bookList.forEach(duplicateBookService::populateDuplicateEditions);
                return bookList;
            })
            .doOnSuccess(bestsellers -> model.addAttribute("currentBestsellers", bestsellers))
            .doOnError(e -> {
                logger.error("Error fetching and filtering current bestsellers: {}", e.getMessage());
                model.addAttribute("currentBestsellers", Collections.emptyList());
            })
            .onErrorReturn(Collections.emptyList());

        // Add recently viewed books to the model
        List<Book> initialRecentBooks = recentlyViewedService.getRecentlyViewedBooks(); // This is synchronous
        
        Mono<List<Book>> recentBooksMono;
        List<Book> trimmedRecentBooks = initialRecentBooks.stream().limit(MAX_RECENT_BOOKS).collect(Collectors.toList());

        if (trimmedRecentBooks.size() < MAX_RECENT_BOOKS) {
            int needed = MAX_RECENT_BOOKS - trimmedRecentBooks.size();
            String randomQuery = EXPLORE_QUERIES.get(RANDOM.nextInt(EXPLORE_QUERIES.size()));
            logger.info("Fetching {} additional books for homepage with query: '{}'", needed, randomQuery);

            recentBooksMono = fetchAdditionalHomepageBooks(randomQuery, needed)
                .map(defaultBooks -> {
                    List<Book> combinedBooks = new ArrayList<>(trimmedRecentBooks);
                    List<Book> booksToAdd = (defaultBooks == null) ? Collections.emptyList() : defaultBooks;
                    for (Book defaultBook : booksToAdd) {
                        if (combinedBooks.size() >= MAX_RECENT_BOOKS) break;
                        if (defaultBook != null && defaultBook.getId() != null &&
                                trimmedRecentBooks.stream().noneMatch(rb -> rb.getId().equals(defaultBook.getId()))) {
                            combinedBooks.add(defaultBook);
                        }
                    }
                    return combinedBooks.stream().limit(MAX_RECENT_BOOKS).collect(Collectors.toList());
                });
        } else {
            recentBooksMono = Mono.just(trimmedRecentBooks);
        }

        Mono<List<Book>> processedRecentBooksMono = recentBooksMono
            .flatMap(this::processBooksCovers)
            .map(bookList -> bookList.stream()
                .filter(this::isActualCover)
                .collect(Collectors.toList())
            )
            .map(this::deduplicateBooksById)
            .map(bookList -> {
                bookList.forEach(duplicateBookService::populateDuplicateEditions);
                return bookList;
            })
            .doOnSuccess(recent -> model.addAttribute("recentBooks", recent))
        .doOnError(e -> {
            logger.error("Error fetching and filtering recent books: {}", e.getMessage());
                model.addAttribute("recentBooks", Collections.emptyList());
            })
            .onErrorReturn(Collections.emptyList());

        // Combine both operations and then return the view name
        return Mono.zip(bestsellersMono, processedRecentBooksMono)
            .map(tuple -> "index")
            .onErrorReturn("index"); // Fallback to rendering index even if one stream fails
    }

    // Helper predicate for filtering out known placeholder images
    private boolean isActualCover(Book book) {
        String coverUrl = book.getS3ImagePath();
        if (coverUrl == null || coverUrl.isEmpty()) {
            return false; // No URL, not an actual cover
        }
        // Check against the primary placeholder path
        if (coverUrl.equals(this.localDiskCoverCacheService.getLocalPlaceholderPath())) {
            return false;
        }
        // Check for other known placeholder patterns/names
        if (coverUrl.contains("placeholder-book-cover.svg") || 
            coverUrl.contains("image-not-available.png") ||
            coverUrl.contains("mock-placeholder.svg") ||
            // Add more known placeholder substrings if necessary
            coverUrl.endsWith("/images/transparent.gif")
        ) {
            return false;
        }
        return true; // Assume it's an actual cover if none of the above match
    }

    // Helper method to de-duplicate a list of books by their ID
    private List<Book> deduplicateBooksById(List<Book> books) {
        if (books == null || books.isEmpty()) {
            return Collections.emptyList();
        }
        return books.stream()
                .filter(distinctByKey(Book::getId))
                .collect(Collectors.toList());
    }

    // Utility for distinctByKey
    private static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        return t -> seen.add(keyExtractor.apply(t));
    }
    
    // Helper method to process covers for a list of books
    private Mono<List<Book>> processBooksCovers(List<Book> books) {
        if (books == null || books.isEmpty()) {
            return Mono.just(Collections.emptyList());
        }
        return Flux.fromIterable(books)
            .concatMap(book -> { // concatMap preserves order and processes one by one
                if (book == null) {
                    return Mono.<Book>empty(); 
                }
                return bookCoverManagementService.getInitialCoverUrlAndTriggerBackgroundUpdate(book)
                    .map(coverImagesResult -> {
                        if (book.getSlug() == null || book.getSlug().isBlank()) {
                            book.setSlug(book.getId());
                        }
                        book.setCoverImages(coverImagesResult);
                        if (coverImagesResult != null && coverImagesResult.getPreferredUrl() != null) {
                            book.setS3ImagePath(coverImagesResult.getPreferredUrl());
                        } else if (book.getS3ImagePath() == null || book.getS3ImagePath().isEmpty()) {
                            book.setS3ImagePath("/images/placeholder-book-cover.svg");
                        }
                        // Clear these as they might not be relevant or consistently set by all sources
                        book.setCoverImageWidth(null);
                        book.setCoverImageHeight(null);
                        book.setIsCoverHighResolution(null);
                        return book;
                    })
                    .onErrorResume(e -> {
                        String identifierForLog = (book.getIsbn13() != null) ? book.getIsbn13() :
                                                 ((book.getIsbn10() != null) ? book.getIsbn10() : book.getId());
                        logger.warn("Error getting initial cover URL for book with identifier '{}' in home: {}", identifierForLog, e.getMessage());
                        if (book.getS3ImagePath() == null || book.getS3ImagePath().isEmpty()) {
                           book.setS3ImagePath("/images/placeholder-book-cover.svg");
                        }
                        if (book.getSlug() == null || book.getSlug().isBlank()) {
                            book.setSlug(book.getId());
                        }
                        // Ensure CoverImages is at least initialized to avoid NPEs in template
                        if (book.getCoverImages() == null) {
                            book.setCoverImages(new CoverImages(
                                book.getS3ImagePath(), book.getS3ImagePath(), CoverImageSource.LOCAL_CACHE
                            ));
                        }
                        return Mono.just(book); // Return the book even if cover fetching failed
                    });
            })
            .filter(java.util.Objects::nonNull) // Filter out any null books if handled that way
            .collectList();
    }
    
    /**
     * Handles requests to the search results page
     * - Sets up model attributes for search view
     * - Configures SEO metadata for search page
     * - Prepares for client-side API calls
     *
     * @param query The search query string from user input
     * @param model The model for the view
     * @return The name of the template to render (search.html)
     */
    @GetMapping("/search")
    public Object search(String query, 
                       @RequestParam(required = false) Integer year,
                       Model model) {
        
        Integer effectiveYear = year;
        String queryForProcessing = query;

        if (!isYearFilteringEnabled) {
            effectiveYear = null;
            // If year filtering is disabled, we don't attempt to extract year from query
            // The original query is used as is
        } else {
            // Only attempt to extract year from query if year param is not already provided
            // AND year filtering is enabled
            if (queryForProcessing != null && effectiveYear == null) {
                java.util.regex.Pattern yearPattern = java.util.regex.Pattern.compile("\\b(19\\d{2}|20\\d{2})\\b");
                java.util.regex.Matcher matcher = yearPattern.matcher(queryForProcessing);
                
                if (matcher.find()) {
                    String yearStr = matcher.group(1);
                    try {
                        int extractedYear = Integer.parseInt(yearStr);
                        logger.info("Detected year {} in query text. Redirecting to use year parameter.", extractedYear);
                        
                        String beforeYear = queryForProcessing.substring(0, matcher.start());
                        String afterYear = queryForProcessing.substring(matcher.end());
                        String processedQueryWithoutYear = (beforeYear + afterYear).trim().replaceAll("\\s+", " ");
                        
                        // Update queryForProcessing for the current request if not redirecting immediately,
                        // though the redirect is typical here
                        // queryForProcessing = processedQueryWithoutYear; 
                        // effectiveYear = extractedYear; // Set effectiveYear if detected

                        String redirectUrl = "/search?query=" + URLEncoder.encode(processedQueryWithoutYear, StandardCharsets.UTF_8) 
                                          + "&year=" + extractedYear;
                        
                        return redirectTo(redirectUrl);
                    } catch (Exception e) {
                        logger.warn("Failed to extract year from query: {}", e.getMessage());
                        // Continue with normal rendering if year extraction fails, effectiveYear remains as initially set
                    }
                }
            }
        }
        
        // Normal rendering path
        model.addAttribute("isDevelopmentMode", environmentService.isDevelopmentMode());
        model.addAttribute("currentEnv", environmentService.getCurrentEnvironmentMode());
        model.addAttribute("query", queryForProcessing); // Use the original query or processed if year was stripped (though redirect handles this)
        model.addAttribute("year", effectiveYear); // Pass effectiveYear (possibly null if disabled or not found)
        model.addAttribute("isYearFilteringEnabled", isYearFilteringEnabled); // Pass the flag
        model.addAttribute("activeTab", "search");
        model.addAttribute("title", "Search Books");
        model.addAttribute("description", "Search our extensive catalog of books by title, author, or ISBN. Find detailed information and recommendations.");
        model.addAttribute("canonicalUrl", "https://findmybook.net/search"); 
        model.addAttribute("ogImage", "https://findmybook.net/images/default-social-image.png"); // Default OG image
        model.addAttribute("keywords", "book search, find books by title, find books by author, isbn lookup, book catalog"); // Default keywords
        return "search";
    }
    
    /**
     * Handles requests to the book detail page
     * - Retrieves detailed book information by ID
     * - Manages cover image retrieval and background updates
     * - Sets up SEO metadata for the book page
     * - Populates model with book data for template rendering
     * - Tracks recently viewed books for user history
     * - Handles search context parameters for navigation
     *
     * @param id The book identifier to display details for
     * @param query The search query that led to this book (for navigation context)
     * @param page The search results page number (for return navigation)
     * @param sort The sort method used in search results
     * @param view The view type used in search results (grid/list)
     * @param model The Spring model for view rendering
     * @return Mono containing the template name for async rendering
     */
    @GetMapping("/book/{id}")
    public Mono<String> bookDetail(@PathVariable String id,
                             @RequestParam(required = false) String query,
                             @RequestParam(required = false, defaultValue = "0") int page,
                             @RequestParam(required = false, defaultValue = "relevance") String sort,
                             @RequestParam(required = false, defaultValue = "grid") String view,
                              Model model) {
        logger.info("Looking up book with ID: {}", id);
        model.addAttribute("isDevelopmentMode", environmentService.isDevelopmentMode());
        model.addAttribute("currentEnv", environmentService.getCurrentEnvironmentMode());
        model.addAttribute("activeTab", "book"); // Set early, not dependent on async data
        model.addAttribute("searchQuery", query);
        model.addAttribute("searchPage", page);
        model.addAttribute("searchSort", sort);
        model.addAttribute("searchView", view);
        // Default SEO attributes in case book is not found or an error occurs
        model.addAttribute("title", "Book Details");
        model.addAttribute("description", "Detailed information about the selected book.");
        model.addAttribute("canonicalUrl", "https://findmybook.net/book/" + id);
        model.addAttribute("ogImage", "https://findmybook.net/images/og-logo.png"); // Default OG image
        model.addAttribute("keywords", "book, literature, reading, book details"); // Default keywords

        // Canonicalize to slug: if the requested identifier is not the slug, redirect to slug
        Mono<String> redirectIfNonCanonical = fetchCanonicalBook(id)
            .flatMap(found -> {
                if (found == null) {
                    return Mono.empty();
                }
                String canonical = (found.getSlug() != null && !found.getSlug().isBlank()) ? found.getSlug() : found.getId();
                if (canonical != null && !canonical.equals(id)) {
                    StringBuilder qs = new StringBuilder();
                    boolean first = true;
                    if (query != null && !query.isBlank()) {
                        qs.append(first ? "?" : "&").append("query=").append(URLEncoder.encode(query, java.nio.charset.StandardCharsets.UTF_8));
                        first = false;
                    }
                    if (page > 0) {
                        qs.append(first ? "?" : "&").append("page=").append(page);
                        first = false;
                    }
                    if (sort != null && !sort.isBlank()) {
                        qs.append(first ? "?" : "&").append("sort=").append(sort);
                        first = false;
                    }
                    if (view != null && !view.isBlank()) {
                        qs.append(first ? "?" : "&").append("view=").append(view);
                    }
                    return Mono.just("redirect:/book/" + canonical + qs);
                }
                return Mono.empty();
            });

        // Use BookDataOrchestrator to get the main book
        Mono<Book> bookMonoWithCover = fetchCanonicalBook(id)
            // If not found by volume ID, fallback to ISBN-based search
            .switchIfEmpty(
                googleBooksService.searchBooksByISBN(id)
                    .filter(list -> list != null && !list.isEmpty())
                    .map(list -> list.get(0))
            )
            .flatMap(book -> {
                if (book == null) {
                    logger.info("No book found with ID: {} via BookDataOrchestrator", id);
                    model.addAttribute("book", null);
                    return Mono.empty(); // No book found, propagate empty to handle later
                }
                // Book found, now fetch its cover images
                return bookCoverManagementService.getInitialCoverUrlAndTriggerBackgroundUpdate(book)
                    .map(coverImagesResult -> {
                        book.setCoverImages(coverImagesResult);
                        String effectiveCoverImageUrl = "/images/placeholder-book-cover.svg";
                        if (coverImagesResult != null && coverImagesResult.getPreferredUrl() != null) {
                            book.setS3ImagePath(coverImagesResult.getPreferredUrl());
                            effectiveCoverImageUrl = coverImagesResult.getPreferredUrl();
                        } else if (book.getS3ImagePath() != null && !book.getS3ImagePath().isEmpty()) {
                            effectiveCoverImageUrl = book.getS3ImagePath();
                        }
                        
                        model.addAttribute("book", book);
                        model.addAttribute("title", book.getTitle() != null ? book.getTitle() : "Book Details");
                        model.addAttribute("description", SeoUtils.truncateDescription(book.getDescription(), 170));
                        
                        // Set ogImage: use specific book cover if not placeholder, else use site default
                        String finalOgImage = (effectiveCoverImageUrl != null && !effectiveCoverImageUrl.contains("/images/placeholder-book-cover.svg")) 
                                            ? effectiveCoverImageUrl 
                                            : "https://findmybook.net/images/og-logo.png";
                        model.addAttribute("ogImage", finalOgImage);
                        
                        String canonicalIdentifier = book.getSlug() != null && !book.getSlug().isBlank()
                            ? book.getSlug()
                            : book.getId();
                        model.addAttribute("canonicalUrl", "https://findmybook.net/book/" + canonicalIdentifier);
                        model.addAttribute("keywords", generateKeywords(book));

                        // Populate other editions/duplicates
                        duplicateBookService.populateDuplicateEditions(book);

                        // Generate Affiliate Links
                        Map<String, String> affiliateLinks = new HashMap<>();
                        String isbn13 = book.getIsbn13();
                        String asin = book.getAsin();
                        String title = book.getTitle(); // Get the book title

                        if (isbn13 != null) {
                            if (barnesNobleCjPublisherId != null && barnesNobleCjWebsiteId != null) {
                                affiliateLinks.put("barnesAndNoble", "https://click.linksynergy.com/deeplink?id=" + barnesNobleCjPublisherId + "&mid=" + barnesNobleCjWebsiteId + "&murl=https://www.barnesandnoble.com/s/" + isbn13);
                            }
                            if (bookshopAffiliateId != null) {
                                affiliateLinks.put("bookshop", "https://bookshop.org/a/" + bookshopAffiliateId + "/" + isbn13);
                            }
                        }
                        // Generate Audible affiliate link
                        if (amazonAssociateTag != null) {
                            String searchTerm = asin != null ? asin : (title != null ? title : "");
                            try {
                                searchTerm = URLEncoder.encode(searchTerm, "UTF-8");
                            } catch (Exception e) {
                                logger.warn("Failed to encode search term for Audible link: {}", searchTerm);
                            }
                            String audibleUrl = "https://www.amazon.com/s?k=" + searchTerm + "&tag=" + amazonAssociateTag + "&linkCode=ur2&linkId=audible";
                            affiliateLinks.put("audible", audibleUrl);
                        }
                        // Generate Amazon affiliate link using ISBN or title
                        String isbnForAmazon = (isbn13 != null && !isbn13.isEmpty()) ? isbn13 : (book.getIsbn10() != null && !book.getIsbn10().isEmpty() ? book.getIsbn10() : null);
                        if (isbnForAmazon != null && amazonAssociateTag != null) {
                            affiliateLinks.put("amazon", "https://www.amazon.com/s?k=" + isbnForAmazon + "&tag=" + amazonAssociateTag + "&linkCode=ur2&linkId=isbn");
                        }
                        
                        model.addAttribute("affiliateLinks", affiliateLinks);

                        try {
                            recentlyViewedService.addToRecentlyViewed(book);
                        } catch (Exception e) {
                            logger.warn("Failed to add book to recently viewed for book ID {}: {}", book.getId(), e.getMessage());
                        }
                        return book;
                    })
                    .onErrorResume(e -> { // Handle errors from getInitialCoverUrlAndTriggerBackgroundUpdate
                        logger.warn("Error getting cover for book ID {}: {}. Using placeholder.", book.getId(), e.getMessage());
                        book.setS3ImagePath("/images/placeholder-book-cover.svg");
                         if (book.getCoverImages() == null) {
                            book.setCoverImages(new CoverImages(
                                book.getS3ImagePath(), book.getS3ImagePath(), CoverImageSource.LOCAL_CACHE
                            ));
                        }
                        model.addAttribute("book", book); // Add book with placeholder cover
                        // Set SEO attributes even on cover error
                        model.addAttribute("title", book.getTitle() != null ? book.getTitle() : "Book Details");
                        model.addAttribute("description", SeoUtils.truncateDescription(book.getDescription(), 170));
                        
                        // Fallback ogImage logic on error
                        String errorOgImage = (book.getS3ImagePath() != null && !book.getS3ImagePath().contains("/images/placeholder-book-cover.svg"))
                                            ? book.getS3ImagePath()
                                            : "https://findmybook.net/images/og-logo.png";
                        model.addAttribute("ogImage", errorOgImage);

                        String canonicalIdentifier = book.getSlug() != null && !book.getSlug().isBlank()
                            ? book.getSlug()
                            : book.getId();
                        model.addAttribute("canonicalUrl", "https://findmybook.net/book/" + canonicalIdentifier);
                        model.addAttribute("keywords", generateKeywords(book));
                        
                        // Populate other editions/duplicates even on cover error, if book object exists
                        duplicateBookService.populateDuplicateEditions(book);

                        // Generate Affiliate Links even on cover error, if book object exists
                        Map<String, String> affiliateLinksOnError = new HashMap<>();
                        String isbn13OnError = book.getIsbn13();
                        String asinOnError = book.getAsin();

                        if (isbn13OnError != null) {
                            if (barnesNobleCjPublisherId != null && barnesNobleCjWebsiteId != null) {
                                affiliateLinksOnError.put("barnesAndNoble", "https://click.linksynergy.com/deeplink?id=" + barnesNobleCjPublisherId + "&mid=" + barnesNobleCjWebsiteId + "&murl=https://www.barnesandnoble.com/s/" + isbn13OnError);
                            }
                            if (bookshopAffiliateId != null) {
                                affiliateLinksOnError.put("bookshop", "https://bookshop.org/a/" + bookshopAffiliateId + "/" + isbn13OnError);
                            }
                        }
                        if (amazonAssociateTag != null) {
                            String searchTermOnError = asinOnError != null ? asinOnError : (book.getTitle() != null ? book.getTitle() : "");
                            try {
                                searchTermOnError = URLEncoder.encode(searchTermOnError, "UTF-8");
                        } catch (Exception ex) {
                            logger.warn("Failed to encode search term for Audible link on error: {}", searchTermOnError);
                            }
                            String audibleUrlOnError = "https://www.amazon.com/s?k=" + searchTermOnError + "&tag=" + amazonAssociateTag + "&linkCode=ur2&linkId=audible";
                            affiliateLinksOnError.put("audible", audibleUrlOnError);
                        }
                        // Generate Amazon affiliate link on error fallback
                        String isbnForAmazonOnError = (isbn13OnError != null && !isbn13OnError.isEmpty()) ? isbn13OnError : (book.getIsbn10() != null && !book.getIsbn10().isEmpty() ? book.getIsbn10() : null);
                        if (isbnForAmazonOnError != null && amazonAssociateTag != null) {
                            affiliateLinksOnError.put("amazon", "https://www.amazon.com/s?k=" + isbnForAmazonOnError + "&tag=" + amazonAssociateTag + "&linkCode=ur2&linkId=isbn");
                        }
                        
                        model.addAttribute("affiliateLinks", affiliateLinksOnError);

                        return Mono.just(book);
                    });
            })
            .doOnError(e -> { // Errors from getBookByIdTiered
                 logger.error("Error getting book with ID: {} via BookDataOrchestrator", id, e);
                 model.addAttribute("error", "An error occurred while retrieving this book. Please try again later.");
                 model.addAttribute("book", null);
            })
            .onErrorResume(e -> Mono.empty()); // If getBookByIdReactive fails, propagate empty

        return redirectIfNonCanonical.switchIfEmpty(
            bookMonoWithCover
            .flatMap(fetchedBook -> { // fetchedBook is the main book, potentially null if initial fetch failed and resulted in empty()
                // Fetch similar books using RecommendationService
                Mono<List<Book>> similarBooksMono = recommendationService.getSimilarBooks(id, 10)  // Request 10 instead of 6
                    .flatMap(similarBooksList -> Flux.fromIterable(similarBooksList)
                        .concatMap(similarBook -> {
                            if (similarBook == null) return Mono.<Book>empty();
                            return bookCoverManagementService.getInitialCoverUrlAndTriggerBackgroundUpdate(similarBook)
                                .map(coverResult -> {
                                    similarBook.setCoverImages(coverResult);
                                    if (coverResult != null && coverResult.getPreferredUrl() != null) {
                                        similarBook.setS3ImagePath(coverResult.getPreferredUrl());
                                    } else if (similarBook.getS3ImagePath() == null || similarBook.getS3ImagePath().isEmpty()) {
                                        similarBook.setS3ImagePath("/images/placeholder-book-cover.svg");
                                    }
                                    return similarBook;
                                })
                                .onErrorResume(e -> {
                                    logger.warn("Error getting cover for similar book ID {}: {}", similarBook.getId(), e.getMessage());
                                    similarBook.setS3ImagePath("/images/placeholder-book-cover.svg");
                                    if (similarBook.getCoverImages() == null) {
                                       similarBook.setCoverImages(new CoverImages(
                                            similarBook.getS3ImagePath(), similarBook.getS3ImagePath(), CoverImageSource.LOCAL_CACHE
                                        ));
                                    }
                                    return Mono.just(similarBook);
                                });
                        })
                        .filter(java.util.Objects::nonNull)
                        // Add the filter for actual covers
                        .filter(this::isActualCover)
                        .collectList()
                    )
                    .map(books -> books.stream().limit(6).collect(Collectors.toList()))
                    .doOnSuccess(similarBooks -> model.addAttribute("similarBooks", similarBooks))
                    .doOnError(e -> {
                        logger.warn("Error fetching similar book recommendations for ID {}: {}", id, e.getMessage());
                        model.addAttribute("similarBooks", Collections.emptyList());
                    })
                    .onErrorReturn(Collections.emptyList());
                
                // If the main book was found, add its model attributes and then proceed with similar books
                if (fetchedBook != null) {
                    return similarBooksMono.thenReturn("book");
                } else {
                    // If main book was not found, show an empty page or an error
                    // The model.addAttribute("book", null) and error attribute would have been set by bookMono's doOnSuccess/doOnError
                    return similarBooksMono.thenReturn("book");
                }
            })
            .defaultIfEmpty("book") 
            .onErrorReturn("book")
        ); 
    }
    
    /**
     * Generates a comma-separated string of keywords for SEO
     * - Extracts keywords from book title, authors, and categories
     * - Removes duplicates and very short words
     * - Adds generic book-related terms
     * - Limits total keywords to prevent keyword stuffing
     * 
     * @param book The book object to generate keywords from
     * @return A comma-separated string of relevant keywords
     */
    private String generateKeywords(Book book) {
        if (book == null) {
            return "book, literature, reading"; // Default keywords
        }
        List<String> keywords = new ArrayList<>();
        if (book.getTitle() != null && !book.getTitle().isEmpty()) {
            keywords.addAll(Arrays.asList(book.getTitle().toLowerCase(Locale.ROOT).split("\\s+")));
        }
        if (book.getAuthors() != null && !book.getAuthors().isEmpty()) {
            for (String author : book.getAuthors()) {
                keywords.addAll(Arrays.asList(author.toLowerCase(Locale.ROOT).split("\\s+")));
            }
        }
        if (book.getCategories() != null && !book.getCategories().isEmpty()) {
            for (String category : book.getCategories()) {
                keywords.addAll(Arrays.asList(category.toLowerCase(Locale.ROOT).split("\\s+")));
            }
        }
        // Add some generic terms
        keywords.add("book");
        keywords.add("details");
        keywords.add("review");
        keywords.add("summary");

        // Remove duplicates and common short words, then join
        return keywords.stream()
                .distinct()
                .filter(kw -> kw.length() > 2) // Filter out very short words
                .limit(15) // Limit number of keywords
                .collect(Collectors.joining(", "));
    }

    private Mono<Book> fetchCanonicalBook(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            return Mono.empty();
        }

        Mono<Book> dbBySlug = Mono.justOrEmpty(bookDataOrchestrator.getBookFromDatabaseBySlug(identifier));
        Mono<Book> tieredBySlug = Mono.defer(() -> {
            Mono<Book> lookup = bookDataOrchestrator.getBookBySlugTiered(identifier);
            return lookup == null ? Mono.empty() : lookup;
        });

        Mono<Book> dbById = isLikelyUuid(identifier)
                ? Mono.justOrEmpty(bookDataOrchestrator.getBookFromDatabase(identifier))
                : Mono.empty();

        Mono<Book> tieredById = Mono.defer(() -> {
            Mono<Book> lookup = bookDataOrchestrator.getBookByIdTiered(identifier);
            return lookup == null ? Mono.empty() : lookup;
        });

        Mono<Book> chainForUuid = dbById
                .switchIfEmpty(dbBySlug)
                .switchIfEmpty(tieredBySlug)
                .switchIfEmpty(tieredById);

        Mono<Book> chainForSlugOrOther = dbBySlug
                .switchIfEmpty(tieredBySlug)
                .switchIfEmpty(dbById)
                .switchIfEmpty(tieredById);

        return isLikelyUuid(identifier) ? chainForUuid : chainForSlugOrOther;
    }

    private boolean isLikelyUuid(String identifier) {
        try {
            UUID.fromString(identifier);
            return true;
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    private Mono<List<Book>> fetchAdditionalHomepageBooks(String query, int limit) {
        return bookDataOrchestrator.searchBooksTiered(query, null, limit, null)
            .defaultIfEmpty(Collections.emptyList())
            .onErrorResume(ex -> {
                logger.warn("Postgres-first homepage filler lookup failed for query '{}': {}", query, ex.getMessage());
                return Mono.just(Collections.emptyList());
            })
            .flatMap(results -> {
                if (results != null && !results.isEmpty()) {
                    logger.debug("Homepage filler populated from Postgres for query '{}' ({} results).", query, results.size());
                    return Mono.just(results);
                }
                if (!googleFallbackEnabled) {
                    logger.debug("Postgres returned no homepage filler for query '{}', Google fallback disabled.", query);
                    return Mono.just(Collections.emptyList());
                }
                logger.debug("Postgres returned no homepage filler for query '{}', falling back to Google Books.", query);
                return googleBooksService.searchBooksAsyncReactive(query, null, limit, null)
                    .defaultIfEmpty(Collections.emptyList())
                    .onErrorResume(fallbackEx -> {
                        logger.error("Google fallback for homepage filler failed for query '{}': {}", query, fallbackEx.getMessage());
                        return Mono.just(Collections.emptyList());
                    });
            });
    }
    
    /**
     * Handle book lookup by ISBN (works with both ISBN-10 and ISBN-13 formats),
     * then redirect to the canonical URL with Google Book ID
     * 
     * @param isbn the book's ISBN (either ISBN-10 or ISBN-13)
     * @return redirect to canonical book URL or homepage if not found
     */
    @GetMapping("/book/isbn/{isbn}")
    public Mono<ResponseEntity<Void>> bookDetailByIsbn(@PathVariable String isbn) {
        // Sanitize input by removing non-ISBN characters
        String sanitized = IsbnUtils.sanitize(isbn);
        final String sanitizedIsbn = sanitized != null ? sanitized : "";
        
        // Validate ISBN format
        if (!isValidIsbn(sanitizedIsbn)) {
            logger.warn("Invalid ISBN format: {}", isbn);
            return Mono.just(redirectTo("/?error=invalidIsbn&originalIsbn=" + isbn));
        }
        
        return bookDataOrchestrator.getBookByIdTiered(sanitizedIsbn)
            .map(book -> {
                if (book == null) {
                    return redirectTo("/?info=bookNotFound&isbn=" + sanitizedIsbn);
                }

                String redirectTarget = book.getSlug();
                if (redirectTarget == null || redirectTarget.isBlank()) {
                    redirectTarget = book.getId();
                }

                if (redirectTarget != null && !redirectTarget.isBlank()) {
                    logger.info("Redirecting ISBN {} to canonical identifier: {}", sanitizedIsbn, redirectTarget);
                    return redirectTo("/book/" + redirectTarget);
                }

                logger.warn("Book fetched for ISBN {} but no canonical identifier available. Redirecting to homepage.", sanitizedIsbn);
                return redirectTo("/?info=bookNotFound&isbn=" + sanitizedIsbn);
            })
            .switchIfEmpty(Mono.fromSupplier(() -> {
                logger.warn("No book found for ISBN: {} (sanitized: {}), redirecting to homepage with notification.", isbn, sanitizedIsbn);
                return redirectTo("/?info=bookNotFound&isbn=" + sanitizedIsbn);
            }))
            .onErrorResume(e -> {
                logger.error("Error during ISBN lookup for {}: {}", isbn, e.getMessage(), e);
                return Mono.just(redirectTo("/?error=lookupError&isbn=" + sanitizedIsbn));
            });
    }
    
    /**
     * Validate if the provided string is a valid ISBN (either ISBN-10 or ISBN-13)
     * 
     * @param isbn ISBN to validate (should be pre-sanitized)
     * @return true if valid, false otherwise
     */
    private boolean isValidIsbn(String isbn) {
        if (isbn == null || isbn.isEmpty()) {
            return false;
        }
        return ISBN_ANY_PATTERN.matcher(isbn).matches();
    }

    private ResponseEntity<Void> redirectTo(String path) {
        return ResponseEntity.status(HttpStatus.SEE_OTHER)
            .location(URI.create(path))
            .build();
    }
    
    /**
     * Handle book lookup by ISBN-13, then redirect to the canonical URL with Google Book ID.
     * Kept for compatibility and explicit format specification
     * 
     * @param isbn13 the book's ISBN-13
     * @return redirect to canonical book URL or homepage if not found
     */
    @GetMapping("/book/isbn13/{isbn13}")
    public Mono<ResponseEntity<Void>> bookDetailByIsbn13(@PathVariable String isbn13) {
        // Sanitize input
        String sanitized = IsbnUtils.sanitize(isbn13);
        final String sanitizedIsbn = sanitized != null ? sanitized : "";
        
        // Validate ISBN-13 format specifically
        if (!ISBN13_PATTERN.matcher(sanitizedIsbn).matches()) {
            logger.warn("Invalid ISBN-13 format: {}", isbn13);
            return Mono.just(redirectTo("/?error=invalidIsbn13&originalIsbn=" + isbn13));
        }
        
        // Forward to the common ISBN handler
        return bookDetailByIsbn(sanitizedIsbn);
    }
    
    /**
     * Handle book lookup by ISBN-10, then redirect to the canonical URL with Google Book ID
     * Kept for compatibility and explicit format specification
     * 
     * @param isbn10 the book's ISBN-10
     * @return redirect to canonical book URL or homepage if not found
     */
    @GetMapping("/book/isbn10/{isbn10}")
    public Mono<ResponseEntity<Void>> bookDetailByIsbn10(@PathVariable String isbn10) {
        // Sanitize input
        String sanitized = IsbnUtils.sanitize(isbn10);
        final String sanitizedIsbn = sanitized != null ? sanitized : "";
        
        // Validate ISBN-10 format specifically
        if (!ISBN10_PATTERN.matcher(sanitizedIsbn).matches()) {
            logger.warn("Invalid ISBN-10 format: {}", isbn10);
            return Mono.just(redirectTo("/?error=invalidIsbn10&originalIsbn=" + isbn10));
        }
        
        // Forward to the common ISBN handler
        return bookDetailByIsbn(sanitizedIsbn);
    }

    @GetMapping("/explore")
    public ResponseEntity<Void> explore() {
        String selectedQuery = EXPLORE_QUERIES.get(RANDOM.nextInt(EXPLORE_QUERIES.size()));
        logger.info("Explore page requested, redirecting to search with query: '{}'", selectedQuery);
        try {
            String encodedQuery = URLEncoder.encode(selectedQuery, StandardCharsets.UTF_8.toString());
            return redirectTo("/search?query=" + encodedQuery + "&source=explore");
        } catch (java.io.UnsupportedEncodingException e) {
            logger.error("Error encoding query parameter for explore redirect: {}", selectedQuery, e);
            // Fallback to redirect without query or to an error page if critical
            return redirectTo("/search?source=explore&error=queryEncoding");
        }
    }
}
