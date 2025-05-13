package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.GoogleBooksService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.service.RecommendationService;
import com.williamcallahan.book_recommendation_engine.service.image.BookCoverCacheService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.view.RedirectView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import java.util.regex.Pattern;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;

/**
 * Controller for handling user-facing web pages such as the homepage, search page, and book detail pages.
 * It integrates with various services to fetch book data and uses BookCoverCacheService
 * to provide optimized cover image URLs for display.
 */
@Controller
public class HomeController {

    private static final Logger logger = LoggerFactory.getLogger(HomeController.class);
    private final GoogleBooksService googleBooksService;
    private final RecentlyViewedService recentlyViewedService;
    private final RecommendationService recommendationService;
    private final BookCoverCacheService bookCoverCacheService; // Added field
    // The minimum number of books to display on the homepage
    private static final int MIN_BOOKS_TO_DISPLAY = 5;
    
    // Regex patterns for ISBN validation
    private static final Pattern ISBN10_PATTERN = Pattern.compile("^[0-9]{9}[0-9X]$");
    private static final Pattern ISBN13_PATTERN = Pattern.compile("^[0-9]{13}$");
    private static final Pattern ISBN_ANY_PATTERN = Pattern.compile("^[0-9]{9}[0-9X]$|^[0-9]{13}$");

    @Autowired
    public HomeController(GoogleBooksService googleBooksService, 
                          RecentlyViewedService recentlyViewedService, 
                          RecommendationService recommendationService,
                          BookCoverCacheService bookCoverCacheService) { // Added to constructor
        this.googleBooksService = googleBooksService;
        this.recentlyViewedService = recentlyViewedService;
        this.recommendationService = recommendationService;
        this.bookCoverCacheService = bookCoverCacheService; // Initialize field
    }

    /**
     * Handles requests to the home page.
     *
     * @param model the model for the view
     * @return the name of the template to render
     */
    @GetMapping("/")
    public Mono<String> home(Model model) {
        // Add recently viewed books to the model
        List<Book> recentBooks = recentlyViewedService.getRecentlyViewedBooks(); // This is synchronous
        model.addAttribute("activeTab", "home"); // Set early

        Mono<List<Book>> recentBooksMono;

        if (recentBooks.size() < MIN_BOOKS_TO_DISPLAY) {
            recentBooksMono = googleBooksService.searchBooksAsyncReactive("Java programming")
                .map(defaultBooks -> {
                    List<Book> combinedBooks = new ArrayList<>(recentBooks);
                    List<Book> booksToAdd = (defaultBooks == null) ? Collections.emptyList() : defaultBooks;
                    for (Book defaultBook : booksToAdd) {
                        if (combinedBooks.size() >= MIN_BOOKS_TO_DISPLAY) break;
                        if (!combinedBooks.stream().anyMatch(rb -> rb.getId().equals(defaultBook.getId()))) {
                            combinedBooks.add(defaultBook);
                        }
                    }
                    return combinedBooks;
                })
                .defaultIfEmpty(recentBooks); // If google books call fails or empty, use original recentBooks
        } else {
            recentBooksMono = Mono.just(recentBooks);
        }

        return recentBooksMono.map(finalRecentBooks -> {
            // Process cover images for all books to be displayed
            for (Book book : finalRecentBooks) {
                if (book != null) {
                    try {
                        String coverUrl = bookCoverCacheService.getInitialCoverUrlAndTriggerBackgroundUpdate(book);
                        book.setCoverImageUrl(coverUrl);
                        book.setCoverImageWidth(null);
                        book.setCoverImageHeight(null);
                        book.setIsCoverHighResolution(null);
                    } catch (Exception e) {
                        String identifierForLog = (book.getIsbn13() != null) ? book.getIsbn13() :
                                                 ((book.getIsbn10() != null) ? book.getIsbn10() : book.getId());
                        logger.warn("Error getting initial cover URL for book with identifier '{}': {}", identifierForLog, e.getMessage());
                        if (book.getCoverImageUrl() == null || book.getCoverImageUrl().isEmpty()) {
                           book.setCoverImageUrl("/images/placeholder-book-cover.svg");
                        }
                    }
                }
            }
            model.addAttribute("recentBooks", finalRecentBooks);
            return "index"; // Return template name
        });
    }
    
    /**
     * Handles requests to the search results page.
     *
     * @param query the search query
     * @param model the model for the view
     * @return the name of the template to render
     */
    @GetMapping("/search")
    public String search(String query, Model model) {
        model.addAttribute("query", query);
        model.addAttribute("activeTab", "search");
        return "search";
    }
    
    /**
     * Handles requests to the book detail page.
     *
     * @param id the book id
     * @param model the model for the view
     * @return the name of the template to render
     */
    @GetMapping("/book/{id}")
    public Mono<String> bookDetail(@PathVariable String id,
                             @RequestParam(required = false) String query,
                             @RequestParam(required = false, defaultValue = "0") int page,
                             @RequestParam(required = false, defaultValue = "relevance") String sort,
                             @RequestParam(required = false, defaultValue = "grid") String view,
                             Model model) {
        logger.info("Looking up book with ID: {}", id);
        model.addAttribute("activeTab", "book"); // Set early, not dependent on async data
        model.addAttribute("searchQuery", query);
        model.addAttribute("searchPage", page);
        model.addAttribute("searchSort", sort);
        model.addAttribute("searchView", view);

        Mono<Book> bookMono = googleBooksService.getBookById(id)
            .doOnSuccess(book -> {
                if (book != null) {
                    model.addAttribute("book", book);
                    try {
                        recentlyViewedService.addToRecentlyViewed(book);
                    } catch (Exception e) {
                        logger.warn("Failed to add book to recently viewed: {}", e.getMessage());
                    }
                } else {
                    logger.info("No book found with ID: {}", id);
                    model.addAttribute("book", null); // Ensure model has book attribute even if null
                }
            })
            .doOnError(e -> {
                 logger.error("Error getting book with ID: {}", id, e);
                 model.addAttribute("error", "An error occurred while retrieving this book. Please try again later.");
                 model.addAttribute("book", null);
            })
            .onErrorResume(e -> Mono.empty()); // Continue to similar books even if main book fails, page will show error

        // Fetch similar books. This needs to be chained after bookMono resolves (or in parallel if appropriate)
        // For simplicity in model population, let's chain it.
        return bookMono
            .flatMap(fetchedBook -> // fetchedBook can be null if getBookById resulted in empty Mono and was caught by doOnError
                recommendationService.getSimilarBooks(id, 6)
                    .doOnSuccess(similarBooks -> model.addAttribute("similarBooks", similarBooks))
                    .doOnError(e -> {
                        logger.warn("Error fetching similar book recommendations for ID {}: {}", id, e.getMessage());
                        model.addAttribute("similarBooks", Collections.emptyList());
                    })
                    .onErrorReturn(Collections.emptyList()) // Return empty list on error to allow page rendering
                    .thenReturn("book") // Return template name after this step
            )
            .defaultIfEmpty("book") // If bookMono was empty (e.g. book not found and error handled), still return template name
            .onErrorReturn("book"); // Fallback template name on any other unexpected error in the chain
    }
    
    /**
     * Handle book lookup by ISBN (works with both ISBN-10 and ISBN-13 formats),
     * then redirect to the canonical URL with Google Book ID.
     * 
     * @param isbn the book's ISBN (either ISBN-10 or ISBN-13)
     * @return redirect to canonical book URL or homepage if not found
     */
    @GetMapping("/book/isbn/{isbn}")
    public RedirectView bookDetailByIsbn(@PathVariable String isbn) {
        // Sanitize input by removing hyphens and spaces
        String sanitizedIsbn = sanitizeIsbn(isbn);
        
        // Validate ISBN format
        if (!isValidIsbn(sanitizedIsbn)) {
            logger.warn("Invalid ISBN format: {}", isbn);
            return new RedirectView("/?invalidIsbn=true");
        }
        
        try {
            logger.info("Looking up book by ISBN: {}", sanitizedIsbn);
            // Call reactive method and block to get the list, then take the first.
            List<Book> books = googleBooksService.searchBooksByISBN(sanitizedIsbn).blockOptional().orElse(Collections.emptyList());
            Book book = books.stream().findFirst().orElse(null);
            
            if (book != null && book.getId() != null) {
                logger.info("Found book with ID: {} for ISBN: {}", book.getId(), sanitizedIsbn);
                return new RedirectView("/book/" + book.getId());
            } else {
                logger.info("No book found for ISBN: {}", sanitizedIsbn);
                return new RedirectView("/?bookNotFound=true");
            }
        } catch (Exception e) {
            logger.error("Error looking up book by ISBN: {}", sanitizedIsbn, e);
            // In case of any error, redirect to home page with an error parameter
            return new RedirectView("/?lookupError=true");
        }
    }
    
    /**
     * Sanitize ISBN by removing hyphens, spaces, and converting to uppercase
     * 
     * @param isbn Raw ISBN input
     * @return Sanitized ISBN
     */
    private String sanitizeIsbn(String isbn) {
        if (isbn == null) {
            return "";
        }
        return isbn.replaceAll("[\\s-]", "").toUpperCase();
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
    
    /**
     * Handle book lookup by ISBN-13, then redirect to the canonical URL with Google Book ID.
     * Kept for compatibility and explicit format specification.
     * 
     * @param isbn13 the book's ISBN-13
     * @return redirect to canonical book URL or homepage if not found
     */
    @GetMapping("/book/isbn13/{isbn13}")
    public RedirectView bookDetailByIsbn13(@PathVariable String isbn13) {
        // Sanitize input
        String sanitizedIsbn = sanitizeIsbn(isbn13);
        
        // Validate ISBN-13 format specifically
        if (!ISBN13_PATTERN.matcher(sanitizedIsbn).matches()) {
            logger.warn("Invalid ISBN-13 format: {}", isbn13);
            return new RedirectView("/?invalidIsbn13=true");
        }
        
        // Forward to the common ISBN handler
        return bookDetailByIsbn(sanitizedIsbn);
    }
    
    /**
     * Handle book lookup by ISBN-10, then redirect to the canonical URL with Google Book ID.
     * Kept for compatibility and explicit format specification.
     * 
     * @param isbn10 the book's ISBN-10
     * @return redirect to canonical book URL or homepage if not found
     */
    @GetMapping("/book/isbn10/{isbn10}")
    public RedirectView bookDetailByIsbn10(@PathVariable String isbn10) {
        // Sanitize input
        String sanitizedIsbn = sanitizeIsbn(isbn10);
        
        // Validate ISBN-10 format specifically
        if (!ISBN10_PATTERN.matcher(sanitizedIsbn).matches()) {
            logger.warn("Invalid ISBN-10 format: {}", isbn10);
            return new RedirectView("/?invalidIsbn10=true");
        }
        
        // Forward to the common ISBN handler
        return bookDetailByIsbn(sanitizedIsbn);
    }
}
