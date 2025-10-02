package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.dto.BookCard;
import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.dto.BookListItem;
import com.williamcallahan.book_recommendation_engine.dto.RecommendationCard;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.service.AffiliateLinkService;
import com.williamcallahan.book_recommendation_engine.service.BookIdentifierResolver;
import com.williamcallahan.book_recommendation_engine.service.BookSearchService;
import com.williamcallahan.book_recommendation_engine.service.DuplicateBookService;
import com.williamcallahan.book_recommendation_engine.service.EnvironmentService;
import com.williamcallahan.book_recommendation_engine.service.NewYorkTimesService;
import com.williamcallahan.book_recommendation_engine.service.RecentlyViewedService;
import com.williamcallahan.book_recommendation_engine.service.image.LocalDiskCoverCacheService;
import com.williamcallahan.book_recommendation_engine.util.ApplicationConstants;
import com.williamcallahan.book_recommendation_engine.model.image.CoverImages;
import com.williamcallahan.book_recommendation_engine.util.BookDomainMapper;
import com.williamcallahan.book_recommendation_engine.util.IsbnUtils;
import com.williamcallahan.book_recommendation_engine.util.PagingUtils;
import com.williamcallahan.book_recommendation_engine.util.SeoUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.util.UriComponentsBuilder;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.Objects;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Controller
@Slf4j
public class HomeController {

    private final RecentlyViewedService recentlyViewedService;
    private final EnvironmentService environmentService;
    private final DuplicateBookService duplicateBookService;
    private final LocalDiskCoverCacheService localDiskCoverCacheService;
    private final NewYorkTimesService newYorkTimesService;
    private final AffiliateLinkService affiliateLinkService;
    private final BookQueryRepository bookQueryRepository;
    private final BookSearchService bookSearchService;
    private final BookIdentifierResolver bookIdentifierResolver;
    private final boolean isYearFilteringEnabled;

    private static final int MAX_RECENT_BOOKS = 8;
    private static final int MAX_BESTSELLERS = 8;
    
    private static final List<String> EXPLORE_QUERIES = List.of(
        "Classic literature", "Modern thrillers", "Space opera adventures", "Historical fiction bestsellers",
        "Award-winning science fiction", "Inspiring biographies", "Mind-bending philosophy", "Beginner's cookbooks",
        "Epic fantasy sagas", "Cyberpunk futures", "Cozy mysteries", "Environmental science",
        "Artificial intelligence ethics", "World mythology", "Travel memoirs"
    );
    private static final Pattern YEAR_PATTERN = Pattern.compile("\\b(19\\d{2}|20\\d{2})\\b");

    // Regex patterns for ISBN validation
    @Value("${app.seo.max-description-length:170}")
    private int maxDescriptionLength;

    // Affiliate IDs from properties
    /**
     * Constructs the HomeController with required services
     *
     * @param recentlyViewedService Service for tracking user book view history
     * @param environmentService Service providing environment configuration information
     * @param duplicateBookService Service for handling duplicate book editions
     * @param bookSearchService Service for search projections
     * @param bookIdentifierResolver Resolver for canonical identifiers
     */
    public HomeController(RecentlyViewedService recentlyViewedService,
                          EnvironmentService environmentService,
                          DuplicateBookService duplicateBookService,
                          LocalDiskCoverCacheService localDiskCoverCacheService,
                          @Value("${app.feature.year-filtering.enabled:false}") boolean isYearFilteringEnabled,
                          NewYorkTimesService newYorkTimesService,
                          AffiliateLinkService affiliateLinkService,
                          BookQueryRepository bookQueryRepository,
                          BookSearchService bookSearchService,
                          BookIdentifierResolver bookIdentifierResolver) {
        this.recentlyViewedService = recentlyViewedService;
        this.environmentService = environmentService;
        this.duplicateBookService = duplicateBookService;
        this.localDiskCoverCacheService = localDiskCoverCacheService;
        this.isYearFilteringEnabled = isYearFilteringEnabled;
        this.newYorkTimesService = newYorkTimesService;
        this.affiliateLinkService = affiliateLinkService;
        this.bookQueryRepository = bookQueryRepository;
        this.bookSearchService = bookSearchService;
        this.bookIdentifierResolver = bookIdentifierResolver;
    }

    private void applyBaseAttributes(Model model, String activeTab) {
        model.addAttribute("isDevelopmentMode", environmentService.isDevelopmentMode());
        model.addAttribute("currentEnv", environmentService.getCurrentEnvironmentMode());
        model.addAttribute("activeTab", activeTab);
    }

    private void applySeo(Model model,
                          String title,
                          String description,
                          String canonicalPath,
                          String keywords,
                          String ogImage) {
        model.addAttribute("title", title);
        model.addAttribute("description", description);
        model.addAttribute("canonicalUrl", canonicalPath.startsWith("http")
            ? canonicalPath
            : ApplicationConstants.Urls.BASE_URL + canonicalPath);
        model.addAttribute("keywords", keywords);
        model.addAttribute("ogImage", ogImage);
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
        applyBaseAttributes(model, "home");
        applySeo(
            model,
            "Home",
            "Discover your next favorite book with our recommendation engine. Explore recently viewed books and new arrivals.",
            ApplicationConstants.Urls.BASE_URL + "/",
            "book recommendations, find books, book suggestions, reading, literature, home",
            ApplicationConstants.Urls.DEFAULT_SOCIAL_IMAGE
        );

        // Set default empty collections for immediate rendering
        model.addAttribute("currentBestsellers", List.<BookCard>of());
        model.addAttribute("recentBooks", List.<BookCard>of());

        // Fetch with increased timeout (3s) for Postgres queries - prevents premature timeouts
        Mono<List<BookCard>> bestsellers = loadCurrentBestsellers()
            .timeout(Duration.ofMillis(3000))
            .onErrorResume(e -> {
                if (e instanceof java.util.concurrent.TimeoutException) {
                    log.warn("Bestsellers timed out after 3000ms");
                } else {
                    log.warn("Bestsellers failed: {}", e.getMessage());
                }
                // Return partial results if available instead of empty
                return Mono.just(List.<BookCard>of());
            })
            .doOnNext(list -> {
                model.addAttribute("currentBestsellers", list);
                if (!list.isEmpty()) {
                    log.debug("Homepage: Loaded {} bestsellers successfully", list.size());
                }
            });

        Mono<List<BookCard>> recentBooks = loadRecentBooks()
            .timeout(Duration.ofMillis(3000))
            .onErrorResume(e -> {
                if (e instanceof java.util.concurrent.TimeoutException) {
                    log.warn("Recent books timed out after 3000ms");
                } else {
                    log.warn("Recent books failed: {}", e.getMessage());
                }
                // Return partial results if available instead of empty
                return Mono.just(List.<BookCard>of());
            })
            .doOnNext(list -> {
                model.addAttribute("recentBooks", list);
                if (!list.isEmpty()) {
                    log.debug("Homepage: Loaded {} recent books successfully", list.size());
                }
            });

        // Use zipDelayError to allow partial failures without blocking entire page
        return Mono.zipDelayError(bestsellers, recentBooks)
            .then(Mono.just("index"))
            .onErrorResume(e -> {
                log.error("Critical error loading homepage sections: {}", e.getMessage());
                return Mono.just("index"); // Render with whatever data we have
            });
    }


    /**
     * Load current NYT bestsellers using optimized BookCard DTOs.
     * SINGLE QUERY replaces 40 queries (8 books × 5 queries each).
     */
    private Mono<List<BookCard>> loadCurrentBestsellers() {
        return newYorkTimesService.getCurrentBestSellersCards("hardcover-fiction", MAX_BESTSELLERS)
            .map(cards -> cards.stream().limit(MAX_BESTSELLERS).collect(Collectors.toList()))
            .onErrorResume(e -> {
                log.error("Error fetching current bestsellers: {}", e.getMessage());
                return Mono.just(List.<BookCard>of());
            });
    }

    /**
     * Load recently viewed books using optimized BookCard DTOs.
     * SINGLE QUERY replaces N queries (1 query per book).
     */
    private Mono<List<BookCard>> loadRecentBooks() {
        return Mono.fromCallable(() -> {
            // Get recently viewed book IDs
            List<String> bookIds = recentlyViewedService.getRecentlyViewedBookIds(MAX_RECENT_BOOKS);
            
            if (bookIds.isEmpty()) {
                log.debug("No recently viewed books, returning empty list");
                return List.<BookCard>of();
            }
            
            // Convert String IDs to UUIDs
            List<UUID> uuids = bookIds.stream()
                .map(id -> {
                    try {
                        return UUID.fromString(id);
                    } catch (IllegalArgumentException e) {
                        log.warn("Invalid UUID in recently viewed: {}", id);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            
            if (uuids.isEmpty()) {
                return List.<BookCard>of();
            }
            
            // Fetch as BookCard DTOs with single query
            List<BookCard> cards = bookQueryRepository.fetchBookCards(uuids);
            log.debug("Loaded {} recent books as BookCard DTOs", cards.size());
            return cards;
        })
        .subscribeOn(Schedulers.boundedElastic())
        .onErrorResume(e -> {
            log.error("Error loading recent books: {}", e.getMessage());
            return Mono.just(List.<BookCard>of());
        });
    }

    private Mono<List<Book>> loadSimilarBooks(String bookId) {
        final int limit = ApplicationConstants.Paging.DEFAULT_TIERED_LIMIT / 2;
        return Mono.fromCallable(() -> bookIdentifierResolver.resolveToUuid(bookId))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(optionalUuid -> {
                if (optionalUuid.isEmpty()) {
                    return Mono.just(List.<Book>of());
                }
                UUID uuid = optionalUuid.get();
                return Mono.defer(() -> {
                        List<RecommendationCard> cards = bookQueryRepository.fetchRecommendationCards(uuid, limit);
                        if (cards == null || cards.isEmpty()) {
                            return Mono.just(List.<Book>of());
                        }
                        List<Book> mapped = new ArrayList<>(cards.size());
                        for (RecommendationCard card : cards) {
                            if (card == null || card.card() == null) {
                                continue;
                            }
                            Book book = BookDomainMapper.fromCard(card.card());
                            if (book != null) {
                                ensureCoverDefaults(book);
                                mapped.add(book);
                            }
                            if (mapped.size() >= 6) {
                                break;
                            }
                        }
                        return Mono.just(mapped);
                    })
                    .subscribeOn(Schedulers.boundedElastic());
            })
            .timeout(Duration.ofMillis(1500))
            .onErrorResume(e -> {
                if (e instanceof java.util.concurrent.TimeoutException) {
                    log.warn("Similar books timed out after 1500ms for book {}", bookId);
                } else {
                    log.warn("Similar books failed for book {}: {}", bookId, e.getMessage());
                }
                return Mono.just(List.<Book>of());
            });
    }

    private List<Book> searchBooksForView(String query, int totalRequested) {
        List<BookSearchService.SearchResult> results = bookSearchService.searchBooks(query, totalRequested);
        if (results == null || results.isEmpty()) {
            return List.of();
        }

        List<UUID> bookIds = results.stream()
            .map(BookSearchService.SearchResult::bookId)
            .filter(Objects::nonNull)
            .toList();

        if (bookIds.isEmpty()) {
            return List.of();
        }

        List<BookListItem> items = bookQueryRepository.fetchBookListItems(bookIds);
        Map<String, BookListItem> itemsById = items.stream()
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(BookListItem::id, Function.identity(), (first, second) -> first, LinkedHashMap::new));

        List<Book> books = new ArrayList<>(results.size());
        for (BookSearchService.SearchResult result : results) {
            UUID bookId = result.bookId();
            if (bookId == null) {
                continue;
            }
            BookListItem item = itemsById.get(bookId.toString());
            if (item == null) {
                continue;
            }
            Book book = BookDomainMapper.fromListItem(item);
            ensureCoverDefaults(book);
            books.add(book);
        }

        return books;
    }

    private void ensureCoverDefaults(Book book) {
        if (book == null) {
            return;
        }
        String placeholder = ApplicationConstants.Cover.PLACEHOLDER_IMAGE_PATH;
        if (!ValidationUtils.hasText(book.getS3ImagePath())) {
            book.setS3ImagePath(placeholder);
        }
        if (!ValidationUtils.hasText(book.getExternalImageUrl())) {
            book.setExternalImageUrl(book.getS3ImagePath());
        }
        if (book.getCoverImages() == null) {
            book.setCoverImages(new CoverImages(book.getS3ImagePath(), book.getS3ImagePath(), null));
        }
    }

    private Mono<Book> locateBook(String identifier) {
        if (!ValidationUtils.hasText(identifier)) {
            return Mono.empty();
        }
        String trimmed = identifier.trim();
        return Mono.fromCallable(() -> findBook(trimmed))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(book -> book == null ? Mono.empty() : Mono.just(book));
    }

    private Book findBook(String identifier) {
        Optional<BookDetail> bySlug = bookQueryRepository.fetchBookDetailBySlug(identifier);
        if (bySlug.isPresent()) {
            Book book = BookDomainMapper.fromDetail(bySlug.get());
            ensureCoverDefaults(book);
            return book;
        }

        Optional<UUID> maybeUuid = bookIdentifierResolver.resolveToUuid(identifier);
        if (maybeUuid.isEmpty()) {
            return null;
        }
        UUID uuid = maybeUuid.get();

        Optional<BookDetail> detail = bookQueryRepository.fetchBookDetail(uuid);
        if (detail.isPresent()) {
            Book book = BookDomainMapper.fromDetail(detail.get());
            ensureCoverDefaults(book);
            return book;
        }

        return bookQueryRepository.fetchBookCard(uuid)
            .map(BookDomainMapper::fromCard)
            .map(book -> {
                ensureCoverDefaults(book);
                return book;
            })
            .orElse(null);
    }

    private void applyBookMetadata(Book book, Model model) {
        model.addAttribute("book", book);
        
        // Load duplicate editions asynchronously after setting model attribute
        // This prevents blocking the render path
        try {
            duplicateBookService.populateDuplicateEditions(book);
        } catch (Exception ex) {
            log.warn("Failed to populate duplicate editions for book {}: {}", book.getId(), ex.getMessage());
        }

        String title = ValidationUtils.hasText(book.getTitle()) ? book.getTitle() : "Book Details";
        String description = SeoUtils.truncateDescription(book.getDescription(), maxDescriptionLength);
        String canonicalIdentifier = ValidationUtils.hasText(book.getSlug()) ? book.getSlug() : book.getId();
        String canonicalUrl = ApplicationConstants.Urls.BASE_URL + "/book/" + canonicalIdentifier;
        String keywords = SeoUtils.generateKeywords(book);
        String ogImage = resolveOgImage(book);

        applySeo(model, title, description, canonicalUrl, keywords, ogImage);
        model.addAttribute("affiliateLinks", affiliateLinkService.generateLinks(book));

        try {
            recentlyViewedService.addToRecentlyViewed(book);
        } catch (Exception ex) {
            log.warn("Failed to add book {} to recently viewed: {}", book.getId(), ex.getMessage());
        }
    }

    /**
     * @deprecated Compute OG images from {@link com.williamcallahan.book_recommendation_engine.model.image.CoverImages}
     * using {@link com.williamcallahan.book_recommendation_engine.util.cover.UrlSourceDetector} rather than
     * {@link ValidationUtils.BookValidator} helpers.
     */
    @Deprecated(since = "2025-10-01", forRemoval = true)
    private String resolveOgImage(Book book) {
        String coverUrl = book.getS3ImagePath();
        if (ValidationUtils.BookValidator.hasActualCover(coverUrl, localDiskCoverCacheService.getLocalPlaceholderPath())) {
            return coverUrl;
        }
        return ApplicationConstants.Urls.OG_LOGO;
    }

    /**
     * Handles requests to the search results page
     * - Server-renders initial search results for immediate display
     * - Sets up model attributes for search view
     * - Configures SEO metadata for search page
     * - JavaScript handles pagination and filtering (progressive enhancement)
     *
     * @param query The search query string from user input
     * @param year Optional publication year filter
     * @param page Page number for pagination (0-indexed internally, 1-indexed in UI)
     * @param sort Sort order (relevance, title, author, newest, rating)
     * @param model The model for the view
     * @return Mono containing the template name for async rendering
     */
    @GetMapping("/search")
    public Mono<String> search(@RequestParam(required = false) String query,
                               @RequestParam(required = false) Integer year,
                               @RequestParam(required = false, defaultValue = "0") int page,
                               @RequestParam(required = false, defaultValue = "newest") String sort,
                               Model model) {
        // Handle year extraction from query if enabled
        if (isYearFilteringEnabled && year == null && ValidationUtils.hasText(query)) {
            Matcher matcher = YEAR_PATTERN.matcher(query);
            if (matcher.find()) {
                int extractedYear = Integer.parseInt(matcher.group(1));
                log.info("Detected year {} in query text. Redirecting to use year parameter.", extractedYear);

                String processedQuery = (query.substring(0, matcher.start()) + query.substring(matcher.end()))
                    .trim()
                    .replaceAll("\\s+", " ");

                if (!ValidationUtils.hasText(processedQuery)) {
                    return Mono.just("redirect:/search?year=" + extractedYear);
                }
                return Mono.just("redirect:/search?query="
                    + URLEncoder.encode(processedQuery, StandardCharsets.UTF_8) + "&year=" + extractedYear);
            }
        }

        Integer effectiveYear = isYearFilteringEnabled ? year : null;

        applyBaseAttributes(model, "search");
        model.addAttribute("query", query);
        model.addAttribute("year", effectiveYear);
        model.addAttribute("currentPage", page);
        model.addAttribute("currentSort", sort);
        model.addAttribute("isYearFilteringEnabled", isYearFilteringEnabled);

        applySeo(
            model,
            "Search Books",
            "Search our extensive catalog of books by title, author, or ISBN. Find detailed information and recommendations.",
            ApplicationConstants.Urls.BASE_URL + "/search",
            "book search, find books by title, find books by author, isbn lookup, book catalog",
            ApplicationConstants.Urls.DEFAULT_SOCIAL_IMAGE
        );

        // Server-render initial results if query provided
        if (ValidationUtils.hasText(query)) {
            final int pageSize = 12; // Default page size
            final int safePage = Math.max(page, 0);
            final int safeStart = Math.multiplyExact(safePage, pageSize);
            final PagingUtils.Window window = PagingUtils.window(
                safeStart,
                pageSize,
                pageSize,
                pageSize,
                pageSize,
                safeStart + pageSize
            );

            return Mono.fromCallable(() -> searchBooksForView(query, window.totalRequested()))
                .subscribeOn(Schedulers.boundedElastic())
                .map(results -> {
                    List<Book> windowed = PagingUtils.slice(results, window.startIndex(), window.limit());
                    model.addAttribute("initialResults", windowed);
                    model.addAttribute("hasInitialResults", !windowed.isEmpty());
                    model.addAttribute("totalResults", results != null ? results.size() : 0);
                    model.addAttribute("initialResultsError", null);
                    log.info("Server-rendered {} search results for query '{}'", windowed.size(), query);
                    return "search";
                })
                .onErrorResume(e -> {
                    String reason;
                    if (e instanceof java.util.concurrent.TimeoutException) {
                        reason = "Server-side search timed out before results were ready";
                        log.warn("Timeout server-rendering search results for '{}': {}", query, e.getMessage());
                    } else {
                        reason = e.getMessage() != null ? e.getMessage() : e.toString();
                        log.warn("Error server-rendering search results for '{}': {}", query, reason, e);
                    }
                    model.addAttribute("initialResults", List.of());
                    model.addAttribute("hasInitialResults", false);
                    model.addAttribute("totalResults", 0);
                    model.addAttribute("initialResultsError", reason);
                    return Mono.just("search");
                });
        }

        // No query - show search page with no results
        model.addAttribute("initialResults", List.of());
        model.addAttribute("hasInitialResults", false);
        return Mono.just("search");
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
        log.info("Looking up book with ID: {}", id);

        applyBaseAttributes(model, "book");
        model.addAttribute("searchQuery", query);
        model.addAttribute("searchPage", page);
        model.addAttribute("searchSort", sort);
        model.addAttribute("searchView", view);

        applySeo(
            model,
            "Book Details",
            "Detailed information about the selected book.",
            ApplicationConstants.Urls.BASE_URL + "/book/" + id,
            "book, literature, reading, book details",
            ApplicationConstants.Urls.OG_LOGO
        );

        Mono<Book> canonicalBookMono = locateBook(id).cache();
        Mono<Book> effectiveBookMono = canonicalBookMono;

        Mono<String> redirectIfNonCanonical = effectiveBookMono
            .flatMap(book -> {
                if (book == null) {
                    return Mono.empty();
                }
                String canonicalIdentifier = ValidationUtils.hasText(book.getSlug()) ? book.getSlug() : book.getId();
                if (!ValidationUtils.hasText(canonicalIdentifier) || canonicalIdentifier.equals(id)) {
                    return Mono.empty();
                }
                var builder = UriComponentsBuilder.fromPath("/book/" + canonicalIdentifier);
                if (ValidationUtils.hasText(query)) {
                    builder.queryParam("query", query);
                }
                if (page > 0) {
                    builder.queryParam("page", page);
                }
                if (ValidationUtils.hasText(sort)) {
                    builder.queryParam("sort", sort);
                }
                if (ValidationUtils.hasText(view)) {
                    builder.queryParam("view", view);
                }
                return Mono.just("redirect:" + builder.build().toUriString());
            });

        // Only display books that exist in our database
        Mono<Book> resolvedBookMono = effectiveBookMono.cache();

        // Load similar books in parallel with increased timeout to allow Postgres queries to complete
        Mono<List<Book>> similarBooksMono = resolvedBookMono
            .flatMap(book -> loadSimilarBooks(book != null ? book.getId() : id)
                .timeout(Duration.ofMillis(2000)) // Increased from 300ms to 2000ms
                .onErrorResume(e -> {
                    if (e instanceof java.util.concurrent.TimeoutException) {
                        log.warn("Similar books timed out after 2000ms for {}", id);
                    } else {
                        log.warn("Similar books failed for {}: {}", id, e.getMessage());
                    }
                    return Mono.just(List.<Book>of());
                }))
            .defaultIfEmpty(List.<Book>of())
            .doOnNext(list -> model.addAttribute("similarBooks", list))
            .cache();

        return redirectIfNonCanonical.switchIfEmpty(
            resolvedBookMono.flatMap(book -> {
                if (book == null) {
                    model.addAttribute("book", null);
                    model.addAttribute("error", "Unable to locate this book right now. Please try again later.");
                    return similarBooksMono.thenReturn("book");
                }

                applyBookMetadata(book, model);
                return similarBooksMono.thenReturn("book");
            }).switchIfEmpty(similarBooksMono.thenReturn("book"))
        ).onErrorResume(e -> {
            log.error("Error rendering book detail for {}: {}", id, e.getMessage(), e);
            model.addAttribute("error", "An error occurred while retrieving this book. Please try again later.");
            model.addAttribute("book", null);
            return Mono.just("book");
        });
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
    /**
     * Handle book lookup by ISBN (works with both ISBN-10 and ISBN-13 formats),
     * then redirect to the canonical URL with Google Book ID
     * 
     * @param isbn the book's ISBN (either ISBN-10 or ISBN-13)
     * @return redirect to canonical book URL or homepage if not found
     */
    @GetMapping("/book/isbn/{isbn}")
    public Mono<ResponseEntity<Void>> bookDetailByIsbn(@PathVariable String isbn) {
        return redirectByIsbn(isbn, value -> IsbnUtils.isValidIsbn13(value) || IsbnUtils.isValidIsbn10(value), "invalidIsbn");
    }
    
    private Mono<ResponseEntity<Void>> redirectByIsbn(String rawIsbn,
                                                      Predicate<String> validator,
                                                      String errorCode) {
        String sanitized = IsbnUtils.sanitize(rawIsbn);
        if (!ValidationUtils.hasText(sanitized) || !validator.test(sanitized)) {
            log.warn("Invalid ISBN format: {}", rawIsbn);
            return Mono.just(redirectTo(String.format("/?error=%s&originalIsbn=%s", errorCode, rawIsbn)));
        }
        Mono<Book> lookupMono = locateBook(sanitized);

        return lookupMono
            .map(book -> book == null ? null : (ValidationUtils.hasText(book.getSlug()) ? book.getSlug() : book.getId()))
            .filter(ValidationUtils::hasText)
            .map(target -> redirectTo("/book/" + target))
            .switchIfEmpty(Mono.fromSupplier(() -> redirectTo("/?info=bookNotFound&isbn=" + sanitized)))
            .onErrorResume(e -> {
                log.error("Error during ISBN lookup for {}: {}", rawIsbn, e.getMessage(), e);
                return Mono.just(redirectTo("/?error=lookupError&isbn=" + sanitized));
            });
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
        return redirectByIsbn(isbn13, IsbnUtils::isValidIsbn13, "invalidIsbn13");
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
        return redirectByIsbn(isbn10, IsbnUtils::isValidIsbn10, "invalidIsbn10");
    }

    @GetMapping("/explore")
    public ResponseEntity<Void> explore() {
        String selectedQuery = EXPLORE_QUERIES.get(ThreadLocalRandom.current().nextInt(EXPLORE_QUERIES.size()));
        log.info("Explore page requested, redirecting to search with query: '{}'", selectedQuery);
        String encodedQuery = URLEncoder.encode(selectedQuery, StandardCharsets.UTF_8);
        return redirectTo("/search?query=" + encodedQuery + "&source=explore");
    }
}
