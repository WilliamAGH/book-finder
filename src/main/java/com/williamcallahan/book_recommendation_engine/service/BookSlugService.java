/**
 * Service for generating and managing URL-friendly slugs for books
 *
 * @author William Callahan
 *
 * Features:
 * - Generates unique, SEO-friendly slugs from book titles and authors
 * - Ensures slug uniqueness across the entire book database
 * - Handles normalization of Unicode characters and special symbols
 * - Provides fallback mechanisms for books with missing titles
 * - Supports automatic slug assignment for new books
 * - Implements collision resolution with year and numerical suffixes
 */

package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
 

@Service
public class BookSlugService {

    private static final Logger logger = LoggerFactory.getLogger(BookSlugService.class);
    private static final Pattern NONLATIN = Pattern.compile("[^\\w-]");
    private static final Pattern WHITESPACE = Pattern.compile("[\\s]");
    private static final Pattern EDGESDHASHES = Pattern.compile("^-|-$");

    private final CachedBookRepository cachedBookRepository;

    public BookSlugService(CachedBookRepository cachedBookRepository) {
        this.cachedBookRepository = cachedBookRepository;
    }

    // Dedicated executor for repository calls, if not using reactive repositories
    // private final Executor ioExecutor = Executors.newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors() / 2));
    // Or use ForkJoinPool.commonPool() if tasks are short-lived or already async-capable.

    /**
     * Asynchronously generates a unique URL-friendly slug for the given book.
     *
     * @param book Book to generate slug for
     * @return CompletableFuture<String> containing the unique slug string
     */
    public CompletableFuture<String> generateSlugAsync(CachedBook book) {
        if (book == null || book.getTitle() == null || book.getTitle().trim().isEmpty()) {
            logger.warn("Cannot generate slug for book with null or empty title. Book ID: {}", book != null ? book.getId() : "null");
            String fallbackId = book != null && book.getId() != null ? book.getId() : java.util.UUID.randomUUID().toString();
            return CompletableFuture.completedFuture(fallbackId);
        }

        String titlePart = book.getTitle();
        List<String> authors = book.getAuthors();
        String yearPart = "";
        if (book.getPublishedDate() != null) {
            yearPart = book.getPublishedDate().format(DateTimeFormatter.ofPattern("yyyy"));
        }

        StringBuilder baseSlugBuilder = new StringBuilder();
        baseSlugBuilder.append(titlePart);

        if (authors != null && !authors.isEmpty()) {
            for (String author : authors) {
                if (author != null && !author.trim().isEmpty()) {
                    baseSlugBuilder.append("-").append(author.trim());
                }
            }
        }
        
        String initialProposedSlug = normalizeAndLowercase(baseSlugBuilder.toString());
        String slugWithYearBase = initialProposedSlug + (yearPart.isEmpty() ? "" : "-" + yearPart);

        // Start checking with the initial proposed slug
        return checkAndGenerateSlugRecursive(initialProposedSlug, slugWithYearBase, 1, book.getId(), book.getTitle());
    }

    private CompletableFuture<String> checkAndGenerateSlugRecursive(String currentSlugToTest, String baseSlugForSuffixing, int suffix, String bookId, String bookTitleForLog) {
        return isSlugUniqueAsync(currentSlugToTest, bookId)
            .thenComposeAsync(isUnique -> {
                if (isUnique) {
                    return CompletableFuture.completedFuture(currentSlugToTest);
                }
                // If not unique, and this was the first attempt (initialProposedSlug without year/suffix)
                // try with year before adding numerical suffixes.
                // Check if this is the first attempt and we haven't tried the year-based slug yet
                if (suffix == 1 && !currentSlugToTest.equals(baseSlugForSuffixing)) {
                     // Check if baseSlugForSuffixing (which includes year) is different and worth trying
                    if (!currentSlugToTest.equals(baseSlugForSuffixing)) { // This inner check might be redundant now depending on how baseSlugForSuffixing is constructed relative to currentSlugToTest initially
                        return checkAndGenerateSlugRecursive(baseSlugForSuffixing, baseSlugForSuffixing, 2, bookId, bookTitleForLog); // Start suffixing from 2 for year-based slug
                    }
                }

                // If currentSlugToTest was already year-based or initial slug with year failed, try with numerical suffix
                if (suffix > 100) { // Max attempts
                    logger.error("Could not generate a unique slug after 100 attempts for book title: {}", bookTitleForLog);
                    String fallbackSlug = baseSlugForSuffixing + "-" + java.util.UUID.randomUUID().toString().substring(0, 8);
                    return CompletableFuture.completedFuture(fallbackSlug);
                }
                String nextSlugToTest = baseSlugForSuffixing + "-" + suffix;
                return checkAndGenerateSlugRecursive(nextSlugToTest, baseSlugForSuffixing, suffix + 1, bookId, bookTitleForLog);
            });
    }


    /**
     * Asynchronously ensures the book has a slug, generating one if needed.
     *
     * @param book Book to ensure has a slug
     * @return CompletableFuture<Void>
     */
    public CompletableFuture<Void> ensureBookHasSlugAsync(CachedBook book) {
        if (book != null && (book.getSlug() == null || book.getSlug().trim().isEmpty())) {
            return generateSlugAsync(book)
                .thenAccept(generatedSlug -> {
                    book.setSlug(generatedSlug);
                    logger.debug("Generated slug '{}' for book ID '{}'", generatedSlug, book.getId());
                });
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Asynchronously checks if the given slug is unique for the specified book.
     *
     * @param slug Slug to check for uniqueness
     * @param currentBookUuid UUID of current book (to allow updating existing book)
     * @return CompletableFuture<Boolean> True if slug is unique or belongs to current book
     */
    private CompletableFuture<Boolean> isSlugUniqueAsync(String slug, String currentBookUuid) {
        // Assuming cachedBookRepository.findBySlug is blocking I/O
        return CompletableFuture.supplyAsync(() -> {
            Optional<CachedBook> conflictingBookOpt = cachedBookRepository.findBySlug(slug);

            if (conflictingBookOpt.isPresent()) {
                CachedBook conflictingBook = conflictingBookOpt.get();
                boolean isSameBook = conflictingBook.getId() != null && conflictingBook.getId().equals(currentBookUuid);
                if (!isSameBook) {
                    logger.debug("Slug '{}' is already in use by book ID '{}'. Current book ID is '{}'.", slug, conflictingBook.getId(), currentBookUuid);
                }
                return isSameBook;
            }
            return true;
        }/*, ioExecutor*/); // Optionally pass a dedicated I/O executor
    }

    /**
     * Normalizes text for slug generation by removing special characters and converting to lowercase
     *
     * @param text Text to normalize
     * @return Normalized slug-friendly text
     */
    private String normalizeAndLowercase(String text) {
        if (text == null || text.trim().isEmpty()) {
            return "";
        }
        String nowhitespace = WHITESPACE.matcher(text).replaceAll("-");
        String normalized = Normalizer.normalize(nowhitespace, Normalizer.Form.NFD);
        String slug = NONLATIN.matcher(normalized).replaceAll("");
        slug = EDGESDHASHES.matcher(slug).replaceAll("");
        return slug.toLowerCase(Locale.ENGLISH);
    }
}
