package com.williamcallahan.book_recommendation_engine.service.image;

import com.williamcallahan.book_recommendation_engine.model.Book;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.springframework.beans.factory.annotation.Value;
import java.util.Comparator;
import java.io.IOException;

/**
 * Integration test for the BookCoverCacheService
 *
 * @author William Callahan
 *
 * Features:
 * - Tests the end-to-end book cover retrieval flow
 * - Verifies initial cover loading and background processing
 * - Checks S3 upload functionality
 * - Tests multiple book scenarios (known good, likely placeholder, new books)
 * - Validates cache directory operations
 */
@SpringBootTest
@ActiveProfiles("test") // Using test profile for safer configurations
public class BookCoverManagementServiceSmokeTest { // Renamed class

    private static final Logger logger = LoggerFactory.getLogger(BookCoverManagementServiceSmokeTest.class); // Renamed logger class

    @Autowired
    private BookCoverManagementService bookCoverManagementService; // Renamed service

    @Value("${app.cover-cache.dir:/tmp/book-covers}") // Ensure this matches your config
    private String cacheDirString;

    private Book book1_knownGood;
    private Book book2_likelyPlaceholder;
    private Book book3_newToSystem;

    /**
     * Set up test data and clean the cache directory before each test
     * 
     * WARNING: This is destructive - it deletes files in the cache directory
     */
    @BeforeEach
    void setUp() {
        // Clean the cache directory before each test for consistent results
        // THIS IS DESTRUCTIVE - USE WITH CAUTION AND ENSURE IT'S A TEST-ONLY DIRECTORY
        try {
            Path cachePath = Paths.get(cacheDirString);
            if (Files.exists(cachePath)) {
                Files.walk(cachePath)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(file -> {
                        try {
                            Files.delete(file.toPath());
                        } catch (IOException e) {
                            logger.error("Failed to delete cached file: {} - {}", file, e.getMessage());
                        }
                    });
                logger.info("Cleaned cache directory: {}", cacheDirString);
            }
             if (!Files.exists(cachePath)) Files.createDirectories(cachePath);
        } catch (IOException e) {
            logger.error("Failed to clean or create cache directory: {} - {}", cacheDirString, e.getMessage(), e);
        }


        // Replace with actual ISBNs/Google IDs from your system or for testing.

        // Example Book 1: A popular book you expect to have a cover
        book1_knownGood = new Book();
        book1_knownGood.setId("knownGoodGoogleId1"); // Google Books Volume ID
        book1_knownGood.setIsbn13("9780553293357"); // Game of Thrones
        book1_knownGood.setTitle("Known Good Book (e.g., Game of Thrones)");
        book1_knownGood.setCoverImageUrl(null);

        // Example Book 2: An obscure book
        book2_likelyPlaceholder = new Book();
        book2_likelyPlaceholder.setId("obscureGoogleId1");
        book2_likelyPlaceholder.setIsbn13("9780000000001"); // fake ISBN
        book2_likelyPlaceholder.setTitle("Obscure Book Likely Placeholder");
        book2_likelyPlaceholder.setCoverImageUrl(null);

        // Example Book 3: Another book, new to the system
        book3_newToSystem = new Book();
        book3_newToSystem.setId("newToSystemGoogleId1");
        book3_newToSystem.setIsbn10("0451524934"); // Example: 1984
        book3_newToSystem.setTitle("New To System Book (e.g., 1984)");
        book3_newToSystem.setCoverImageUrl(null);
        
        logger.info("Test setup complete. Cache directory is: {}", cacheDirString);
    }

    /**
     * Tests the full book cover loading workflow including background processing
     * 
     * @throws InterruptedException If the test is interrupted while waiting for background processing
     */
    @Test
    void testBookCoverLoadingFlow() throws InterruptedException {
        logger.info("--- Starting Smoke Test for Book Cover Loading ---");

        // Test Book 1
        performTestForBook(book1_knownGood, "Book 1 (Known Good)");

        // Test Book 2
        performTestForBook(book2_likelyPlaceholder, "Book 2 (Likely Placeholder)");

        // Test Book 3
        performTestForBook(book3_newToSystem, "Book 3 (New To System)");

        logger.info("--- Smoke Test Finished. Check logs above for behavior and any S3/placeholder resolutions. ---");
        logger.info("--- Remember to manually check image rendering in a browser if S3 URLs are generated. ---");
    }

    /**
     * Helper method to test cover retrieval for a specific book
     * 
     * @param book The book to test cover retrieval for
     * @param bookLabel A descriptive label for logging
     * @throws InterruptedException If the test is interrupted while waiting for background processing
     */
    private void performTestForBook(Book book, String bookLabel) throws InterruptedException {
        logger.info("Testing cover for: {} - ISBN13: {}, GoogleID: {}", bookLabel, book.getIsbn13(), book.getId());

        // Initial call
        com.williamcallahan.book_recommendation_engine.types.CoverImages initialCoverImages = bookCoverManagementService.getInitialCoverUrlAndTriggerBackgroundUpdate(book); // Use renamed service
        String initialUrl = (initialCoverImages != null && initialCoverImages.getPreferredUrl() != null) ? initialCoverImages.getPreferredUrl() : "/images/placeholder-book-cover.svg";
        logger.info("[{}] Initial URL: {}", bookLabel, initialUrl);

        logger.info("[{}] Waiting 20 seconds for background processing...", bookLabel);
        Thread.sleep(20000);

        // Call again to see if the URL has been updated by background processing (e.g., to S3 or a final placeholder)
        com.williamcallahan.book_recommendation_engine.types.CoverImages finalCoverImages = bookCoverManagementService.getInitialCoverUrlAndTriggerBackgroundUpdate(book); // Use renamed service
        String finalUrl = (finalCoverImages != null && finalCoverImages.getPreferredUrl() != null) ? finalCoverImages.getPreferredUrl() : "/images/placeholder-book-cover.svg";
        logger.info("[{}] URL after background processing: {}", bookLabel, finalUrl);

        if (initialUrl.equals(finalUrl)) {
            logger.info("[{}] URL did not change after background processing.", bookLabel);
        } else {
            logger.info("[{}] URL CHANGED from {} to {}", bookLabel, initialUrl, finalUrl);
        }
        logger.info("--- Finished test for {} ---", bookLabel);
    }
}
