/**
 * Service for managing and identifying duplicate book entries
 *
 * @author William Callahan
 *
 * Features:
 * - Identifies duplicate books based on title and author matching
 * - Populates cross-references between duplicate editions
 * - Finds canonical/primary book entries for deduplication
 * - Merges metadata from duplicate sources to enrich primary entries
 * - Enables unified book view across different identifiers and editions
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.model.CachedBook;
import com.williamcallahan.book_recommendation_engine.repository.CachedBookRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.Collections;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
@Service
public class DuplicateBookService {

    private static final Logger logger = LoggerFactory.getLogger(DuplicateBookService.class);

    private final CachedBookRepository cachedBookRepository;

    /**
     * Constructs a new DuplicateBookService
     *
     * @param cachedBookRepository Repository for querying cached book information
     */
    public DuplicateBookService(CachedBookRepository cachedBookRepository) {
        this.cachedBookRepository = cachedBookRepository;
    }

    /**
     * Finds existing cached books that are potential duplicates of the given book based on title and authors
     *
     * @param book The book to check for duplicates
     * @param excludeId The ID of the book itself, to exclude from duplicate search results
     * @return A list of CachedBook entities that are considered duplicates
     */
    public List<CachedBook> findPotentialDuplicates(Book book, String excludeId) {
        if (book == null || book.getTitle() == null || book.getAuthors() == null || book.getAuthors().isEmpty()) {
            return Collections.emptyList();
        }

        List<CachedBook> candidates = cachedBookRepository.findByTitleIgnoreCaseAndIdNot(book.getTitle(), excludeId);
        
        Set<String> bookAuthorsLower = book.getAuthors().stream()
            .map(String::toLowerCase)
            .collect(Collectors.toSet());

        return candidates.stream()
            .filter(candidate -> {
                if (candidate.getAuthors() == null || candidate.getAuthors().isEmpty()) {
                    return false; // Cannot be a duplicate if it has no authors and the book does
                }
                Set<String> candidateAuthorsLower = candidate.getAuthors().stream()
                    .map(String::toLowerCase)
                    .collect(Collectors.toSet());
                return candidateAuthorsLower.equals(bookAuthorsLower);
            })
            .collect(Collectors.toList());
    }

    /**
     * Populates the 'otherEditions' field of a primary book with information from its duplicates
     *
     * @param primaryBook The main book object whose otherEditions will be populated
     */
    public void populateDuplicateEditions(Book primaryBook) {
        if (primaryBook == null || primaryBook.getId() == null) {
            return;
        }

        // Clear any existing other editions to start fresh
        if (primaryBook.getOtherEditions() == null) {
            primaryBook.setOtherEditions(new java.util.ArrayList<>());
        } else {
            primaryBook.getOtherEditions().clear();
        }

        List<CachedBook> duplicates = findPotentialDuplicates(primaryBook, primaryBook.getId());
        
        if (duplicates.isEmpty()) {
            return; // No duplicates found, return early
        }
        
        for (CachedBook dupCachedBook : duplicates) {
            // Skip if this is the same book with same GoogleBooksId
            if (dupCachedBook.getGoogleBooksId() != null && 
                dupCachedBook.getGoogleBooksId().equals(primaryBook.getId())) {
                logger.debug("Skipping exact same book with Google ID: {}", primaryBook.getId());
                continue;
            }
            
            // Check if ISBNs match the primary book exactly - if both match, it's not a different edition
            if (primaryBook.getIsbn13() != null && primaryBook.getIsbn13().equals(dupCachedBook.getIsbn13()) &&
                primaryBook.getIsbn10() != null && primaryBook.getIsbn10().equals(dupCachedBook.getIsbn10())) {
                logger.debug("Skipping identical edition with matching ISBN-10 and ISBN-13");
                continue;
            }
            
            // If the Google ID is different but we have matching ISBNs, this is truly a different edition
            boolean hasAtLeastOneUniqueIdentifier = false;
            
            // The duplicate has a different Google ID
            if (dupCachedBook.getGoogleBooksId() != null && 
                !dupCachedBook.getGoogleBooksId().equals(primaryBook.getId())) {
                hasAtLeastOneUniqueIdentifier = true;
            }
            
            // The duplicate has a unique ISBN-13 that doesn't match the primary book
            if (dupCachedBook.getIsbn13() != null && 
                (primaryBook.getIsbn13() == null || !dupCachedBook.getIsbn13().equals(primaryBook.getIsbn13()))) {
                hasAtLeastOneUniqueIdentifier = true;
            }
            
            // The duplicate has a unique ISBN-10 that doesn't match the primary book
            if (dupCachedBook.getIsbn10() != null && 
                (primaryBook.getIsbn10() == null || !dupCachedBook.getIsbn10().equals(primaryBook.getIsbn10()))) {
                hasAtLeastOneUniqueIdentifier = true;
            }
            
            if (!hasAtLeastOneUniqueIdentifier) {
                logger.debug("Skipping duplicate with no unique identifiers");
                continue;
            }
            
            // This is a genuine other edition, create the EditionInfo
            Book.EditionInfo editionInfo = new Book.EditionInfo();

            // Set core information for the edition
            if (dupCachedBook.getGoogleBooksId() != null && !dupCachedBook.getGoogleBooksId().isEmpty()) {
                editionInfo.setGoogleBooksId(dupCachedBook.getGoogleBooksId());
            }
            editionInfo.setEditionIsbn10(dupCachedBook.getIsbn10());
            editionInfo.setEditionIsbn13(dupCachedBook.getIsbn13());
            
            // Convert LocalDateTime to Date for publishedDate
            LocalDateTime ldt = dupCachedBook.getPublishedDate();
            if (ldt != null) {
                editionInfo.setPublishedDate(Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant()));
            } else {
                // If the duplicate doesn't have a date but the primary book does, use that
                // This provides more context for alternative editions
                editionInfo.setPublishedDate(primaryBook.getPublishedDate());
            }
            // editionInfo.setCoverImageUrl(dupCachedBook.getCoverImageUrl());

            // Determine a primary display identifier and type for quick reference
            String displayIdentifier = dupCachedBook.getIsbn13();
            String displayType = "ISBN-13";

            if (displayIdentifier == null || displayIdentifier.trim().isEmpty()) {
                displayIdentifier = dupCachedBook.getIsbn10();
                displayType = "ISBN-10";
            }
            
            if (displayIdentifier == null || displayIdentifier.trim().isEmpty()) {
                if (dupCachedBook.getGoogleBooksId() != null && !dupCachedBook.getGoogleBooksId().isEmpty()) {
                    displayIdentifier = dupCachedBook.getGoogleBooksId();
                    displayType = "GoogleID";
                } else { 
                    displayIdentifier = "Ref: " + dupCachedBook.getId(); 
                    displayType = "InternalRef";
                }
            }
            editionInfo.setIdentifier(displayIdentifier); // This is a fallback/summary identifier
            editionInfo.setType(displayType); // Describes the fallback/summary identifier
            
            // Check if this edition is already in the list to avoid duplicates
            boolean alreadyExists = primaryBook.getOtherEditions().stream()
                .anyMatch(oe -> {
                    if (oe.getGoogleBooksId() != null && 
                        oe.getGoogleBooksId().equals(editionInfo.getGoogleBooksId())) {
                        return true;
                    }
                    // If GoogleBooksId is null, check ISBNs
                    if (editionInfo.getGoogleBooksId() == null && oe.getGoogleBooksId() == null) {
                         if (oe.getEditionIsbn13() != null && 
                             oe.getEditionIsbn13().equals(editionInfo.getEditionIsbn13())) {
                             return true;
                         }
                         if (oe.getEditionIsbn10() != null && 
                             oe.getEditionIsbn10().equals(editionInfo.getEditionIsbn10())) {
                             return true;
                         }
                    }
                    return false;
                });
            
            if (!alreadyExists) {
                 primaryBook.getOtherEditions().add(editionInfo);
                 logger.debug("Added edition (GoogleID: {}, ISBN13: {}, ISBN10: {}) to primary book {}. DisplayID: {}, DisplayType: {}", 
                              editionInfo.getGoogleBooksId(), editionInfo.getEditionIsbn13(), editionInfo.getEditionIsbn10(), 
                              primaryBook.getId(), displayIdentifier, displayType);
            } else {
                logger.debug("Skipped adding duplicate edition that already exists in the list");
            }
        }
        
        // If no valid different editions were found, ensure the list is empty
        if (primaryBook.getOtherEditions().isEmpty()) {
            logger.debug("No valid different editions found for book {}", primaryBook.getId());
        }
    }

    /**
     * Finds a "primary" or "canonical" existing CachedBook for a new book based on title and authors
     *
     * @param newBook The new book (typically from an API) to find a canonical version for
     * @return Optional containing the primary/canonical CachedBook if one exists, otherwise empty
     */
    public Optional<CachedBook> findPrimaryCanonicalBook(Book newBook) {
        if (newBook == null || newBook.getTitle() == null || newBook.getAuthors() == null || newBook.getAuthors().isEmpty()) {
            return Optional.empty();
        }
        // When searching for a canonical book for a *new* book, we don't exclude any ID yet.
        // If the newBook has an ID that might already exist, findPotentialDuplicates will handle it if called with that ID.
        // Here, we want to find *any* existing match.
        List<CachedBook> potentialPrimaries = findPotentialDuplicates(newBook, "__NON_EXISTENT_ID__" + System.currentTimeMillis()); // Use a dummy ID that won't match

        if (potentialPrimaries.isEmpty()) {
            return Optional.empty();
        }
        // Prioritization logic for selecting the "best" primary if multiple found.
        // For now, just take the first one. Could be enhanced (e.g., most recently updated, most complete data).
        logger.debug("Found {} potential primary books for new book title '{}'. Selecting first one: {}", 
            potentialPrimaries.size(), newBook.getTitle(), potentialPrimaries.get(0).getId());
        return Optional.of(potentialPrimaries.get(0)); 
    }

    /**
     * Merges data from a new book (source) into an existing primary cached book (target)
     * This is a simple merge: fills null fields in target from source
     * More sophisticated merging (e.g., preferring longer descriptions) could be added
     *
     * @param primaryCachedBook The target book to update
     * @param newBookFromApi The source book with potentially newer/better data
     * @return True if the primaryCachedBook was modified, false otherwise
     */
    public boolean mergeDataIfBetter(CachedBook primaryCachedBook, Book newBookFromApi) {
        boolean modified = false;
        if (primaryCachedBook.getDescription() == null && newBookFromApi.getDescription() != null) {
            primaryCachedBook.setDescription(newBookFromApi.getDescription());
            modified = true;
        }
        if (primaryCachedBook.getCoverImageUrl() == null && newBookFromApi.getCoverImageUrl() != null) {
            primaryCachedBook.setCoverImageUrl(newBookFromApi.getCoverImageUrl());
            modified = true;
        }
        if (primaryCachedBook.getIsbn10() == null && newBookFromApi.getIsbn10() != null) {
            primaryCachedBook.setIsbn10(newBookFromApi.getIsbn10());
            modified = true;
        }
        if (primaryCachedBook.getIsbn13() == null && newBookFromApi.getIsbn13() != null) {
            primaryCachedBook.setIsbn13(newBookFromApi.getIsbn13());
            modified = true;
        }
        if (primaryCachedBook.getPageCount() == null && newBookFromApi.getPageCount() != null) {
            primaryCachedBook.setPageCount(newBookFromApi.getPageCount());
            modified = true;
        }
        if (primaryCachedBook.getPublisher() == null && newBookFromApi.getPublisher() != null) {
            primaryCachedBook.setPublisher(newBookFromApi.getPublisher());
            modified = true;
        }
        if (primaryCachedBook.getLanguage() == null && newBookFromApi.getLanguage() != null) {
            primaryCachedBook.setLanguage(newBookFromApi.getLanguage());
            modified = true;
        }
        // Add more fields as necessary, e.g., ratings, categories (list merge could be complex)

        if (modified) {
            logger.info("Merged data from new book (ID: {}) into primary cached book (ID: {}).", 
                        newBookFromApi.getId() != null ? newBookFromApi.getId() : "N/A", primaryCachedBook.getId());
        }
        return modified;
    }
}
