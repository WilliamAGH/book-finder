/**
 * Resolves external provider IDs to canonical book URLs
 *
 * @author William Callahan
 * 
 * Features:
 * - Handles /r/{source}/{externalId} URLs from search results
 * - Resolves external IDs to internal book_id using ExternalBookIdResolver
 * - 301 redirect to canonical /book/{slug} when mapping exists
 * - Enqueues high-priority backfill when mapping doesn't exist
 * - 302 redirect to holding page during backfill processing
 * 
 * Use case:
 * Frontend search results use /r/gbooks/{googleBooksId} instead of generating
 * slugs prematurely. This prevents dead links when books aren't in DB yet.
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.BackfillCoordinator;
import com.williamcallahan.book_recommendation_engine.service.BookDataOrchestrator;
import com.williamcallahan.book_recommendation_engine.service.ExternalBookIdResolver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.Optional;
import java.util.UUID;

/**
 * Controller for resolving external provider IDs to canonical book URLs.
 * 
 * Endpoint: /r/{source}/{externalId}
 * 
 * Flow:
 * 1. Try to resolve (source, externalId) → book_id via ExternalBookIdResolver
 * 2. If found: Lookup book by book_id, 301 redirect to /book/{slug}
 * 3. If not found: Enqueue high-priority backfill task, 302 redirect to holding page
 * 
 * Supported sources:
 * - gbooks: Google Books (e.g., /r/gbooks/abc123)
 * - (future): openlibrary, amazon, etc.
 */
@RestController
@RequestMapping("/r")
@Slf4j
public class ResolveController {
    
    private final ExternalBookIdResolver resolver;
    private final BookDataOrchestrator bookDataOrchestrator;
    
    @Autowired(required = false)
    private BackfillCoordinator backfillCoordinator;
    
    public ResolveController(
        ExternalBookIdResolver resolver,
        BookDataOrchestrator bookDataOrchestrator
    ) {
        this.resolver = resolver;
        this.bookDataOrchestrator = bookDataOrchestrator;
    }
    
    /**
     * Resolves external ID to canonical book URL.
     * 
     * @param source External provider name (e.g., "gbooks", "openlibrary")
     * @param externalId Provider's book identifier
     * @return 301 redirect to /book/{slug} if found, 302 to holding page if not found
     */
    @GetMapping("/{source}/{externalId}")
    public ResponseEntity<Void> resolve(
        @PathVariable String source,
        @PathVariable String externalId
    ) {
        log.info("Resolving external ID: {} {}", source, externalId);
        
        // Normalize source name
        String normalizedSource = normalizeSource(source);
        if (normalizedSource == null) {
            log.warn("Unsupported source: {}", source);
            return ResponseEntity.notFound().build();
        }
        
        // Step 1: Try to resolve to internal book_id
        Optional<UUID> bookIdOpt = resolver.resolve(normalizedSource, externalId);
        
        if (bookIdOpt.isPresent()) {
            UUID bookId = bookIdOpt.get();
            log.debug("Resolved {} {} → book_id={}", source, externalId, bookId);
            
            // Step 2: Lookup book to get slug
            Optional<Book> bookOpt = bookDataOrchestrator.getBookFromDatabase(bookId.toString());
            
            if (bookOpt.isPresent() && bookOpt.get().getSlug() != null) {
                String slug = bookOpt.get().getSlug();
                log.info("Redirecting {} {} → /book/{}", source, externalId, slug);
                
                // 301 Permanent Redirect to canonical URL
                return ResponseEntity
                    .status(HttpStatus.MOVED_PERMANENTLY)
                    .location(URI.create("/book/" + slug))
                    .build();
            } else {
                log.warn("Book {} found but has no slug, falling through to backfill", bookId);
            }
        }
        
        // Step 3: Not found - enqueue backfill
        log.info("Mapping not found for {} {}, enqueuing backfill", source, externalId);
        
        if (backfillCoordinator != null) {
            // Enqueue with highest priority (1) since user is waiting
            backfillCoordinator.enqueue(normalizedSource, externalId, 1);
        }
        
        // Option A: Redirect to holding page with polling/WebSocket
        String holdingUrl = String.format("/book/pending?src=%s&id=%s", source, externalId);
        
        log.info("Redirecting to holding page: {}", holdingUrl);
        
        // 302 Found (temporary redirect) to holding page
        return ResponseEntity
            .status(HttpStatus.FOUND)
            .location(URI.create(holdingUrl))
            .build();
    }
    
    /**
     * Normalizes source name to canonical format used in database.
     * 
     * @param source Frontend source name (e.g., "gbooks")
     * @return Normalized source name (e.g., "GOOGLE_BOOKS") or null if unsupported
     */
    private String normalizeSource(String source) {
        if (source == null || source.isBlank()) {
            return null;
        }
        
        return switch (source.toLowerCase()) {
            case "gbooks", "googlebooks", "google-books" -> "GOOGLE_BOOKS";
            case "openlibrary", "open-library", "ol" -> "OPEN_LIBRARY";
            // Add more sources as needed
            default -> null;
        };
    }
}
