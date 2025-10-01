package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.dto.BookDetail;
import com.williamcallahan.book_recommendation_engine.repository.BookQueryRepository;
import com.williamcallahan.book_recommendation_engine.util.UuidUtils;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

/**
 * Resolves user-facing identifiers (slug, ISBN, external ID) to canonical UUIDs.
 * Centralising this logic prevents duplicate lookup heuristics across controllers.
 */
@Service
public class BookIdentifierResolver {

    private final BookLookupService bookLookupService;
    private final BookQueryRepository bookQueryRepository;

    public BookIdentifierResolver(BookLookupService bookLookupService,
                                  BookQueryRepository bookQueryRepository) {
        this.bookLookupService = bookLookupService;
        this.bookQueryRepository = bookQueryRepository;
    }

    public Optional<UUID> resolveToUuid(String identifier) {
        return resolveCanonicalId(identifier)
            .map(UuidUtils::parseUuidOrNull)
            .filter(uuid -> uuid != null);
    }

    public Optional<String> resolveCanonicalId(String identifier) {
        if (!ValidationUtils.hasText(identifier)) {
            return Optional.empty();
        }

        String trimmed = identifier.trim();

        UUID uuid = UuidUtils.parseUuidOrNull(trimmed);
        if (uuid != null) {
            return Optional.of(uuid.toString());
        }

        // Try slug resolution via Postgres projections
        Optional<BookDetail> bySlug = bookQueryRepository.fetchBookDetailBySlug(trimmed);
        if (bySlug.isPresent() && ValidationUtils.hasText(bySlug.get().id())) {
            return Optional.of(bySlug.get().id());
        }

        if (bookLookupService == null) {
            return Optional.empty();
        }

        return bookLookupService.findBookIdByExternalIdentifier(trimmed)
            .or(() -> bookLookupService.findBookIdByIsbn(trimmed))
            .or(() -> bookLookupService.findBookById(trimmed));
    }
}
