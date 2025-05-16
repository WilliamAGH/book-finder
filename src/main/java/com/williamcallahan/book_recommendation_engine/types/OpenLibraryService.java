package com.williamcallahan.book_recommendation_engine.types;

/**
 * Interface for the Open Library book cover service
 *
 * @author William Callahan
 *
 * Features:
 * - Specializes ExternalCoverService for the Open Library API
 * - Provides open-source book cover images
 * - Supports lookups by ISBN, OLID, and other identifiers
 * - Offers multiple image sizes (small, medium, large)
 * - Accesses the Internet Archive's book cover collection
 */
public interface OpenLibraryService extends ExternalCoverService {
}
