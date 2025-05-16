package com.williamcallahan.book_recommendation_engine.types;

/**
 * Interface for the Longitood book cover service
 *
 * @author William Callahan
 *
 * Features:
 * - Specializes ExternalCoverService for the Longitood API
 * - Provides high-resolution book cover images
 * - Supports lookups by ISBN and title
 * - Offers access to publisher-provided cover artwork
 */
public interface LongitoodService extends ExternalCoverService {
}
