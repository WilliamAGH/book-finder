package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.util.ValidationUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Service for generating affiliate links for various book retailers.
 * Centralizes affiliate link generation logic to reduce duplication across controllers.
 */
@Service
public class AffiliateLinkService {

    @Value("${affiliate.amazon.associate-tag:#{null}}")
    private String amazonAssociateTag;

    @Value("${affiliate.barnesandnoble.publisher-id:#{null}}")
    private String barnesNobleCjPublisherId;

    @Value("${affiliate.barnesandnoble.website-id:#{null}}")
    private String barnesNobleCjWebsiteId;

    @Value("${affiliate.bookshop.affiliate-id:#{null}}")
    private String bookshopAffiliateId;

    /**
     * Generate affiliate links for all configured retailers.
     * Links work even without affiliate IDs (fall back to direct retailer links).
     */
    public Map<String, String> generateLinks(String isbn13, String isbn10) {
        Map<String, String> links = new HashMap<>();

        // Prefer ISBN-13 for most links
        if (isbn13 != null) {
            String barnesNobleLink = buildBarnesNobleLink(isbn13);
            if (barnesNobleLink != null) {
                links.put("barnesAndNoble", barnesNobleLink);
            }
            
            String bookshopLink = buildBookshopLink(isbn13);
            if (bookshopLink != null) {
                links.put("bookshop", bookshopLink);
            }
        }

        // Amazon can use either ISBN
        String isbnForAmazon = isbn13 != null ? isbn13 : isbn10;
        if (isbnForAmazon != null) {
            String amazonLink = buildAmazonLink(isbnForAmazon);
            if (amazonLink != null) {
                links.put("amazon", amazonLink);
            }
        }

        return links;
    }

    /**
     * Generate affiliate links using book metadata (adds Audible search when possible).
     */
    public Map<String, String> generateLinks(Book book) {
        if (book == null) {
            return Map.of();
        }

        Map<String, String> links = new HashMap<>(generateLinks(book.getIsbn13(), book.getIsbn10()));
        addAudibleLink(links, book.getAsin(), book.getTitle());
        return links;
    }

    /**
     * Build Amazon link (with affiliate tag if configured, direct link otherwise).
     * Uses Amazon's search format with full affiliate parameters for better tracking.
     */
    public String buildAmazonLink(String isbn) {
        if (isbn == null) {
            return null;
        }
        // If affiliate tag is configured, use Amazon's search format with full affiliate params
        if (amazonAssociateTag != null) {
            return String.format(
                "https://www.amazon.com/s?k=%s&linkCode=ll2&tag=%s&linkId=%s&language=en_US&ref_=as_li_ss_tl",
                isbn,
                amazonAssociateTag,
                "book_" + isbn.hashCode()
            );
        }
        return String.format("https://www.amazon.com/s?k=%s", isbn);
    }

    /**
     * Build Barnes & Noble link (with CJ affiliate if configured, direct link otherwise).
     */
    public String buildBarnesNobleLink(String isbn13) {
        if (isbn13 == null) {
            return null;
        }
        // If affiliate is configured, use CJ tracking link; otherwise, direct link
        if (ValidationUtils.allNotNull(barnesNobleCjPublisherId, barnesNobleCjWebsiteId)) {
            return String.format(
                "https://www.anrdoezrs.net/links/%s/type/dlg/sid/%s/https://www.barnesandnoble.com/w/?ean=%s",
                barnesNobleCjPublisherId, barnesNobleCjWebsiteId, isbn13
            );
        }
        return String.format("https://www.barnesandnoble.com/w/?ean=%s", isbn13);
    }

    /**
     * Build Bookshop.org link (with affiliate ID if configured, direct link otherwise).
     */
    public String buildBookshopLink(String isbn13) {
        if (isbn13 == null) {
            return null;
        }
        // If affiliate ID is configured, use affiliate link; otherwise, direct ISBN search
        if (bookshopAffiliateId != null) {
            return String.format("https://bookshop.org/a/%s/%s", bookshopAffiliateId, isbn13);
        }
        return String.format("https://bookshop.org/books?keywords=%s", isbn13);
    }

    /**
     * Check if any affiliate configuration is available.
     */
    public boolean hasAnyAffiliateConfig() {
        return amazonAssociateTag != null ||
               ValidationUtils.allNotNull(barnesNobleCjPublisherId, barnesNobleCjWebsiteId) ||
               bookshopAffiliateId != null;
    }

    /**
     * Check if Amazon affiliate is configured.
     */
    public boolean hasAmazonConfig() {
        return amazonAssociateTag != null;
    }

    /**
     * Check if Barnes & Noble affiliate is configured.
     */
    public boolean hasBarnesNobleConfig() {
        return ValidationUtils.allNotNull(barnesNobleCjPublisherId, barnesNobleCjWebsiteId);
    }

    /**
     * Check if Bookshop affiliate is configured.
     */
    public boolean hasBookshopConfig() {
        return bookshopAffiliateId != null;
    }

    private void addAudibleLink(Map<String, String> links, String asin, String title) {
        String searchTerm = Optional.ofNullable(asin)
            .filter(ValidationUtils::hasText)
            .orElseGet(() -> Optional.ofNullable(title).orElse(""));

        if (!ValidationUtils.hasText(searchTerm)) {
            return;
        }

        try {
            String encoded = URLEncoder.encode(searchTerm, StandardCharsets.UTF_8);
            // If affiliate tag is configured, use it; otherwise, direct Audible search
            if (amazonAssociateTag != null) {
                links.put("audible", String.format(
                    "https://www.amazon.com/s?k=%s&tag=%s&linkCode=ur2&linkId=audible",
                    encoded,
                    amazonAssociateTag
                ));
            } else {
                links.put("audible", String.format(
                    "https://www.audible.com/search?keywords=%s",
                    encoded
                ));
            }
        } catch (Exception ignored) {
            // Encoding should not fail for UTF-8, but in case it does we skip audible link.
        }
    }
}
