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
     */
    public Map<String, String> generateLinks(String isbn13, String isbn10) {
        Map<String, String> links = new HashMap<>();

        // Prefer ISBN-13 for most links
        Optional.ofNullable(isbn13).ifPresent(isbn -> {
            if (ValidationUtils.allNotNull(barnesNobleCjPublisherId, barnesNobleCjWebsiteId)) {
                links.put("barnesNoble", buildBarnesNobleLink(isbn));
            }
            if (bookshopAffiliateId != null) {
                links.put("bookshop", buildBookshopLink(isbn));
            }
        });

        // Amazon can use either ISBN
        String isbnForAmazon = isbn13 != null ? isbn13 : isbn10;
        if (isbnForAmazon != null && amazonAssociateTag != null) {
            links.put("amazon", buildAmazonLink(isbnForAmazon));
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
     * Build Amazon affiliate link.
     */
    public String buildAmazonLink(String isbn) {
        if (isbn == null || amazonAssociateTag == null) {
            return null;
        }
        return String.format("https://www.amazon.com/dp/%s?tag=%s", isbn, amazonAssociateTag);
    }

    /**
     * Build Barnes & Noble affiliate link via CJ.
     */
    public String buildBarnesNobleLink(String isbn13) {
        if (isbn13 == null || !ValidationUtils.allNotNull(barnesNobleCjPublisherId, barnesNobleCjWebsiteId)) {
            return null;
        }
        return String.format(
            "https://www.anrdoezrs.net/links/%s/type/dlg/sid/%s/https://www.barnesandnoble.com/w/?ean=%s",
            barnesNobleCjPublisherId, barnesNobleCjWebsiteId, isbn13
        );
    }

    /**
     * Build Bookshop.org affiliate link.
     */
    public String buildBookshopLink(String isbn13) {
        if (isbn13 == null || bookshopAffiliateId == null) {
            return null;
        }
        return String.format("https://bookshop.org/a/%s/%s", bookshopAffiliateId, isbn13);
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
        if (amazonAssociateTag == null) {
            return;
        }

        String searchTerm = Optional.ofNullable(asin)
            .filter(ValidationUtils::hasText)
            .orElseGet(() -> Optional.ofNullable(title).orElse(""));

        if (!ValidationUtils.hasText(searchTerm)) {
            return;
        }

        try {
            String encoded = URLEncoder.encode(searchTerm, StandardCharsets.UTF_8);
            links.put("audible", String.format(
                "https://www.amazon.com/s?k=%s&tag=%s&linkCode=ur2&linkId=audible",
                encoded,
                amazonAssociateTag
            ));
        } catch (Exception ignored) {
            // Encoding should not fail for UTF-8, but in case it does we skip audible link.
        }
    }
}
