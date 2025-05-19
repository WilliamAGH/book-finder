/**
 * Service for generating affiliate links to online book retailers
 * 
 * This service provides functionality for:
 * - Creating affiliate links to Barnes & Noble using CJ Affiliate
 * - Creating affiliate links to Bookshop.org using their affiliate program
 * - Creating affiliate links to Audible/Amazon using Amazon Associates
 * - Graceful fallback to non-affiliate links when IDs are missing
 * - URL encoding and formatting for various retailer requirements
 * - Support for different identifier types (ISBN, ASIN, etc.)
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@Service
public class AffiliateLinkService {

    private static final Logger logger = LoggerFactory.getLogger(AffiliateLinkService.class);

    @Value("${affiliate.barnesandnoble.publisher-id:#{null}}")
    private String defaultCjPublisherId;

    @Value("${affiliate.barnesandnoble.website-id:#{null}}")
    private String defaultCjWebsiteId;

    @Value("${affiliate.bookshop.affiliate-id:#{null}}")
    private String defaultBookshopAffiliateId;

    @Value("${affiliate.amazon.associate-tag:#{null}}")
    private String defaultAmazonAssociateTag;

    /**
     * Generates an affiliate link for Barnes & Noble using CJ Affiliate
     *
     * @param isbn The book's ISBN (preferably ISBN-13)
     * @param cjPublisherId CJ Publisher ID
     * @param cjWebsiteId CJ Website ID for Barnes & Noble
     * @return Affiliate link or direct search link if IDs are missing
     */
    public String generateBarnesAndNobleLink(String isbn, String cjPublisherId, String cjWebsiteId) {
        if (isbn == null || isbn.isEmpty()) {
            // Cannot generate a specific link without ISBN; return base URL or null
            return "https://www.barnesandnoble.com/"; 
        }
        // If affiliate IDs are missing, provide a direct search link
        if (cjPublisherId == null || cjPublisherId.isEmpty() || cjWebsiteId == null || cjWebsiteId.isEmpty()) {
            return "https://www.barnesandnoble.com/w/?ean=" + isbn; // ean is used for ISBN search
        }
        
        // Affiliate IDs present, generate affiliate link
        String productUrl = "https://www.barnesandnoble.com/w/?ean=" + isbn;
        try {
            String encodedProductUrl = URLEncoder.encode(productUrl, StandardCharsets.UTF_8.toString());
            return String.format("https://www.anrdoezrs.net/click-%s-%s?url=%s&sid=%s",
                                 cjPublisherId, cjWebsiteId, encodedProductUrl, isbn);
        } catch (UnsupportedEncodingException e) {
            logger.error("Error encoding Barnes & Noble URL: {}", e.getMessage(), e);
            return "https://www.barnesandnoble.com/w/?ean=" + isbn; // Fallback to direct search on error
        }
    }

    /**
     * Generates an affiliate link for Bookshop.org
     *
     * @param isbn The book's ISBN (preferably ISBN-13)
     * @param bookshopAffiliateId Bookshop.org affiliate ID
     * @return Affiliate link or direct product link if ID is missing
     */
    public String generateBookshopLink(String isbn, String bookshopAffiliateId) {
        if (isbn == null || isbn.isEmpty()) {
            // Cannot generate a specific link without ISBN; return base URL or null
            return "https://bookshop.org/";
        }
        // If affiliate ID is missing, provide a direct product link
        if (bookshopAffiliateId == null || bookshopAffiliateId.isEmpty()) {
            return String.format("https://bookshop.org/book/%s", isbn);
        }
        // Affiliate ID present
        return String.format("https://bookshop.org/a/%s/%s", bookshopAffiliateId, isbn);
    }

    /**
     * Generates an affiliate link for Audible via Amazon Associates
     *
     * @param asin Book's ASIN for the Audible version
     * @param title Book title (used for search if ASIN unavailable)
     * @param amazonAssociateTag Amazon Associates Tag
     * @return Affiliate link, title search, or generic search based on available data
     */
    public String generateAudibleLink(String asin, String title, String amazonAssociateTag) {
        if (asin != null && !asin.isEmpty()) {
            // ASIN is present, construct direct Audible product link
            String baseUrl = String.format("https://www.audible.com/pd/%s", asin);
            if (amazonAssociateTag == null || amazonAssociateTag.isEmpty()) {
                return baseUrl; // No associate tag, provide direct Audible product link
            }
            // ASIN and tag present: affiliate link
            return String.format("https://www.audible.com/pd/%s?tag=%s", asin, amazonAssociateTag);
        }
        
        // ASIN is missing, try to use title for search
        if (title != null && !title.isEmpty()) {
            try {
                String encodedTitle = URLEncoder.encode(title, StandardCharsets.UTF_8.toString());
                String searchUrl = String.format("https://www.audible.com/search?keywords=%s", encodedTitle);
                if (amazonAssociateTag != null && !amazonAssociateTag.isEmpty()) {
                    // Attempt to add tag to search URL. Note: Effectiveness may vary by platform/program rules.
                    // It's generally safer for product links, but let's include it if present.
                    searchUrl += String.format("&tag=%s", amazonAssociateTag);
                }
                return searchUrl;
            } catch (UnsupportedEncodingException e) {
                logger.error("Error encoding title for Audible search: {}", e.getMessage(), e);
                // Fallback to generic search if title encoding fails
            }
        }
        
        // Fallback to generic Audible search if no ASIN and no title (or title encoding failed)
        // If a tag is present, we can still try to append it to the generic search, though its utility here is low.
        if (amazonAssociateTag != null && !amazonAssociateTag.isEmpty()) {
            return String.format("https://www.audible.com/search?tag=%s", amazonAssociateTag);
        }
        return "https://www.audible.com/search"; 
    }
}
