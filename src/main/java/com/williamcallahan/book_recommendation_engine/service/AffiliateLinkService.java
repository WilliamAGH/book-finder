/**
 * Service for generating and monitoring affiliate links to various online book retailers
 * Handles link creation for Barnes & Noble, Bookshop_org, and Audible/Amazon
 * Includes fallback mechanisms and URL encoding
 * Tracks link generation and encoding errors using Micrometer
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Service class responsible for generating affiliate links and tracking their usage
 * 
 */
@Service
public class AffiliateLinkService {

    private static final Logger logger = LoggerFactory.getLogger(AffiliateLinkService.class);

    // Counters for generated links
    private final Counter barnesAndNobleLinksGenerated;
    private final Counter bookshopLinksGenerated;
    private final Counter audibleLinksGenerated;

    // Counters for encoding errors
    private final Counter barnesAndNobleEncodingErrors;
    private final Counter audibleEncodingErrors;
    private final Counter amazonLinksGenerated;
    private final Counter amazonEncodingErrors;

    @Value("${affiliate.barnesandnoble.publisher-id:#{null}}")
    private String defaultCjPublisherId;

    @Value("${affiliate.barnesandnoble.website-id:#{null}}")
    private String defaultCjWebsiteId;

    @Value("${affiliate.bookshop.affiliate-id:#{null}}")
    private String defaultBookshopAffiliateId;

    // Amazon Associates tag, defaults to 'williamagh-20' if not configured
    @Value("${affiliate.amazon.associate-tag:williamagh-20}")
    private String defaultAmazonAssociateTag;

    /**
     * Constructs the AffiliateLinkService and initializes Micrometer counters
     * @param meterRegistry The MeterRegistry for creating counters
     */
    public AffiliateLinkService(MeterRegistry meterRegistry) {
        this.barnesAndNobleLinksGenerated = meterRegistry.counter("affiliate.links.generated", "type", "barnesandnoble");
        this.bookshopLinksGenerated = meterRegistry.counter("affiliate.links.generated", "type", "bookshop");
        this.audibleLinksGenerated = meterRegistry.counter("affiliate.links.generated", "type", "audible");
        this.barnesAndNobleEncodingErrors = meterRegistry.counter("affiliate.links.encoding.errors", "method", "generateBarnesAndNobleLink");
        this.audibleEncodingErrors = meterRegistry.counter("affiliate.links.encoding.errors", "method", "generateAudibleLink");
        this.amazonLinksGenerated = meterRegistry.counter("affiliate.links.generated", "type", "amazon");
        this.amazonEncodingErrors = meterRegistry.counter("affiliate.links.encoding.errors", "method", "generateAmazonLink");
    }

    /**
     * Generates an affiliate link for Barnes & Noble using CJ Affiliate
     *
     * @param isbn The book's ISBN (preferably ISBN-13)
     * @param cjPublisherId CJ Publisher ID
     * @param cjWebsiteId CJ Website ID for Barnes & Noble, uses default if null/empty
     * @return Affiliate link or direct search link if IDs are missing
     */
    public String generateBarnesAndNobleLink(String isbn, String cjPublisherId, String cjWebsiteId) {
        if (isbn == null || isbn.isEmpty()) {
            // Cannot generate a specific link without ISBN; return base URL or null
            return "https://www.barnesandnoble.com/";
        }

        String actualCjPublisherId = (cjPublisherId == null || cjPublisherId.isEmpty()) ? defaultCjPublisherId : cjPublisherId;
        String actualCjWebsiteId = (cjWebsiteId == null || cjWebsiteId.isEmpty()) ? defaultCjWebsiteId : cjWebsiteId;

        // If affiliate IDs are missing (even after checking defaults), provide a direct search link
        if (actualCjPublisherId == null || actualCjPublisherId.isEmpty() || actualCjWebsiteId == null || actualCjWebsiteId.isEmpty()) {
            return "https://www.barnesandnoble.com/w/?ean=" + isbn; // ean is used for ISBN search
        }
        
        // Affiliate IDs present, generate affiliate link
        String productUrl = "https://www.barnesandnoble.com/w/?ean=" + isbn;
        try {
            String encodedProductUrl = URLEncoder.encode(productUrl, StandardCharsets.UTF_8.toString());
            barnesAndNobleLinksGenerated.increment();
            return String.format("https://www.anrdoezrs.net/click-%s-%s?url=%s&sid=%s",
                                 actualCjPublisherId, actualCjWebsiteId, encodedProductUrl, isbn);
        } catch (UnsupportedEncodingException e) {
            logger.error("Error encoding Barnes & Noble URL: {}", e.getMessage(), e);
            barnesAndNobleEncodingErrors.increment();
            return "https://www.barnesandnoble.com/w/?ean=" + isbn; // Fallback to direct search on error
        }
    }

    /**
     * Generates an affiliate link for Bookshop.org
     *
     * @param isbn The book's ISBN (preferably ISBN-13)
     * @param bookshopAffiliateId Bookshop_org affiliate ID, uses default if null/empty
     * @return Affiliate link or direct product link if ID is missing
     */
    public String generateBookshopLink(String isbn, String bookshopAffiliateId) {
        if (isbn == null || isbn.isEmpty()) {
            // Cannot generate a specific link without ISBN; return base URL or null
            return "https://bookshop.org/";
        }
        String actualBookshopAffiliateId = (bookshopAffiliateId == null || bookshopAffiliateId.isEmpty()) ? defaultBookshopAffiliateId : bookshopAffiliateId;

        // If affiliate ID is missing (even after checking default), provide a direct product link
        if (actualBookshopAffiliateId == null || actualBookshopAffiliateId.isEmpty()) {
            return String.format("https://bookshop.org/book/%s", isbn);
        }
        // Affiliate ID present
        bookshopLinksGenerated.increment();
        return String.format("https://bookshop.org/a/%s/%s", actualBookshopAffiliateId, isbn);
    }

    /**
     * Generates an affiliate link for Audible via Amazon Associates
     *
     * @param asin Book's ASIN for the Audible version
     * @param title Book title (used for search if ASIN unavailable)
     * @param amazonAssociateTag Amazon Associates Tag, uses default if null/empty
     * @return Affiliate link, title search, or generic search based on available data
     */
    public String generateAudibleLink(String asin, String title, String amazonAssociateTag) {
        String actualAmazonAssociateTag = (amazonAssociateTag == null || amazonAssociateTag.isEmpty()) ? defaultAmazonAssociateTag : amazonAssociateTag;

        if (asin != null && !asin.isEmpty()) {
            // ASIN is present, construct direct Audible product link
            String baseUrl = String.format("https://www.audible.com/pd/%s", asin);
            if (actualAmazonAssociateTag == null || actualAmazonAssociateTag.isEmpty()) {
                return baseUrl; // No associate tag, provide direct Audible product link
            }
            // ASIN and tag present: affiliate link
            audibleLinksGenerated.increment();
            return String.format("https://www.audible.com/pd/%s?tag=%s", asin, actualAmazonAssociateTag);
        }
        
        // ASIN is missing, try to use title for search
        if (title != null && !title.isEmpty()) {
            try {
                String encodedTitle = URLEncoder.encode(title, StandardCharsets.UTF_8.toString());
                String searchUrl = String.format("https://www.audible.com/search?keywords=%s", encodedTitle);
                if (actualAmazonAssociateTag != null && !actualAmazonAssociateTag.isEmpty()) {
                    // Attempt to add tag to search URL. Note: Effectiveness may vary by platform/program rules.
                    // It's generally safer for product links, but let's include it if present.
                    searchUrl += String.format("&tag=%s", actualAmazonAssociateTag);
                    // We consider this a generated link if a tag is applied, even to a search
                    audibleLinksGenerated.increment(); 
                }
                return searchUrl;
            } catch (UnsupportedEncodingException e) {
                logger.error("Error encoding title for Audible search: {}", e.getMessage(), e);
                audibleEncodingErrors.increment();
                // Fallback to generic search if title encoding fails
            }
        }
        
        // Fallback to generic Audible search if no ASIN and no title (or title encoding failed)
        // If a tag is present, we can still try to append it to the generic search, though its utility here is low.
        if (actualAmazonAssociateTag != null && !actualAmazonAssociateTag.isEmpty()) {
            // We consider this a generated link if a tag is applied, even to a generic search
            audibleLinksGenerated.increment();
            return String.format("https://www.audible.com/search?tag=%s", actualAmazonAssociateTag);
        }
        return "https://www.audible.com/search"; 
    }

    /**
     * Generates an affiliate link for Amazon.com
     *
     * @param isbn The book's ISBN (preferably ISBN-13 or ISBN-10)
     * @param title Book title (used for search if ISBN unavailable)
     * @param amazonAssociateTag Amazon Associates Tag, uses default if null/empty
     * @return Affiliate link or direct search link if ID/title missing
     */
    public String generateAmazonLink(String isbn, String title, String amazonAssociateTag) {
        String actualTag = (amazonAssociateTag == null || amazonAssociateTag.isEmpty()) ? defaultAmazonAssociateTag : amazonAssociateTag;
        if (isbn != null && !isbn.isEmpty()) {
            String searchUrl = String.format("https://www.amazon.com/s?k=%s", isbn);
            if (actualTag != null && !actualTag.isEmpty()) {
                searchUrl += String.format("&ref=nosim&tag=%s", actualTag);
                amazonLinksGenerated.increment();
            }
            return searchUrl;
        }
        if (title != null && !title.isEmpty()) {
            try {
                String encodedTitle = URLEncoder.encode(title, StandardCharsets.UTF_8.toString());
                String searchUrl = String.format("https://www.amazon.com/s?k=%s", encodedTitle);
                if (actualTag != null && !actualTag.isEmpty()) {
                    searchUrl += String.format("&ref=nosim&tag=%s", actualTag);
                    amazonLinksGenerated.increment();
                }
                return searchUrl;
            } catch (UnsupportedEncodingException e) {
                logger.error("Error encoding title for Amazon search: {}", e.getMessage(), e);
                amazonEncodingErrors.increment();
                return "https://www.amazon.com/";
            }
        }
        if (actualTag != null && !actualTag.isEmpty()) {
            amazonLinksGenerated.increment();
            return String.format("https://www.amazon.com/?tag=%s", actualTag);
        }
        return "https://www.amazon.com/";
    }
}
