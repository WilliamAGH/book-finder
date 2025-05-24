/**
 * Unit tests for the AffiliateLinkService
 * Verifies correct link generation for various affiliate programs and scenarios
 * Mocks MeterRegistry for testing Micrometer integration
 * 
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

/**
 * Test class for {@link AffiliateLinkService}
 * Ensures affiliate links are generated correctly under various conditions
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AffiliateLinkServiceTest {

    @InjectMocks
    private AffiliateLinkService affiliateLinkService;

    @Mock
    private MeterRegistry meterRegistry;
    
    @Mock
    private Counter mockCounter;


    private static final String TEST_ISBN = "9780061120084";
    private static final String TEST_ASIN = "B000FC1P30";
    private static final String TEST_TITLE = "To Kill a Mockingbird";
    private static final String CJ_PUBLISHER_ID = "testPublisherId";
    private static final String CJ_WEBSITE_ID = "testWebsiteId";
    private static final String BOOKSHOP_AFFILIATE_ID = "testBookshopId";
    private static final String AMAZON_ASSOCIATE_TAG = "testAmazonTag-20";

    /**
     * Sets up mock behavior for MeterRegistry and initializes default affiliate IDs before each test
     */
    @BeforeEach
    void setUp() {
        // Stub counters to return mockCounter
        when(meterRegistry.counter(anyString(), anyString(), anyString())).thenReturn(mockCounter);
        when(meterRegistry.counter(anyString())).thenReturn(mockCounter);
        // Assign mockCounter to all counters in the service instance
        ReflectionTestUtils.setField(affiliateLinkService, "barnesAndNobleLinksGenerated", mockCounter);
        ReflectionTestUtils.setField(affiliateLinkService, "bookshopLinksGenerated", mockCounter);
        ReflectionTestUtils.setField(affiliateLinkService, "audibleLinksGenerated", mockCounter);
        ReflectionTestUtils.setField(affiliateLinkService, "barnesAndNobleEncodingErrors", mockCounter);
        ReflectionTestUtils.setField(affiliateLinkService, "audibleEncodingErrors", mockCounter);
        ReflectionTestUtils.setField(affiliateLinkService, "amazonLinksGenerated", mockCounter);
        ReflectionTestUtils.setField(affiliateLinkService, "amazonEncodingErrors", mockCounter);

        // Initialize default affiliate IDs using ReflectionTestUtils as they are @Value injected
        ReflectionTestUtils.setField(affiliateLinkService, "defaultBookshopAffiliateId", BOOKSHOP_AFFILIATE_ID);
        ReflectionTestUtils.setField(affiliateLinkService, "defaultAmazonAssociateTag", AMAZON_ASSOCIATE_TAG);
    }

    // Barnes & Noble Tests
    /**
     * Tests Barnes & Noble link generation with valid ISBN and CJ IDs
     * Verifies correct affiliate link format and metric increment
     * @throws UnsupportedEncodingException if URL encoding fails
     */
    @Test
    void generateBarnesAndNobleLink_validIsbnAndCjIds_returnsAffiliateLink() throws UnsupportedEncodingException {
        String productUrl = "https://www.barnesandnoble.com/w/?ean=" + TEST_ISBN;
        String encodedUrl = URLEncoder.encode(productUrl, StandardCharsets.UTF_8.toString());
        String expectedLink = String.format("https://www.anrdoezrs.net/click-%s-%s?url=%s&sid=%s",
                CJ_PUBLISHER_ID, CJ_WEBSITE_ID, encodedUrl, TEST_ISBN);
        assertEquals(expectedLink, affiliateLinkService.generateBarnesAndNobleLink(TEST_ISBN, CJ_PUBLISHER_ID, CJ_WEBSITE_ID));
        verify(meterRegistry).counter("affiliate.links.generated", "type", "barnesandnoble");
        verify(mockCounter).increment();
    }

    /**
     * Tests Barnes & Noble link generation with valid ISBN and null CJ Publisher ID
     * Verifies fallback to direct search link
     */
    @Test
    void generateBarnesAndNobleLink_validIsbnNullCjPublisherId_returnsDirectSearchLink() {
        String expectedLink = "https://www.barnesandnoble.com/w/?ean=" + TEST_ISBN;
        assertEquals(expectedLink, affiliateLinkService.generateBarnesAndNobleLink(TEST_ISBN, null, CJ_WEBSITE_ID));
    }
    
    /**
     * Tests Barnes & Noble link generation with valid ISBN and empty CJ Publisher ID
     * Verifies fallback to direct search link
     */
    @Test
    void generateBarnesAndNobleLink_validIsbnEmptyCjPublisherId_returnsDirectSearchLink() {
        String expectedLink = "https://www.barnesandnoble.com/w/?ean=" + TEST_ISBN;
        assertEquals(expectedLink, affiliateLinkService.generateBarnesAndNobleLink(TEST_ISBN, "", CJ_WEBSITE_ID));
    }

    /**
     * Tests Barnes & Noble link generation with valid ISBN and null CJ Website ID
     * Verifies fallback to direct search link
     */
    @Test
    void generateBarnesAndNobleLink_validIsbnNullCjWebsiteId_returnsDirectSearchLink() {
        String expectedLink = "https://www.barnesandnoble.com/w/?ean=" + TEST_ISBN;
        assertEquals(expectedLink, affiliateLinkService.generateBarnesAndNobleLink(TEST_ISBN, CJ_PUBLISHER_ID, null));
    }

    /**
     * Tests Barnes & Noble link generation with valid ISBN and empty CJ Website ID
     * Verifies fallback to direct search link
     */
    @Test
    void generateBarnesAndNobleLink_validIsbnEmptyCjWebsiteId_returnsDirectSearchLink() {
        String expectedLink = "https://www.barnesandnoble.com/w/?ean=" + TEST_ISBN;
        assertEquals(expectedLink, affiliateLinkService.generateBarnesAndNobleLink(TEST_ISBN, CJ_PUBLISHER_ID, ""));
    }

    /**
     * Tests Barnes & Noble link generation with valid ISBN and both CJ IDs null
     * Verifies fallback to direct search link
     */
    @Test
    void generateBarnesAndNobleLink_validIsbnBothCjIdsNull_returnsDirectSearchLink() {
        String expectedLink = "https://www.barnesandnoble.com/w/?ean=" + TEST_ISBN;
        assertEquals(expectedLink, affiliateLinkService.generateBarnesAndNobleLink(TEST_ISBN, null, null));
    }
    
    /**
     * Tests Barnes & Noble link generation with valid ISBN and both CJ IDs empty
     * Verifies fallback to direct search link
     */
    @Test
    void generateBarnesAndNobleLink_validIsbnBothCjIdsEmpty_returnsDirectSearchLink() {
        String expectedLink = "https://www.barnesandnoble.com/w/?ean=" + TEST_ISBN;
        assertEquals(expectedLink, affiliateLinkService.generateBarnesAndNobleLink(TEST_ISBN, "", ""));
    }

    /**
     * Tests Barnes & Noble link generation with null ISBN
     * Verifies return of base URL
     */
    @Test
    void generateBarnesAndNobleLink_nullIsbn_returnsBaseUrl() {
        assertEquals("https://www.barnesandnoble.com/", affiliateLinkService.generateBarnesAndNobleLink(null, CJ_PUBLISHER_ID, CJ_WEBSITE_ID));
    }

    /**
     * Tests Barnes & Noble link generation with empty ISBN
     * Verifies return of base URL
     */
    @Test
    void generateBarnesAndNobleLink_emptyIsbn_returnsBaseUrl() {
        assertEquals("https://www.barnesandnoble.com/", affiliateLinkService.generateBarnesAndNobleLink("", CJ_PUBLISHER_ID, CJ_WEBSITE_ID));
    }

    // Test for encoding failure is tricky without deeper mocking of URLEncoder itself or refactoring the service
    // For now, we assume URLEncoder works as expected by the JDK.
    // If we could mock static URLEncoder.encode, we would throw UnsupportedEncodingException.
    // A possible refactor would be to wrap URLEncoder_encode in a protected method that can be overridden in a test subclass

    // Bookshop.org Tests
    /**
     * Tests Bookshop_org link generation with valid ISBN and affiliate ID
     * Verifies correct affiliate link format and metric increment
     */
    @Test
    void generateBookshopLink_validIsbnAndAffiliateId_returnsAffiliateLink() {
        String expectedLink = String.format("https://bookshop.org/a/%s/%s", BOOKSHOP_AFFILIATE_ID, TEST_ISBN);
        assertEquals(expectedLink, affiliateLinkService.generateBookshopLink(TEST_ISBN, BOOKSHOP_AFFILIATE_ID));
        verify(meterRegistry).counter("affiliate.links.generated", "type", "bookshop");
        verify(mockCounter, times(1)).increment(); // Called for Bookshop only
    }

    /**
     * Tests Bookshop_org link generation with valid ISBN and null affiliate ID
     * Verifies usage of default affiliate ID
s     */
    @Test
    void generateBookshopLink_validIsbnNullAffiliateId_usesDefaultAffiliateId() {
        // Test with default ID from @Value (set in setUp)
        String expectedLink = String.format("https://bookshop.org/a/%s/%s", BOOKSHOP_AFFILIATE_ID, TEST_ISBN);
        assertEquals(expectedLink, affiliateLinkService.generateBookshopLink(TEST_ISBN, null));
    }
    
    /**
     * Tests Bookshop_org link generation with valid ISBN and empty affiliate ID
     * Verifies usage of default affiliate ID
     */
    @Test
    void generateBookshopLink_validIsbnEmptyAffiliateId_usesDefaultAffiliateId() {
        String expectedLink = String.format("https://bookshop.org/a/%s/%s", BOOKSHOP_AFFILIATE_ID, TEST_ISBN);
        assertEquals(expectedLink, affiliateLinkService.generateBookshopLink(TEST_ISBN, ""));
    }
    
    /**
     * Tests Bookshop_org link generation with valid ISBN, null affiliate ID, and no default ID set
     * Verifies fallback to direct product link
     */
    @Test
    void generateBookshopLink_validIsbnNoDefaultAffiliateIdAndNullParam_returnsDirectProductLink() {
        ReflectionTestUtils.setField(affiliateLinkService, "defaultBookshopAffiliateId", null);
        String expectedLink = String.format("https://bookshop.org/book/%s", TEST_ISBN);
        assertEquals(expectedLink, affiliateLinkService.generateBookshopLink(TEST_ISBN, null));
    }

    /**
     * Tests Bookshop_org link generation with null ISBN
     * Verifies return of base URL
     */
    @Test
    void generateBookshopLink_nullIsbn_returnsBaseUrl() {
        assertEquals("https://bookshop.org/", affiliateLinkService.generateBookshopLink(null, BOOKSHOP_AFFILIATE_ID));
    }

    /**
     * Tests Bookshop_org link generation with empty ISBN
     * Verifies return of base URL
     */
    @Test
    void generateBookshopLink_emptyIsbn_returnsBaseUrl() {
        assertEquals("https://bookshop.org/", affiliateLinkService.generateBookshopLink("", BOOKSHOP_AFFILIATE_ID));
    }

    // Audible Tests
    /**
     * Tests Audible link generation with valid ASIN and associate tag
     * Verifies correct affiliate link format and metric increment
     */
    @Test
    void generateAudibleLink_validAsinAndAssociateTag_returnsAffiliateLink() {
        String expectedLink = String.format("https://www.audible.com/pd/%s?tag=%s", TEST_ASIN, AMAZON_ASSOCIATE_TAG);
        assertEquals(expectedLink, affiliateLinkService.generateAudibleLink(TEST_ASIN, TEST_TITLE, AMAZON_ASSOCIATE_TAG));
        verify(meterRegistry).counter("affiliate.links.generated", "type", "audible");
        verify(mockCounter, times(1)).increment(); // Called for Audible only
    }

    /**
     * Tests Audible link generation with valid ASIN and null associate tag
     * Verifies usage of default associate tag
     */
    @Test
    void generateAudibleLink_validAsinNullAssociateTag_usesDefaultAssociateTag() {
        String expectedLink = String.format("https://www.audible.com/pd/%s?tag=%s", TEST_ASIN, AMAZON_ASSOCIATE_TAG);
        assertEquals(expectedLink, affiliateLinkService.generateAudibleLink(TEST_ASIN, TEST_TITLE, null));
    }
    
    /**
     * Tests Audible link generation with valid ASIN and empty associate tag
     * Verifies usage of default associate tag
     */
    @Test
    void generateAudibleLink_validAsinEmptyAssociateTag_usesDefaultAssociateTag() {
        String expectedLink = String.format("https://www.audible.com/pd/%s?tag=%s", TEST_ASIN, AMAZON_ASSOCIATE_TAG);
        assertEquals(expectedLink, affiliateLinkService.generateAudibleLink(TEST_ASIN, TEST_TITLE, ""));
    }

    /**
     * Tests Audible link generation with valid ASIN, null associate tag, and no default tag set
     * Verifies fallback to direct product link
     */
    @Test
    void generateAudibleLink_validAsinNoDefaultAssociateTagAndNullParam_returnsDirectProductLink() {
        ReflectionTestUtils.setField(affiliateLinkService, "defaultAmazonAssociateTag", null);
        String expectedLink = String.format("https://www.audible.com/pd/%s", TEST_ASIN);
        assertEquals(expectedLink, affiliateLinkService.generateAudibleLink(TEST_ASIN, TEST_TITLE, null));
    }

    /**
     * Tests Audible link generation with null ASIN, valid title, and associate tag
     * Verifies generation of search link with tag
     * @throws UnsupportedEncodingException if URL encoding fails
     */
    @Test
    void generateAudibleLink_nullAsinValidTitleAndAssociateTag_returnsSearchLinkWithTag() throws UnsupportedEncodingException {
        String encodedTitle = URLEncoder.encode(TEST_TITLE, StandardCharsets.UTF_8.toString());
        String expectedLink = String.format("https://www.audible.com/search?keywords=%s&tag=%s", encodedTitle, AMAZON_ASSOCIATE_TAG);
        assertEquals(expectedLink, affiliateLinkService.generateAudibleLink(null, TEST_TITLE, AMAZON_ASSOCIATE_TAG));
    }

    /**
     * Tests Audible link generation with null ASIN, valid title, and null associate tag
     * Verifies usage of default associate tag for search link
     * @throws UnsupportedEncodingException if URL encoding fails
     */
    @Test
    void generateAudibleLink_nullAsinValidTitleNullAssociateTag_usesDefaultAssociateTagForSearch() throws UnsupportedEncodingException {
        String encodedTitle = URLEncoder.encode(TEST_TITLE, StandardCharsets.UTF_8.toString());
        String expectedLink = String.format("https://www.audible.com/search?keywords=%s&tag=%s", encodedTitle, AMAZON_ASSOCIATE_TAG);
        assertEquals(expectedLink, affiliateLinkService.generateAudibleLink(null, TEST_TITLE, null));
    }
    
    /**
     * Tests Audible link generation with null ASIN, valid title, and empty associate tag
     * Verifies usage of default associate tag for search link
     * @throws UnsupportedEncodingException if URL encoding fails
     */
    @Test
    void generateAudibleLink_nullAsinValidTitleEmptyAssociateTag_usesDefaultAssociateTagForSearch() throws UnsupportedEncodingException {
        String encodedTitle = URLEncoder.encode(TEST_TITLE, StandardCharsets.UTF_8.toString());
        String expectedLink = String.format("https://www.audible.com/search?keywords=%s&tag=%s", encodedTitle, AMAZON_ASSOCIATE_TAG);
        assertEquals(expectedLink, affiliateLinkService.generateAudibleLink(null, TEST_TITLE, ""));
    }

    /**
     * Tests Audible link generation with null ASIN, valid title, null associate tag, and no default tag
     * Verifies generation of search link without tag
     * @throws UnsupportedEncodingException if URL encoding fails
     */
    @Test
    void generateAudibleLink_nullAsinValidTitleNoDefaultAssociateTagAndNullParam_returnsSearchLinkWithoutTag() throws UnsupportedEncodingException {
        ReflectionTestUtils.setField(affiliateLinkService, "defaultAmazonAssociateTag", null);
        String encodedTitle = URLEncoder.encode(TEST_TITLE, StandardCharsets.UTF_8.toString());
        String expectedLink = String.format("https://www.audible.com/search?keywords=%s", encodedTitle);
        assertEquals(expectedLink, affiliateLinkService.generateAudibleLink(null, TEST_TITLE, null));
    }

    /**
     * Tests Audible link generation with null ASIN, null title, and associate tag
     * Verifies generation of generic search link with tag
     */
    @Test
    void generateAudibleLink_nullAsinNullTitleWithAssociateTag_returnsGenericSearchWithTag() {
        String expectedLink = String.format("https://www.audible.com/search?tag=%s", AMAZON_ASSOCIATE_TAG);
        assertEquals(expectedLink, affiliateLinkService.generateAudibleLink(null, null, AMAZON_ASSOCIATE_TAG));
    }
    
    /**
     * Tests Audible link generation with null ASIN, empty title, and associate tag
     * Verifies generation of generic search link with tag
     */
    @Test
    void generateAudibleLink_nullAsinEmptyTitleWithAssociateTag_returnsGenericSearchWithTag() {
        String expectedLink = String.format("https://www.audible.com/search?tag=%s", AMAZON_ASSOCIATE_TAG);
        assertEquals(expectedLink, affiliateLinkService.generateAudibleLink(null, "", AMAZON_ASSOCIATE_TAG));
    }

    /**
     * Tests Audible link generation with null ASIN, null title, and null associate tag
     * Verifies usage of default associate tag for generic search
     */
    @Test
    void generateAudibleLink_nullAsinNullTitleNullAssociateTag_usesDefaultAssociateTagForGenericSearch() {
        String expectedLink = String.format("https://www.audible.com/search?tag=%s", AMAZON_ASSOCIATE_TAG);
        assertEquals(expectedLink, affiliateLinkService.generateAudibleLink(null, null, null));
    }
    
    /**
     * Tests Audible link generation with null ASIN, null title, and empty associate tag
     * Verifies usage of default associate tag for generic search
     */
    @Test
    void generateAudibleLink_nullAsinNullTitleEmptyAssociateTag_usesDefaultAssociateTagForGenericSearch() {
        String expectedLink = String.format("https://www.audible.com/search?tag=%s", AMAZON_ASSOCIATE_TAG);
        assertEquals(expectedLink, affiliateLinkService.generateAudibleLink(null, null, ""));
    }

    /**
     * Tests Audible link generation with null ASIN, null title, null associate tag, and no default tag
     * Verifies generation of generic search link without tag
     */
    @Test
    void generateAudibleLink_nullAsinNullTitleNoDefaultAssociateTagAndNullParam_returnsGenericSearchWithoutTag() {
        ReflectionTestUtils.setField(affiliateLinkService, "defaultAmazonAssociateTag", null);
        assertEquals("https://www.audible.com/search", affiliateLinkService.generateAudibleLink(null, null, null));
    }
    
    // Test for encoding failure in Audible link generation (similar to B&N, would require refactor or deeper mocking)

    // Amazon Tests
    /**
     * Tests Amazon link generation with valid ISBN and associate tag
     * Verifies correct affiliate link format and metric increment
     */
    @Test
    void generateAmazonLink_validIsbnAndAssociateTag_returnsAffiliateLink() {
        String expectedLink = String.format("https://www.amazon.com/dp/%s?tag=%s", TEST_ISBN, AMAZON_ASSOCIATE_TAG);
        assertEquals(expectedLink, affiliateLinkService.generateAmazonLink(TEST_ISBN, TEST_TITLE, AMAZON_ASSOCIATE_TAG));
        verify(meterRegistry).counter("affiliate.links.generated", "type", "amazon");
        verify(mockCounter, times(1)).increment();
    }

    /**
     * Tests Amazon link generation with null ISBN, valid title, and associate tag
     * Verifies generation of search link with tag
     * @throws UnsupportedEncodingException if URL encoding fails
     */
    @Test
    void generateAmazonLink_nullIsbnValidTitleAndAssociateTag_returnsSearchLinkWithTag() throws UnsupportedEncodingException {
        String encodedTitle = URLEncoder.encode(TEST_TITLE, StandardCharsets.UTF_8.toString());
        String expectedLink = String.format("https://www.amazon.com/s?k=%s&tag=%s", encodedTitle, AMAZON_ASSOCIATE_TAG);
        assertEquals(expectedLink, affiliateLinkService.generateAmazonLink(null, TEST_TITLE, AMAZON_ASSOCIATE_TAG));
    }
}
