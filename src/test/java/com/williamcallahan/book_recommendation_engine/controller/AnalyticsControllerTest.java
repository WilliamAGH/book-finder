/**
 * Unit tests for AnalyticsController
 *
 * @author William Callahan
 *
 * Features:
 * - Tests Clicky analytics script serving functionality
 * - Validates site ID security checks
 * - Tests caching behavior and error handling
 * - Verifies correct HTTP headers and content types
 */

package com.williamcallahan.book_recommendation_engine.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.mockito.Mockito;
import com.williamcallahan.book_recommendation_engine.config.SecurityConfig;
import com.williamcallahan.book_recommendation_engine.config.CustomBasicAuthenticationEntryPoint;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.RestTemplate;
import org.springframework.security.test.context.support.WithAnonymousUser;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.eq;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import org.springframework.test.web.servlet.MvcResult;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;

@WebMvcTest(AnalyticsController.class)
@Import({SecurityConfig.class, CustomBasicAuthenticationEntryPoint.class})
@WithAnonymousUser
@TestPropertySource(properties = {
    "app.security.admin.password=testadminpass",
    "app.security.user.password=testuserpass",
    "app.clicky.enabled=true",
    "app.clicky.site-id=101484793",
    "app.clicky.cache-duration-minutes=60"
})
class AnalyticsControllerTest {

    /**
     * Test configuration for AnalyticsController tests
     */
    @TestConfiguration
    static class AnalyticsControllerTestConfig {
        /**
         * Provides mocked RestTemplate for testing
         *
         * @return Mocked RestTemplate instance
         */
        @Bean
        public RestTemplate restTemplate() {
            return Mockito.mock(RestTemplate.class);
        }
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private RestTemplate restTemplate;

    /**
     * Tests successful script retrieval with valid site ID
     */
    @Test
    void testGetClickyScriptWithValidSiteId() throws Exception {
        String expectedScriptContent = "var clicky_site_ids = clicky_site_ids || []; clicky_site_ids.push(101484793);";
        when(restTemplate.getForObject(eq("https://static.getclicky.com/101484793.js"), eq(String.class)))
            .thenReturn(expectedScriptContent);

        MvcResult mvcResult = mockMvc.perform(get("/analytics/clicky/101484793.js"))
            .andExpect(request().asyncStarted())
            .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
            .andExpect(status().isOk())
            .andExpect(header().string("Content-Type", "application/javascript"))
            .andExpect(header().exists("Cache-Control"))
            .andExpect(content().string(expectedScriptContent));
    }

    /**
     * Tests rejection of invalid site ID format
     */
    @Test
    void testGetClickyScriptWithInvalidSiteId() throws Exception {
        MvcResult mvcResult = mockMvc.perform(get("/analytics/clicky/invalid.js"))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isNotFound());
    }

    /**
     * Tests rejection of incorrect but valid-format site ID
     */
    @Test
    void testGetClickyScriptWithWrongSiteId() throws Exception {
        MvcResult mvcResult = mockMvc.perform(get("/analytics/clicky/999999999.js"))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isNotFound());
    }
}
