/**
 * Test suite for RobotsController validating robots.txt content in different environments
 * 
 * This test class verifies:
 * - Correct robots.txt content for production environment
 * - Correct robots.txt content for non-production environments
 * - Proper handling of different branch configurations
 * - Default behavior when configuration properties are missing
 * - Security configuration for public access to robots.txt endpoint
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.controller;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.beans.factory.annotation.Autowired;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Test class for RobotsController with nested test scenarios for different environments
 */
@WebMvcTest(RobotsController.class)
class RobotsControllerTest {

    /**
     * Test security configuration allowing anonymous access to robots.txt
     * 
     */
    @Configuration
    @EnableWebSecurity
    static class TestSecurityConfig {
        /**
         * Configures security filter chain for test environment
         *
         * @param http HttpSecurity to configure
         * @return SecurityFilterChain allowing public access to robots.txt
         * @throws Exception if security configuration fails
         */
        @Bean
        public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
            http
                // Use a specific matcher to avoid conflict with the main application security config
                .securityMatcher("/robots.txt")
                .authorizeHttpRequests(authorizeRequests -> 
                    authorizeRequests.requestMatchers("/robots.txt").permitAll())
                .csrf(csrf -> csrf.disable());
            return http.build();
        }
    }

    private static final String PERMISSIVE_ROBOTS_TXT = String.join("\n",
            "User-agent: *",
            "Allow: /",
            "Sitemap: https://findmybook.net/sitemap.xml"
    ) + "\n";
    private static final String RESTRICTIVE_ROBOTS_TXT = "User-agent: *\nDisallow: /\n";

    /**
     * Production domain + main branch test scenario
     * 
     */
    @Nested
    @WebMvcTest(RobotsController.class)
    @Import(TestSecurityConfig.class)
    @TestPropertySource(properties = {"coolify.url=https://findmybook.net", "coolify.branch=main"})
    class ProductionDomainAndMainBranch {
        @Autowired
        private MockMvc mockMvc;
        
        /**
         * Verifies permissive robots.txt content for production environment
         * 
         * @throws Exception if test execution fails
         */
        @Test
        void shouldReturnPermissiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(PERMISSIVE_ROBOTS_TXT));
        }
    }

    /**
     * Non-production domain + main branch test scenario
     * 
     */
    @Nested
    @WebMvcTest(RobotsController.class)
    @Import(TestSecurityConfig.class)
    @TestPropertySource(properties = {"coolify.url=https://staging.findmybook.net", "coolify.branch=main"})
    class NonProductionDomain {
        @Autowired
        private MockMvc mockMvc;
        
        /**
         * Verifies restrictive robots.txt content for staging domain
         * 
         * @throws Exception if test execution fails
         */
        @Test
        void shouldReturnRestrictiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(RESTRICTIVE_ROBOTS_TXT));
        }
    }

    /**
     * Production domain + non-main branch test scenario
     * 
     */
    @Nested
    @WebMvcTest(RobotsController.class)
    @Import(TestSecurityConfig.class)
    @TestPropertySource(properties = {"coolify.url=https://findmybook.net", "coolify.branch=develop"})
    class NonMainBranch {
        @Autowired
        private MockMvc mockMvc;
        
        /**
         * Verifies restrictive robots.txt content for non-main branch
         * 
         * @throws Exception if test execution fails
         */
        @Test
        void shouldReturnRestrictiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(RESTRICTIVE_ROBOTS_TXT));
        }
    }

    /**
     * Non-production domain + non-main branch test scenario
     * 
     */
    @Nested
    @WebMvcTest(RobotsController.class)
    @Import(TestSecurityConfig.class)
    @TestPropertySource(properties = {"coolify.url=https://staging.findmybook.net", "coolify.branch=develop"})
    class NonProductionDomainAndNonMainBranch {
        @Autowired
        private MockMvc mockMvc;
        
        /**
         * Verifies restrictive robots.txt content for staging with non-main branch
         * 
         * @throws Exception if test execution fails
         */
        @Test
        void shouldReturnRestrictiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(RESTRICTIVE_ROBOTS_TXT));
        }
    }

    /**
     * Empty URL property test scenario
     * 
     */
    @Nested
    @WebMvcTest(RobotsController.class)
    @Import(TestSecurityConfig.class)
    @TestPropertySource(properties = {"coolify.url=", "coolify.branch=main"})
    class UrlNotSet {
        @Autowired
        private MockMvc mockMvc;
        
        /**
         * Verifies restrictive robots.txt content when URL is not configured
         * 
         * @throws Exception if test execution fails
         */
        @Test
        void shouldReturnRestrictiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(RESTRICTIVE_ROBOTS_TXT));
        }
    }

    /**
     * Empty branch property test scenario
     * 
     */
    @Nested
    @WebMvcTest(RobotsController.class)
    @Import(TestSecurityConfig.class)
    @TestPropertySource(properties = {"coolify.url=https://findmybook.net", "coolify.branch="})
    class BranchNotSet {
        @Autowired
        private MockMvc mockMvc;
        
        /**
         * Verifies restrictive robots.txt content when branch is not configured
         * 
         * @throws Exception if test execution fails
         */
        @Test
        void shouldReturnRestrictiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(RESTRICTIVE_ROBOTS_TXT));
        }
    }

    /**
     * Default configuration test scenario with no properties set
     * 
     */
    @Nested
    @WebMvcTest(RobotsController.class)
    @Import(TestSecurityConfig.class)
    // No @TestPropertySource here, properties should be default/null
    class NoPropertiesSet {
        @Autowired
        private MockMvc mockMvc;
        
        /**
         * Verifies restrictive robots.txt content with default properties
         * 
         * @throws Exception if test execution fails
         */
        @Test
        void shouldReturnRestrictiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(RESTRICTIVE_ROBOTS_TXT));
        }
    }
}
