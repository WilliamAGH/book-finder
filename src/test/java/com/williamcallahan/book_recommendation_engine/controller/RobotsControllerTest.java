package com.williamcallahan.book_recommendation_engine.controller;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.beans.factory.annotation.Autowired;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Test class for RobotsController
 *
 * @author William Callahan
 *
 * Verifies robots.txt content generation under different environment conditions
 */
@WebMvcTest(RobotsController.class)
class RobotsControllerTest {

    private static final String PERMISSIVE_ROBOTS_TXT = String.join("\n",
            "User-agent: *",
            "Allow: /",
            "Sitemap: https://findmybook.net/sitemap.xml"
    ) + "\n";
    private static final String RESTRICTIVE_ROBOTS_TXT = "User-agent: *\nDisallow: /\n";

    @Nested
    @WebMvcTest(RobotsController.class)
    @TestPropertySource(properties = {"coolify.url=https://findmybook.net", "coolify.branch=main"})
    class ProductionDomainAndMainBranch {
        @Autowired
        private MockMvc mockMvc;
        @Test
        void shouldReturnPermissiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(PERMISSIVE_ROBOTS_TXT));
        }
    }

    @Nested
    @WebMvcTest(RobotsController.class)
    @TestPropertySource(properties = {"coolify.url=https://staging.findmybook.net", "coolify.branch=main"})
    class NonProductionDomain {
        @Autowired
        private MockMvc mockMvc;
        @Test
        void shouldReturnRestrictiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(RESTRICTIVE_ROBOTS_TXT));
        }
    }

    @Nested
    @WebMvcTest(RobotsController.class)
    @TestPropertySource(properties = {"coolify.url=https://findmybook.net", "coolify.branch=develop"})
    class NonMainBranch {
        @Autowired
        private MockMvc mockMvc;
        @Test
        void shouldReturnRestrictiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(RESTRICTIVE_ROBOTS_TXT));
        }
    }

    @Nested
    @WebMvcTest(RobotsController.class)
    @TestPropertySource(properties = {"coolify.url=https://staging.findmybook.net", "coolify.branch=develop"})
    class NonProductionDomainAndNonMainBranch {
        @Autowired
        private MockMvc mockMvc;
        @Test
        void shouldReturnRestrictiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(RESTRICTIVE_ROBOTS_TXT));
        }
    }

    @Nested
    @WebMvcTest(RobotsController.class)
    @TestPropertySource(properties = {"coolify.url=", "coolify.branch=main"})
    class UrlNotSet {
        @Autowired
        private MockMvc mockMvc;
        @Test
        void shouldReturnRestrictiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(RESTRICTIVE_ROBOTS_TXT));
        }
    }

    @Nested
    @WebMvcTest(RobotsController.class)
    @TestPropertySource(properties = {"coolify.url=https://findmybook.net", "coolify.branch="})
    class BranchNotSet {
        @Autowired
        private MockMvc mockMvc;
        @Test
        void shouldReturnRestrictiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(RESTRICTIVE_ROBOTS_TXT));
        }
    }

    @Nested
    @WebMvcTest(RobotsController.class)
    // No @TestPropertySource here, properties should be default/null
    class NoPropertiesSet {
        @Autowired
        private MockMvc mockMvc;
        @Test
        void shouldReturnRestrictiveRobotsTxt() throws Exception {
            mockMvc.perform(get("/robots.txt"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(RESTRICTIVE_ROBOTS_TXT));
        }
    }
}
