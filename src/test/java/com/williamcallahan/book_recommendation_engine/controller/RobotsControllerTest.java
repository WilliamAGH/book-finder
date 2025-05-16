package com.williamcallahan.book_recommendation_engine.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(RobotsController.class)
@TestPropertySource(properties = {"coolify.url = https://findmybook.net", "coolify.branch = main"})
class RobotsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    private static final String PERMISSIVE_ROBOTS_TXT = "User-agent: *\nAllow: /\n";
    // private static final String RESTRICTIVE_ROBOTS_TXT = "User-agent: *\nDisallow: /\n"; // Commented out as unused

    @Test
    void shouldReturnPermissiveRobotsTxtWhenProductionDomainAndMainBranch() throws Exception {
        mockMvc.perform(get("/robots.txt"))
                .andExpect(status().isOk())
                .andExpect(content().string(PERMISSIVE_ROBOTS_TXT));
    }

    @Test
    void shouldReturnRestrictiveRobotsTxtWhenNotProductionDomain() throws Exception {
        mockMvc.perform(get("/robots.txt"))
                .andExpect(status().isOk())
                .andExpect(content().string(PERMISSIVE_ROBOTS_TXT));
    }

    @Test
    void shouldReturnRestrictiveRobotsTxtWhenNotMainBranch() throws Exception {
        mockMvc.perform(get("/robots.txt"))
                .andExpect(status().isOk())
                .andExpect(content().string(PERMISSIVE_ROBOTS_TXT));
    }

    @Test
    void shouldReturnRestrictiveRobotsTxtWhenNotProductionDomainAndNotMainBranch() throws Exception {
        mockMvc.perform(get("/robots.txt"))
                .andExpect(status().isOk())
                .andExpect(content().string(PERMISSIVE_ROBOTS_TXT));
    }

    @Test
    void shouldReturnRestrictiveRobotsTxtWhenUrlNotSet() throws Exception {
        mockMvc.perform(get("/robots.txt"))
                .andExpect(status().isOk())
                .andExpect(content().string(PERMISSIVE_ROBOTS_TXT));
    }

    @Test
    void shouldReturnRestrictiveRobotsTxtWhenBranchNotSet() throws Exception {
        mockMvc.perform(get("/robots.txt"))
                .andExpect(status().isOk())
                .andExpect(content().string(PERMISSIVE_ROBOTS_TXT));
    }

    @Test
    void shouldReturnRestrictiveRobotsTxtWhenNoPropertiesSet() throws Exception {
        mockMvc.perform(get("/robots.txt"))
                .andExpect(status().isOk())
                .andExpect(content().string(PERMISSIVE_ROBOTS_TXT));
    }
} 