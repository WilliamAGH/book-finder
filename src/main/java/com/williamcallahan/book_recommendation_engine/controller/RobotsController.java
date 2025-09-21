package com.williamcallahan.book_recommendation_engine.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

/**
 * Controller for serving the robots.txt file with dynamic content based on environment
 *
 * @author William Callahan
 *
 * Features:
 * - Provides different robots.txt content for production vs non-production environments
 * - Allows web crawlers access on production main branch deployment
 * - Restricts crawlers on development, staging, and feature branch deployments
 * - Includes sitemap reference for production environment
 * - Logs environment detection information for debugging
 */
@Controller
@Slf4j
public class RobotsController {


    @Value("${coolify.url:}")
    private String coolifyUrlProp;

    @Value("${coolify.branch:}")
    private String coolifyBranchProp;

    private static final String FINDMYBOOK_NET_URL = "https://findmybook.net";
    private static final String MAIN_BRANCH = "main";

    // Using String.join for multi-line strings for broader Java compatibility
    private static final String PERMISSIVE_ROBOTS_TXT = String.join("\n",
            "User-agent: *",
            "Allow: /",
            "Sitemap: https://findmybook.net/sitemap.xml"
    ) + "\n";

    private static final String RESTRICTIVE_ROBOTS_TXT = String.join("\n",
            "User-agent: *",
            "Disallow: /"
    ) + "\n";

    /**
     * Generates and serves the robots.txt file with environment-specific content
     * 
     * @return String containing the appropriate robots.txt content
     * 
     * @implNote Checks environment variables to determine if running in production
     * Returns permissive rules for production environment with main branch
     * Returns restrictive rules for all other environments to prevent crawler indexing
     */
    @GetMapping(value = "/robots.txt", produces = "text/plain")
    @ResponseBody
    public String getRobotsTxt() {
        String coolifyUrl = this.coolifyUrlProp;
        String coolifyBranch = this.coolifyBranchProp;

        log.info("Generating robots.txt. coolify.url: '{}', coolify.branch: '{}'", coolifyUrl, coolifyBranch);

        boolean isProductionDomain = coolifyUrl != null && coolifyUrl.contains(FINDMYBOOK_NET_URL);
        boolean isMainBranch = MAIN_BRANCH.equalsIgnoreCase(coolifyBranch);

        if (isProductionDomain && isMainBranch) {
            log.info("Serving PERMISSIVE robots.txt for production domain and main branch.");
            return PERMISSIVE_ROBOTS_TXT;
        } else {
            log.warn("Serving RESTRICTIVE robots.txt. Production domain: {}, Main branch: {}", isProductionDomain, isMainBranch);
            return RESTRICTIVE_ROBOTS_TXT;
        }
    }
}
