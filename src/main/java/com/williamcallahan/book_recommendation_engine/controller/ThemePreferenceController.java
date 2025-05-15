package com.williamcallahan.book_recommendation_engine.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Controller for handling theme preferences
 */
@Controller
@RequestMapping("/api/theme")
public class ThemePreferenceController {

    private static final String THEME_COOKIE_NAME = "preferred_theme";
    private static final int COOKIE_MAX_AGE = 365 * 24 * 60 * 60; // 1 year in seconds

    /**
     * Get the current theme preference
     *
     * @param request HTTP request
     * @return Theme preference map
     */
    @GetMapping
    @ResponseBody
    public ResponseEntity<Map<String, String>> getThemePreference(HttpServletRequest request) {
        String themeValue = getThemeCookieValue(request);
        Map<String, String> response = new HashMap<>();
        response.put("theme", themeValue);
        response.put("source", themeValue == null ? "system" : "user");
        
        return ResponseEntity.ok(response);
    }

    /**
     * Update the theme preference
     *
     * @param themeData Theme data map
     * @param response HTTP response
     * @return Updated theme preference
     */
    @PostMapping
    @ResponseBody
    public ResponseEntity<Map<String, String>> updateThemePreference(
            @RequestBody Map<String, String> themeData,
            HttpServletResponse response) {
        
        String themeValue = themeData.get("theme");
        boolean useSystem = Boolean.parseBoolean(themeData.getOrDefault("useSystem", "false"));
        
        Map<String, String> responseData = new HashMap<>();

        if (useSystem) {
            // Delete the cookie to use system preference
            Cookie cookie = new Cookie(THEME_COOKIE_NAME, null);
            cookie.setMaxAge(0);
            cookie.setPath("/");
            response.addCookie(cookie);
            
            responseData.put("theme", null);
            responseData.put("source", "system");
        } else if (themeValue != null && (themeValue.equals("light") || themeValue.equals("dark"))) {
            // Set a cookie with the theme preference
            Cookie cookie = new Cookie(THEME_COOKIE_NAME, themeValue);
            cookie.setMaxAge(COOKIE_MAX_AGE);
            cookie.setPath("/");
            response.addCookie(cookie);
            
            responseData.put("theme", themeValue);
            responseData.put("source", "user");
        } else {
            return ResponseEntity.badRequest().build();
        }
        
        return ResponseEntity.ok(responseData);
    }
    
    /**
     * Helper method to get theme cookie value
     */
    private String getThemeCookieValue(HttpServletRequest request) {
        Cookie[] cookies = request.getCookies();
        if (cookies == null) {
            return null;
        }
        
        Optional<Cookie> themeCookie = Arrays.stream(cookies)
                .filter(c -> THEME_COOKIE_NAME.equals(c.getName()))
                .findFirst();
                
        return themeCookie.map(Cookie::getValue).orElse(null);
    }
}