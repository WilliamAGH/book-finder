package com.williamcallahan.book_recommendation_engine.controller;

import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestParam;

import com.williamcallahan.book_recommendation_engine.model.image.CoverImageSource;
import java.util.Locale;

/**
 * Controller advice for managing book cover image source preferences
 * 
 * @author William Callahan
 * 
 * Features:
 * - Adds cover source preference to all models automatically
 * - Exposes cover source options for templates
 * - Parses and validates user cover source preferences
 * - Provides fallbacks for invalid preference values
 * - Implements global controller advice pattern
 */
@ControllerAdvice
public class BookCoverPreferenceController {

    /**
     * Add cover source preference to all models for use in templates
     * 
     * @param coverSource The preferred cover source
     * @return The preferred cover source enum
     */
    @ModelAttribute("coverSourcePreference")
    public CoverImageSource addCoverSourcePreference(
            @RequestParam(required = false, defaultValue = "ANY") String coverSource) {
        
        // Parse the source parameter
        CoverImageSource preferredSource;
        try {
            preferredSource = CoverImageSource.valueOf(coverSource.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            preferredSource = CoverImageSource.ANY;
        }
        
        // Return the preferred source to be added to the model
        return preferredSource;
    }
    
    /**
     * Add all available cover source options to the model
     * 
     * @return Array of all cover image source options
     */
    @ModelAttribute("coverSourceOptions")
    public CoverImageSource[] addCoverSourceOptions() {
        return CoverImageSource.values();
    }
}
