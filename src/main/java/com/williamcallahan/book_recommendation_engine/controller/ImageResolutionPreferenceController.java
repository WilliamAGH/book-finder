package com.williamcallahan.book_recommendation_engine.controller;

import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestParam;

import com.williamcallahan.book_recommendation_engine.types.ImageResolutionPreference;

/**
 * Controller advice for managing image resolution preferences
 *
 * @author William Callahan
 *
 * Features:
 * - Automatically adds resolution preference to all models
 * - Exposes resolution options for template selection UI
 * - Parses and validates user resolution preferences
 * - Provides sensible defaults for invalid parameter values
 * - Implements global controller advice pattern
 */
@ControllerAdvice
public class ImageResolutionPreferenceController {

    /**
     * Add resolution preference to all models for use in templates
     * 
     * @param resolutionPref The preferred image resolution
     * @return The preferred image resolution enum
     */
    @ModelAttribute("resolutionPreference")
    public ImageResolutionPreference addResolutionPreference(
            @RequestParam(required = false, defaultValue = "ANY") String resolutionPref) {
        
        // Parse the resolution parameter
        ImageResolutionPreference preferredResolution;
        try {
            preferredResolution = ImageResolutionPreference.valueOf(resolutionPref.toUpperCase());
        } catch (IllegalArgumentException e) {
            preferredResolution = ImageResolutionPreference.ANY;
        }
        
        // Return the preferred resolution to be added to the model
        return preferredResolution;
    }
    
    /**
     * Add all available resolution options to the model
     * 
     * @return Array of all image resolution preference options
     */
    @ModelAttribute("resolutionOptions")
    public ImageResolutionPreference[] addResolutionOptions() {
        return ImageResolutionPreference.values();
    }
}
