package com.williamcallahan.book_recommendation_engine.controller;

import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestParam;

import com.williamcallahan.book_recommendation_engine.model.image.ImageResolutionPreference;
import com.williamcallahan.book_recommendation_engine.util.EnumParsingUtils;

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
        
        return EnumParsingUtils.parseOrDefault(resolutionPref,
                ImageResolutionPreference.class,
                ImageResolutionPreference.ANY);
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
