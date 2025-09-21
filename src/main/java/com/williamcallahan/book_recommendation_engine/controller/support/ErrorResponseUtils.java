package com.williamcallahan.book_recommendation_engine.controller.support;

import java.util.HashMap;
import java.util.Map;

/**
 * Small helper for producing consistent error payloads across controllers.
 */
public final class ErrorResponseUtils {

    private ErrorResponseUtils() {
        // Utility class
    }

    public static Map<String, String> errorBody(String message) {
        return errorBody(message, null);
    }

    public static Map<String, String> errorBody(String message, String detail) {
        Map<String, String> body = new HashMap<>();
        body.put("error", message);
        if (detail != null && !detail.isBlank()) {
            body.put("message", detail);
        }
        return body;
    }
}
