/**
 * Controller for handling application errors and providing diagnostic information
 *
 * @author William Callahan
 *
 * Features:
 * - Implements Spring Boot's ErrorController interface
 * - Captures detailed error information including stack traces
 * - Renders user-friendly error diagnostic page
 * - Provides enhanced error details for debugging
 * - Extracts exception type information when available
 */

package com.williamcallahan.book_recommendation_engine.controller;

import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.servlet.error.ErrorAttributes;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.WebRequest;
import java.util.Map;

@Controller
@ConditionalOnWebApplication
public class ErrorDiagnosticsController implements ErrorController {

    private ErrorAttributes errorAttributes;

    /**
     * Handles all application errors and prepares diagnostic information
     * - Extracts error attributes from the request
     * - Includes stack traces and exception details
     * - Populates model with error information for template rendering
     * - Extracts exception class name when available
     * 
     * @param request WebRequest containing error information
     * @param model Spring MVC model for view rendering
     * @return Template name for error page
     */
    @RequestMapping("/error")
    public String handleError(WebRequest request, Model model) {
        Map<String, Object> errors = errorAttributes.getErrorAttributes(request, 
            ErrorAttributeOptions.of(
                ErrorAttributeOptions.Include.STACK_TRACE, 
                ErrorAttributeOptions.Include.MESSAGE, 
                ErrorAttributeOptions.Include.EXCEPTION,
                ErrorAttributeOptions.Include.BINDING_ERRORS
            ));
        model.addAttribute("timestamp", errors.get("timestamp"));
        model.addAttribute("status", errors.get("status"));
        model.addAttribute("error", errors.get("error"));
        model.addAttribute("message", errors.get("message"));
        model.addAttribute("trace", errors.get("trace"));
        model.addAttribute("path", errors.get("path"));

        Object message = errors.get("message");
        if ((message == null || message.toString().isEmpty() || "No message available".equals(message)) && errors.get("exception") != null) {
            try {
                String exceptionClassName = (String) errors.get("exception");
                model.addAttribute("exceptionType", Class.forName(exceptionClassName).getSimpleName());
            } catch (ClassNotFoundException e) {
            }
        }
        
        return "error_diagnostics";
    }
}
