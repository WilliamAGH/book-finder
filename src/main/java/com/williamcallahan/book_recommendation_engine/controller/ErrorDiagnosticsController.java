package com.williamcallahan.book_recommendation_engine.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.servlet.error.ErrorAttributes;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.context.request.WebRequest;
import java.util.Map;

@Controller
public class ErrorDiagnosticsController implements ErrorController {

    @Autowired
    private ErrorAttributes errorAttributes;

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
