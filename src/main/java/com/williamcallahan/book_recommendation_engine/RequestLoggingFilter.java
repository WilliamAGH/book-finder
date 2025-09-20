/**
 * Request logging and timing filter for HTTP requests
 *
 * @author William Callahan
 *
 * Features:
 * - Logs incoming HTTP requests with method, URI, and source IP
 * - Measures and logs request processing duration
 * - Records HTTP response status codes
 * - Selectively filters static resource requests to reduce log noise
 * - Special handling for API endpoints for comprehensive API logging
 * - Smart extension-based filtering to focus on important requests
 */
package com.williamcallahan.book_recommendation_engine;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;

@Component
public class RequestLoggingFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(RequestLoggingFilter.class);

    /**
     * Processes HTTP request through the filter chain with logging
     * - Logs request details before processing
     * - Tracks request processing time
     * - Logs completion status and duration
     * - Skips logging for common static resource extensions
     * 
     * @param request The incoming servlet request
     * @param response The servlet response
     * @param chain The filter processing chain
     * @throws IOException If an I/O error occurs during request processing
     * @throws ServletException If a servlet error occurs during processing
     */
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        String uri = req.getRequestURI();
        // Determine extension
        String ext = "";
        int dotIdx = uri.lastIndexOf('.');
        if (dotIdx > 0 && dotIdx < uri.length() - 1) {
            ext = uri.substring(dotIdx + 1).toLowerCase(Locale.ROOT);
        }
        boolean isApi = uri.startsWith("/api");
        Set<String> logExts = Set.of("png","jpg","jpeg","svg","css","js","ico","html");
        // Skip logging for non-API with non-whitelisted extensions
        if (!isApi && !ext.isEmpty() && !logExts.contains(ext)) {
            chain.doFilter(request, response);
            return;
        }
        long startTime = System.currentTimeMillis();
        logger.info("Incoming request: {} {} from {}", req.getMethod(), uri, req.getRemoteAddr());
        chain.doFilter(request, response);
        long duration = System.currentTimeMillis() - startTime;
        int status = response instanceof HttpServletResponse ? ((HttpServletResponse) response).getStatus() : 0;
        logger.info("Completed request: {} {} with status {} in {} ms", req.getMethod(), uri, status, duration);
    }
}
