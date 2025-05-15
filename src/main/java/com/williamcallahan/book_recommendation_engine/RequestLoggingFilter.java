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
import java.util.Set;

@Component
public class RequestLoggingFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(RequestLoggingFilter.class);

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        String uri = req.getRequestURI();
        // Determine extension
        String ext = "";
        int dotIdx = uri.lastIndexOf('.');
        if (dotIdx > 0 && dotIdx < uri.length() - 1) {
            ext = uri.substring(dotIdx + 1).toLowerCase();
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
