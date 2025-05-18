/**
 * Controller for exposing API usage metrics
 * - Provides endpoints for monitoring API call volume
 * - Exposes both human-readable and JSON formats
 * - Supports operational monitoring of external API usage
 * - Helps identify excessive API call patterns
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.service.ApiRequestMonitor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST controller for accessing API usage metrics
 * Primarily used for operational monitoring and debugging
 */
@RestController
@RequestMapping("/admin/api-metrics")
public class ApiMetricsController {

    private final ApiRequestMonitor apiRequestMonitor;

    @Autowired
    public ApiMetricsController(ApiRequestMonitor apiRequestMonitor) {
        this.apiRequestMonitor = apiRequestMonitor;
    }

    /**
     * Returns a human-readable report of API usage metrics
     * @return Plain text report of current API metrics
     */
    @GetMapping(value = "/report", produces = MediaType.TEXT_PLAIN_VALUE)
    public String getApiMetricsReport() {
        return apiRequestMonitor.generateReport();
    }

    /**
     * Returns API metrics in JSON format
     * Useful for integration with monitoring systems
     * @return JSON object containing all API metrics
     */
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public Map<String, Object> getApiMetrics() {
        return apiRequestMonitor.getMetricsMap();
    }

    /**
     * Returns current hourly request count
     * @return Current hour's request count
     */
    @GetMapping("/hourly-count")
    public int getHourlyRequestCount() {
        return apiRequestMonitor.getCurrentHourlyRequests();
    }

    /**
     * Returns current daily request count
     * @return Current day's request count
     */
    @GetMapping("/daily-count")
    public int getDailyRequestCount() {
        return apiRequestMonitor.getCurrentDailyRequests();
    }

    /**
     * Returns total request count since application start
     * @return Total API request count
     */
    @GetMapping("/total-count")
    public long getTotalRequestCount() {
        return apiRequestMonitor.getTotalRequests();
    }
}