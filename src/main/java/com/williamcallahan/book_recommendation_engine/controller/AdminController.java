package com.williamcallahan.book_recommendation_engine.controller;

import com.williamcallahan.book_recommendation_engine.service.S3CoverCleanupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin")
public class AdminController {

    private static final Logger logger = LoggerFactory.getLogger(AdminController.class);

    private final S3CoverCleanupService s3CoverCleanupService;
    private final String configuredS3Prefix;
    private final int defaultBatchLimit;
    private final String configuredQuarantinePrefix; // Added

    @Autowired
    public AdminController(S3CoverCleanupService s3CoverCleanupService,
                           @Value("${app.s3.cleanup.prefix:images/book-covers/}") String configuredS3Prefix,
                           @Value("${app.s3.cleanup.default-batch-limit:100}") int defaultBatchLimit,
                           @Value("${app.s3.cleanup.quarantine-prefix:images/non-covers-pages/}") String configuredQuarantinePrefix) { // Added
        this.s3CoverCleanupService = s3CoverCleanupService;
        this.configuredS3Prefix = configuredS3Prefix;
        this.defaultBatchLimit = defaultBatchLimit;
        this.configuredQuarantinePrefix = configuredQuarantinePrefix; // Added
    }

    /**
     * Triggers a dry run of the S3 cover cleanup process.
     * The S3 prefix to scan can be overridden by a request parameter,
     * otherwise, the configured 'app.s3.cleanup.prefix' is used.
     * The number of items to process can be limited by a request parameter,
     * otherwise, the configured 'app.s3.cleanup.default-batch-limit' is used.
     *
     * @param prefixOptional Optional request parameter to override the S3 prefix.
     * @param limitOptional Optional request parameter to override the batch processing limit.
     * @return A ResponseEntity containing a plain text summary and list of flagged files.
     */
    @GetMapping(value = "/s3-cleanup/dry-run", produces = MediaType.TEXT_PLAIN_VALUE)
    public ResponseEntity<String> triggerS3CoverCleanupDryRun(
            @RequestParam(name = "prefix", required = false) String prefixOptional,
            @RequestParam(name = "limit", required = false) Integer limitOptional) {
        
        String prefixToUse = prefixOptional != null ? prefixOptional : configuredS3Prefix;
        int batchLimitToUse = limitOptional != null ? limitOptional : defaultBatchLimit;
        if (batchLimitToUse <= 0) { // Ensure batch limit is positive, or treat 0/negative as no limit (process all)
            // This behavior can be adjusted. For now, let's say 0 or negative means a very large number (effectively no limit for practical purposes)
            // or stick to a sane default if that's preferred.
            // The S3CoverCleanupService currently handles batchLimit > 0.
            // If batchLimit is 0 or negative, it processes all.
            logger.warn("Batch limit specified as {} (or defaulted to it), which means no effective limit. Processing all found items.", batchLimitToUse);
        }
        
        logger.info("Admin endpoint /admin/s3-cleanup/dry-run invoked. Triggering S3 Cover Cleanup Dry Run with prefix: '{}', limit: {}", prefixToUse, batchLimitToUse);

        // Note: This is a synchronous call. For very long operations,
        // consider making performDryRun @Async or wrapping this call.
        try {
            com.williamcallahan.book_recommendation_engine.types.DryRunSummary summary = s3CoverCleanupService.performDryRun(prefixToUse, batchLimitToUse);
            
            StringBuilder responseBuilder = new StringBuilder();
            responseBuilder.append(String.format(
                "S3 Cover Cleanup Dry Run completed for prefix: '%s', limit: %d.\n",
                prefixToUse, batchLimitToUse
            ));
            responseBuilder.append(String.format(
                "Total Objects Scanned: %d, Total Objects Flagged: %d\n",
                summary.getTotalScanned(), summary.getTotalFlagged()
            ));

            if (summary.getTotalFlagged() > 0) {
                responseBuilder.append("\nFlagged File Keys:\n");
                for (String key : summary.getFlaggedFileKeys()) {
                    responseBuilder.append(key).append("\n");
                }
            } else {
                responseBuilder.append("\nNo files were flagged.\n");
            }
            
            String responseBody = responseBuilder.toString();
            logger.info("S3 Cover Cleanup Dry Run response prepared for prefix: '{}', limit: {}. Summary: {} flagged out of {} scanned.", 
                        prefixToUse, batchLimitToUse, summary.getTotalFlagged(), summary.getTotalScanned());
            return ResponseEntity.ok(responseBody);
        } catch (Exception e) {
            String errorMessage = String.format("Failed to complete S3 Cover Cleanup Dry Run with prefix: '%s', limit: %d. Error: %s", prefixToUse, batchLimitToUse, e.getMessage());
            logger.error(errorMessage, e);
            return ResponseEntity.internalServerError().body("Error during S3 Cover Cleanup Dry Run: " + e.getMessage());
        }
    }

    /**
     * Triggers the action of moving flagged S3 cover images to a quarantine prefix.
     *
     * @param prefixOptional Optional request parameter to override the S3 source prefix.
     * @param limitOptional Optional request parameter to override the batch processing limit.
     * @param quarantinePrefixOptional Optional request parameter to override the quarantine prefix.
     * @return A ResponseEntity containing the MoveActionSummary as JSON.
     */
    @PostMapping(value = "/s3-cleanup/move-flagged", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> triggerS3CoverMoveAction(
            @RequestParam(name = "prefix", required = false) String prefixOptional,
            @RequestParam(name = "limit", required = false) Integer limitOptional,
            @RequestParam(name = "quarantinePrefix", required = false) String quarantinePrefixOptional) {

        String sourcePrefixToUse = prefixOptional != null ? prefixOptional : configuredS3Prefix;
        int batchLimitToUse = limitOptional != null ? limitOptional : defaultBatchLimit;
        String quarantinePrefixToUse = quarantinePrefixOptional != null ? quarantinePrefixOptional : configuredQuarantinePrefix;

        if (batchLimitToUse <= 0) {
            logger.warn("Batch limit for move action specified as {} (or defaulted to it), processing all found items.", batchLimitToUse);
        }
        if (quarantinePrefixToUse.isEmpty() || quarantinePrefixToUse.equals(sourcePrefixToUse)) {
            String errorMsg = "Invalid quarantine prefix: cannot be empty or same as source prefix.";
            logger.error(errorMsg + " Source: '{}', Quarantine: '{}'", sourcePrefixToUse, quarantinePrefixToUse);
            return ResponseEntity.badRequest().body("{\"error\": \"" + errorMsg + "\"}");
        }
        
        logger.info("Admin endpoint /admin/s3-cleanup/move-flagged invoked. " +
                        "Source Prefix: '{}', Limit: {}, Quarantine Prefix: '{}'",
                sourcePrefixToUse, batchLimitToUse, quarantinePrefixToUse);

        try {
            com.williamcallahan.book_recommendation_engine.types.MoveActionSummary summary = 
                s3CoverCleanupService.performMoveAction(sourcePrefixToUse, batchLimitToUse, quarantinePrefixToUse);
            
            logger.info("S3 Cover Cleanup Move Action completed. Summary: {}", summary.toString());
            return ResponseEntity.ok(summary);
        } catch (Exception e) {
            String errorMessage = String.format(
                "Failed to complete S3 Cover Cleanup Move Action. Source Prefix: '%s', Limit: %d, Quarantine Prefix: '%s'. Error: %s",
                sourcePrefixToUse, batchLimitToUse, quarantinePrefixToUse, e.getMessage()
            );
            logger.error(errorMessage, e);
            return ResponseEntity.internalServerError().body("{\"error\": \"" + errorMessage.replace("\"", "\\\"") + "\"}");
        }
    }
}
