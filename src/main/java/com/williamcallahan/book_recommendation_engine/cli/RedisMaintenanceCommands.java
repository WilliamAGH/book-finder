/**
 * Spring Shell CLI commands for Redis cache maintenance operations
 * Provides command-line interface for cache diagnostics, repair, and migration tasks
 * Integrates with RedisBookMaintenanceService for comprehensive cache management
 *
 * @author William Callahan
 *
 * Features:
 * - Interactive command-line interface using Spring Shell framework
 * - Real-time cache integrity diagnostics with detailed statistics reporting
 * - Safe cache repair operations with dry-run capabilities
 * - Data format migration support for schema evolution
 */

package com.williamcallahan.book_recommendation_engine.cli;

import com.williamcallahan.book_recommendation_engine.repository.RedisBookMaintenanceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.util.Map;

@ShellComponent
public class RedisMaintenanceCommands {

    private static final Logger logger = LoggerFactory.getLogger(RedisMaintenanceCommands.class);
    private final RedisBookMaintenanceService maintenanceService;

    /**
     * Constructs CLI commands with maintenance service dependency
     *
     * @param maintenanceService service providing cache maintenance operations
     */
    public RedisMaintenanceCommands(RedisBookMaintenanceService maintenanceService) {
        this.maintenanceService = maintenanceService;
    }

    /**
     * Executes comprehensive cache integrity diagnostic scan
     * Analyzes all cached book entries for corruption and structural issues
     *
     * @return statistics map with integrity analysis results including total keys, valid entries, and corruption counts
     */
    @ShellMethod(
        value = "Diagnose Redis cache integrity. Returns statistics for total_keys, valid_json, valid_redisJson, corrupted",
        key = "redis diagnose-integrity"
    )
    public Map<String, Integer> diagnoseIntegrity() {
        try {
            return maintenanceService.diagnoseCacheIntegrity();
        } catch (Exception e) {
            logger.error("Failed to diagnose cache integrity: {}", e.getMessage(), e);
            return Map.of("error", 1, "total_keys", 0, "valid_json", 0, "valid_redisJson", 0, "corrupted", 0);
        }
    }

    /**
     * Performs cache repair operations on corrupted entries
     * Uses non-destructive repair methods to preserve valid data
     *
     * @param dryRun when true, performs analysis without making changes
     * @return formatted message indicating repair results
     */
    @ShellMethod(
        value = "Repair corrupted Redis cache entries. Use --dry-run to log repairs without applying them",
        key = "redis repair-cache"
    )
    public String repairCache(
        @ShellOption(defaultValue = "false", help = "Dry run only, no changes applied") boolean dryRun
    ) {
        try {
            int count = maintenanceService.repairCorruptedCache(dryRun);
            if (dryRun) {
                return String.format("Dry run: %d keys would be repaired", count);
            } else {
                return String.format("Repaired %d keys", count);
            }
        } catch (Exception e) {
            logger.error("Failed to repair cache: {}", e.getMessage(), e);
            return String.format("Cache repair failed: %s", e.getMessage());
        }
    }

    /**
     * Executes data format migration for schema evolution
     * Performs safe verification and migration of cached book data
     *
     * @return formatted message indicating migration results
     */
    @ShellMethod(
        value = "Perform safe book data format migration (verification only)",
        key = "redis migrate-format"
    )
    public String migrateFormat() {
        try {
            int count = maintenanceService.migrateBookDataFormat();
            return String.format("Format migration processed/verified %d entries", count);
        } catch (Exception e) {
            logger.error("Failed to migrate format: {}", e.getMessage(), e);
            return String.format("Format migration failed: %s", e.getMessage());
        }
    }
}
