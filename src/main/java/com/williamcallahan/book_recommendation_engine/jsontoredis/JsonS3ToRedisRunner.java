/**
 * Command line runner for executing the S3-to-Redis JSON migration process
 *
 * @author William Callahan
 *
 * Features:
 * - Initializes and triggers the JSON migration from S3 to Redis
 * - Activates only when 'jsontoredis' Spring profile is enabled
 * - Validates Redis connection before starting migration
 * - Provides comprehensive logging throughout the process
 * - Can be executed as part of Spring Boot application startup
 */
package com.williamcallahan.book_recommendation_engine.jsontoredis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component("jsonS3ToRedis_JsonS3ToRedisRunner")
@Profile("jsontoredis") // Activate this runner only when 'jsontoredis' profile is active
public class JsonS3ToRedisRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(JsonS3ToRedisRunner.class);

    private final JsonS3ToRedisService jsonS3ToRedisService;
    private final RedisJsonService redisJsonService; // For initial ping test

    public JsonS3ToRedisRunner(
            @Qualifier("jsonS3ToRedis_JsonS3ToRedisService") JsonS3ToRedisService jsonS3ToRedisService,
            @Qualifier("jsonS3ToRedis_RedisJsonService") RedisJsonService redisJsonService) {
        this.jsonS3ToRedisService = jsonS3ToRedisService;
        this.redisJsonService = redisJsonService;
    }

    /**
     * Executes the migration process when the application starts
     * Performs initial Redis connection check before starting the migration
     * 
     * @param args Command line arguments passed to the application
     * @throws Exception if an error occurs during execution
     */
    @Override
    public void run(String... args) throws Exception {
        log.info("JsonS3ToRedisRunner started with 'jsontoredis' profile.");

        // 1. Test Redis Connection
        try {
            log.info("Attempting to PING Redis server...");
            String pingResponse = redisJsonService.ping();
            log.info("Redis PING successful. Response: {}", pingResponse);
        } catch (Exception e) {
            log.error("Redis PING failed. Migration will not proceed. Error: {}", e.getMessage(), e);
            log.error("Please ensure Redis is running, accessible, and configured correctly (spring.redis.url in application.properties and REDIS_SERVER env var).");
            // Optionally, rethrow or exit if Redis connection is critical for startup with this profile
            // For now, just logging and preventing migration.
            return; 
        }

        // 2. Perform Migration
        log.info("Proceeding with S3 JSON to Redis migration.");
        try {
            jsonS3ToRedisService.performMigrationAsync().join();
            log.info("S3 JSON to Redis migration process completed by runner.");
        } catch (Exception e) {
            log.error("S3 JSON to Redis migration process failed with an error: {}", e.getMessage(), e);
            // Rethrow the exception to ensure the application exits with an error status,
            // indicating the batch job failed
            throw e; 
        }

        // Optionally, shut down the application after migration if this runner's sole purpose is the migration
        // log.info("Migration complete. Consider shutting down the application if it's a one-time task.");
        // System.exit(0); // Uncomment if application should exit after migration
    }
}
