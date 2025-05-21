# Cron Schedulers & Manual Job Triggers

This document lists scheduled jobs, their configurations, and manual trigger instructions.

## Scheduled Jobs

Jobs are enabled by `@EnableScheduling` in `BookRecommendationEngineApplication.java` and defined in the `.../scheduler` package using `@Scheduled`.

| Scheduler File Path                                  | Main Method                        | Default Schedule                  | Cron Property (`application.properties`) | Description                                      |
| :--------------------------------------------------- | :--------------------------------- | :-------------------------------- | :--------------------------------------- | :----------------------------------------------- |
| `.../scheduler/BookCacheWarmingScheduler.java`       | `warmPopularBookCaches()`          | Daily at 3 AM (`0 0 3 * * ?`)     | `app.cache.warming.cron`                 | Caches popular/recent books.                     |
| `.../scheduler/SitemapUpdateScheduler.java`          | `scheduleSitemapBookIdUpdate()`    | Hourly (`0 0 * * * *`)            | (Hardcoded)                              | Updates S3 book ID list for sitemap.             |
| `.../scheduler/NewYorkTimesBestsellerScheduler.java` | `processNewYorkTimesBestsellers()` | Sunday at 4 AM (`0 0 4 * * SUN`)  | `app.nyt.scheduler.cron`                 | Fetches NYT bestsellers, updates S3 data.        |

**Notes on Cron:**
- Use [Crontab.guru](https://crontab.guru/) to interpret cron expressions.
- Override defaults via specified properties or environment variables.

## Updating Schedules

- **Configurable Jobs**: Modify the `cron` property (e.g., `app.nyt.scheduler.cron`) in `application.properties` or an environment variable.
- **Hardcoded Jobs**: Edit the `cron` attribute in the `@Scheduled` annotation in the Java file and recompile.

## Manual Job Triggers

Manually trigger jobs/tasks via API endpoints. Useful for testing or immediate execution.
All admin endpoints require HTTP Basic Authentication (user: `admin`, password from `APP_ADMIN_PASSWORD` env var, as defined in `.env.example`). See `README.md` for `curl` examples.

| Job / Task                               | Manual Trigger Command / Endpoint                                                                                                                     | Notes                                                                                                                                                                                             |
| :--------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Sitemap Update**                       | `POST http://localhost:{SERVER_PORT}/admin/trigger-sitemap-update`                                                                                        | Defined in `SitemapController.java`. Example: `dotenv run sh -c 'curl -X POST -u admin:$APP_ADMIN_PASSWORD http://localhost:${SERVER_PORT}/admin/trigger-sitemap-update'`                                         |
| **Book Cache Warming**                   | None currently implemented.                                                                                                                             | *To implement:* Add POST endpoint in `AdminController` calling `bookCacheWarmingScheduler.warmPopularBookCaches()`.                                                                               |
| **New York Times Bestseller Processing** | `POST http://localhost:{SERVER_PORT}/admin/trigger-nyt-bestsellers`                                                                                       | Defined in `AdminController.java`. Calls `newYorkTimesBestsellerScheduler.processNewYorkTimesBestsellers()`. Example: `dotenv run sh -c 'curl -X POST -u admin:$APP_ADMIN_PASSWORD http://localhost:${SERVER_PORT}/admin/trigger-nyt-bestsellers'` |
| **S3 Cover Cleanup (Dry Run)**           | `GET http://localhost:{SERVER_PORT}/admin/s3-cleanup/dry-run`                                                                                             | Defined in `AdminController.java`. Optional params: `prefix`, `limit`. Example: `dotenv run sh -c 'curl -u admin:$APP_ADMIN_PASSWORD "http://localhost:${SERVER_PORT}/admin/s3-cleanup/dry-run?limit=10"'`        |
| **S3 Cover Cleanup (Move Flagged)**      | `POST http://localhost:{SERVER_PORT}/admin/s3-cleanup/move-flagged`                                                                                       | Defined in `AdminController.java`. Optional params: `prefix`, `limit`, `quarantinePrefix`. Example: `dotenv run sh -c 'curl -X POST -u admin:$APP_ADMIN_PASSWORD "http://localhost:${SERVER_PORT}/admin/s3-cleanup/move-flagged?limit=5"'` |

**Notes on Manual Triggers:**
- Replace `{SERVER_PORT}` with your application's port (e.g., 8081).
- `curl` examples use `dotenv run sh -c 'curl ...'` to load variables like `$APP_ADMIN_PASSWORD` and `$SERVER_PORT` from your `.env` file.
- If using `dotenv-cli`, the syntax is `dotenv curl ...`.
- Alternatively, export variables directly: `export APP_ADMIN_PASSWORD='your_password'`.

---

## Data Migration Utilities

### S3 JSON to Redis Migration

This utility migrates JSON data from S3 (Google Books, NYT Bestsellers) to a Redis Stack instance. It's implemented as a Spring Boot `CommandLineRunner` that activates when a specific profile is used.

| Utility Component                                  | Description                                                                                                                               |
| :------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------- |
| `.../jsontoredis/JsonS3ToRedisRunner.java`         | Main runner, triggers the migration. Activated by the `jsontoredis` Spring profile.                                                       |
| `.../jsontoredis/JsonS3ToRedisService.java`        | Orchestrates the two-phase migration: 1. Ingest Google Books. 2. Merge NYT data. Moves processed S3 files to an `in-redis/` subfolder.    |
| `.../jsontoredis/S3Service.java`                   | Handles S3 interactions (list, get, move objects) using AWS SDK v2.                                                                       |
| `.../jsontoredis/RedisJsonService.java`            | Handles Redis JSON operations using Lettuce and `lettuce-modules`.                                                                        |
| `.../jsontoredis/config/RedisConfig.java`          | Configures the `StatefulRedisConnection` for Lettuce.                                                                                     |
| `application.properties` (relevant entries)        | `spring.redis.url=${REDIS_SERVER}`, `jsontoredis.s3.google-books-prefix`, `jsontoredis.s3.nyt-bestsellers-key`                               |
| `.env.example` (relevant entries)                  | `REDIS_SERVER`, S3 credentials (`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, etc.)                                                           |

**How to Run:**

1.  **Environment Variables**:
    *   Ensure `REDIS_SERVER` is set in your `.env` file or environment, pointing to your Redis Stack instance (e.g., `rediss://user:pass@host:port/0`).
    *   Ensure S3 credentials and configuration (`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_SERVER_URL`, `S3_REGION`, `S3_BUCKET_NAME`) are correctly set in your environment.
2.  **Activate Profile & Run**:
    *   **Using Maven**:
        ```bash
        mvn spring-boot:run -Dspring-boot.run.profiles=jsontoredis
        ```
    *   **Using a packaged JAR**:
        ```bash
        java -jar -Dspring.profiles.active=jsontoredis target/book_recommendation_engine-0.0.1-SNAPSHOT.jar
        ```
        (Replace `target/book_recommendation_engine-0.0.1-SNAPSHOT.jar` with the actual path to your built JAR.)

**Process:**
- The runner will first attempt to PING the Redis server.
- If the PING is successful, it will proceed with:
    1.  Ingesting Google Books JSON files from the `jsontoredis.s3.google-books-prefix` into Redis. Processed files are moved to an `in-redis/` subfolder under this prefix.
    2.  Merging data from the NYT Bestsellers JSON file (specified by `jsontoredis.s3.nyt-bestsellers-key`) into the corresponding Redis records. The processed NYT file is also moved to an `in-redis/` subfolder.
- Check the application logs for detailed progress, successes, warnings, and errors.

**Post-Migration:**
- If you intend to use RediSearch capabilities on the migrated data, you will need to manually create the appropriate RediSearch index(es) in your Redis instance after the migration is complete.
