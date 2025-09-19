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

- Replace `{SERVER_PORT}` with your application's port (e.g., 8095).
- `curl` examples use `dotenv run sh -c 'curl ...'` to load variables like `$APP_ADMIN_PASSWORD` and `$SERVER_PORT` from your `.env` file.
- If using `dotenv-cli`, the syntax is `dotenv curl ...`.
- Alternatively, export variables directly: `export APP_ADMIN_PASSWORD='your_password'`.

---

## Data Migration Utilities

### [REMOVED] S3 JSON to Redis Migration

> **Note:** Redis functionality has been removed from this application. The S3 JSON to Redis migration feature and all associated components (jsontoredis package) are no longer available.
