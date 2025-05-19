# Book Finder

**Live Demo:** [findmybook.net](https://findmybook.net)

A Spring Boot application for book lookup and recommendations using Spring AI with OpenAI and Google Books API.

## Core Technologies

- Java 21, Spring Boot 3.4.5 (WebFlux, Thymeleaf, HTMX, Spring AI)
- PostgreSQL, Google Books API, OpenAI

## Prerequisites

- Java 21
- Maven 3.6+ (Packages available at [search.maven.org](https://search.maven.org/))
- PostgreSQL
- OpenAI API Key (optional, for AI features)
- Google Books API Key (optional, for enhanced book data)

### Maven Commands

Use standard `mvn` commands when Maven is installed. If Maven is not installed, use the included wrapper:
```bash
# With Maven installed (preferred):
mvn clean install

# Without Maven:
./mvnw clean install  # macOS/Linux
mvnw.cmd clean install  # Windows
```

## Configuration

Use a `.env` file for local setup (copy from `.env.example` and update values). Key variables:
- `SERVER_PORT`
- `SPRING_DATASOURCE_URL`, `_USERNAME`, `_PASSWORD`
- `SPRING_AI_OPENAI_API_KEY`
- `GOOGLE_BOOKS_API_KEY`
- `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, etc. (for S3 features)
- `APP_ADMIN_PASSWORD` (for the built-in 'admin' user)
- `APP_USER_PASSWORD` (for the built-in 'user' user)

### Built-in User Accounts

The application includes two pre-defined in-memory user accounts configured via Spring Security:

1.  **Admin User:**
    *   **Username:** `admin` (hardcoded)
    *   **Password:** Configured via the `APP_ADMIN_PASSWORD` environment variable (see `.env.example`).
    *   **Roles:** `ADMIN`, `USER`
    *   **Access:** Can access administrative functions under the `/admin/**` path (e.g., S3 cleanup utilities).
    *   **Usage:** Intended for application administrators to perform maintenance tasks.

2.  **Regular User:**
    *   **Username:** `user` (hardcoded)
    *   **Password:** Configured via the `APP_USER_PASSWORD` environment variable (see `.env.example`).
    *   **Roles:** `USER`
    *   **Access:** Cannot access `/admin/**` paths. Can access general application features (currently, most non-admin paths are `permitAll()`).
    *   **Usage:** Can be used for testing features that might require a standard authenticated user in the future, or if parts of the application are secured for `USER` role.

These accounts are primarily for basic authentication to protected administrative endpoints. For production, ensure strong, unique passwords are set for `APP_ADMIN_PASSWORD` and `APP_USER_PASSWORD` via your environment.

## Running the Application

Spring Profiles: `prod` (default), `dev`.

**Development Mode (with `dev` profile):**
```bash
mvn spring-boot:run -Dspring.profiles.active=dev
```
Hot reload:
```bash
mvn spring-boot:run -Dspring.profiles.active=dev -Dspring-boot.run.jvmArguments="-Dspring.devtools.restart.enabled=true"
```
App typically at `http://localhost:8081` (or `SERVER_PORT`).

**Production Mode (default or explicit `prod` profile):**
```bash
mvn spring-boot:run -Dspring.profiles.active=prod
```

## Building and Deployment

**Build JAR:**
```bash
mvn clean package
```

**Run JAR:**
```bash
java -jar target/book_recommendation_engine-0.0.1-SNAPSHOT.jar
# Explicitly set profile for JAR (e.g., prod):
# java -Dspring.profiles.active=prod -jar target/book_recommendation_engine-0.0.1-SNAPSHOT.jar
```

**Docker:**
1. Build image: `mvn spring-boot:build-image`
2. Run container (example using port 8081, adjust if `SERVER_PORT` differs):
```bash
docker run -p 8081:8081 --env-file .env book_recommendation_engine:0.0.1-SNAPSHOT
```

## Key Endpoints

- **Web Interface:** `http://localhost:{SERVER_PORT}` or `https://findmybook.net`
- **Health Check:** `/actuator/health`
  - Production: `https://findmybook.net/actuator/health`
  - Development: `https://dev.findmybook.net/actuator/health`
  - Dockerfile Example:
    ```Dockerfile
    HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
      CMD curl --fail http://localhost:8080/actuator/health || exit 1
    # (Adjust localhost:8080 to internal container port if different)
    ```
  - Provides overall status and detailed `components` status (Redis, S3, DB, homepage, search page, book detail page, etc.). 
  - The `book_detail_page` health check requires the `healthcheck.test-book-id` property to be set (e.g., in `.env` or as an environment variable) to a valid book ID.
  - Resilient checks report `UP` even with transient issues for certain components (like Redis or S3 if disabled/misconfigured), providing details in the response.

- **Book API Examples:**
  - `GET /api/books/search?query={keyword}`
  - `GET /api/books/{id}`

## Logging

Incoming HTTP requests are logged. Configure levels in `application.properties` or `application.yml`.

## Testing

To run tests:
```bash
mvn test
```

### Silencing JVM Warnings During Maven Execution

During Maven execution (e.g., with `mvn spring-boot:run`, `mvn clean package`, `mvn test`, etc.), you might encounter JVM warnings such as:
- `WARNING: A Java agent has been loaded dynamically...`
- `Java HotSpot(TM) 64-Bit Server VM warning: Sharing is only supported for boot loader classes...`

These warnings typically arise when Maven plugins (like `wro4j-maven-plugin` or others that use Java agents) run within the main Maven JVM. To silence them, set the `MAVEN_OPTS` environment variable before executing your Maven command. This applies universally to the Maven JVM, regardless of any Spring profiles (`dev`, `prod`) activated for your application.

**Recommended `MAVEN_OPTS`:**
```bash
export MAVEN_OPTS="-XX:+EnableDynamicAgentLoading -Xshare:off"
```

**Usage Examples:**

*   **For the current terminal session:**
    Set the variable, then run your Maven commands.
    ```bash
    export MAVEN_OPTS="-XX:+EnableDynamicAgentLoading -Xshare:off"
    mvn clean install
    mvn spring-boot:run -Dspring.profiles.active=dev
    ```

*   **For a single command:**
    Prepend the variable assignment to the Maven command.
    ```bash
    MAVEN_OPTS="-XX:+EnableDynamicAgentLoading -Xshare:off" mvn test
    ```

*   **For permanent configuration:**
    Add the `export MAVEN_OPTS="..."` line to your shell's startup file (e.g., `~/.bashrc`, `~/.zshrc` for Linux/macOS, or set it as a system environment variable on Windows).

**Note:** JVM arguments specified within plugin configurations in your `pom.xml` (e.g., for `spring-boot-maven-plugin` or `maven-surefire-plugin`) apply to new JVMs *forked by those specific plugins*. `MAVEN_OPTS` is for configuring the main Maven JVM itself.

## Debugging Overrides

For development and troubleshooting, certain default behaviors can be overridden using Spring Boot properties (e.g., in your `.env` file or as command-line arguments):

- **Bypass Caches:** To bypass all caching layers (in-memory, S3, etc.) for book lookups and go directly to the Google Books API:
  ```properties
  googlebooks.api.override.bypass-caches=true
  ```
  When this is true, `GoogleBooksCachingStrategy` will skip cache checks.

- **Bypass Rate Limiter:** To effectively bypass the Google Books API rate limiter for the `googleBooksServiceRateLimiter` instance, you can make its configuration very permissive by setting the following properties:
  ```properties
  resilience4j.ratelimiter.instances.googleBooksServiceRateLimiter.limitForPeriod=2147483647
  resilience4j.ratelimiter.instances.googleBooksServiceRateLimiter.limitRefreshPeriod=1ms
  resilience4j.ratelimiter.instances.googleBooksServiceRateLimiter.timeoutDuration=0ms
  ```
  This allows a very high number of requests in a very short period, effectively disabling the limit for debugging purposes.

These overrides should be used with caution, especially the rate limiter bypass, as they can lead to exceeding actual API quotas if used against production services.

## Troubleshooting

**Port Conflicts (macOS/Linux):**
1. Find PID: `lsof -i :{PORT}` (e.g., `lsof -i :8081`)
2. Kill PID: `kill -9 <PID>`

**Port Conflicts (Windows):**
1. Find PID: `netstat -ano | findstr :{PORT}`
2. Kill PID: `taskkill /PID <PID> /F`

## UML Diagram

See [UML README](src/main/resources/uml/README.md).

## Manual Sitemap Generation

To manually trigger the update of book IDs for the sitemap (e.g., `sitemap_books.xml`):

**Local Development:**
```bash
curl -X POST http://localhost:8081/admin/trigger-sitemap-update
```

**Production (if accessible and secured appropriately):**
```bash
# Ensure this admin endpoint is appropriately secured in a production environment if exposed publicly.
# Replace https://findmybook.net with your actual admin domain if different,
# and ensure authentication/authorization is in place.
curl -X POST https://findmybook.net/admin/trigger-sitemap-update
```

## License

MIT License

## Contributing

Pull requests and issues are welcome!
