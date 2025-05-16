# Book Recommendation Engine

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

## Configuration

Use a `.env` file for local setup (copy from `.env.example` and update values). Key variables:
- `SERVER_PORT`
- `SPRING_DATASOURCE_URL`, `_USERNAME`, `_PASSWORD`
- `SPRING_AI_OPENAI_API_KEY`
- `GOOGLE_BOOKS_API_KEY`
- `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, etc. (for S3 features)

## Running the Application

Spring Profiles: `prod` (default), `dev`.

**Development Mode (with `dev` profile):**
```bash
./mvnw spring-boot:run -Dspring.profiles.active=dev
```
Hot reload:
```bash
./mvnw spring-boot:run -Dspring.profiles.active=dev -Dspring-boot.run.jvmArguments="-Dspring.devtools.restart.enabled=true"
```
App typically at `http://localhost:8081` (or `SERVER_PORT`).

**Production Mode (default or explicit `prod` profile):**
```bash
./mvnw spring-boot:run -Dspring.profiles.active=prod
```

## Building and Deployment

**Build JAR:**
```bash
./mvnw clean package
```

**Run JAR:**
```bash
java -jar target/book_recommendation_engine-0.0.1-SNAPSHOT.jar
# Explicitly set profile for JAR (e.g., prod):
# java -Dspring.profiles.active=prod -jar target/book_recommendation_engine-0.0.1-SNAPSHOT.jar
```

**Docker:**
1. Build image: `./mvnw spring-boot:build-image`
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

```bash
./mvnw test
```

## Troubleshooting

**Port Conflicts (macOS/Linux):**
1. Find PID: `lsof -i :{PORT}` (e.g., `lsof -i :8081`)
2. Kill PID: `kill -9 <PID>`

**Port Conflicts (Windows):**
1. Find PID: `netstat -ano | findstr :{PORT}`
2. Kill PID: `taskkill /PID <PID> /F`

**MacOS DNS Resolution:** Includes `netty-resolver-dns-native-macos` for M1/M2 Macs.

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
