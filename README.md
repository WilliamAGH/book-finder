# Book Finder

**Live Demo:** [findmybook.net](https://findmybook.net)

Spring Boot application for book lookup and recommendations using OpenAI and Google Books API.

## Quick Start

**Prerequisites:** Java 21, Maven 3.6+

1. **Configure:** Copy `.env.example` to `.env` and update values
2. **Run:** `mvn spring-boot:run -P dev` 
3. **Access:** http://localhost:8081 (or configured `SERVER_PORT`)

## Development Shortcuts

| Command | Description |
|---------|-------------|
| `mvn spring-boot:run -P dev` | Run in dev mode with hot reload (includes clean + compile) |
| `mvn clean compile -DskipTests` | Quick clean and compile without tests |
| `mvn test` | Run tests only |
| `mvn spring-boot:run -Dspring.profiles.active=nodb` | Run without database |
| `mvn spring-boot:run -Dspring.profiles.active=prod` | Run in production mode |
| `mvn dependency:tree` | Display dependencies |
| `mvn clean package` | Build JAR |

## Environment Variables

Key variables in `.env`:

| Variable | Purpose |
|----------|---------|
| `SERVER_PORT` | App server port |
| `SPRING_DATASOURCE_*` | Database connection |
| `SPRING_AI_OPENAI_API_KEY` | OpenAI integration |
| `GOOGLE_BOOKS_API_KEY` | Book data source |
| `S3_*` | S3 storage (if used) |
| `APP_ADMIN_PASSWORD` | Admin user password |
| `APP_USER_PASSWORD` | Basic user password |

## User Accounts

| Username | Role(s) | Access | Password Env Variable |
|----------|---------|--------|----------------------|
| `admin` | `ADMIN`, `USER` | All + `/admin/**` | `APP_ADMIN_PASSWORD` |
| `user` | `USER` | General features | `APP_USER_PASSWORD` |

## Key Endpoints

- **Web Interface:** `http://localhost:{SERVER_PORT}` or `https://findmybook.net`
- **Health Check:** `/actuator/health`
- **Book API Examples:**
  - `GET /api/books/search?query={keyword}`
  - `GET /api/books/{id}`

## Troubleshooting

**JVM Warnings:** `export MAVEN_OPTS="-XX:+EnableDynamicAgentLoading -Xshare:off"`

**Port Conflicts:**
```bash
# macOS/Linux
kill -9 $(lsof -ti :8081)
```

```bash
# Windows
FOR /F "tokens=5" %i IN ('netstat -ano ^| findstr :8081') DO taskkill /F /PID %i
```

## Additional Features

<details>
<summary><b>UML Diagram</b></summary>
See <a href="src/main/resources/uml/README.md">UML README</a>.
</details>

<details>
<summary><b>Manual Sitemap Generation</b></summary>

```bash
curl -X POST http://localhost:8081/admin/trigger-sitemap-update
```
</details>

<details>
<summary><b>Debugging Overrides</b></summary>

To bypass caches for book lookups:
```properties
googlebooks.api.override.bypass-caches=true
```

To bypass rate limiter:
```properties
resilience4j.ratelimiter.instances.googleBooksServiceRateLimiter.limitForPeriod=2147483647
resilience4j.ratelimiter.instances.googleBooksServiceRateLimiter.limitRefreshPeriod=1ms
resilience4j.ratelimiter.instances.googleBooksServiceRateLimiter.timeoutDuration=0ms
```
</details>

<details>
<summary><b>Code Analysis Tools</b></summary>

- **PMD:** `mvn pmd:pmd && open target/site/pmd.html`
- **SpotBugs:** `mvn spotbugs:spotbugs && open target/site/spotbugs/index.html`
- **Dependency Analysis:** `mvn dependency:analyze`
</details>

<details>
<summary><b>Admin API Authentication</b></summary>

Admin endpoints require HTTP Basic Authentication:
- Username: `admin`
- Password: Set via `APP_SECURITY_ADMIN_PASSWORD` environment variable

Example:
```bash
curl -u admin:$APP_SECURITY_ADMIN_PASSWORD -X POST 'http://localhost:8081/admin/s3-cleanup/move-flagged?limit=100'
```
</details>

#### References
- [Java Docs](https://docs.oracle.com/en/java/index.html)
- [Spring Boot Docs](https://docs.spring.io/spring-boot/docs/current/reference/html/)
- [PMD Maven Plugin](https://maven.apache.org/plugins/maven-pmd-plugin/)
- [SpotBugs Maven Plugin](https://spotbugs.github.io/)
- [Maven Dependency Plugin](https://maven.apache.org/plugins/maven-dependency-plugin/)

## License

MIT License 

## Contributing

Pull requests and issues are welcome!
