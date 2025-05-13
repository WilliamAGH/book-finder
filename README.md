# Live Demo

Visit [findmybook.net](https://findmybook.net) or [book-recommendation-engine.williamcallahan.com](https://book-recommendation-engine.williamcallahan.com) to see a live working demo!

# Book Recommendation Engine

A Spring Boot application that provides the ability to look up almost any book ever written and get book recommendations using OpenAI integration.

## Technologies

- Spring Boot 3.4.5
- Spring Web & WebFlux
- Thymeleaf with HTMX
- Spring AI with OpenAI
- Google Books API integration
- PostgreSQL
- Spring Session

## Prerequisites

- Java 21
- Maven 3.6+
- PostgreSQL database
- OpenAI API key

## Configuration

The application uses environment variables for configuration, which can be set in a `.env` file for local development. The application is configured to use HTTPS with a self-signed certificate.

1. Copy the `.env.example` file to `.env` and update the values:

```bash
cp .env.example .env
```

2. Edit the `.env` file with your specific configuration, refer to the `.env.example` file for more details.

The application will automatically load these variables at startup using the spring-dotenv library.

## Running the Application

### Development Mode

```bash
./mvnw spring-boot:run
```

The server will be available at https://localhost:8081 using the environment variables set in the `.env` example file.

For hot-reload during development:

```bash
./mvnw spring-boot:run -Dspring-boot.run.jvmArguments="-Dspring.devtools.restart.enabled=true"
```

### Production Mode

For production, set environment variables directly or use a production-specific .env file:

```bash
./mvnw spring-boot:run -Dspring.profiles.active=prod
```

## Managing Server Ports

If you need to kill previous instances of the web server or free up the application port (default: 8080):

### On macOS/Linux

1. Find the process using the port (replace 8080 if using a different port):

```bash
lsof -i :8080
```

2. Kill the process (replace <PID> with the actual process ID):

```bash
kill -9 <PID>
```

### On Windows

1. Find the process:

```cmd
netstat -ano | findstr :8080
```

2. Kill the process (replace <PID> with the actual process ID):

```cmd
taskkill /PID <PID> /F
```

## Building and Deployment

### Build JAR

```bash
./mvnw clean package
```

### Run JAR

```bash
java -jar target/book_recommendation_engine-0.0.1-SNAPSHOT.jar
```

### Docker

Build and run with Docker:

```bash
./mvnw spring-boot:build-image
# The Docker image now defaults to port 8080 internally.
# The SERVER_PORT environment variable (e.g., from your .env file) can override this.
#
# If your .env file (typically copied from .env.example) sets SERVER_PORT=8081,
# the application will listen on 8081 inside the container. Use:
docker run -p 8081:8081 --env-file .env book_recommendation_engine:0.0.1-SNAPSHOT
#
# If SERVER_PORT is not set in your .env file, or is set to 8080,
# the application will listen on the default 8080 inside the container. Use:
# docker run -p 8080:8080 --env-file .env book_recommendation_engine:0.0.1-SNAPSHOT
# (You can change the first '8080' in '-p 8080:8080' to any host port you prefer)
```

### Cloud Deployment

Environment variables should be configured in your cloud provider's settings.

## API and Web Interface

A basic web interface is available at https://localhost:8081. The application currently includes:

- A home page controller (`HomeController`) that serves the main page
- A simple welcome page template
- A Book API with the following endpoints:
  - `GET /api/books/search?query={query}` - Search books by keyword
  - `GET /api/books/search/title?title={title}` - Search books by title
  - `GET /api/books/search/author?author={author}` - Search books by author
  - `GET /api/books/search/isbn?isbn={isbn}` - Search books by ISBN
  - `GET /api/books/{id}` - Get a book by its ID

The Book API integrates with Google Books API to provide access to an extensive database of books, including their cover images, descriptions, authors, and more. This integration enables book search and recommendation features.

Additional API routes and features might be added in future updates.

## Logging

This application logs all incoming HTTP requests and responses, including method, path, status, and response time, using a custom servlet filter (`RequestLoggingFilter`).

Log output appears in the console by default and can be configured via `application.properties` for advanced logging needs.

## Testing

```bash
./mvnw test
```

## Known Issues and Solutions

1**MacOS DNS Resolution Error**: For MacOS users, especially on Apple Silicon (M1/M2), the application includes the `netty-resolver-dns-native-macos` dependency to fix DNS resolution issues.

## UML Diagram

A UML class diagram for this project is available in the `src/main/resources/uml` directory. The diagram shows the main classes, their properties, methods, and relationships in the Book Recommendation Engine project.

To generate the UML diagram, you can use PlantUML. See the [UML README](src/main/resources/uml/README.md) for more information.

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue here on Github!
