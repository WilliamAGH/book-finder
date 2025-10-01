FROM maven:3.9.6-eclipse-temurin-21-alpine AS build
WORKDIR /app

# Copy pom.xml first for better layer caching
COPY pom.xml .
RUN mvn dependency:go-offline

# Copy source code
COPY src/ /app/src/

# Build the application
RUN mvn package -DskipTests

# Use JRE for smaller runtime image
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
ENV SERVER_PORT=8095
EXPOSE 8095

# Copy the built jar from the build stage
COPY --from=build /app/target/*.jar app.jar

# Run the application (SERVER_PORT env var automatically bound to server.port by Spring Boot)
ENTRYPOINT ["java", "-jar", "app.jar"]
