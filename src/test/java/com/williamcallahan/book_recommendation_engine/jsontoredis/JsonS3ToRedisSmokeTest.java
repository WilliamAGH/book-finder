package com.williamcallahan.book_recommendation_engine.jsontoredis;

import com.williamcallahan.book_recommendation_engine.controller.MigrationMonitorController;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.AfterAll;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.localstack.LocalStackContainer.Service;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@SpringBootTest
@ActiveProfiles("jsontoredis")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SuppressWarnings("resource")
@Disabled("Testcontainers integration requires Docker and proper configuration - disabled for CI/CD stability")
public class JsonS3ToRedisSmokeTest {

    @Container
    private static final LocalStackContainer localstack =
        new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.1"))
            .withServices(Service.S3);

    @Container
    private static final GenericContainer<?> redis =
        new GenericContainer<>(DockerImageName.parse("redis/redis-stack-server:latest"))
            .withExposedPorts(6379);

    @Autowired
    private S3Client s3Client;

    @Autowired
    private RedisJsonService redisJsonService;

    @Autowired
    private JsonS3ToRedisService migrationService;

    @Autowired
    private MigrationMonitorController monitor;

    @BeforeAll
    public void setup() {
        // Create S3 bucket in LocalStack
        s3Client.createBucket(CreateBucketRequest.builder()
            .bucket("test-bucket")
            .build());
    }

    @Test
    public void migrationLoadsDataIntoRedis() throws Exception {
        String key = "books/v1/sample.json";
        String content = "{\"foo\":\"bar\"}";
        // Upload sample JSON to S3
        s3Client.putObject(
            PutObjectRequest.builder()
                .bucket("test-bucket")
                .key(key)
                .build(),
            RequestBody.fromBytes(content.getBytes(StandardCharsets.UTF_8))
        );
        // Perform migration
        migrationService.performMigrationAsync().get(30, TimeUnit.SECONDS);
        // Verify data in Redis
        String json = redisJsonService.jsonGetRaw("sample-key");
        assertNotNull(json);
        assertTrue(json.contains("\"foo\":\"bar\""));
    }

    @Test
    public void monitorControllerReflectsProgressAndErrors() {
        // Trigger migration without waiting
        migrationService.performMigrationAsync();

        // Verify progress endpoint via controller
        ResponseEntity<Map<String, Object>> progressResponse = monitor.getMigrationProgress();
        assertNotNull(progressResponse);
        Map<String, Object> progress = progressResponse.getBody();
        assertNotNull(progress);
        assertTrue(progress.containsKey("progress"));

        // Verify errors endpoint via controller
        ResponseEntity<Map<String, Object>> errorsResponse = monitor.getMigrationErrors();
        assertNotNull(errorsResponse);
        Map<String, Object> errors = errorsResponse.getBody();
        assertNotNull(errors);
        assertTrue(errors.containsKey("totalErrors"));
    }

    @AfterAll
    public static void tearDown() throws Exception {
        // Close Testcontainers resources to prevent resource leaks
        if (localstack != null) {
            localstack.close();
        }
        if (redis != null) {
            redis.close();
        }
    }
} 