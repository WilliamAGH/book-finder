package com.williamcallahan.book_recommendation_engine.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

@Configuration
public class S3Config {
    private static final Logger logger = LoggerFactory.getLogger(S3Config.class);

    @Value("${s3.access-key-id:}")
    private String accessKeyId;

    @Value("${s3.secret-access-key:}")
    private String secretAccessKey;

    @Value("${s3.server-url:}")
    private String s3ServerUrl;

    @Value("${s3.region:us-west-2}") // Default to us-west-2 if not specified
    private String s3Region;
    
    @Value("${s3.enabled:false}") // Add s3.enabled check
    private boolean s3Enabled;

    @Bean
    public S3Client s3Client() {
        if (!s3Enabled) {
            logger.info("S3 integration is disabled via s3.enabled=false. S3Client bean will not be created.");
            return null;
        }
        if (accessKeyId == null || accessKeyId.isEmpty() || secretAccessKey == null || secretAccessKey.isEmpty() || s3ServerUrl == null || s3ServerUrl.isEmpty()) {
            logger.warn("S3 credentials (access-key-id, secret-access-key, or server-url) are not fully configured. S3Client bean will not be created.");
            return null; // Avoid creating client with incomplete config
        }
        
        try {
            logger.info("Configuring S3Client with server URL: {} and region: {}", s3ServerUrl, s3Region);
            return S3Client.builder()
                    .region(Region.of(s3Region))
                    .endpointOverride(URI.create(s3ServerUrl))
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
                    .build();
        } catch (Exception e) {
            logger.error("Failed to create S3Client bean due to configuration error: {}", e.getMessage(), e);
            return null; // Prevent application startup with a broken S3 client
        }
    }
}
