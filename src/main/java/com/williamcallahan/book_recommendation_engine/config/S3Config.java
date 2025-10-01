/**
 * Configuration for Amazon S3 client used for book cover storage
 *
 * @author William Callahan
 *
 * Features:
 * - Creates S3Client bean conditionally based on environment variables
 * - Supports custom endpoint URL for MinIO or local S3 compatible services
 * - Handles graceful degradation when configuration is incomplete
 * - Prevents application startup with misconfigured credentials
 * - Logs configuration status and errors for diagnostics
 */
package com.williamcallahan.book_recommendation_engine.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.williamcallahan.book_recommendation_engine.util.LoggingUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

@Configuration
@Conditional(S3EnvironmentCondition.class)
public class S3Config {
    private static final Logger logger = LoggerFactory.getLogger(S3Config.class);

    @Value("${s3.access-key-id:${S3_ACCESS_KEY_ID:}}")
    private String accessKeyId;

    @Value("${s3.secret-access-key:${S3_SECRET_ACCESS_KEY:}}")
    private String secretAccessKey;

    @Value("${s3.server-url:${S3_SERVER_URL:}}")
    private String s3ServerUrl;

    @Value("${s3.region:${AWS_REGION:us-west-2}}") // Default to us-west-2 if not specified
    private String s3Region;

    /**
     * Creates and configures S3Client bean for AWS S3 interactions
     * - Only created when S3 environment variables are detected
     * - Validates required configuration parameters before creating client
     * - Overrides endpoint for compatibility with MinIO or local S3 services
     * - Uses static credentials provider for authentication
     *
     * @return Configured S3Client instance or null if misconfigured
     */
    @Bean(destroyMethod = "close") // Ensure Spring calls close() on S3Client shutdown
    public S3Client s3Client() {
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
            LoggingUtils.error(logger, e, "Failed to create S3Client bean due to configuration error");
            return null; // Prevent application startup with a broken S3 client
        }
    }
}
