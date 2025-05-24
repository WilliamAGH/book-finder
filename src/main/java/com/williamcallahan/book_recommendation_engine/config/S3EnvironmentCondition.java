/**
 * Custom condition that checks if S3 environment variables are present
 * This enables S3 services automatically when the required environment variables are available
 *
 * @author William Callahan
 *
 * Features:
 * - Validates presence of S3_ACCESS_KEY_ID environment variable
 * - Validates presence of S3_SECRET_ACCESS_KEY environment variable
 * - Validates presence of S3_BUCKET environment variable
 * - Validates presence of S3_SERVER_URL environment variable
 * - Enables S3-dependent beans only when all required variables are present
 * - Provides console output indicating S3 availability status (only once)
 */
package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.lang.NonNull;

public class S3EnvironmentCondition implements Condition {

    private static boolean messageLogged = false;

    @Override
    public boolean matches(@NonNull ConditionContext context, @NonNull AnnotatedTypeMetadata metadata) {
        String accessKeyId = context.getEnvironment().getProperty("S3_ACCESS_KEY_ID");
        String secretAccessKey = context.getEnvironment().getProperty("S3_SECRET_ACCESS_KEY");
        String bucket = context.getEnvironment().getProperty("S3_BUCKET");
        String serverUrl = context.getEnvironment().getProperty("S3_SERVER_URL");
        
        boolean hasRequiredVars = accessKeyId != null && !accessKeyId.trim().isEmpty() &&
                                 secretAccessKey != null && !secretAccessKey.trim().isEmpty() &&
                                 bucket != null && !bucket.trim().isEmpty() &&
                                 serverUrl != null && !serverUrl.trim().isEmpty();
        
        // Only log the message once to avoid spam during startup
        if (!messageLogged) {
            if (hasRequiredVars) {
                System.out.println("S3 environment variables detected - enabling S3 services");
            } else {
                System.out.println("S3 environment variables not found - S3 services will be disabled");
            }
            messageLogged = true;
        }
        
        return hasRequiredVars;
    }
}
