/**
 * Sitemap configuration properties
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "sitemap")
public class SitemapConfigurationProperties {
    @NestedConfigurationProperty
    private S3 s3 = new S3();
    
    public S3 getS3() { return s3; }
    public void setS3(S3 s3) { this.s3 = s3; }
    
    public static class S3 {
        private String accumulatedIdsKey = "sitemap/accumulated-book-ids.json";
        
        public String getAccumulatedIdsKey() { return accumulatedIdsKey; }
        public void setAccumulatedIdsKey(String accumulatedIdsKey) { this.accumulatedIdsKey = accumulatedIdsKey; }
    }
} 