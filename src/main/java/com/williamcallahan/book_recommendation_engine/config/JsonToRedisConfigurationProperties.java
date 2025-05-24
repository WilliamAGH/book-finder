/**
 * JSON to Redis migration configuration properties
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "jsontoredis")
public class JsonToRedisConfigurationProperties {
    @NestedConfigurationProperty
    private S3 s3 = new S3();
    
    public S3 getS3() { return s3; }
    public void setS3(S3 s3) { this.s3 = s3; }
    
    public static class S3 {
        private String googleBooksPrefix = "books/v1/";
        private String nytBestsellersKey = "nyt-bestsellers/current.json";
        
        public String getGoogleBooksPrefix() { return googleBooksPrefix; }
        public void setGoogleBooksPrefix(String googleBooksPrefix) { this.googleBooksPrefix = googleBooksPrefix; }
        
        public String getNytBestsellersKey() { return nytBestsellersKey; }
        public void setNytBestsellersKey(String nytBestsellersKey) { this.nytBestsellersKey = nytBestsellersKey; }
    }
} 