/**
 * New York Times API configuration properties
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "nyt")
public class NytConfigurationProperties {
    @NestedConfigurationProperty
    private Api api = new Api();
    
    public Api getApi() { return api; }
    public void setApi(Api api) { this.api = api; }
    
    public static class Api {
        private String baseUrl = "https://api.nytimes.com/svc/books/v3";
        private String key;
        
        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }
        
        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }
    }
} 