/**
 * Google Books API configuration properties
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "google")
public class GoogleConfigurationProperties {
    @NestedConfigurationProperty
    private Books books = new Books();
    
    public Books getBooks() { return books; }
    public void setBooks(Books books) { this.books = books; }
    
    public static class Books {
        @NestedConfigurationProperty
        private Api api = new Api();
        
        public Api getApi() { return api; }
        public void setApi(Api api) { this.api = api; }
        
        public static class Api {
            private String key;
            private String baseUrl = "https://www.googleapis.com/books/v1";
            
            public String getKey() { return key; }
            public void setKey(String key) { this.key = key; }
            
            public String getBaseUrl() { return baseUrl; }
            public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }
        }
    }
} 