/**
 * OpenLibrary API configuration properties
 *
 * @author William Callahan
 */

package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "openlibrary")
public class OpenLibraryConfigurationProperties {
    @NestedConfigurationProperty
    private Data data = new Data();
    
    public Data getData() { return data; }
    public void setData(Data data) { this.data = data; }
    
    public static class Data {
        @NestedConfigurationProperty
        private Api api = new Api();
        
        public Api getApi() { return api; }
        public void setApi(Api api) { this.api = api; }
        
        public static class Api {
            private String url = "https://openlibrary.org";
            
            public String getUrl() { return url; }
            public void setUrl(String url) { this.url = url; }
        }
    }
} 