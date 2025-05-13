package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class GoogleBooksConfig {

    @Value("${google.books.api.base-url:https://www.googleapis.com/books/v1}")
    private String googleBooksApiBaseUrl;

    @Value("${google.books.api.max-results:40}")
    private int maxResults;

    @Value("${google.books.api.connect-timeout:5000}")
    private int connectTimeout;

    @Value("${google.books.api.read-timeout:5000}")
    private int readTimeout;

    @Bean
    public WebClient googleBooksWebClient() {
        final int size = 16 * 1024 * 1024; // 16MB buffer size to handle large responses
        
        final ExchangeStrategies strategies = ExchangeStrategies.builder()
                .codecs(codecs -> codecs.defaultCodecs().maxInMemorySize(size))
                .build();
        
        return WebClient.builder()
                .baseUrl(googleBooksApiBaseUrl)
                .exchangeStrategies(strategies)
                .build();
    }
    
    public String getGoogleBooksApiBaseUrl() {
        return googleBooksApiBaseUrl;
    }
    
    public int getMaxResults() {
        return maxResults;
    }
    
    public int getConnectTimeout() {
        return connectTimeout;
    }
    
    public int getReadTimeout() {
        return readTimeout;
    }
}