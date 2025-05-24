/**
 * Configuration properties for Redis key prefixes
 * Allows external configuration of Redis key patterns
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app.redis.keys")
public class RedisKeyProperties {
    
    private String bookPrefix = "book:";
    private String isbn10IndexPrefix = "isbn10_idx:";
    private String isbn13IndexPrefix = "isbn13_idx:";
    private String googleBooksIdIndexPrefix = "gbid_idx:";
    private String searchCachePrefix = "search:";
    
    public String getBookPrefix() {
        return bookPrefix;
    }
    
    public void setBookPrefix(String bookPrefix) {
        this.bookPrefix = bookPrefix;
    }
    
    public String getIsbn10IndexPrefix() {
        return isbn10IndexPrefix;
    }
    
    public void setIsbn10IndexPrefix(String isbn10IndexPrefix) {
        this.isbn10IndexPrefix = isbn10IndexPrefix;
    }
    
    public String getIsbn13IndexPrefix() {
        return isbn13IndexPrefix;
    }
    
    public void setIsbn13IndexPrefix(String isbn13IndexPrefix) {
        this.isbn13IndexPrefix = isbn13IndexPrefix;
    }
    
    public String getGoogleBooksIdIndexPrefix() {
        return googleBooksIdIndexPrefix;
    }
    
    public void setGoogleBooksIdIndexPrefix(String googleBooksIdIndexPrefix) {
        this.googleBooksIdIndexPrefix = googleBooksIdIndexPrefix;
    }
    
    public String getSearchCachePrefix() {
        return searchCachePrefix;
    }
    
    public void setSearchCachePrefix(String searchCachePrefix) {
        this.searchCachePrefix = searchCachePrefix;
    }
}